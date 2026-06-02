from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import logging, os, asyncio, aiomysql, traceback, ssl, json
import matplotlib.pyplot as plt
from io import BytesIO
import aiomqtt

token = os.environ["TB_TOKEN"]
DEVICE_ID = "TERMOSTATO_DUTRA"

logging.basicConfig(format='%(asctime)s - TelegramBot - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

# --- Publicar Órdenes vía MQTTS ---
async def publicar_orden(subtopico: str, mensaje: str):
    """Establece una conexión rápida por MQTTS y publica la orden al termostato."""
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()

    try:
        async with aiomqtt.Client(
            os.environ["SERVIDOR"],
            username=os.environ["MQTT_USR"],
            password=os.environ["MQTT_PASS"],
            port=int(os.environ["PUERTO_MQTTS"]),
            tls_context=tls_context,
        ) as client:
            topico_completo = f"{DEVICE_ID}/{subtopico}"
            await client.publish(topico_completo, payload=str(mensaje))
            logging.info(f"MQTT Publicado -> {topico_completo}: {mensaje}")
    except Exception as e:
        logging.error(f"Error al publicar en MQTT: {e}")

# --- Handlers Telegram ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("se conectó: " + str(update.message.from_user.id))
    nombre = update.message.from_user.first_name or ""
    apellido = update.message.from_user.last_name or ""
    
    # Nueva botonera
    kb = [
        ["temperatura", "humedad"],
        ["gráfico temperatura", "gráfico humedad"],
        ["Modo Auto", "Modo Manual"],
        ["Relé ON", "Relé OFF"],
        ["Destello LED"]
    ]
    
    mensaje = (
        f"Bienvenido al Bot {nombre} {apellido}\n\n"
        "📊 **Consultas a DB:** Usá las dos primeras filas de botones.\n"
        "⚙️ **Control del Termostato:** Usá los botones inferiores o comandos:\n"
        "• `/setpoint [valor]` - Ej: `/setpoint 24.5`\n"
        "• `/periodo [segundos]` - Ej: `/periodo 10`"
    )
    await context.bot.send_message(update.message.chat.id, text=mensaje, reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))

async def acercade(update: Update, context):
    await context.bot.send_message(update.message.chat.id, text="Este bot fue creado para el curso de IoT FIO.")

async def rawr(update: Update, context):
    logging.info(context.args)
    if context.args and context.args[0] == '@e':
        await context.bot.send_animation(update.message.chat.id, "CgACAgEAAxkBAAMdahiJ0yZOpRCPMpD6rZur7mLD3oYAArIGAAKyu8hEsnmuSELvqEc7BA")
        await asyncio.sleep(6)
        await context.bot.send_message(update.message.chat.id, text="¡¡¡☠️☠️☠️☠️☠️☠️☠️☠️☠️☠️☠️☠️☠️☠️☠️!!!")
    else:
        await context.bot.send_message(update.message.chat.id, text="☠️ ¡¡¡Esto es muy peligroso!!! ☠️")


async def medicion(update: Update, context):
    logging.info(update.message.text)
    sql = f"SELECT timestamp, {update.message.text} FROM mediciones ORDER BY timestamp DESC LIMIT 1"
    conn = await aiomysql.connect(host=os.environ["MARIADB_SERVER"], port=3306,
                                    user=os.environ["MARIADB_USER"],
                                    password=os.environ["MARIADB_USER_PASS"],
                                    db=os.environ["MARIADB_DB"])
    async with conn.cursor() as cur:
        await cur.execute(sql)
        r = await cur.fetchone()
        unidad = 'ºC' if update.message.text == 'temperatura' else '%'
        
        await context.bot.send_message(update.message.chat.id,
                                    text="La última {} es de {} {},\nregistrada a las {:%H:%M:%S %d/%m/%Y}"
                                    .format(update.message.text, str(r[1]).replace('.', ','), unidad, r[0]))
        logging.info("La última {} es de {} {}, medida a las {:%H:%M:%S %d/%m/%Y}".format(update.message.text, r[1], unidad, r[0]))
    conn.close()


async def graficos(update: Update, context):
    logging.info(update.message.text)
    sql = f"""SELECT timestamp, {update.message.text.split()[1]}
            FROM (
                SELECT timestamp, {update.message.text.split()[1]},
                    ROW_NUMBER() OVER (ORDER BY id) AS rn
                FROM mediciones
                WHERE timestamp >= NOW() - INTERVAL 1 DAY
                AND sensor_id LIKE 'sensor_1'
            ) AS t
            WHERE rn % 2 = 0
            ORDER BY timestamp;"""
    conn = await aiomysql.connect(host=os.environ["MARIADB_SERVER"], port=3306,
                                    user=os.environ["MARIADB_USER"],
                                    password=os.environ["MARIADB_USER_PASS"],
                                    db=os.environ["MARIADB_DB"])
    async with conn.cursor() as cur:
        await cur.execute(sql)
        filas = await cur.fetchall()

        fig, ax = plt.subplots(figsize=(7, 4))
        fecha, var = zip(*filas)
        ax.plot(fecha, var)
        ax.grid(True, which='both')
        ax.set_title(update.message.text, fontsize=14, verticalalignment='bottom')
        ax.set_xlabel('fecha')
        ax.set_ylabel('unidad')

        buffer = BytesIO()
        fig.tight_layout()
        fig.savefig(buffer, format='png')
        plt.close()
        buffer.seek(0)
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=buffer)
        buffer.close()
    conn.close()

# --- Handlers MQTT ---

async def cambiar_modo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text
    modo = "auto" if "Auto" in texto else "manual"
    await publicar_orden("modo", modo)
    await update.message.reply_text(f"⚙️ Orden enviada: Modo cambiado a `{modo}`", parse_mode="Markdown")

async def cambiar_rele(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text
    estado = "1" if "ON" in texto else "0"
    await publicar_orden("rele", estado)
    await update.message.reply_text(f"🔌 Orden enviada: Estado del relé seteado en `{estado}`", parse_mode="Markdown")

async def solicitar_destello(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await publicar_orden("destello", "ping")
    await update.message.reply_text("⚡ Comando enviado: Destellar LED perimetral.")

async def set_setpoint(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        try:
            valor = float(context.args[0])
            await publicar_orden("setpoint", str(valor))
            await update.message.reply_text(f"🎯 Setpoint enviado correctamente: `{valor} ºC`", parse_mode="Markdown")
        except ValueError:
            await update.message.reply_text("❌ Error: El setpoint debe ser un número decimal válido (Ej: 24.5).")
    else:
        await update.message.reply_text("⚠️ Uso correcto: `/setpoint 25.0`")

async def set_periodo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        try:
            valor = int(context.args[0])
            await publicar_orden("periodo", str(valor))
            await update.message.reply_text(f"⏱️ Periodo enviado correctamente: `{valor} segundos`", parse_mode="Markdown")
        except ValueError:
            await update.message.reply_text("❌ Error: El periodo debe ser un entero de segundos.")
    else:
        await update.message.reply_text("⚠️ Uso correcto: `/periodo 15`")


def main():
    application = Application.builder().token(token).build()
    
    
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('acercade', acercade))
    application.add_handler(CommandHandler('rawr', rawr))
    application.add_handler(MessageHandler(filters.Regex("^(temperatura|humedad)$"), medicion))
    application.add_handler(MessageHandler(filters.Regex("^(gráfico temperatura|gráfico humedad)$"), graficos))
    
    # Acciones MQTT
    application.add_handler(CommandHandler('setpoint', set_setpoint))
    application.add_handler(CommandHandler('periodo', set_periodo))
    application.add_handler(MessageHandler(filters.Regex("^(Modo Auto|Modo Manual)$"), cambiar_modo))
    application.add_handler(MessageHandler(filters.Regex("^(Relé ON|Relé OFF)$"), cambiar_rele))
    application.add_handler(MessageHandler(filters.Regex("^Destello LED$"), solicitar_destello))
    
    application.run_polling()

if __name__ == '__main__':
    main()