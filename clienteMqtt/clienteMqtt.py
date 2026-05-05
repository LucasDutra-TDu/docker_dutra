import asyncio
import ssl
import logging
import os
import aiomqtt

# Filtro para inyectar %(taskName)s en Python 3.11
class AsyncioTaskFilter(logging.Filter):
    def filter(self, record):
        try:
            task = asyncio.current_task()
            record.taskName = task.get_name() if task else "MainTask"
        except RuntimeError:
            record.taskName = "MainTask"
        return True

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(taskName)s - %(levelname)s: %(message)s', 
        datefmt='%d/%m/%Y %H:%M:%S %z'
    )
    handler.setFormatter(formatter)
    handler.addFilter(AsyncioTaskFilter())
    logger.addHandler(handler)


async def incrementar_contador(estado):
    """Incrementa el contador cada 3 segundos."""
    while True:
        await asyncio.sleep(3)
        estado['contador'] += 1
        logging.info(f"Contador interno incrementado a {estado['contador']}")

async def publicar_contador(client, topic, estado):
    """Publica el estado del contador cada 5 segundos."""
    while True:
        await asyncio.sleep(5)
        await client.publish(topic, payload=str(estado['contador']))
        logging.info(f"Contador ({estado['contador']}) publicado en {topic}")

async def atender_topico(topic_name, queue):
    """Corrutina dedicada que atiende los mensajes de su cola correspondiente."""
    while True:
        # Espera hasta que el despachador le envíe un mensaje
        message = await queue.get()
        logging.info(f"Mensaje recibido en {topic_name}: {message.payload.decode('utf-8')}")
        queue.task_done()


async def main():
    broker = os.environ.get('servidor')
    topico_out1 = os.environ.get('topico_out1')
    topico_out2 = os.environ.get('topico_out2')
    topico_cont = os.environ.get('topico_cont')

    # Diccionario para compartir el estado del contador sin variables globales
    estado = {'contador': 0}

    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()

    # Colas para enrutar los mensajes a la corrutina correcta
    cola_t1 = asyncio.Queue()
    cola_t2 = asyncio.Queue()

    async with aiomqtt.Client(
        broker,
        port=8883,
        tls_context=tls_context,
    ) as client:
        
        await client.subscribe(topico_out1)
        await client.subscribe(topico_out2)

        # 1. Iniciamos las corrutinas en segundo plano (Tasks)
        asyncio.create_task(incrementar_contador(estado), name="Task-Incremento")
        asyncio.create_task(publicar_contador(client, topico_cont, estado), name="Task-Publicador")
        
        # Cada lector recibe su propia cola donde recibirá los mensajes
        asyncio.create_task(atender_topico(topico_out1, cola_t1), name="Task-Lector1")
        asyncio.create_task(atender_topico(topico_out2, cola_t2), name="Task-Lector2")

        # 2. Bucle principal (Despachador)
        # Iteramos sobre el generador de mensajes y los enrutamos a la cola correspondiente
        async for message in client.messages:
            if message.topic.matches(topico_out1):
                await cola_t1.put(message)
            elif message.topic.matches(topico_out2):
                await cola_t2.put(message)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupción de teclado.")