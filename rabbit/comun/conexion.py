"""Conexión a RabbitMQ con reintentos. La configuración sale de variables de entorno."""
import os
import time

import pika

HOST = os.environ.get("RABBITMQ_HOST", "localhost")
USUARIO = os.environ.get("RABBITMQ_USUARIO", "guest")
CLAVE = os.environ.get("RABBITMQ_CLAVE", "guest")


def conectar(intentos=10, espera=2.0):
    """Abre una conexión. Si RabbitMQ aún no responde, reintenta antes de rendirse."""
    parametros = pika.ConnectionParameters(
        host=HOST,
        credentials=pika.PlainCredentials(USUARIO, CLAVE),
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    for intento in range(1, intentos + 1):
        try:
            return pika.BlockingConnection(parametros)
        except pika.exceptions.AMQPConnectionError:
            print(f"[RabbitMQ] sin conexión con {HOST} (intento {intento}/{intentos}); "
                  f"reintento en {espera} s")
            time.sleep(espera)
    raise ConnectionError(f"No se pudo conectar a RabbitMQ en {HOST} tras {intentos} intentos")
