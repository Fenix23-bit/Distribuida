"""Cliente directo de una sola operación simple (sin orquestador): habla con la cola op.<rol>.

Uso:  python cliente_calc.py "5 + 3"
Para expresiones compuestas usa cliente_expresion.py.
"""
import json
import sys
import time
import uuid

import pika

from calculadora import detectar_rol_operacion
from comun.conexion import conectar
from comun.topologia import declarar

expresion = sys.argv[1]
rol = detectar_rol_operacion(expresion)
if rol is None:
    sys.exit("Solo operaciones simples, por ejemplo: 5 + 3 o raiz 25 (para otras, cliente_expresion.py)")

conexion = conectar()
canal = conexion.channel()
declarar(canal, rol)

cola_respuesta = canal.queue_declare(queue="", exclusive=True).method.queue
id_correlacion = str(uuid.uuid4())
respuesta = {}


def recibir(canal, metodo, propiedades, cuerpo):
    if propiedades.correlation_id == id_correlacion:
        respuesta["texto"] = json.loads(cuerpo.decode("utf-8"))["texto"]


canal.basic_consume(queue=cola_respuesta, on_message_callback=recibir, auto_ack=True)
canal.basic_publish(
    exchange="operaciones",
    routing_key=rol,
    properties=pika.BasicProperties(reply_to=cola_respuesta, correlation_id=id_correlacion),
    body=expresion.encode("utf-8"),
)

limite = time.time() + 40
while "texto" not in respuesta and time.time() < limite:
    conexion.process_data_events(time_limit=0.5)
print(respuesta.get("texto", "Sin respuesta en 40 s"))
conexion.close()
