"""Cliente: manda una expresión al orquestador y muestra los pasos y el resultado.

Uso:  python cliente_expresion.py "8*8+3/5-2"
"""
import json
import sys
import time
import uuid

import pika

from comun.conexion import conectar
from comun.topologia import declarar_todo

expresion = sys.argv[1]
conexion = conectar()
canal = conexion.channel()
declarar_todo(canal)
cola_respuesta = canal.queue_declare(queue="", exclusive=True).method.queue
id_correlacion = str(uuid.uuid4())
terminado = False


def recibir(canal, metodo, propiedades, cuerpo):
    global terminado
    if propiedades.correlation_id != id_correlacion:
        return
    datos = json.loads(cuerpo.decode("utf-8"))
    print(f"[{datos['tipo']}] {datos['texto']}")
    if datos["tipo"] in ("final", "error"):
        terminado = True


canal.basic_consume(queue=cola_respuesta, on_message_callback=recibir, auto_ack=True)
canal.basic_publish(
    exchange="",
    routing_key="calc.solicitudes",
    properties=pika.BasicProperties(reply_to=cola_respuesta, correlation_id=id_correlacion),
    body=expresion.encode("utf-8"),
)

limite = time.time() + 60
while not terminado and time.time() < limite:
    conexion.process_data_events(time_limit=0.5)
if not terminado:
    print("Sin respuesta en 60 s")
conexion.close()
