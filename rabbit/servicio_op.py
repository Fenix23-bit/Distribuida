"""Nodo de operación: atiende las peticiones de UN rol (suma, resta, ...).

Como programa:  python servicio_op.py suma
Como hilo:      threading.Thread(target=ejecutar, args=("suma", detener))   (ver servicios.py)

Variables de entorno opcionales para pruebas de fallos:
  PAUSA=3                      el nodo tarda 3 s en cada operación
  FALLAR=nack|rechazar|caer    simula un fallo en cada mensaje
"""
import json
import os
import sys
import threading
import time

import pika

from calculadora import calcular_operacion
from comun.conexion import conectar
from comun.topologia import ROLES, declarar


class CaidaSimulada(Exception):
    """FALLAR=caer cuando el nodo corre como hilo: termina el hilo sin confirmar el mensaje."""


def ejecutar(rol, detener, log=print, proceso_propio=False):
    """Atiende la cola op.<rol> hasta que `detener` (threading.Event) se active.

    Cada llamada abre su PROPIA conexión a RabbitMQ: pika no se puede compartir entre hilos.
    proceso_propio=True indica que corre como programa suelto (FALLAR=caer mata el proceso,
    como una caída real); como hilo solo termina el hilo, para no tumbar a los demás."""
    if rol not in ROLES:
        raise ValueError(f"rol desconocido '{rol}'. Válidos: {', '.join(ROLES)}")
    pausa = float(os.environ.get("PAUSA", "0"))
    fallar = os.environ.get("FALLAR", "")

    conexion = conectar()
    canal = conexion.channel()
    declarar(canal, rol)
    canal.basic_qos(prefetch_count=1)

    def atender(canal, metodo, propiedades, cuerpo):
        expresion = cuerpo.decode("utf-8")
        log(f"[{rol}] {expresion} (reentregado: {metodo.redelivered})")
        if fallar == "nack":
            canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=True)
            return
        if fallar == "rechazar":
            canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
            return
        if fallar == "caer":
            if proceso_propio:
                os._exit(1)
            raise CaidaSimulada()
        time.sleep(pausa)
        texto, valor = calcular_operacion(expresion, rol_esperado=rol)
        canal.basic_publish(
            exchange="",
            routing_key=propiedades.reply_to,
            properties=pika.BasicProperties(correlation_id=propiedades.correlation_id),
            body=json.dumps({"texto": texto, "valor": valor}).encode("utf-8"),
        )
        canal.basic_ack(delivery_tag=metodo.delivery_tag)

    canal.basic_consume(queue=f"op.{rol}", on_message_callback=atender)
    log(f"Nodo de '{rol}' listo.")
    try:
        while not detener.is_set():
            conexion.process_data_events(time_limit=0.5)    # atiende mensajes y heartbeats
    except CaidaSimulada:
        log(f"[{rol}] caída simulada: el mensaje en proceso vuelve a la cola")
    finally:
        try:
            conexion.close()       # lo que no se confirmó regresa a la cola
        except Exception:
            pass


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"Uso: python servicio_op.py <rol>   (roles: {', '.join(ROLES)})")
    try:
        ejecutar(sys.argv[1], threading.Event(), proceso_propio=True)
    except KeyboardInterrupt:
        pass
