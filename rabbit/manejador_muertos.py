"""Manejador de mensajes muertos: responde un error al cliente cuando una operación caduca
(TTL de 30 s), se rechaza o supera el límite de entregas.

Como programa:  python manejador_muertos.py
Como hilo:      threading.Thread(target=ejecutar, args=(detener,))   (ver servicios.py)
"""
import json
import threading

import pika

from comun.conexion import conectar


def ejecutar(detener, log=print):
    """Atiende la cola op.muertas hasta que `detener` (threading.Event) se active."""
    conexion = conectar()
    canal = conexion.channel()
    canal.exchange_declare(exchange="operaciones.muertas", exchange_type="fanout", durable=True)
    canal.queue_declare(queue="op.muertas", durable=True)
    canal.queue_bind(queue="op.muertas", exchange="operaciones.muertas")

    def atender(canal, metodo, propiedades, cuerpo):
        muertes = (propiedades.headers or {}).get("x-death", [])
        motivo = muertes[0].get("reason", "desconocido") if muertes else "desconocido"
        texto = f"SISTEMA_ERROR: no se pudo resolver '{cuerpo.decode('utf-8', errors='replace')}' (motivo: {motivo})"
        log(texto)
        if propiedades.reply_to:
            canal.basic_publish(
                exchange="",
                routing_key=propiedades.reply_to,
                properties=pika.BasicProperties(correlation_id=propiedades.correlation_id),
                body=json.dumps({"texto": texto, "valor": None}).encode("utf-8"),
            )
        canal.basic_ack(delivery_tag=metodo.delivery_tag)

    canal.basic_consume(queue="op.muertas", on_message_callback=atender)
    log("Manejador de mensajes muertos listo.")
    try:
        while not detener.is_set():
            conexion.process_data_events(time_limit=0.5)
    finally:
        try:
            conexion.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        ejecutar(threading.Event())
    except KeyboardInterrupt:
        pass
