import sys

from comun.conexion import conectar

# '*' exactamente 1 palabra, '#' 0 o más palabras
patron = sys.argv[1]
conexion = conectar()
canal = conexion.channel()
canal.exchange_declare(exchange="chat", exchange_type="topic", durable=True)
cola = canal.queue_declare(queue="", exclusive=True).method.queue
canal.queue_bind(queue=cola, exchange="chat", routing_key=patron)


def recibir(canal, metodo, propiedades, cuerpo):
    print(f"[{metodo.routing_key}] {cuerpo.decode('utf-8')}")


canal.basic_consume(queue=cola, on_message_callback=recibir, auto_ack=True)
print(f"Suscrito a '{patron}'. Ctrl+C para salir.")
canal.start_consuming()
