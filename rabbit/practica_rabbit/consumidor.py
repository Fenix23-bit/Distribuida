import pika

conexion = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
canal = conexion.channel()
canal.queue_declare(queue="hola", durable=True)

def recibir(canal, metodo, propiedades, cuerpo):
    print("Recibí:", cuerpo.decode("utf-8"))
    canal.basic_ack(delivery_tag=metodo.delivery_tag)

canal.basic_consume(queue="hola", on_message_callback=recibir)
print("Esperando mensajes. Ctrl+C para salir.")
canal.start_consuming()