import pika

conexion = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
canal = conexion.channel()
canal.queue_declare(queue="hola", durable=True)
canal.basic_publish(exchange="", routing_key="hola", body="Hola desde RabbitMQ".encode("utf-8"),
                    properties=pika.BasicProperties(delivery_mode=2))
print("Enviado")
conexion.close()