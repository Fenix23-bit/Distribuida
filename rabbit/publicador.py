import sys

from comun.conexion import conectar

topico, texto = sys.argv[1], sys.argv[2]
conexion = conectar()
canal = conexion.channel()
canal.exchange_declare(exchange="chat", exchange_type="topic", durable=True)
canal.basic_publish(exchange="chat", routing_key=topico, body=texto.encode("utf-8"))
conexion.close()
