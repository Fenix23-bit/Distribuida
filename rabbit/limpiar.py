"""Borra las colas de la calculadora (hace falta si cambias sus argumentos en topologia.py)."""
from comun.conexion import conectar
from comun.topologia import ROLES

conexion = conectar()
canal = conexion.channel()
for nombre in [f"op.{rol}" for rol in ROLES] + ["op.muertas", "calc.solicitudes"]:
    canal.queue_delete(queue=nombre)
print("Colas borradas")
conexion.close()
