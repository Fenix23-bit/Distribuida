from comun.conexion import conectar

conexion = conectar()
print("Conectado:", conexion.is_open)
conexion.close()
