"""Lanza un nodo de operación por cada rol. Funciona desde cualquier carpeta."""
import os
import subprocess
import sys
import time

from comun.topologia import ROLES

CARPETA = os.path.dirname(os.path.abspath(__file__))
SERVICIO = os.path.join(CARPETA, "servicio_op.py")

procesos = [subprocess.Popen([sys.executable, SERVICIO, rol]) for rol in ROLES]
print(f"{len(ROLES)} nodos de operación corriendo. Ctrl+C para detenerlos.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    for p in procesos:
        p.terminate()
