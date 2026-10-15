"""Arranca los servicios de la calculadora como hilos de UN solo proceso (una sola terminal).

  python servicios.py                         manejador + orquestador + los 7 nodos de operación
  python servicios.py --roles suma,resta      solo esos nodos (más el orquestador y el manejador)
  python servicios.py --roles suma --sin-orquestador --sin-manejador    aportar solo un rol
  python servicios.py --silencioso            sin una línea por cada operación

Cada hilo abre su propia conexión a RabbitMQ (pika no se puede compartir entre hilos).
RabbitMQ en sí es un servidor aparte que debe estar corriendo.
"""
import argparse
import threading
import time

import manejador_muertos
import orquestador
import servicio_op
from comun.topologia import ROLES


_lock_salida = threading.Lock()


def imprimir(texto):
    """print seguro entre hilos: sin esto las líneas de varios hilos se pegan entre sí."""
    with _lock_salida:
        print(texto)


def _proteger(nombre, objetivo, args):
    """Un hilo que falla lo dice, en vez de morir en silencio."""
    try:
        objetivo(*args)
    except Exception as e:
        imprimir(f"[{nombre}] se detuvo por un error: {type(e).__name__}: {e}")


def iniciar_servicios(detener, roles=ROLES, con_orquestador=True, con_manejador=True, log=imprimir):
    """Arranca los servicios pedidos y devuelve la lista de hilos."""
    hilos = []

    def lanzar(nombre, objetivo, *args):
        hilo = threading.Thread(target=_proteger, args=(nombre, objetivo, args), name=nombre, daemon=True)
        hilo.start()
        hilos.append(hilo)

    if con_manejador:
        lanzar("manejador", manejador_muertos.ejecutar, detener, log)
    if con_orquestador:
        lanzar("orquestador", orquestador.ejecutar, detener, log)
    for rol in roles:
        lanzar(f"nodo-{rol}", servicio_op.ejecutar, rol, detener, log)
    return hilos


def main():
    parser = argparse.ArgumentParser(description="Servicios de la calculadora en una sola terminal")
    parser.add_argument("--roles", default=",".join(ROLES), help="roles a atender, separados por coma")
    parser.add_argument("--sin-orquestador", action="store_true")
    parser.add_argument("--sin-manejador", action="store_true")
    parser.add_argument("--silencioso", action="store_true", help="no muestra cada operación procesada")
    args = parser.parse_args()

    roles = [r.strip() for r in args.roles.split(",") if r.strip()]
    desconocidos = [r for r in roles if r not in ROLES]
    if desconocidos:
        parser.error(f"roles desconocidos: {', '.join(desconocidos)}. Válidos: {', '.join(ROLES)}")

    log = (lambda *_: None) if args.silencioso else imprimir
    detener = threading.Event()
    hilos = iniciar_servicios(detener, roles, not args.sin_orquestador, not args.sin_manejador, log)
    imprimir(f"{len(hilos)} servicios corriendo ({', '.join(h.name for h in hilos)}). Ctrl+C para detenerlos.")
    try:
        while any(h.is_alive() for h in hilos):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    detener.set()
    for h in hilos:
        h.join(timeout=3)
    imprimir("Servicios detenidos.")


if __name__ == "__main__":
    main()
