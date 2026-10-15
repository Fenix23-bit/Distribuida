"""Nodo interactivo: chat por tópicos + calculadora distribuida sobre RabbitMQ, en una sola terminal.

  python nodo.py                          cliente: chat y /calculadora
  python nodo.py --todo                   además levanta manejador, orquestador y los 7 nodos de operación
  python nodo.py --roles suma,resta       además atiende solo esos roles
  python nodo.py --orquestador            además corre un orquestador
  python nodo.py --apodo Ana --verboso    sin preguntar el apodo, mostrando cada operación procesada

RabbitMQ es un servidor aparte que debe estar corriendo. Se configura con las variables de
entorno RABBITMQ_HOST, RABBITMQ_USUARIO y RABBITMQ_CLAVE (ver README.md).
"""
import argparse
import random
import re
import sys
import threading
import time

import pika

import servicios
from calculadora import EJEMPLOS_CALC, USO_CALCULADORA, es_expresion_valida
from calculadora_cliente import ClienteCalculadora
from chat import TOPICO_POR_DEFECTO, Chat
from comun.conexion import HOST, conectar
from comun.topologia import ROLES

MAX_SOLICITUDES_LOTE = 100
NOMBRE_VALIDO = re.compile(r"^[\w.*#-]+$")

AYUDA = """
========================== MENÚ DE COMANDOS ==========================
  /help o /ayuda          : Muestra este menú.
  salir                   : Cierra el nodo (y los servicios que corran en él).

--- CHAT POR TÓPICOS (lo que escribas sin '/' se publica en tu tópico actual) ---
  /topicos                : Muestra tus suscripciones y dónde publicas.
  /topico <nombre>        : Cambia de forma exclusiva a ese tópico.
  /topico +<nombre>       : Te suscribe a otro tópico sin dejar los actuales.
  /topico -<nombre>       : Sales de ese tópico.
      Los nombres distinguen mayúsculas. Al suscribirte puedes usar patrones:
      '*' = exactamente una palabra, '#' = cero o más (deportes.*, deportes.#).

--- CALCULADORA DISTRIBUIDA ---
  /calculadora <expresión>: Ej. /calculadora 5 + 3    /calculadora raiz 25
                            Compuestas: /calculadora 8*8+3/5-2   /calculadora (2+3)*4-raiz(16)
  /calculadora <cantidad> : Te pide esa cantidad de operaciones y las manda juntas.
  /calculadora <cantidad> <expresión>
                          : Manda la misma operación N veces a la vez.

--- ESTADO ---
  /cola                   : Mensajes en espera y cuántos nodos atienden cada operación.
  /enviar, /estres        : Aún no migrados a RabbitMQ.
=======================================================================
"""


class Nodo:
    def __init__(self, apodo, session):
        self.apodo = apodo
        self.session = session                       # PromptSession, o None si se usa input()
        self.topico_publicacion = TOPICO_POR_DEFECTO
        self.chat = Chat(apodo, self.mostrar_mensaje)
        self.calc = ClienteCalculadora(self.mostrar)

    # --- salida y entrada ------------------------------------------------------------------
    def mostrar(self, texto):
        print(texto)

    def preguntar(self, texto):
        return self.session.prompt(texto) if self.session else input(texto)

    def mostrar_mensaje(self, topico, remitente, texto, latencia_ms):
        enviado = time.localtime(time.time() - latencia_ms / 1000)
        self.mostrar(f"[{time.strftime('%H:%M:%S', enviado)} | Latencia: {latencia_ms} ms] "
                     f"@{topico} {remitente}: {texto}")

    def cerrar(self):
        self.chat.cerrar()
        self.calc.cerrar()

    # --- comandos --------------------------------------------------------------------------
    def comando(self, texto):
        """Ejecuta una línea escrita por el usuario. Devuelve False para salir."""
        bajo = texto.lower()
        if bajo in ("/help", "/ayuda"):
            self.mostrar(AYUDA)
        elif bajo == "salir":
            return False
        elif bajo == "/topicos":
            self.mostrar_topicos()
        elif bajo == "/cola":
            self.mostrar_colas()
        else:
            comando, _, argumento = texto.partition(" ")
            comando, argumento = comando.lower(), argumento.strip()
            if comando == "/topico":
                self.topico(argumento)
            elif comando == "/calculadora":
                self.calculadora(argumento)
            elif comando in ("/enviar", "/estres"):
                self.mostrar(f"[PENDIENTE] {comando} todavía no está migrado a RabbitMQ.")
            elif comando.startswith("/"):
                self.mostrar(f"[ERROR] Comando desconocido: {comando}. Escribe /help.")
            else:
                self.chat.publicar(self.topico_publicacion, texto)
        return True

    def mostrar_topicos(self):
        self.mostrar(f"[INFO] Suscrito a: {', '.join(sorted(self.chat.topicos)) or '(ninguno)'} "
                     f"| Publicando en: '{self.topico_publicacion}'")

    def topico(self, argumento):
        if not argumento:
            self.mostrar("[ERROR] Uso: /topico <nombre> (exclusivo) | /topico +<nombre> | /topico -<nombre>")
            return
        modo = argumento[0] if argumento[0] in "+-" else ""
        nombre = argumento[len(modo):].strip()
        if not NOMBRE_VALIDO.match(nombre):
            self.mostrar(f"[ERROR] Nombre de tópico no válido: '{nombre}' "
                         "(solo letras, números, '_', '-', '.', y los comodines '*' y '#').")
            return
        es_patron = "*" in nombre or "#" in nombre   # no se puede publicar en un patrón

        if modo == "-":
            if nombre not in self.chat.topicos:
                self.mostrar(f"[INFO] No estás suscrito a '{nombre}'.")
                return
            self.chat.desuscribir(nombre)
            publicables = sorted(t for t in self.chat.topicos if "*" not in t and "#" not in t)
            if not publicables:                      # solo quedan patrones (o nada): vuelve a 'general'
                self.chat.suscribir(TOPICO_POR_DEFECTO)
                publicables = [TOPICO_POR_DEFECTO]
            if self.topico_publicacion not in self.chat.topicos:
                self.topico_publicacion = publicables[0]
        else:
            if modo == "":
                for otro in list(self.chat.topicos - {nombre}):
                    self.chat.desuscribir(otro)
            self.chat.suscribir(nombre)
            if not es_patron:
                self.topico_publicacion = nombre
        self.mostrar_topicos()

    def calculadora(self, argumento):
        expresiones = self.interpretar_calculadora(argumento)
        if not expresiones:
            return
        lote = self.calc.pedir(expresiones)
        if len(expresiones) > 1:
            self.mostrar(f"[LOTE {lote}] Enviando {len(expresiones)} solicitudes simultáneas...")

    def interpretar_calculadora(self, argumento):
        """Devuelve la lista de expresiones a enviar, o None si hubo error o se canceló."""
        if not argumento:
            self.mostrar(USO_CALCULADORA)
            return None
        if es_expresion_valida(argumento):
            return [argumento]
        m = re.match(r"^(\d+)(?:\s+(.+))?$", argumento)
        if not m:
            self.mostrar(f"[ERROR] Expresión no válida. Ejemplos: {EJEMPLOS_CALC}")
            return None
        cantidad = int(m.group(1))
        if not 1 <= cantidad <= MAX_SOLICITUDES_LOTE:
            self.mostrar(f"[ERROR] La cantidad debe estar entre 1 y {MAX_SOLICITUDES_LOTE}.")
            return None
        if m.group(2):
            expr = m.group(2).strip()
            if not es_expresion_valida(expr):
                self.mostrar(f"[ERROR] Expresión no válida: '{expr}'. Ejemplos: {EJEMPLOS_CALC}")
                return None
            return [expr] * cantidad
        return self.pedir_expresiones(cantidad)

    def pedir_expresiones(self, cantidad):
        self.mostrar(f"[CALCULADORA] Escribe las {cantidad} operaciones. "
                     "Se enviarán todas juntas al final (ENTER vacío para cancelar).")
        expresiones = []
        while len(expresiones) < cantidad:
            try:
                expr = self.preguntar(f"  Operación {len(expresiones) + 1}/{cantidad}: ").strip()
            except (KeyboardInterrupt, EOFError):
                expr = ""
            if not expr:
                self.mostrar("[CALCULADORA] Envío cancelado.")
                return None
            if not es_expresion_valida(expr):
                self.mostrar(f"  [ERROR] Expresión no válida, escríbela de nuevo. Ejemplos: {EJEMPLOS_CALC}")
                continue
            expresiones.append(expr)
        return expresiones

    def mostrar_colas(self):
        """Quién atiende qué: reemplaza a la antigua tabla de roles del broker."""
        try:
            conexion = conectar(intentos=1)
        except ConnectionError as e:
            self.mostrar(f"[ERROR] {e}")
            return
        canal = conexion.channel()

        def consultar(cola):
            nonlocal canal
            try:
                r = canal.queue_declare(queue=cola, passive=True)
                return r.method.message_count, r.method.consumer_count
            except pika.exceptions.ChannelClosedByBroker:   # la cola no existe: el canal se cierra
                canal = conexion.channel()
                return None

        lineas = [f"--- ESTADO DE LAS COLAS EN {HOST} ---"]
        for rol in ROLES:
            datos = consultar(f"op.{rol}")
            lineas.append(f"  op.{rol:<15} " + ("(aún no existe)" if datos is None else
                          f"en espera: {datos[0]:<4} nodos que la atienden: {datos[1]}"))
        for cola, quien in (("calc.solicitudes", "orquestadores"), ("op.muertas", "manejadores")):
            datos = consultar(cola)
            lineas.append(f"  {cola:<19} " + ("(aún no existe)" if datos is None else
                          f"en espera: {datos[0]:<4} {quien}: {datos[1]}"))
        conexion.close()
        self.mostrar("\n".join(lineas))


def leer_argumentos():
    p = argparse.ArgumentParser(description="Nodo interactivo sobre RabbitMQ")
    p.add_argument("--apodo", help="tu nombre en el chat (si se omite, se pregunta)")
    p.add_argument("--todo", action="store_true", help="manejador + orquestador + los 7 nodos de operación")
    p.add_argument("--roles", default="", help="roles que atiende este proceso, separados por coma")
    p.add_argument("--orquestador", action="store_true", help="corre un orquestador en este proceso")
    p.add_argument("--manejador", action="store_true", help="corre el manejador de mensajes muertos")
    p.add_argument("--verboso", action="store_true", help="muestra cada operación que procesan los servicios")
    p.add_argument("--simple", action="store_true", help="usa input() en vez de prompt_toolkit")
    args = p.parse_args()
    args.lista_roles = [r.strip() for r in args.roles.split(",") if r.strip()]
    desconocidos = [r for r in args.lista_roles if r not in ROLES]
    if desconocidos:
        p.error(f"roles desconocidos: {', '.join(desconocidos)}. Válidos: {', '.join(ROLES)}")
    return args


def crear_session(simple):
    """Devuelve (PromptSession, patch_stdout) o (None, None) si no se usa prompt_toolkit."""
    if simple:
        return None, None
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.patch_stdout import patch_stdout
    except ImportError:
        print("[AVISO] Sin 'prompt_toolkit' (pip install prompt_toolkit) el texto que llega mientras "
              "escribes puede mezclarse con tu línea. Se usa input().")
        return None, None
    return PromptSession(), patch_stdout


def main():
    args = leer_argumentos()
    apodo = (args.apodo or input("Ingresa tu apodo: ")).strip() or f"Nodo_{random.randint(100, 999)}"
    session, patch_stdout = crear_session(args.simple)

    print(f"[INFO] Conectando con RabbitMQ en {HOST}...")
    try:
        nodo = Nodo(apodo, session)
    except ConnectionError as e:
        sys.exit(f"[ERROR] {e}. ¿Está corriendo RabbitMQ? (README.md)")

    detener = threading.Event()
    roles = ROLES if args.todo else args.lista_roles
    con_orquestador = args.todo or args.orquestador
    con_manejador = args.todo or args.manejador
    hilos = []
    if roles or con_orquestador or con_manejador:
        log = servicios.imprimir if args.verboso else (lambda *_: None)
        hilos = servicios.iniciar_servicios(detener, roles, con_orquestador, con_manejador, log)
        time.sleep(1.5)                                  # que alcancen a conectarse
        print(f"[SERVICIOS] En este proceso: {', '.join(h.name for h in hilos)}")

    print(f"[INFO] Hola, {apodo}. Suscrito a '{TOPICO_POR_DEFECTO}'. Escribe /help para ver los comandos.")

    def ciclo():
        while True:
            try:
                entrada = nodo.preguntar(f"[{apodo}]: ").strip()
            except (KeyboardInterrupt, EOFError):
                return
            if not entrada:
                continue
            try:
                if not nodo.comando(entrada):
                    return
            except Exception as e:                       # un comando con error no debe cerrar el nodo
                nodo.mostrar(f"[ERROR] {type(e).__name__}: {e}")

    if patch_stdout:
        with patch_stdout():
            ciclo()
    else:
        ciclo()

    detener.set()
    nodo.cerrar()
    for h in hilos:
        h.join(timeout=3)
    print("Hasta luego.")


if __name__ == "__main__":
    main()
