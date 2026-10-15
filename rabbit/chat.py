"""Chat por tópicos sobre RabbitMQ.

Todo el trabajo con RabbitMQ ocurre en UN hilo (el que consume). Los demás hilos le piden
cosas con add_callback_threadsafe, la forma segura de usar pika desde varios hilos. Ese hilo
también atiende los heartbeats, aunque el usuario no escriba nada.
"""
import json
import threading
import time
import uuid

from comun.conexion import conectar

TOPICO_POR_DEFECTO = "general"


class Chat:
    def __init__(self, nick, al_recibir):
        self.nick = nick
        self.id = uuid.uuid4().hex[:8]       # distingue a dos nodos con el mismo apodo
        self.al_recibir = al_recibir         # función(topico, remitente, texto, latencia_ms)
        self.topicos = set()
        self._error = None
        self._listo = threading.Event()
        self._hilo = threading.Thread(target=self._consumir, daemon=True)
        self._hilo.start()
        self._listo.wait()
        if self._error:
            raise self._error
        self.suscribir(TOPICO_POR_DEFECTO)

    def _consumir(self):
        try:
            self._conexion = conectar()
            self._canal = self._conexion.channel()
            self._canal.exchange_declare(exchange="chat", exchange_type="topic", durable=True)
            self._cola = self._canal.queue_declare(queue="", exclusive=True).method.queue
            self._canal.basic_consume(queue=self._cola, on_message_callback=self._recibir, auto_ack=True)
        except Exception as e:               # se lo avisamos al hilo que creó el Chat
            self._error = e
            self._listo.set()
            return
        self._listo.set()
        self._canal.start_consuming()
        self._conexion.close()

    def _hacer(self, funcion):
        """Ejecuta funcion() dentro del hilo de la conexión (seguro desde cualquier hilo)."""
        self._conexion.add_callback_threadsafe(funcion)

    def _recibir(self, canal, metodo, propiedades, cuerpo):
        try:
            datos = json.loads(cuerpo.decode("utf-8"))
            origen, remitente, texto, ts = datos["origen"], datos["de"], datos["texto"], datos["ts"]
        except (ValueError, KeyError, TypeError):
            return                           # no tiene el formato del chat: se ignora
        if origen == self.id:
            return                           # mis propios mensajes no se muestran
        latencia = max(0, int(time.time() * 1000) - ts)
        try:
            self.al_recibir(metodo.routing_key, remitente, texto, latencia)
        except Exception as e:               # un fallo al mostrar tampoco debe matar el hilo
            print(f"[chat] error al mostrar un mensaje: {e}")

    def suscribir(self, topico):
        self.topicos.add(topico)
        self._hacer(lambda: self._canal.queue_bind(queue=self._cola, exchange="chat", routing_key=topico))

    def desuscribir(self, topico):
        self.topicos.discard(topico)
        self._hacer(lambda: self._canal.queue_unbind(queue=self._cola, exchange="chat", routing_key=topico))

    def publicar(self, topico, texto):
        cuerpo = json.dumps({
            "origen": self.id, "de": self.nick,
            "ts": int(time.time() * 1000), "texto": texto,
        }).encode("utf-8")
        self._hacer(lambda: self._canal.basic_publish(exchange="chat", routing_key=topico, body=cuerpo))

    def cerrar(self):
        self._hacer(self._canal.stop_consuming)