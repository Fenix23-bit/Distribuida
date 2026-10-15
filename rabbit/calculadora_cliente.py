"""Cliente de la calculadora distribuida: manda expresiones al orquestador y sigue sus respuestas.

Igual que Chat: todo el trabajo con RabbitMQ ocurre en UN hilo (el que consume); los demás
hilos le piden cosas con add_callback_threadsafe. Las solicitudes se agrupan en lotes y cada
una viaja con un correlation_id "<lote>.<número>" para emparejar las respuestas.
"""
import json
import threading
import time
import uuid

import pika

from comun.conexion import conectar
from comun.topologia import declarar_todo

ESPERA_MAX_S = 60        # un lote sin respuesta completa pasado este tiempo se abandona con un aviso


class ClienteCalculadora:
    def __init__(self, mostrar):
        self.mostrar = mostrar               # función(texto): cómo se imprime una línea
        self._lotes = {}                     # id de lote -> progreso
        self._lock = threading.Lock()
        self._error = None
        self._listo = threading.Event()
        self._hilo = threading.Thread(target=self._consumir, daemon=True)
        self._hilo.start()
        self._listo.wait()
        if self._error:
            raise self._error

    # --- hilo de la conexión ---------------------------------------------------------------
    def _consumir(self):
        try:
            self._conexion = conectar()
            self._canal = self._conexion.channel()
            declarar_todo(self._canal)       # las peticiones esperan en la cola aunque aún no haya nodos
            self._cola = self._canal.queue_declare(queue="", exclusive=True).method.queue
            self._canal.basic_consume(queue=self._cola, on_message_callback=self._recibir, auto_ack=True)
            self._conexion.call_later(5, self._vigilar)
        except Exception as e:
            self._error = e
            self._listo.set()
            return
        self._listo.set()
        self._canal.start_consuming()
        self._conexion.close()

    def _vigilar(self):
        """Cada 5 s revisa si algún lote lleva demasiado tiempo sin completarse."""
        ahora = time.time()
        vencidos = []
        with self._lock:
            for lote_id, lote in list(self._lotes.items()):
                if ahora - lote["inicio"] > ESPERA_MAX_S:
                    vencidos.append((lote_id, lote))
                    del self._lotes[lote_id]
        for lote_id, lote in vencidos:
            self.mostrar(f"[LOTE {lote_id}] Sin respuesta tras {ESPERA_MAX_S} s: llegaron "
                         f"{lote['recibidos']}/{lote['total']}. ¿Están corriendo el orquestador y los nodos? (usa /cola)")
        self._conexion.call_later(5, self._vigilar)

    def _recibir(self, canal, metodo, propiedades, cuerpo):
        try:
            datos = json.loads(cuerpo.decode("utf-8"))
            tipo, texto = datos["tipo"], datos["texto"]
            lote_id, _, numero = propiedades.correlation_id.partition(".")
        except (ValueError, KeyError, TypeError, AttributeError):
            return                           # no tiene el formato esperado: se ignora
        resumen = None
        with self._lock:
            lote = self._lotes.get(lote_id)
            if lote is None:
                return                       # respuesta de un lote ya cerrado
            ms = int((time.time() - lote["inicio"]) * 1000)
            if tipo == "paso":
                linea = f"  #{numero} {texto}"
            else:
                if numero in lote["vistos"]:
                    return                   # RabbitMQ entrega "al menos una vez": se ignoran los duplicados
                lote["vistos"].add(numero)
                lote["recibidos"] += 1
                if tipo == "error":
                    lote["errores"] += 1
                progreso = f"[{numero}/{lote['total']}] " if lote["total"] > 1 else ""
                linea = f"{progreso}{texto}"
                if lote["recibidos"] >= lote["total"]:
                    del self._lotes[lote_id]
                    if lote["total"] > 1:
                        resumen = (f"[LOTE {lote_id}] Completo: {lote['total']} resultados en {ms} ms "
                                   f"({lote['errores']} con error)")
        hora = time.strftime("%H:%M:%S")
        self.mostrar(f"[{hora} | {ms} ms] @calculadora {linea}")
        if resumen:
            self.mostrar(resumen)

    # --- desde cualquier hilo --------------------------------------------------------------
    def pedir(self, expresiones):
        """Manda todas las expresiones de una vez. Devuelve el id del lote."""
        lote_id = uuid.uuid4().hex[:6]
        with self._lock:
            self._lotes[lote_id] = {"total": len(expresiones), "recibidos": 0, "errores": 0,
                                    "vistos": set(), "inicio": time.time()}
        solicitudes = [(f"{lote_id}.{i}", expr) for i, expr in enumerate(expresiones, start=1)]

        def enviar():
            for correlacion, expresion in solicitudes:
                self._canal.basic_publish(
                    exchange="",
                    routing_key="calc.solicitudes",
                    properties=pika.BasicProperties(reply_to=self._cola, correlation_id=correlacion),
                    body=expresion.encode("utf-8"),
                )

        self._conexion.add_callback_threadsafe(enviar)
        return lote_id

    def cerrar(self):
        self._conexion.add_callback_threadsafe(self._canal.stop_consuming)
