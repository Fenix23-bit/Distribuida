"""Orquestador: divide las expresiones compuestas en pasos y los manda, uno por uno, a los nodos.

El broker (RabbitMQ) solo enruta; esta pieza solo planifica y enruta, nunca calcula.
La solicitud se confirma (ack) hasta que el plan termina: si el orquestador se cae a mitad,
RabbitMQ la devuelve a la cola y otro orquestador reinicia el plan (garantía "al menos una vez").

Como programa:  python orquestador.py
Como hilo:      threading.Thread(target=ejecutar, args=(detener,))   (ver servicios.py)
"""
import json
import threading
import uuid

import pika

from calculadora import expresion_del_paso, formatear_numero, planificar_expresion
from comun.conexion import conectar
from comun.topologia import declarar_todo


def ejecutar(detener, log=print):
    """Atiende la cola calc.solicitudes hasta que `detener` (threading.Event) se active."""
    conexion = conectar()
    canal = conexion.channel()
    declarar_todo(canal)
    canal.basic_qos(prefetch_count=50)          # hasta 50 planes sin confirmar a la vez
    cola_respuestas = canal.queue_declare(queue="", exclusive=True).method.queue

    planes = {}   # id del paso en vuelo -> plan al que pertenece

    def avisar(plan, tipo, texto):
        canal.basic_publish(
            exchange="",
            routing_key=plan["reply_to"],
            properties=pika.BasicProperties(correlation_id=plan["correlation_id"]),
            body=json.dumps({"tipo": tipo, "texto": texto}).encode("utf-8"),
        )

    def enviar_paso(plan):
        paso = plan["pasos"][len(plan["valores"])]
        rol, expresion = expresion_del_paso(paso, plan["valores"])
        plan["paso_id"] = str(uuid.uuid4())
        planes[plan["paso_id"]] = plan
        canal.basic_publish(
            exchange="operaciones",
            routing_key=rol,
            properties=pika.BasicProperties(reply_to=cola_respuestas, correlation_id=plan["paso_id"]),
            body=expresion.encode("utf-8"),
        )

    def terminar(plan):
        planes.pop(plan["paso_id"], None)
        canal.basic_ack(delivery_tag=plan["delivery_tag"])   # solo ahora se confirma la solicitud

    def nueva_solicitud(canal, metodo, propiedades, cuerpo):
        expresion = cuerpo.decode("utf-8", errors="replace")
        plan = {
            "expresion": expresion, "valores": [], "trazas": [],
            "reply_to": propiedades.reply_to, "correlation_id": propiedades.correlation_id,
            "delivery_tag": metodo.delivery_tag,
        }
        try:
            plan["pasos"] = planificar_expresion(expresion)
            if not plan["pasos"]:
                raise ValueError("no contiene ninguna operación")
        except ValueError as e:
            if plan["reply_to"]:
                avisar(plan, "error", f"SISTEMA_ERROR: expresión no válida: {e}")
            canal.basic_ack(delivery_tag=metodo.delivery_tag)
            return
        log(f"[plan] '{expresion}' en {len(plan['pasos'])} paso(s)")
        enviar_paso(plan)

    def respuesta_de_paso(canal, metodo, propiedades, cuerpo):
        plan = planes.get(propiedades.correlation_id)
        if plan is None:
            return                                   # respuesta tardía de un plan ya cerrado
        try:
            datos = json.loads(cuerpo.decode("utf-8"))
            texto, valor = datos["texto"], datos["valor"]
        except (ValueError, KeyError, TypeError):
            return                                   # respuesta con formato desconocido: se ignora
        total = len(plan["pasos"])

        if valor is None:                            # error de un nodo o del manejador de muertos
            avisar(plan, "error", f"{texto}  (paso {len(plan['valores']) + 1}/{total} de '{plan['expresion']}')")
            terminar(plan)
            return

        plan["valores"].append(valor)
        plan["trazas"].append(texto)
        if len(plan["valores"]) == total:
            if total == 1:
                final = texto
            else:
                final = f"{plan['expresion']} = {formatear_numero(valor)}  [pasos: {' → '.join(plan['trazas'])}]"
            avisar(plan, "final", final)
            terminar(plan)
            return

        avisar(plan, "paso", f"Paso {len(plan['valores'])}/{total}: {texto}")
        planes.pop(propiedades.correlation_id)       # este paso ya terminó
        enviar_paso(plan)

    canal.basic_consume(queue="calc.solicitudes", on_message_callback=nueva_solicitud)
    canal.basic_consume(queue=cola_respuestas, on_message_callback=respuesta_de_paso, auto_ack=True)
    log("Orquestador listo.")
    try:
        while not detener.is_set():
            conexion.process_data_events(time_limit=0.5)
    finally:
        try:
            conexion.close()          # las solicitudes sin confirmar regresan a la cola
        except Exception:
            pass


if __name__ == "__main__":
    try:
        ejecutar(threading.Event())
    except KeyboardInterrupt:
        pass
