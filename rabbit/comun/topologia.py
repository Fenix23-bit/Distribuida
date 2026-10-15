def declarar(canal, rol):
    canal.exchange_declare(exchange="operaciones", exchange_type="direct", durable=True)
    canal.exchange_declare(exchange="operaciones.muertas", exchange_type="fanout", durable=True)
    canal.queue_declare(queue="op.muertas", durable=True)
    canal.queue_bind(queue="op.muertas", exchange="operaciones.muertas")
    canal.queue_declare(
        queue=f"op.{rol}",
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-message-ttl": 30000,
            "x-delivery-limit": 3,
            "x-dead-letter-exchange": "operaciones.muertas",
        },
    )
    canal.queue_bind(queue=f"op.{rol}", exchange="operaciones", routing_key=rol)

ROLES = ["suma", "resta", "multiplicacion", "division", "potencia", "modulo", "raiz"]


def declarar_todo(canal):
    """Declara las colas de todos los roles y la cola de solicitudes del orquestador."""
    for rol in ROLES:
        declarar(canal, rol)
    canal.queue_declare(
        queue="calc.solicitudes",
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": 3,
            "x-dead-letter-exchange": "operaciones.muertas",
        },
    )