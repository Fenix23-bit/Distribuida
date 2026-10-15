# Calculadora distribuida sobre RabbitMQ

Chat por tópicos y calculadora distribuida (con expresiones compuestas como `8*8+3/5-2`)
usando RabbitMQ como broker. Todo lo necesario está en esta carpeta.

## Requisitos

- Python 3.10 o superior
- Un RabbitMQ 4.x en ejecución
- La librería `pika`:

```
pip install -r requirements.txt
```

### Levantar RabbitMQ con Docker

```
docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

La página de administración queda en http://localhost:15672 (usuario `guest`, clave `guest`,
solo desde la misma máquina).

## Configuración (variables de entorno, opcionales)

| Variable | Valor por defecto | Para qué |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | Dirección del RabbitMQ (otra máquina) |
| `RABBITMQ_USUARIO` | `guest` | Usuario (`guest` solo sirve desde localhost) |
| `RABBITMQ_CLAVE` | `guest` | Clave |

Ejemplo en PowerShell: `$env:RABBITMQ_HOST = "192.168.1.20"`.

## Cómo probar la calculadora

### Opción A: todos los servicios en UNA terminal (recomendada)

```
python servicios.py                  # manejador + orquestador + los 7 nodos, como hilos
python cliente_expresion.py "8*8+3/5-2"      # en otra terminal
```

Variantes: `--roles suma,resta` (solo esos nodos), `--sin-orquestador`, `--sin-manejador`
(para repartir las piezas entre varias máquinas) y `--silencioso` (sin una línea por operación).
RabbitMQ en sí es un servidor aparte y debe estar corriendo.

### Opción B: una terminal por pieza

```
python manejador_muertos.py      # responde errores cuando una operación caduca o se rechaza
python orquestador.py            # divide las expresiones compuestas en pasos
python lanzar_nodos.py           # un proceso por cada operación (suma, resta, ...)
python cliente_expresion.py "8*8+3/5-2"
```

El cliente muestra cada paso y el resultado final. Otros ejemplos: `"(2+3)*4-raiz(16)"`,
`"2*3/(1-1)+5"` (falla en el paso de la división entre cero).

## Nodo interactivo (chat + calculadora en una sola terminal)

```
python nodo.py --todo                 # nodo interactivo + manejador, orquestador y los 7 nodos de operación
python nodo.py                        # solo cliente: chat y /calculadora (los servicios corren en otro lado)
python nodo.py --roles suma,resta     # cliente que además atiende esos roles
```

Dentro: `/help` lista los comandos (`/topico`, `/topicos`, `/calculadora`, `/cola`, `salir`).
Lo que escribes sin `/` se publica en tu tópico actual. Con `--verboso` se ve cada operación que
procesan los servicios; con `--simple` se usa `input()` en vez de `prompt_toolkit`.

Para repartir el trabajo entre máquinas: una corre `python nodo.py --todo` (o solo las piezas que
quiera) y las demás `python nodo.py --roles ...` apuntando al mismo RabbitMQ (`RABBITMQ_HOST`).

## Chat por tópicos (scripts sueltos de práctica)

```
python suscriptor.py deportes          # en una terminal por suscriptor
python publicador.py deportes "hola"
```

En un tópico, `*` coincide con exactamente una palabra y `#` con cero o más
(`deportes.*`, `deportes.#`, `#`).

## Estructura

```
calculadora.py        Calculadora y planificador de expresiones (sin dependencias)
comun/conexion.py     Conexión a RabbitMQ con reintentos
comun/topologia.py    Exchanges y colas (operaciones, mensajes muertos, solicitudes)
nodo.py               Nodo interactivo: chat, /calculadora, /topico, /cola (+ servicios con --todo)
calculadora_cliente.py  Cliente de la calculadora (lotes, pasos y duplicados) para nodo.py
servicios.py          Arranca manejador, orquestador y nodos como hilos de un solo proceso
servicio_op.py        Nodo de operación (un rol); programa suelto o hilo (función ejecutar)
lanzar_nodos.py       Arranca un proceso por cada rol (una terminal por pieza)
orquestador.py        Divide expresiones compuestas en pasos; programa suelto o hilo
manejador_muertos.py  Convierte mensajes caducados o rechazados en errores para el cliente
chat.py               Módulo de chat por tópicos (un hilo dueño de la conexión)
probar_chat.py        Prueba manual del chat: python probar_chat.py <apodo>
cliente_expresion.py  Cliente de la calculadora (simples y compuestas)
cliente_calc.py       Cliente directo de una operación simple (sin orquestador)
suscriptor.py, publicador.py   Chat por tópicos
limpiar.py            Borra las colas de la calculadora
probar_conexion.py    Comprueba la conexión a RabbitMQ
practica_rabbit/      Primeros ejercicios (hola mundo con una cola)
```

## Pruebas de fallos (variables para `servicio_op.py`)

- `PAUSA=3`: el nodo tarda 3 s en cada operación.
- `FALLAR=rechazar`: rechaza el mensaje y va al manejador de muertos (`rejected`).
- `FALLAR=caer`: el proceso se cae con el mensaje (tras 4 entregas llega `delivery_limit`).
  Con `servicios.py` solo termina el hilo de ese nodo, para no tumbar a los demás.
- `FALLAR=nack`: lo devuelve a la cola en bucle hasta que caduca (`expired`, 30 s).

## Problemas frecuentes

- `PRECONDITION_FAILED`: cambiaste los argumentos de una cola ya creada. Ejecuta `python limpiar.py`.
- `ACCESS_REFUSED` desde otra máquina: el usuario `guest` solo funciona en localhost; crea un usuario
  propio y usa `RABBITMQ_USUARIO` y `RABBITMQ_CLAVE`.
- Una cola sin consumidores acumula mensajes: arranca `lanzar_nodos.py` o `limpiar.py`.
