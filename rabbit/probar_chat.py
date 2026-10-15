import sys

from chat import Chat

nick = sys.argv[1]


def mostrar(topico, remitente, texto, latencia_ms):
    print(f"\n[{topico}] {remitente}: {texto}  ({latencia_ms} ms)")


chat = Chat(nick, mostrar)
print(f"Hola {nick}. Suscrito a: {sorted(chat.topicos)}")
print("Comandos: /sub <tópico>   /unsub <tópico>   /pub <tópico> <texto>   salir")
while True:
    linea = input("> ").strip()
    if linea == "salir":
        break
    partes = linea.split(" ", 2)
    if partes[0] == "/sub" and len(partes) >= 2:
        chat.suscribir(partes[1])
    elif partes[0] == "/unsub" and len(partes) >= 2:
        chat.desuscribir(partes[1])
    elif partes[0] == "/pub" and len(partes) == 3:
        chat.publicar(partes[1], partes[2])
    else:
        print("Comando no válido")
chat.cerrar()