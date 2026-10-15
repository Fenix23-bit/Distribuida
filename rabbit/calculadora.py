"""Lógica pura de la calculadora: parseo, planificación y evaluación (sin red ni estado).

Dos niveles:
  - Operación simple (una sola operación binaria o raíz): la resuelve un nodo.
  - Expresión compuesta (varias operaciones, paréntesis, precedencia): `planificar_expresion`
    la descompone en pasos simples en orden de evaluación; el broker manda cada paso al nodo
    que tiene ese rol y usa el resultado para armar el siguiente. El broker nunca calcula.
"""
import math
import re

_NUM = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
_RE_RAIZ = re.compile(rf"^(?:raiz|√)\s*\(?\s*({_NUM})\s*\)?$", re.IGNORECASE)
_RE_BINARIA = re.compile(rf"^({_NUM})\s*(\*\*|[-+*/^%x×÷])\s*({_NUM})$", re.IGNORECASE)

OPERADORES = {
    "+": "suma",
    "-": "resta",
    "*": "multiplicacion", "x": "multiplicacion", "×": "multiplicacion",
    "/": "division", "÷": "division",
    "^": "potencia", "**": "potencia",
    "%": "modulo",
}
SIMBOLO_ROL = {"suma": "+", "resta": "-", "multiplicacion": "*", "division": "/",
               "potencia": "^", "modulo": "%"}
OPERACIONES = {
    "suma": lambda a, b: a + b,
    "resta": lambda a, b: a - b,
    "multiplicacion": lambda a, b: a * b,
    "division": lambda a, b: a / b,
    "potencia": math.pow,
    "modulo": lambda a, b: a % b,
}
EJEMPLOS_CALC = "5 + 3, 10 - 4, 6 * 7, 20 / 4, 2 ^ 8, 10 % 3, raiz 25, 8*8+3/5-2, (2+3)*4"
USO_CALCULADORA = ("[ERROR] Uso: /calculadora <expresión>            Ej: /calculadora 5 + 3\n"
                   "            /calculadora 8*8+3/5-2              Expresión compuesta (varios pasos)\n"
                   "            /calculadora <cantidad>             Te pide esa cantidad de operaciones\n"
                   "            /calculadora <cantidad> <expresión> Manda la misma operación N veces")

MAX_LONGITUD_EXPRESION = 200
MAX_PASOS_EXPRESION = 30


class ErrorExpresion(ValueError):
    """La expresión no se puede interpretar (sintaxis, demasiado larga, demasiados pasos)."""


# =========================================================================
# OPERACIÓN SIMPLE
# =========================================================================
def parsear_expresion(expr):
    """Devuelve (rol, a, b) o None si no es una operación simple. En la raíz b es None."""
    expr = expr.strip()
    m = _RE_RAIZ.match(expr)
    if m:
        return "raiz", float(m.group(1)), None
    m = _RE_BINARIA.match(expr)
    if m:
        return OPERADORES[m.group(2).lower()], float(m.group(1)), float(m.group(3))
    return None


def detectar_rol_operacion(expr):
    resultado = parsear_expresion(expr)
    return resultado[0] if resultado else None


def formatear_numero(n):
    if n == int(n) and abs(n) < 1e15:
        return str(int(n))
    return str(round(n, 10))


def resolver_con_valor(rol, a, b):
    """Devuelve (texto, valor). valor es None si la operación dio error.
    Puede lanzar OverflowError / ValueError (p. ej. resultado infinito)."""
    if rol == "raiz":
        if a < 0:
            return "Error: raíz cuadrada de número negativo no permitida", None
        valor = math.sqrt(a)
        return f"√{formatear_numero(a)} = {formatear_numero(valor)}", valor

    if rol in ("division", "modulo") and b == 0:
        return f"Error: {'división' if rol == 'division' else 'módulo'} entre cero no permitida", None

    valor = OPERACIONES[rol](a, b)
    return f"{formatear_numero(a)} {SIMBOLO_ROL[rol]} {formatear_numero(b)} = {formatear_numero(valor)}", valor


def resolver(rol, a, b):
    """Solo el texto de la respuesta de una operación ya parseada."""
    return resolver_con_valor(rol, a, b)[0]


def calcular_operacion(expr, rol_esperado=None):
    """Resuelve una operación simple. Devuelve (texto, valor); valor es None si hubo error."""
    operacion = parsear_expresion(expr)
    if not operacion:
        return f"Error: expresión no válida. Ejemplos: {EJEMPLOS_CALC}", None
    rol, a, b = operacion

    if rol_esperado and rol != rol_esperado:
        return f"Error: se esperaba una operación de '{rol_esperado}' y se recibió '{rol}'.", None

    try:
        return resolver_con_valor(rol, a, b)
    except (OverflowError, ValueError) as e:
        return f"Error al evaluar la operación: {e}", None


def procesar_operacion_matematica(expr, rol_esperado=None):
    return calcular_operacion(expr, rol_esperado)[0]


def resolver_para_red(expr, rol_esperado=None):
    """Cuerpo de la respuesta de un nodo: 'valor;texto'. El valor viaja sin redondear para que
    el broker lo use en el paso siguiente de una expresión compuesta ('-' si hubo error)."""
    texto, valor = calcular_operacion(expr, rol_esperado)
    return f"{'-' if valor is None else repr(float(valor))};{texto}"


# =========================================================================
# EXPRESIÓN COMPUESTA: analizador y plan de pasos
# =========================================================================
_RE_TOKEN = re.compile(
    r"\s*(?:(?P<num>\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)"
    r"|(?P<op>\*\*|[-+*/^%x×÷()])"
    r"|(?P<raiz>raiz|√))", re.IGNORECASE)


def _tokenizar(texto):
    tokens, pos = [], 0
    texto = texto.rstrip()
    while pos < len(texto):
        m = _RE_TOKEN.match(texto, pos)
        if not m:
            raise ErrorExpresion(f"carácter no reconocido en la posición {pos + 1}: '{texto[pos:].strip()[:1]}'")
        if m.group("num"):
            tokens.append(("num", float(m.group("num"))))
        elif m.group("op"):
            op = m.group("op").lower()
            tokens.append(("op", "^" if op == "**" else op))
        else:
            tokens.append(("raiz", None))
        pos = m.end()
    return tokens


class _Parser:
    """Descenso recursivo. Precedencia (de menor a mayor): + -  |  * / %  |  menos unario  |  ^ (derecha)."""

    def __init__(self, tokens):
        self.t, self.i = tokens, 0

    def _mirar(self):
        return self.t[self.i] if self.i < len(self.t) else (None, None)

    def _tomar(self):
        tok = self._mirar()
        self.i += 1
        return tok

    def _es_op(self, *ops):
        tipo, val = self._mirar()
        return tipo == "op" and val in ops

    def parsear(self):
        arbol = self._suma()
        if self.i != len(self.t):
            raise ErrorExpresion("sobran símbolos al final de la expresión")
        return arbol

    def _suma(self):
        izq = self._producto()
        while self._es_op("+", "-"):
            op = self._tomar()[1]
            izq = ("op", OPERADORES[op], izq, self._producto())
        return izq

    def _producto(self):
        izq = self._unario()
        while self._es_op("*", "x", "×", "/", "÷", "%"):
            op = self._tomar()[1]
            izq = ("op", OPERADORES[op], izq, self._unario())
        return izq

    def _unario(self):
        if self._es_op("-", "+"):
            signo = self._tomar()[1]
            hijo = self._unario()
            if signo == "+":
                return hijo
            if hijo[0] == "num":
                return ("num", -hijo[1])
            return ("op", "resta", ("num", 0.0), hijo)
        return self._potencia()

    def _potencia(self):
        base = self._atomo()
        if self._es_op("^"):
            self._tomar()
            return ("op", "potencia", base, self._unario())   # asocia a la derecha: 2^3^2 = 2^(3^2)
        return base

    def _atomo(self):
        tipo, val = self._tomar()
        if tipo == "num":
            return ("num", val)
        if tipo == "op" and val == "(":
            arbol = self._suma()
            if not self._es_op(")"):
                raise ErrorExpresion("falta cerrar un paréntesis")
            self._tomar()
            return arbol
        if tipo == "raiz":
            return ("raiz", self._atomo())
        if tipo is None:
            raise ErrorExpresion("la expresión termina antes de tiempo")
        raise ErrorExpresion(f"no se esperaba '{val}' aquí")


def planificar_expresion(expr):
    """Descompone una expresión en pasos simples, en el orden en que se deben resolver.

    Devuelve una lista de pasos {"rol", "izq", "der"}; cada operando es ("num", valor) o
    ("ref", índice_de_paso_previo). Para `8*8+3/5-2` salen: 8*8, 3/5, (ref0)+(ref1), (ref2)-2.
    Un mismo nivel de precedencia se evalúa de izquierda a derecha.
    Lanza ErrorExpresion si no se puede interpretar."""
    expr = expr.strip()
    if not expr:
        raise ErrorExpresion("la expresión está vacía")
    if len(expr) > MAX_LONGITUD_EXPRESION:
        raise ErrorExpresion(f"la expresión supera {MAX_LONGITUD_EXPRESION} caracteres")

    arbol = _Parser(_tokenizar(expr)).parsear()
    pasos = []

    def emitir(nodo):
        if nodo[0] == "num":
            return nodo
        if nodo[0] == "raiz":
            izq, der, rol = emitir(nodo[1]), None, "raiz"
        else:
            izq = emitir(nodo[2])
            der = emitir(nodo[3])
            rol = nodo[1]
        pasos.append({"rol": rol, "izq": izq, "der": der})
        return ("ref", len(pasos) - 1)

    emitir(arbol)
    if len(pasos) > MAX_PASOS_EXPRESION:
        raise ErrorExpresion(f"la expresión necesita {len(pasos)} pasos (máximo {MAX_PASOS_EXPRESION})")
    return pasos


def es_expresion_valida(expr):
    """True si es una operación simple o una expresión compuesta con al menos un paso."""
    if detectar_rol_operacion(expr):
        return True
    try:
        return len(planificar_expresion(expr)) >= 1
    except ErrorExpresion:
        return False


def _numero_para_red(valor):
    """Texto que el nodo puede volver a leer sin perder precisión."""
    return repr(float(valor))


def expresion_del_paso(paso, valores):
    """Devuelve (rol, expresión simple) del paso, con los resultados previos ya sustituidos."""
    def operando(op):
        return op[1] if op[0] == "num" else valores[op[1]]

    a = _numero_para_red(operando(paso["izq"]))
    if paso["rol"] == "raiz":
        return "raiz", f"raiz {a}"
    b = _numero_para_red(operando(paso["der"]))
    return paso["rol"], f"{a} {SIMBOLO_ROL[paso['rol']]} {b}"
