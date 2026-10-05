"""Números guardados como texto: leerlos bien o decir que no se puede.

Lo que había (P0 de la auditoría verificada el 04-oct): ``calcular`` convertía
texto a número con ``^-?[0-9]+(\\.[0-9]+)?$``. Eso acepta "12.500" y lo lee
como 12,5, y deja en NULL todo número con dos o más puntos. Reproducido en
prod con el código real: Pauta publicitaria CABA sumaba 119.968 en vez de
242.635.557; presupuesto sancionado CABA, 4.043.915 en vez de
1.024.198.746.431.

Copiar el CASE por celda del legacy (``nl2sql.txt:67``) mete el error
inverso: toma "-59.796" (una longitud en formato inglés) como -59796. En
staging hay 1.565 columnas con decimales ingleses de tres cifras.

La regla de acá:

- cada valor que se puede leer de una sola forma se lee así, sea cual sea la
  columna ("1.234.567" y "1.234,5" son argentinos; "1,234.5", "12.5" y
  "0.125", ingleses);
- quedan dos formas ambiguas: "12.500" (¿doce mil quinientos o doce coma
  cinco?) y "1,250" (¿uno coma veinticinco o mil doscientos cincuenta?). Esas
  se resuelven POR COLUMNA, mirando una muestra: si la muestra trae valores
  que sólo pueden ser argentinos (o sólo ingleses), la columna es de ese
  formato. Si no alcanza para decidir, se rechaza el cálculo con un mensaje
  claro: un error de mil veces es peor que no contestar.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from app.application.consultas.sql import CatalogRequestError, quote_ident

NUMERIC_TYPES = re.compile(
    r"^(smallint|integer|bigint|numeric|real|double precision|decimal)", re.IGNORECASE
)

# Las mismas expresiones en Python y en el CASE de SQL (ARE de Postgres).
RE_ENTERO = r"^[+-]?[0-9]+$"
# "12.500": miles argentinos o tres decimales ingleses.
RE_AMBIGUO_PUNTO = r"^[+-]?[1-9][0-9]{0,2}\.[0-9]{3}$"
# "1,250": decimal argentino o miles ingleses.
RE_AMBIGUO_COMA = r"^[+-]?[1-9][0-9]{0,2},[0-9]{3}$"
# Sólo argentino: coma decimal (con o sin puntos de miles) o dos grupos de miles.
RE_AR = r"^[+-]?(([0-9]{1,3}(\.[0-9]{3})+|[0-9]+),[0-9]+|[0-9]{1,3}(\.[0-9]{3}){2,})$"
# Sólo inglés: punto decimal (con o sin comas de miles) o dos grupos de miles.
RE_EN = r"^[+-]?(([0-9]{1,3}(,[0-9]{3})+|[0-9]+)\.[0-9]+|[0-9]{1,3}(,[0-9]{3}){2,})$"

_ENTERO = re.compile(RE_ENTERO)
_AMB_PUNTO = re.compile(RE_AMBIGUO_PUNTO)
_AMB_COMA = re.compile(RE_AMBIGUO_COMA)
_AR = re.compile(RE_AR)
_EN = re.compile(RE_EN)

FORMATO_AR = "ar"
FORMATO_EN = "en"


def es_tipo_numerico(tipo: str) -> bool:
    return bool(NUMERIC_TYPES.match(tipo or ""))


def clase_valor(valor: object) -> str | None:
    """``entero``, ``ar``, ``en``, ``ambiguo_punto``, ``ambiguo_coma`` o None."""
    if valor is None or isinstance(valor, bool):
        return None
    if isinstance(valor, int | float | Decimal):
        return "entero" if float(valor).is_integer() else "en"
    texto = str(valor).strip()
    if not texto:
        return None
    # El orden importa: los ambiguos antes que los patrones generales, que
    # también los aceptarían.
    if _ENTERO.match(texto):
        return "entero"
    if _AMB_PUNTO.match(texto):
        return "ambiguo_punto"
    if _AMB_COMA.match(texto):
        return "ambiguo_coma"
    if _AR.match(texto):
        return FORMATO_AR
    if _EN.match(texto):
        return FORMATO_EN
    return None


@dataclass(frozen=True)
class PerfilNumerico:
    """Lo que dice la muestra de una columna de texto sobre su formato."""

    columna: str
    # "ar", "en" o None (la muestra no tiene valores ambiguos, o no se pudo decidir).
    formato: str | None
    # Si no se puede calcular: por qué, en castellano, para el modelo.
    problema: str | None = None
    numericos: int = 0
    muestra: int = 0


def perfil_columna(columna: str, valores: Iterable[object]) -> PerfilNumerico:
    """Decide el formato de una columna de texto a partir de una muestra.

    - Sin valores ambiguos en la muestra: no hace falta decidir (``formato``
      None). Los ambiguos que haya fuera de la muestra quedan en NULL, no
      mal leídos, y ``filas_con_valor`` lo deja ver.
    - Con ambiguos y evidencia de un solo formato: ese formato.
    - Con ambiguos y enteros cortos ("500", "999") y nada más: "12.500" es
      argentino (el separador aparece sólo desde mil) y "1,250" es inglés,
      por la misma razón.
    - Si no: no se puede.
    """
    conteo = {"entero": 0, FORMATO_AR: 0, FORMATO_EN: 0, "ambiguo_punto": 0, "ambiguo_coma": 0}
    enteros_largos = 0
    total = 0
    ejemplos: list[str] = []
    for valor in valores:
        total += 1
        clase = clase_valor(valor)
        if clase is None:
            continue
        conteo[clase] += 1
        if clase == "entero" and len(str(valor).strip().lstrip("+-")) > 3:
            enteros_largos += 1
        if clase.startswith("ambiguo") and len(ejemplos) < 3:
            ejemplos.append(str(valor).strip())
    numericos = sum(conteo.values())

    def perfil(formato: str | None, problema: str | None = None) -> PerfilNumerico:
        return PerfilNumerico(columna, formato, problema, numericos=numericos, muestra=total)

    if not conteo["ambiguo_punto"] and not conteo["ambiguo_coma"]:
        return perfil(None)
    ar, en = conteo[FORMATO_AR], conteo[FORMATO_EN]
    if ar and not en:
        return perfil(FORMATO_AR)
    if en and not ar:
        return perfil(FORMATO_EN)
    if not ar and not en and conteo["entero"] and not enteros_largos:
        if not conteo["ambiguo_coma"]:
            return perfil(FORMATO_AR)
        if not conteo["ambiguo_punto"]:
            return perfil(FORMATO_EN)
    lista = ", ".join(f"«{e}»" for e in ejemplos)
    if ar and en:
        motivo = "mezcla números en formato argentino y en formato inglés"
    else:
        motivo = "no trae ningún valor que diga en qué formato está"
    return perfil(
        None,
        f"La columna {columna!r} tiene números como {lista} que se pueden leer de dos formas "
        "(por ejemplo «12.500»: doce mil quinientos en formato argentino, doce coma cinco en "
        f"formato inglés) y la muestra {motivo}. No calculo sobre esa columna: el error "
        "posible es de mil veces. Mostrá los valores con obtener_datos o usá otra columna.",
    )


def expresion_numero(columna: str, tipo: str, formato: str | None) -> str:
    """La columna como número: directa si ya lo es; si es texto, convertida.

    Un valor que no es número ("s/d", "-") o que es ambiguo en una columna de
    formato desconocido da NULL: nunca un error ni una lectura equivocada.
    """
    ident = quote_ident(columna)
    if es_tipo_numerico(tipo):
        return ident
    x = f"btrim({ident}::text)"
    ar = f"replace(replace({x}, '.', ''), ',', '.')::numeric"
    en = f"replace({x}, ',', '')::numeric"
    if formato == FORMATO_AR:
        punto, coma = f"replace({x}, '.', '')::numeric", f"replace({x}, ',', '.')::numeric"
    elif formato == FORMATO_EN:
        punto, coma = f"{x}::numeric", f"replace({x}, ',', '')::numeric"
    else:
        punto = coma = "NULL"
    return (
        "(CASE"
        f" WHEN {x} ~ '{RE_ENTERO}' THEN {x}::numeric"
        f" WHEN {x} ~ '{RE_AMBIGUO_PUNTO}' THEN {punto}"
        f" WHEN {x} ~ '{RE_AMBIGUO_COMA}' THEN {coma}"
        f" WHEN {x} ~ '{RE_AR}' THEN {ar}"
        f" WHEN {x} ~ '{RE_EN}' THEN {en}"
        " END)"
    )


def leer_numero(valor: object, formato: str | None = None) -> Decimal | None:
    """Lo mismo que ``expresion_numero``, en Python (para valores de filtros y tests)."""
    clase = clase_valor(valor)
    if clase is None:
        return None
    texto = str(valor).strip()
    try:
        if clase == "entero" or (clase == "en" and not isinstance(valor, str)):
            return Decimal(texto)
        if clase == FORMATO_AR:
            return Decimal(texto.replace(".", "").replace(",", "."))
        if clase == FORMATO_EN:
            return Decimal(texto.replace(",", ""))
        if clase == "ambiguo_punto":
            if formato == FORMATO_AR:
                return Decimal(texto.replace(".", ""))
            return Decimal(texto) if formato == FORMATO_EN else None
        if clase == "ambiguo_coma":
            if formato == FORMATO_AR:
                return Decimal(texto.replace(",", "."))
            return Decimal(texto.replace(",", "")) if formato == FORMATO_EN else None
    except InvalidOperation:
        return None
    return None


def numero_de_filtro(valor: str, columna: str, operador: str) -> Decimal:
    """El número de una comparación (``>``, ``<``…), o un error para el modelo.

    "1000000.5", "1.000.000" o "1000000,5" se entienden; "1.500" no (puede
    ser mil quinientos o uno coma cinco).
    """
    clase = clase_valor(valor)
    numero = leer_numero(valor)
    if numero is None:
        if clase and clase.startswith("ambiguo"):
            raise CatalogRequestError(
                f"El filtro {operador} sobre {columna!r}: {valor!r} se puede leer de dos formas. "
                "Escribí el número sin separador de miles (p. ej. 1500 o 1.5)."
            )
        raise CatalogRequestError(
            f"El filtro {operador} sobre {columna!r} necesita un número (recibí {valor!r})."
        )
    return numero
