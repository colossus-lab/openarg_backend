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
  se resuelven POR COLUMNA, mirando una muestra: si la muestra trae al menos
  ``MIN_EVIDENCIA`` valores distintos que sólo pueden ser de un formato y
  ninguno del otro, la columna es de ese formato. Si no alcanza para decidir,
  se rechaza el cálculo con un mensaje claro: un error de mil veces es peor
  que no contestar;
- en tablas chicas, lo que decidió la muestra se confirma en la columna
  entera (``confirmar_formato``): un solo valor del otro formato es mezcla.

Revisión independiente del 05-oct: el desempate "enteros cortos y «12.500»
es argentino" leía mil veces más altos los minutos por día de la ENUT, que
son decimales ingleses de tres cifras (H001), y un solo valor de la muestra
decidía la columna entera (H009). Ninguno de los dos queda.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, replace
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
# Cualquiera de los dos, en un solo regex (para contarlos: ver `expresion_ambiguo`).
RE_AMBIGUO = r"^[+-]?[1-9][0-9]{0,2}[.,][0-9]{3}$"
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

# Cuánta evidencia hace falta para decidir el formato de una columna: al menos
# MIN_EVIDENCIA valores DISTINTOS que sólo se leen de una forma, y ninguno que
# sólo se lea de la otra. Con uno solo, el mismo dataset de CABA (SADE) daba
# promedios de 7,567 y 8,820 en dos tablas, y la respuesta podía cambiar con
# cada ANALYZE (H009).
#
# Revisión del PR #148:
# - se cuentan valores distintos, no apariciones: la muestra junta pg_stats y
#   las primeras filas, así que cada valor frecuente llega dos o tres veces, y
#   un «1,22» repetido alcanzaba el mínimo;
# - no hay mayoría: con el 90 % se decidían columnas con los dos formatos, y la
#   muestra no representa a la columna (fr1_km del subte: 209 argentinos contra
#   8 ingleses en la muestra, 50.494 contra 159.571 en la columna entera).
MIN_EVIDENCIA = 5

# Lo que se limpia alrededor de cada valor, igual en Python y en SQL: espacio,
# tab, salto de línea, retorno de carro y espacio duro (nbsp). `str.strip()`
# sacaba esos y más; `btrim(x)` de Postgres, sólo el espacio. El perfil
# contaba como número un «0,82\r\r\n» que el SQL dejaba en NULL (H042: 98
# columnas en staging, entre ellas las mediciones de radiaciones de CABA).
ESPACIOS = " \t\n\r\u00a0"
_ESPACIOS_SQL = " || ".join(f"chr({ord(c)})" for c in ESPACIOS)


def es_tipo_numerico(tipo: str) -> bool:
    return bool(NUMERIC_TYPES.match(tipo or ""))


def _texto(columna: str) -> str:
    """La columna como texto, sin ``ESPACIOS`` alrededor (en SQL)."""
    return f"btrim({quote_ident(columna)}::text, {_ESPACIOS_SQL})"


def clase_valor(valor: object) -> str | None:
    """``entero``, ``ar``, ``en``, ``ambiguo_punto``, ``ambiguo_coma`` o None."""
    if valor is None or isinstance(valor, bool):
        return None
    if isinstance(valor, int | float | Decimal):
        return "entero" if float(valor).is_integer() else "en"
    texto = str(valor).strip(ESPACIOS)
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
    # "ar", "en" o None (sin evidencia suficiente: con ambiguos en la muestra,
    # `problema` dice por qué no se calcula).
    formato: str | None
    # Si no se puede calcular: por qué, en castellano, para el modelo.
    problema: str | None = None
    numericos: int = 0
    muestra: int = 0
    # Hasta tres números ambiguos de la muestra (vacío si no trajo ninguno).
    ambiguos: tuple[str, ...] = ()


def _problema(columna: str, ambiguos: Iterable[str], motivo: str) -> str:
    lista = ", ".join(f"«{e}»" for e in ambiguos)
    # Sin ejemplos cuando no se los pudo buscar (`confirmar_formato`).
    tiene = f"tiene números como {lista}" if lista else "puede tener números"
    return (
        f"La columna {columna!r} {tiene} que se pueden leer de dos formas "
        "(por ejemplo «12.500»: doce mil quinientos en formato argentino, doce coma cinco en "
        f"formato inglés) y {motivo}. No calculo sobre esa columna: el error posible es de mil "
        "veces. Mostrá los valores con obtener_datos o usá otra columna."
    )


def perfil_columna(columna: str, valores: Iterable[object]) -> PerfilNumerico:
    """Decide el formato de una columna de texto a partir de una muestra.

    - Con evidencia suficiente de un formato (``MIN_EVIDENCIA`` valores
      distintos que sólo se leen de una forma) y ninguna del otro: ese
      formato, haya o no ambiguos en la muestra. Si no hay, los de fuera de
      la muestra se leen igual (H041: antes quedaban en NULL).
    - Sin evidencia suficiente, o con evidencia de los dos formatos, y sin
      ambiguos en la muestra: no hace falta decidir (``formato`` None). Los
      ambiguos que haya fuera de la muestra quedan en NULL, no mal leídos. Si
      son de la columna del cálculo, el cálculo los cuenta aparte; si son de
      un filtro de orden (``>``, ``<``…), quedan afuera sin aviso (H041
      parcial: contarlos es otra consulta que recorre la tabla).
    - Sin evidencia suficiente, o con evidencia de los dos formatos, y con
      ambiguos: no se puede. Los enteros cortos no son evidencia: "500" y
      "58.333" son también una columna inglesa con tres decimales (H001:
      minutos por día de la ENUT).
    """
    conteo = {"entero": 0, FORMATO_AR: 0, FORMATO_EN: 0, "ambiguo_punto": 0, "ambiguo_coma": 0}
    distintos: dict[str, set[str]] = {FORMATO_AR: set(), FORMATO_EN: set()}
    total = 0
    ejemplos: list[str] = []
    for valor in valores:
        total += 1
        clase = clase_valor(valor)
        if clase is None:
            continue
        conteo[clase] += 1
        if clase in distintos:
            distintos[clase].add(str(valor).strip(ESPACIOS))
        if clase.startswith("ambiguo") and len(ejemplos) < 3:
            ejemplos.append(str(valor).strip(ESPACIOS))
    numericos = sum(conteo.values())

    def perfil(formato: str | None, problema: str | None = None) -> PerfilNumerico:
        return PerfilNumerico(
            columna,
            formato,
            problema,
            numericos=numericos,
            muestra=total,
            ambiguos=tuple(ejemplos),
        )

    ar, en = conteo[FORMATO_AR], conteo[FORMATO_EN]
    if not (ar and en):
        for formato in (FORMATO_AR, FORMATO_EN):
            if len(distintos[formato]) >= MIN_EVIDENCIA:
                return perfil(formato)
    if not conteo["ambiguo_punto"] and not conteo["ambiguo_coma"]:
        return perfil(None)
    if ar and en:
        motivo = "mezcla números en formato argentino y en formato inglés"
    elif ar or en:
        n = len(distintos[FORMATO_AR]) + len(distintos[FORMATO_EN])
        cuales = "valor distinto que dice" if n == 1 else "valores distintos que dicen"
        motivo = (
            f"trae sólo {n} {cuales} en qué formato está, y hacen falta al menos {MIN_EVIDENCIA}"
        )
    else:
        motivo = "no trae ningún valor que diga en qué formato está"
    return perfil(None, _problema(columna, ejemplos, f"la muestra {motivo}"))


# Revisión del PR #148 (segunda ronda): la regla «sin mezcla» de
# `perfil_columna` sólo ve la muestra. En consultas_medicas (rendimiento de
# establecimientos de PBA, staging `a7ce7a82`, 2.371 filas) la muestra trae 25
# valores que sólo pueden ser ingleses («1.07», «24.87») y ninguno argentino;
# la columna entera tiene 8 argentinos («1.005.915») y 1.842 ambiguos con
# punto, que se leían como decimales ingleses: 120.813 consultas eran 120,813.
# Igual en paginas_vistas de la web del GCBA y en Pasos de los peajes de AUSA.


def expresion_otro_formato(columna: str, formato: str) -> str:
    """Condición SQL: el valor sólo se puede leer en el formato contrario a ``formato``.

    Los ambiguos no cuentan: ``RE_EN`` también acepta "12.500" y ``RE_AR``,
    "1,250" (``clase_valor`` los prueba antes por eso).
    """
    x = _texto(columna)
    if formato == FORMATO_AR:
        return f"({x} ~ '{RE_EN}' AND {x} !~ '{RE_AMBIGUO_PUNTO}')"
    return f"({x} ~ '{RE_AR}' AND {x} !~ '{RE_AMBIGUO_COMA}')"


def confirmar_formato(
    perfil: PerfilNumerico, del_otro: int | None, ambiguos: tuple[str, ...] | None = None
) -> PerfilNumerico:
    """El perfil, visto cuántos valores del otro formato hay en la columna entera.

    ``del_otro`` es la cuenta de ``expresion_otro_formato`` sobre toda la
    columna, o None si no se pudo hacer (timeout, error). ``ambiguos``: hasta
    tres números ambiguos de la columna entera, que hace falta buscar sólo si
    hay alguno del otro formato y la muestra no trajo ambiguos; None si no se
    buscaron o la búsqueda falló.

    - Si la muestra no decidió, o la columna no tiene ninguno del otro
      formato, el perfil queda como estaba.
    - Si no se pudo contar, también: vale lo que decidió la muestra, como
      desde ``TOLERANTE_MAX_FILAS``.
    - Si tiene alguno es mezcla, igual que en la muestra: rechazo si hay
      ambiguos (en la muestra o en la columna entera) o no se los pudo
      buscar; sin formato si no hay ninguno.

    Tercera revisión del PR #148: dejar el formato en None no es neutral si
    hay ambiguos. El cálculo cuenta aparte los de su columna, pero un filtro
    ``>`` (en ``agregar`` o ``obtener_datos``) los deja afuera sin aviso. Con
    la cuenta en timeout, el conteo de partidas con crédito presupuestado
    mayor a 5 del presupuesto de la APN daba 16.496 en vez de 17.326.
    """
    if perfil.formato is None or not del_otro:
        return perfil
    ejemplos = perfil.ambiguos or ambiguos
    if ejemplos == ():
        return replace(perfil, formato=None)
    nombre = {FORMATO_AR: "argentino", FORMATO_EN: "inglés"}
    otro = FORMATO_EN if perfil.formato == FORMATO_AR else FORMATO_AR
    cuantos = (
        "un valor que sólo se lee" if del_otro == 1 else f"{del_otro} valores que sólo se leen"
    )
    motivo = (
        "la columna entera mezcla los dos formatos: la muestra parecía en formato "
        f"{nombre[perfil.formato]}, pero la tabla tiene {cuantos} en formato {nombre[otro]}"
    )
    return replace(perfil, formato=None, problema=_problema(perfil.columna, ejemplos or (), motivo))


def expresion_numero(columna: str, tipo: str, formato: str | None) -> str:
    """La columna como número: directa si ya lo es; si es texto, convertida.

    Un valor que no es número ("s/d", "-") o que es ambiguo en una columna de
    formato desconocido da NULL: nunca un error ni una lectura equivocada.
    """
    ident = quote_ident(columna)
    if es_tipo_numerico(tipo):
        return ident
    x = _texto(columna)
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


def expresion_ambiguo(columna: str, tipo: str) -> str | None:
    """Condición SQL: el valor es un número ambiguo ("12.500", "1,250").

    Con el formato sin decidir, esas filas quedan en NULL en
    ``expresion_numero``: el cálculo las cuenta aparte para no decir que "no
    tienen un número" (H041). None si la columna ya es numérica. Un solo
    ``btrim`` y un solo regex por fila, no dos de cada uno (revisión del PR
    #148: la cuenta recorre la tabla entera).
    """
    if es_tipo_numerico(tipo):
        return None
    return f"({_texto(columna)} ~ '{RE_AMBIGUO}')"


def leer_numero(valor: object, formato: str | None = None) -> Decimal | None:
    """Lo mismo que ``expresion_numero``, en Python (para valores de filtros y tests)."""
    clase = clase_valor(valor)
    if clase is None:
        return None
    texto = str(valor).strip(ESPACIOS)
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
