"""La gramática de filtros, una sola para el modo datos y para el agente.

Antes había dos: ``obtener_datos`` (MCP y agente) sólo sabía igualdad exacta
(``"col"::text = 'valor'``) y ``calcular`` tenía ``=``, ``!=``, ``>``…
``contiene`` en ``aggregates.py``. Ninguna tenía ``en``, las dos comparaban
byte a byte («Educacion y Cultura» no encontraba «Educación y Cultura», 0
filas sin pista) y los valores iban interpolados en el SQL.

Ahora:

- operadores ``=``, ``!=``, ``>``, ``>=``, ``<``, ``<=``, ``contiene`` y ``en``
  (con los alias ``igual``, ``distinto``, ``mayor_que``, ``menor_que``…);
- igualdad y ``contiene`` sin distinguir mayúsculas ni acentos en las tablas
  donde plegar cada fila no pasa el timeout (``tolerante``); en las grandes,
  igualdad exacta con lo pedido MÁS los valores reales que ``pg_stats`` ya
  conoce para eso («Educación y Cultura» para «educacion y cultura»);
- las comparaciones de orden son numéricas y usan el formato de la columna
  (``numeros``);
- todos los valores van como parámetros ligados.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from app.application.consultas.fechas import es_nombre_de_fecha
from app.application.consultas.numeros import (
    es_tipo_numerico,
    expresion_numero,
    numero_de_filtro,
)
from app.application.consultas.sql import CatalogRequestError, Params, quote_ident
from app.application.consultas.texto import escapar_like, plegar, plegar_sql

OPERADORES = ("=", "!=", ">", ">=", "<", "<=", "contiene", "en")
ALIAS = {
    "igual": "=",
    "==": "=",
    "distinto": "!=",
    "<>": "!=",
    "mayor_que": ">",
    "menor_que": "<",
    "mayor_o_igual": ">=",
    "menor_o_igual": "<=",
    "in": "en",
}
OPERADORES_DE_TEXTO = ("=", "!=", "contiene", "en")
MAX_VALOR = 200
MAX_VALORES_EN = 50

_FECHA_ISO = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")
_DATE_TYPE = re.compile(r"^(date|timestamp)", re.IGNORECASE)


@dataclass(frozen=True)
class Filter:
    columna: str
    operador: str
    # Un texto; una tupla para `en`.
    valor: str | tuple[str, ...]
    # Valores reales de la columna que corresponden a lo pedido (de
    # `pg_stats`). En una tabla grande se suman a lo pedido en la igualdad
    # exacta; nunca lo reemplazan.
    canonicos: tuple[str, ...] = field(default=())

    @property
    def valores(self) -> tuple[str, ...]:
        return self.valor if isinstance(self.valor, tuple) else (self.valor,)


def _texto(valor: Any, columna: str) -> str:
    if isinstance(valor, dict | list | tuple | set):
        raise CatalogRequestError(f"El valor del filtro sobre {columna!r} tiene que ser un texto.")
    texto = str(valor)
    if len(texto) > MAX_VALOR:
        raise CatalogRequestError(f"El valor del filtro sobre {columna!r} es demasiado largo.")
    return texto


def _como_texto(valor: Any) -> str:
    """El valor de un filtro como texto: ``2020.0`` (un JSON numérico) es "2020"."""
    if isinstance(valor, float) and valor.is_integer():
        return str(int(valor))
    return "" if valor is None else str(valor)


def normalizar_operador(operador: Any) -> str:
    op = str(operador or "=").strip().lower()
    return ALIAS.get(op, op)


def leer_filtros(raw: Any, max_filtros: int) -> list[Filter]:
    """Los filtros de un pedido, en cualquiera de las dos formas aceptadas.

    - ``{"columna": "valor"}``: la forma vieja del modo datos, igualdad (una
      lista como valor es ``en``). Se sigue aceptando para no romper clientes.
    - ``[{"columna": ..., "operador": ..., "valor": ...}]``: la de ``calcular``;
      ``en`` lleva la lista en ``valores`` (o en ``valor``).
    """
    if raw is None or raw == {} or raw == []:
        return []
    filtros: list[Filter] = []
    if isinstance(raw, Mapping):
        for columna, crudo in raw.items():
            if isinstance(crudo, list | tuple):
                filtros.append(Filter(str(columna), "en", tuple(_como_texto(v) for v in crudo)))
            else:
                filtros.append(Filter(str(columna), "=", _como_texto(crudo)))
    elif isinstance(raw, list):
        for item in raw:
            if not isinstance(item, Mapping) or "columna" not in item:
                raise CatalogRequestError(
                    "Cada filtro es un objeto {columna, operador, valor} "
                    "(para `en`, {columna, operador: 'en', valores: [...]})."
                )
            operador = normalizar_operador(item.get("operador"))
            valor: Any = item.get("valores")
            if valor is None:
                valor = item.get("valor")
            if operador == "en":
                if isinstance(valor, str):
                    valor = [valor]
                if not isinstance(valor, list | tuple):
                    raise CatalogRequestError(
                        f"El filtro `en` sobre {item['columna']!r} lleva una lista en `valores`."
                    )
                filtros.append(
                    Filter(str(item["columna"]), "en", tuple(_como_texto(v) for v in valor))
                )
            else:
                if valor is None:
                    raise CatalogRequestError(
                        f"Falta el `valor` del filtro sobre {item['columna']!r}."
                    )
                filtros.append(Filter(str(item["columna"]), operador, _como_texto(valor)))
    else:
        raise CatalogRequestError("`filtros` es un objeto {columna: valor} o una lista de filtros.")
    if len(filtros) > max_filtros:
        raise CatalogRequestError(f"Como mucho {max_filtros} filtros.")
    return filtros


def validar_filtros(filtros: list[Filter], tipos: Mapping[str, str]) -> list[Filter]:
    """Columnas que existen, operadores conocidos, valores acotados."""
    validos: list[Filter] = []
    for f in filtros:
        if f.columna not in tipos:
            raise CatalogRequestError(
                f"No se puede filtrar por {f.columna!r}: no es una columna de la tabla. "
                "Usá describir_tabla para ver las disponibles."
            )
        operador = normalizar_operador(f.operador)
        if operador not in OPERADORES:
            raise CatalogRequestError(
                f"Operador {f.operador!r}: usá {', '.join(OPERADORES)} "
                "(o mayor_que, menor_que, distinto)."
            )
        if operador == "en":
            if not f.valores:
                raise CatalogRequestError(f"El filtro `en` sobre {f.columna!r} no tiene valores.")
            if len(f.valores) > MAX_VALORES_EN:
                raise CatalogRequestError(
                    f"Como mucho {MAX_VALORES_EN} valores en el filtro `en` sobre {f.columna!r}."
                )
            valor: str | tuple[str, ...] = tuple(_texto(v, f.columna) for v in f.valores)
        else:
            if isinstance(f.valor, tuple):
                raise CatalogRequestError(
                    f"El filtro {operador} sobre {f.columna!r} lleva un solo valor; "
                    "para varios usá `en`."
                )
            valor = _texto(f.valor, f.columna)
        validos.append(replace(f, operador=operador, valor=valor))
    return validos


def es_columna_de_texto(tipo: str) -> bool:
    return not es_tipo_numerico(tipo) and not _DATE_TYPE.match(tipo or "")


def columnas_numericas(filtros: list[Filter]) -> list[str]:
    """Las columnas que los filtros comparan como número (``>``, ``<``…)."""
    return [f.columna for f in filtros if f.operador not in OPERADORES_DE_TEXTO]


def sql_filtro(
    f: Filter,
    tipos: Mapping[str, str],
    params: Params,
    *,
    tolerante: bool = True,
    formatos: Mapping[str, str | None] | None = None,
) -> str:
    """La condición de un filtro ya validado. Los valores van a ``params``."""
    ident = quote_ident(f.columna)
    tipo = tipos.get(f.columna, "text")
    texto = f"{ident}::text"
    plegable = tolerante and es_columna_de_texto(tipo)
    if f.operador in ("=", "!=", "en"):
        if plegable:
            # Plegar cada fila encuentra también las variantes que `pg_stats`
            # no lista («EDUCACION Y CULTURA» en tres filas sueltas).
            plegados = list(dict.fromkeys(plegar(v) for v in f.valores))
            condicion = (
                f"{plegar_sql(texto)} = {params.bind(plegados[0])}"
                if len(plegados) == 1
                else f"{plegar_sql(texto)} = ANY({params.bind(plegados)})"
            )
        else:
            # Tabla grande (o columna no de texto): igualdad exacta con lo
            # pedido y con los valores reales que `pg_stats` conoce para eso
            # («CORDOBA» para «Córdoba»). Lo pedido va SIEMPRE: el most_common_vals
            # lista sólo los frecuentes, y reemplazar el filtro por los
            # canónicos perdía los demás sin aviso (censo de hogares, 1,4 M de
            # filas, staging 05-oct: `en [Gnral.Pueyrredon, Gnral Viamonte]`
            # contaba sólo el primero, 110.822 en vez de 113.255).
            valores = list(dict.fromkeys([*f.canonicos, *f.valores]))
            condicion = (
                f"{texto} = {params.bind(valores[0])}"
                if len(valores) == 1
                else f"{texto} = ANY({params.bind(valores)})"
            )
        return f"NOT ({condicion})" if f.operador == "!=" else condicion
    valor = f.valores[0]
    if f.operador == "contiene":
        if plegable:
            return (
                f"{plegar_sql(texto)} LIKE {params.bind('%' + escapar_like(plegar(valor)) + '%')}"
            )
        return f"{texto} ILIKE {params.bind('%' + escapar_like(valor) + '%')}"
    # Comparaciones de orden: numéricas.
    if _FECHA_ISO.match(valor.strip()) and (
        _DATE_TYPE.match(tipo or "") or es_nombre_de_fecha(f.columna)
    ):
        raise CatalogRequestError(
            f"Para filtrar {f.columna!r} por fecha usá `desde`/`hasta` "
            "(y `columna_fecha` si no es la columna de fecha de la tabla)."
        )
    numero = numero_de_filtro(valor.strip(), f.columna, f.operador)
    formato = (formatos or {}).get(f.columna)
    return f"{expresion_numero(f.columna, tipo, formato)} {f.operador} {params.bind(numero)}"


def describir_filtro(f: Filter, *, tolerante: bool, de_texto: bool = True) -> str | None:
    """Con qué valores de la tabla se comparó cada valor pedido; None si con ninguno distinto.

    El legacy aprendió esto el 27-jul: si se le dice al modelo que hubo un
    reemplazo sin decirle cuál, inventa la categoría. La nota va valor por
    valor y nunca presenta una lista parcial como el filtro entero: con
    ``en [salud, defensa]`` y sólo «Salud» en ``pg_stats``, "filtré por
    «Salud»" hacía creer que «defensa» había quedado afuera.

    En una tabla grande (``tolerante=False``) dice además qué valores se
    buscaron tal cual: ahí otra forma de escribirlos no entra.
    """
    if f.operador not in ("=", "!=", "en") or not de_texto:
        return None
    partes: list[str] = []
    for pedido in dict.fromkeys(f.valores):
        clave = plegar(pedido)
        reales = [c for c in f.canonicos if c != pedido and plegar(c) == clave]
        lista = ", ".join(f"«{c}»" for c in reales)
        if tolerante:
            if reales:
                partes.append(f"«{pedido}» coincide con {lista}")
        elif reales:
            partes.append(f"«{pedido}» tal cual y como {lista}")
        elif pedido not in f.canonicos:
            partes.append(f"«{pedido}» tal cual")
    if not partes:
        return None
    if tolerante:
        return (
            f"{f.columna}: {'; '.join(partes)}, como figura en la tabla "
            "(comparé sin distinguir mayúsculas ni acentos)"
        )
    return (
        f"{f.columna}: busqué {'; '.join(partes)}. La tabla es muy grande para ignorar "
        "mayúsculas y acentos en cada fila: otra forma de escribir el valor no entra"
    )


def notas_de_filtros(
    filtros: list[Filter], tipos: Mapping[str, str], *, tolerante: bool
) -> list[str]:
    """``describir_filtro`` de cada filtro, sin los que no tienen nada que decir."""
    notas = (
        describir_filtro(
            f, tolerante=tolerante, de_texto=es_columna_de_texto(tipos.get(f.columna, "text"))
        )
        for f in filtros
    )
    return [n for n in notas if n]
