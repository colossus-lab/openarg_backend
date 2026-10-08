"""Declaraciones juradas patrimoniales, servidas desde `raw.cache_ddjj_*`.

Hasta el 08-oct-2026 este adaptador leía un JSON fijo con 195 diputados de 2024,
cargado a mano desde PDFs y nunca actualizado. Ahora lee las tablas que carga
`ddjj_tasks`:

- **Oficina Anticorrupción** (`fuente = 'oficina_anticorrupcion'`, jurisdicción
  nacional): 2012 en adelante, todos los que presentan ante la OA (Poder
  Ejecutivo, Diputados, Senado, parte del Judicial y del Ministerio Público),
  con el detalle de bienes y deudas.
- **Ciudad de Buenos Aires** (`fuente = 'caba'`): 2023 en adelante, funcionarios
  del Ejecutivo porteño, con el total de bienes por tipo y sin deudas (sin
  patrimonio neto).

Las cifras que no cierran se marcan al cargar (H005, ver `ddjj_tasks`):

- `inconsistente`: el total de bienes no cierra con su propio detalle (o, en
  CABA, pasa 1.000 veces la mediana del año). Queda fuera de rankings y
  estadísticas.
- `ingresos_inconsistentes`: el ahorro declarado no se refleja en los bienes.
  Queda fuera sólo del ranking por ingresos.

Las filas que se le pasan al modelo y al frontend conservan los nombres de
antes (`patrimonio_cierre`, `bienes_detalle`, `resumen_bienes`…): las tarjetas
de DDJJ del frontend los leen así.
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.ddjj import organismos
from app.domain.entities.connectors.data_result import DataResult

logger = logging.getLogger(__name__)

TABLA = "raw.cache_ddjj_declaraciones"
TABLA_BIENES = "raw.cache_ddjj_bienes"

FUENTE_OA = "oficina_anticorrupcion"
FUENTE_CABA = "caba"
# jurisdicción que pide el agente → fuente de la tabla
JURISDICCIONES = {"nacional": FUENTE_OA, "caba": FUENTE_CABA}

_PORTALES = {
    FUENTE_OA: (
        "Declaraciones Juradas Patrimoniales — Oficina Anticorrupción",
        "https://datos.jus.gob.ar/dataset/declaraciones-juradas-patrimoniales-integrales",
    ),
    FUENTE_CABA: (
        "Declaraciones Juradas — Ciudad de Buenos Aires",
        "https://data.buenosaires.gob.ar/dataset/declaraciones-juradas",
    ),
}
_NOMBRE_FUENTE = {FUENTE_OA: "Oficina Anticorrupción", FUENTE_CABA: "Ciudad de Buenos Aires"}

# Una persona puede presentar varias en el mismo año (inicial, anual, baja): en
# rankings, estadísticas y evolución cuenta una, la anual si la hay.
_PRIORIDAD_TIPO = (
    "CASE tipo WHEN 'Anual' THEN 0 WHEN 'Baja' THEN 1 WHEN 'Inicial' THEN 2 ELSE 3 END"
)
# CABA no trae CUIT: la persona es el nombre.
_PERSONA = "coalesce(cuit, nombre)"
_SIN_TILDES = "translate(upper({col}), 'ÁÉÍÓÚÜÑ', 'AEIOUUN')"

_ORDEN = {"patrimonio": "patrimonio", "bienes": "bienes", "ingresos": "ingresos_netos"}

_COBERTURA_TTL_S = 600

# H005 review: nadie verificó si una cifra viene así de la fuente o se rompió al
# publicarla, así que el motivo habla del registro, nunca de la persona.
_NO_COMPARABLE = (
    "Es un probable error de carga en el registro publicado, no verificado contra la "
    "declaración original: esta DDJJ no es comparable y queda fuera de rankings, promedios "
    "y variación patrimonial."
)
_INGRESOS_NO_COMPARABLES = (
    "Lo que la declaración dice ahorrar no se refleja en sus bienes ni en sus deudas, así "
    "que esos ingresos no son comparables y quedan fuera del ranking por ingresos (sus "
    "bienes sí cierran y siguen en los demás rankings)."
)


def _sin_tildes(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    ).upper()


def _escapar_like(valor: str) -> str:
    return valor.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _numero(valor: Any) -> float | None:
    if valor is None:
        return None
    if isinstance(valor, Decimal):
        return float(valor)
    return float(valor) if isinstance(valor, int | float) else None


def _millones(valor: float) -> str:
    """``31275522665.75`` → ``"$31.275,5 millones"`` (es-AR)."""
    texto = f"{valor / 1_000_000:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"${texto} millones"


def _veces(ratio: float) -> str:
    """Truncado, nunca redondeado para arriba: 472.5 → ``"472"``."""
    return f"{int(ratio):,}".replace(",", ".")


def _declaraciones(n: int) -> str:
    return f"{n} declaración" if n == 1 else f"{n} declaraciones"


def motivo_inconsistencia(fila: Mapping[str, Any]) -> str | None:
    """Por qué el total de bienes no cierra, con los números de la fila, o ``None``."""
    if not fila.get("inconsistente"):
        return None
    bienes = _numero(fila.get("bienes")) or 0.0
    if fila.get("fuente") == FUENTE_CABA:
        return (
            f"En el registro de la Ciudad, el total de bienes ({_millones(bienes)}) pasa "
            "1.000 veces la mediana de las declaraciones de ese año. " + _NO_COMPARABLE
        )
    inicial = fila.get("tipo") == "Inicial"
    detalle = (
        _numero(fila.get("detalle_bienes_inicio" if inicial else "detalle_bienes_cierre")) or 0.0
    )
    if detalle <= 0:
        return f"En el registro de la Oficina Anticorrupción el total de bienes no cierra. {_NO_COMPARABLE}"
    if bienes >= detalle:
        motivo = (
            f"En el registro de la Oficina Anticorrupción, el total de bienes ({_millones(bienes)}) "
            f"es {_veces(bienes / detalle)} veces la suma de los bienes del detalle "
            f"({_millones(detalle)})"
        )
    elif bienes > 0:
        motivo = (
            f"En el registro de la Oficina Anticorrupción, la suma de los bienes del detalle "
            f"({_millones(detalle)}) es {_veces(detalle / bienes)} veces el total de bienes "
            f"({_millones(bienes)})"
        )
    else:
        motivo = (
            "En el registro de la Oficina Anticorrupción, el total de bienes es cero pero los "
            f"bienes del detalle suman {_millones(detalle)}"
        )
    return f"{motivo}. {_NO_COMPARABLE}"


def motivo_ingresos(fila: Mapping[str, Any]) -> str | None:
    if not fila.get("ingresos_inconsistentes"):
        return None
    ingresos = _numero(fila.get("ingresos_netos")) or 0.0
    gastos = _numero(fila.get("gastos_personales")) or 0.0
    base = max(
        _numero(fila.get("bienes_inicio")) or 0.0,
        _numero(fila.get("bienes_cierre")) or 0.0,
        _numero(fila.get("deudas_inicio")) or 0.0,
    )
    if base <= 0:
        return _INGRESOS_NO_COMPARABLES
    return (
        f"Los ingresos netos ({_millones(ingresos)}) menos los gastos personales "
        f"({_millones(gastos)}) son {_veces((ingresos - gastos) / base)} veces lo mayor entre "
        f"los bienes al inicio, los bienes al cierre y las deudas al inicio ({_millones(base)}). "
        + _INGRESOS_NO_COMPARABLES
    )


_RE_TIPO_BIEN = re.compile(r"\s*EN EL (?:PAIS|EXTERIOR)\s*")


def resumen_bienes(detalle: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    resumen: dict[str, float] = {}
    for b in detalle:
        tipo = _RE_TIPO_BIEN.sub(" ", str(b.get("tipo") or "OTROS")).strip()
        resumen[tipo] = resumen.get(tipo, 0.0) + (_numero(b.get("importe")) or 0.0)
    return resumen


def registro(
    fila: Mapping[str, Any],
    detalle: Sequence[Mapping[str, Any]] | None = None,
    *,
    compacto: bool = False,
) -> dict[str, Any]:
    """Una fila de la tabla como la leen el modelo y las tarjetas del frontend.

    Con ``compacto`` (rankings) no van el detalle ni el resumen de bienes, y las
    marcas sólo cuando valen true: con todo, un top 20 pasaba el tope del
    contenido de la herramienta (FR-004a / FIX-007).
    """
    motivo = motivo_inconsistencia(fila)
    motivo_ing = motivo_ingresos(fila)
    fuente = str(fila.get("fuente") or "")
    row: dict[str, Any] = {
        "fuente": _NOMBRE_FUENTE.get(fuente, fuente),
        "cuit": fila.get("cuit") or "",
        "nombre": fila.get("nombre") or "",
        "cargo": fila.get("cargo") or "",
        "organismo": fila.get("organismo") or "",
        "poder": fila.get("poder") or "",
        "anio_declaracion": fila.get("anio"),
        "tipo_declaracion": fila.get("tipo") or "",
        "bienes_cierre": _numero(fila.get("bienes")),
        "deudas_cierre": _numero(fila.get("deudas")),
        "patrimonio_cierre": _numero(fila.get("patrimonio")),
        "variacion_patrimonial": _numero(fila.get("variacion_patrimonial")),
        "ingresos_trabajo_neto": _numero(fila.get("ingresos_netos")),
        "gastos_personales": _numero(fila.get("gastos_personales")),
    }
    if fuente == FUENTE_CABA:
        row["nota"] = "La Ciudad publica sólo bienes, sin deudas: no hay patrimonio neto."
    if not compacto or motivo is not None:
        row["inconsistente"] = motivo is not None
    if not compacto or motivo_ing is not None:
        row["ingresos_inconsistentes"] = motivo_ing is not None
    if motivo is not None:
        # Los totales declarados quedan visibles (es lo que dice la DDJJ), pero la
        # variación de un total que no cierra no es una variación.
        row["motivo_inconsistencia"] = motivo
        row["variacion_patrimonial"] = None
    if motivo_ing is not None:
        row["motivo_inconsistencia_ingresos"] = motivo_ing
    if not compacto:
        if detalle is not None:
            row["cantidad_bienes"] = len(detalle)
            row["bienes_detalle"] = [
                {
                    "tipo": b.get("tipo") or "",
                    "descripcion": b.get("descripcion") or "",
                    "importe": _numero(b.get("importe")),
                    "titularidad": ""
                    if b.get("titularidad") is None
                    else f"{_numero(b['titularidad']):g}%",
                }
                for b in detalle
            ]
            row["resumen_bienes"] = resumen_bienes(detalle)
        elif isinstance(fila.get("bienes_por_tipo"), dict):
            row["resumen_bienes"] = {
                k.replace("_", " ").upper(): float(v) for k, v in fila["bienes_por_tipo"].items()
            }
        row["url_fuente"] = fila.get("url_fuente") or ""
    return row


def _patrones_cargo(cargo: str) -> list[str]:
    """Patrones LIKE para el cargo, sin género y desde el principio.

    "diputado nacional" tiene que traer "DIPUTADA NACIONAL" y "Diputado Nacional
    por la Provincia de Córdoba", pero no "CANDIDATO A DIPUTADO NACIONAL" ni
    "ASESOR DEL DIPUTADO": en 2024, contener "DIPUTADO NACIONAL" traía 388
    personas para 257 bancas. La primera palabra va al principio del cargo y las
    demás en cualquier lugar; una palabra terminada en o/a vale por las dos.
    """
    palabras = [p for p in re.split(r"[\s,/]+", _sin_tildes(cargo)) if p]
    patrones = []
    for i, palabra in enumerate(palabras[:5]):
        raiz = palabra[:-1] if len(palabra) > 3 and palabra[-1] in "OA" else palabra
        raiz = _escapar_like(raiz)
        patrones.append(f"{raiz}%" if i == 0 else f"%{raiz}%")
    return patrones


class DDJJAdapter:
    """Consultas sobre `raw.cache_ddjj_declaraciones` (async, como `StaffAdapter`)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._cobertura: dict[str, tuple[int, int]] = {}
        self._cobertura_hasta = 0.0

    # ── helpers ─────────────────────────────────────────────

    async def _filas(self, sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            res = await session.execute(text(sql), dict(params))
            return [dict(r._mapping) for r in res]

    async def cobertura(self) -> dict[str, tuple[int, int]]:
        """Primer y último año por fuente (cacheado unos minutos)."""
        if self._cobertura and time.monotonic() < self._cobertura_hasta:
            return self._cobertura
        filas = await self._filas(
            f"SELECT fuente, min(anio) AS desde, max(anio) AS hasta FROM {TABLA} GROUP BY fuente",
            {},
        )
        self._cobertura = {str(f["fuente"]): (int(f["desde"]), int(f["hasta"])) for f in filas}
        self._cobertura_hasta = time.monotonic() + _COBERTURA_TTL_S
        return self._cobertura

    async def contar(self) -> int:
        """Cuántas declaraciones hay (estimado de Postgres, para el health check)."""
        filas = await self._filas(
            "SELECT GREATEST(reltuples, 0)::bigint AS n FROM pg_class WHERE oid = to_regclass(:t)",
            {"t": TABLA},
        )
        return int(filas[0]["n"]) if filas else 0

    def describir_cobertura(self, cobertura: Mapping[str, tuple[int, int]]) -> str:
        partes = []
        if FUENTE_OA in cobertura:
            desde, hasta = cobertura[FUENTE_OA]
            partes.append(
                f"Oficina Anticorrupción {desde}–{hasta} (funcionarios nacionales: Poder "
                "Ejecutivo, diputados, senadores, parte del Poder Judicial y del Ministerio "
                "Público)"
            )
        if FUENTE_CABA in cobertura:
            desde, hasta = cobertura[FUENTE_CABA]
            partes.append(f"Ciudad de Buenos Aires {desde}–{hasta} (Poder Ejecutivo porteño)")
        return "; ".join(partes) or "sin declaraciones cargadas"

    def _resultado(
        self,
        titulo: str,
        records: list[dict[str, Any]],
        *,
        fuente: str | None = None,
        descripcion: str = "",
        **metadata: Any,
    ) -> DataResult:
        portal, url = _PORTALES.get(fuente or FUENTE_OA, _PORTALES[FUENTE_OA])
        if fuente is None:
            portal = "Declaraciones Juradas Patrimoniales"
        return DataResult(
            source=f"ddjj:{fuente or 'todas'}",
            portal_name=portal,
            portal_url=url,
            dataset_title=titulo,
            format="json",
            records=records,
            metadata={
                "total_records": len(records),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": descripcion
                or "Declaraciones juradas patrimoniales — parte pública (sin grupo familiar)",
                **metadata,
            },
        )

    @staticmethod
    def _condicion_nombre(consulta: str, params: dict[str, Any]) -> str:
        """Todas las palabras de la consulta en el nombre, sin importar orden ni tildes;
        o el CUIT, si la consulta es un número."""
        digitos = re.sub(r"[\s\-.]", "", consulta)
        if len(digitos) >= 7 and digitos.isdigit():
            params["cuit"] = f"%{digitos}%"
            return "cuit LIKE :cuit"
        palabras = [p for p in re.split(r"[\s,]+", _sin_tildes(consulta)) if p]
        if not palabras:
            return "false"
        condiciones = []
        for i, palabra in enumerate(palabras[:6]):
            params[f"p{i}"] = f"%{_escapar_like(palabra)}%"
            condiciones.append(f"{_SIN_TILDES.format(col='nombre')} LIKE :p{i}")
        return " AND ".join(condiciones)

    async def _anio_por_defecto(self, fuente: str) -> int | None:
        cobertura = await self.cobertura()
        return cobertura.get(fuente, (None, None))[1]

    @staticmethod
    def _filtros(
        params: dict[str, Any],
        *,
        poder: str | None,
        organismo: str | None,
        cargo: str | None,
    ) -> str:
        condiciones = []
        if poder:
            params["poder"] = poder
            condiciones.append("poder = :poder")
        if organismo:
            # Siglas y nombres viejos (ARCA/AFIP) o palabras enteras: ver
            # application/ddjj/organismos.py. El nombre se normaliza igual que ahí:
            # sin tildes y con la puntuación como espacio.
            columna = (
                "regexp_replace("
                + _SIN_TILDES.format(col="organismo")
                + ", '[^A-Z0-9 ]+', ' ', 'g')"
            )
            cond = organismos.condicion(organismo)
            if cond.patrones:
                opciones = []
                for i, patron in enumerate(cond.patrones):
                    params[f"org{i}"] = patron
                    opciones.append(f"{columna} LIKE :org{i}")
                condiciones.append("(" + " OR ".join(opciones) + ")")
            for i, expresion in enumerate(cond.regex):
                params[f"orgre{i}"] = expresion
                condiciones.append(f"{columna} ~ :orgre{i}")
            if not cond.patrones and not cond.regex:
                condiciones.append("false")
        for i, patron in enumerate(_patrones_cargo(cargo or "")):
            params[f"cargo{i}"] = patron
            condiciones.append(f"ltrim({_SIN_TILDES.format(col='cargo')}) LIKE :cargo{i}")
        return "".join(f" AND {c}" for c in condiciones)

    async def _detalle(self, dj_ids: Sequence[int], periodos: Mapping[int, str]) -> dict[int, list]:
        """El detalle de bienes de la OA del período declarado de cada DDJJ."""
        if not dj_ids:
            return {}
        filas = await self._filas(
            f"SELECT dj_id, periodo, tipo, descripcion, titularidad, importe FROM {TABLA_BIENES} "
            "WHERE dj_id = ANY(:ids) ORDER BY dj_id, importe DESC NULLS LAST",
            {"ids": list(dj_ids)},
        )
        detalle: dict[int, list] = {i: [] for i in dj_ids}
        for f in filas:
            if f["periodo"] == periodos.get(f["dj_id"]):
                detalle[f["dj_id"]].append(f)
        return detalle

    # ── consultas ───────────────────────────────────────────

    async def search(
        self,
        query: str,
        limit: int = 10,
        *,
        anio: int | None = None,
        jurisdiccion: str | None = None,
    ) -> DataResult:
        """Declaraciones de una persona (por nombre o CUIT), la más nueva primero."""
        params: dict[str, Any] = {"lim": limit}
        condicion = self._condicion_nombre(query, params)
        if anio:
            params["anio"] = anio
            condicion += " AND anio = :anio"
        fuente = JURISDICCIONES.get(jurisdiccion or "")
        if fuente:
            params["fuente"] = fuente
            condicion += " AND fuente = :fuente"
        filas = await self._filas(
            f"SELECT * FROM {TABLA} WHERE {condicion} "
            f"ORDER BY anio DESC, nombre, {_PRIORIDAD_TIPO}, dj_id DESC LIMIT :lim",
            params,
        )
        oa = [f for f in filas if f["fuente"] == FUENTE_OA]
        detalle = await self._detalle(
            [f["dj_id"] for f in oa],
            {f["dj_id"]: "inicio" if f["tipo"] == "Inicial" else "cierre" for f in oa},
        )
        records = [
            registro(f, detalle.get(f["dj_id"]) if f["fuente"] == FUENTE_OA else None)
            for f in filas
        ]
        resultado = self._resultado(f'Búsqueda DDJJ: "{query}"', records, fuente=fuente)
        if not records:
            resultado.metadata["cobertura"] = self.describir_cobertura(await self.cobertura())
        return resultado

    async def get_by_name(self, name: str) -> DataResult:
        return await self.search(name, 5)

    async def ranking(
        self,
        sort_by: str = "patrimonio",
        top: int = 10,
        order: str = "desc",
        *,
        anio: int | None = None,
        jurisdiccion: str | None = "nacional",
        poder: str | None = None,
        organismo: str | None = None,
        cargo: str | None = None,
    ) -> DataResult:
        fuente = JURISDICCIONES.get(jurisdiccion or "nacional", FUENTE_OA)
        if fuente == FUENTE_CABA and sort_by == "patrimonio":
            # Sin deudas no hay patrimonio neto: el ranking de la Ciudad es por bienes.
            sort_by = "bienes"
        columna = _ORDEN.get(sort_by, "patrimonio")
        anio = anio or await self._anio_por_defecto(fuente)
        params: dict[str, Any] = {"fuente": fuente, "anio": anio, "top": top}
        filtros = self._filtros(params, poder=poder, organismo=organismo, cargo=cargo)
        excluye = "inconsistente" + (" OR ingresos_inconsistentes" if sort_by == "ingresos" else "")
        direccion = "DESC" if order == "desc" else "ASC"
        base = (
            f"WITH base AS (SELECT DISTINCT ON ({_PERSONA}) * FROM {TABLA} "
            f"WHERE fuente = :fuente AND anio = :anio AND {columna} IS NOT NULL{filtros} "
            f"ORDER BY {_PERSONA}, {_PRIORIDAD_TIPO}, rectificativa DESC NULLS LAST, dj_id DESC) "
        )
        filas = await self._filas(
            base + f"SELECT * FROM base WHERE NOT ({excluye}) "
            f"ORDER BY {columna} {direccion}, nombre LIMIT :top",
            params,
        )
        # H005: una DDJJ cuyas cifras no cierran nunca entra a un ranking. Sólo se
        # cuentan las que habrían entrado en el recorte, y el modelo recibe cuántas:
        # nombrarlas en cada ranking («los 3 más pobres») ataba a una persona a un
        # registro roto en respuestas que no tenían que ver con ella.
        crudas = await self._filas(
            base + f"SELECT nombre, ({excluye}) AS excluida FROM base "
            f"ORDER BY {columna} {direccion}, nombre LIMIT :top",
            params,
        )
        excluidas = [str(f["nombre"]) for f in crudas if f["excluida"]]
        etiqueta = "mayor" if order == "desc" else "menor"
        quienes = {FUENTE_OA: "funcionarios nacionales", FUENTE_CABA: "funcionarios porteños"}[
            fuente
        ]
        alcance = ", ".join(x for x in (poder, organismo, cargo) if x)
        descripcion = (
            f"Ranking de {quienes} con {etiqueta} {sort_by} declarado en {anio}"
            + (f" ({alcance})" if alcance else "")
            + ". Una declaración por persona (la anual si presentó varias)."
        )
        resultado = self._resultado(
            f"Ranking DDJJ {anio}: {top} {quienes} con {etiqueta} {sort_by}",
            [registro(f, compacto=True) for f in filas],
            fuente=fuente,
            descripcion=descripcion,
            ranking=True,  # el orden de las filas es el puesto: las tarjetas no se saltean
            anio=anio,
        )
        if excluidas:
            verbo = "excluyó" if len(excluidas) == 1 else "excluyeron"
            habria = "habría" if len(excluidas) == 1 else "habrían"
            resultado.metadata["excluidas_por_inconsistencia"] = len(excluidas)
            # Sólo para auditoría: nunca le llega al modelo.
            resultado.metadata["excluidas_por_inconsistencia_nombres"] = excluidas
            resultado.metadata["description"] += (
                f" Se {verbo} {_declaraciones(len(excluidas))} que {habria} entrado en este "
                "ranking: su registro publicado tiene cifras que no cierran con la propia DDJJ "
                "(probable error de carga), así que no es comparable "
                "(excluidas_por_inconsistencia)."
            )
        return resultado

    async def stats(
        self,
        *,
        anio: int | None = None,
        jurisdiccion: str | None = "nacional",
        poder: str | None = None,
        organismo: str | None = None,
        cargo: str | None = None,
    ) -> DataResult:
        fuente = JURISDICCIONES.get(jurisdiccion or "nacional", FUENTE_OA)
        medida = "patrimonio" if fuente == FUENTE_OA else "bienes"
        anio = anio or await self._anio_por_defecto(fuente)
        params: dict[str, Any] = {"fuente": fuente, "anio": anio}
        filtros = self._filtros(params, poder=poder, organismo=organismo, cargo=cargo)
        base = (
            f"WITH base AS (SELECT DISTINCT ON ({_PERSONA}) * FROM {TABLA} "
            f"WHERE fuente = :fuente AND anio = :anio{filtros} "
            f"ORDER BY {_PERSONA}, {_PRIORIDAD_TIPO}, rectificativa DESC NULLS LAST, dj_id DESC), "
            f"usables AS (SELECT * FROM base WHERE NOT inconsistente AND {medida} IS NOT NULL) "
        )
        [agregado] = await self._filas(
            base
            + f"""
            SELECT (SELECT count(*) FROM base) AS total,
                   (SELECT count(*) FROM base WHERE inconsistente) AS excluidas,
                   count(*) AS usables,
                   avg({medida}) AS promedio,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY {medida}) AS mediana,
                   count(*) FILTER (WHERE {medida} < 0) AS negativos
            FROM usables
            """,
            params,
        )
        extremos = await self._filas(
            base + f"(SELECT 'max' AS cual, nombre, {medida} AS monto FROM usables "
            f"ORDER BY {medida} DESC LIMIT 1) UNION ALL "
            f"(SELECT 'min', nombre, {medida} FROM usables ORDER BY {medida} ASC LIMIT 1)",
            params,
        )
        quienes = {FUENTE_OA: "funcionarios nacionales", FUENTE_CABA: "funcionarios porteños"}[
            fuente
        ]
        if not agregado["usables"]:
            return self._resultado(f"Estadísticas DDJJ {anio}", [], fuente=fuente)
        por_extremo = {e["cual"]: e for e in extremos}
        fila: dict[str, Any] = {
            "total": int(agregado["total"]),
            "anio": anio,
            "alcance": quienes + "".join(f", {x}" for x in (poder, organismo, cargo) if x),
            f"{medida}_promedio": _numero(agregado["promedio"]),
            # La mediana de verdad: con una cantidad par, el promedio de los dos centrales.
            f"{medida}_mediano": _numero(agregado["mediana"]),
            f"{medida}_maximo_nombre": por_extremo["max"]["nombre"],
            f"{medida}_maximo_monto": _numero(por_extremo["max"]["monto"]),
            f"{medida}_minimo_nombre": por_extremo["min"]["nombre"],
            f"{medida}_minimo_monto": _numero(por_extremo["min"]["monto"]),
        }
        if medida == "patrimonio":
            # La descripción de la herramienta promete este conteo; sin él, el modelo
            # leía el mínimo (una persona) como la cantidad.
            fila["cantidad_con_patrimonio_negativo"] = int(agregado["negativos"])
        else:
            fila["nota"] = "La Ciudad publica sólo bienes, sin deudas: no hay patrimonio neto."
        descripcion = (
            f"Estadísticas de {int(agregado['total'])} declaraciones juradas de {quienes} de "
            f"{anio} (una por persona)"
        )
        metadata: dict[str, Any] = {"anio": anio}
        if agregado["excluidas"]:
            # Al modelo le llega cuántas, nunca quiénes.
            fila["excluidas_por_inconsistencia"] = int(agregado["excluidas"])
            descripcion += (
                f". Promedio, mediana, máximo y mínimo calculados sin "
                f"{_declaraciones(int(agregado['excluidas']))} cuyo registro publicado tiene un "
                "total de bienes que no cierra con su propio detalle (probable error de carga; "
                "excluidas_por_inconsistencia)."
            )
        return self._resultado(
            f"Estadísticas DDJJ {anio}: {quienes}",
            [fila],
            fuente=fuente,
            descripcion=descripcion,
            **metadata,
        )

    async def evolucion(self, persona: str, *, jurisdiccion: str | None = None) -> DataResult:
        """Lo declarado por una persona, año por año. Si el nombre coincide con varias
        personas, devuelve la lista para que se elija (por CUIT)."""
        params: dict[str, Any] = {}
        condicion = self._condicion_nombre(persona, params)
        fuente = JURISDICCIONES.get(jurisdiccion or "")
        if fuente:
            params["fuente"] = fuente
            condicion += " AND fuente = :fuente"
        personas = await self._filas(
            f"SELECT fuente, {_PERSONA} AS persona, max(nombre) AS nombre, max(cuit) AS cuit, "
            "array_agg(DISTINCT anio ORDER BY anio) AS anios, max(anio) AS ultimo "
            f"FROM {TABLA} WHERE {condicion} GROUP BY fuente, {_PERSONA} "
            "ORDER BY max(anio) DESC LIMIT 8",
            params,
        )
        titulo = f'Evolución DDJJ: "{persona}"'
        if not personas:
            resultado = self._resultado(titulo, [], fuente=fuente)
            resultado.metadata["cobertura"] = self.describir_cobertura(await self.cobertura())
            return resultado
        if len(personas) > 1:
            records = [
                {
                    "fuente": _NOMBRE_FUENTE.get(p["fuente"], p["fuente"]),
                    "nombre": p["nombre"],
                    "cuit": p["cuit"] or "",
                    "anios": list(p["anios"]),
                }
                for p in personas
            ]
            return self._resultado(
                titulo,
                records,
                fuente=fuente,
                descripcion=(
                    f"El nombre coincide con {len(personas)} personas: pedí la evolución de una "
                    "con su CUIT o su nombre completo."
                ),
                varias_personas=True,
            )
        elegida = personas[0]
        filas = await self._filas(
            f"SELECT DISTINCT ON (anio) * FROM {TABLA} "
            f"WHERE fuente = :f AND {_PERSONA} = :p "
            f"ORDER BY anio, {_PRIORIDAD_TIPO}, rectificativa DESC NULLS LAST, dj_id DESC",
            {"f": elegida["fuente"], "p": elegida["persona"]},
        )
        records = []
        for f in filas:
            r = registro(f, compacto=True)
            records.append(
                {"anio": f["anio"], **{k: v for k, v in r.items() if k != "anio_declaracion"}}
            )
        return self._resultado(
            f"Evolución patrimonial declarada: {elegida['nombre']}",
            records,
            fuente=elegida["fuente"],
            descripcion=(
                "Lo declarado cada año (una declaración por año, la anual si hubo varias). En "
                "una inicial lo declarado es al inicio; en las demás, al cierre. Los montos son "
                "nominales, sin ajustar por inflación."
            ),
        )
