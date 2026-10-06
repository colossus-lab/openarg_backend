"""Lo común a todas las herramientas del agente.

Cada herramienta es un envoltorio fino sobre código que ya existe (el modo
datos del MCP, los puertos de los conectores). Devuelve dos cosas:

- ``content``: lo que lee el modelo, en JSON compacto y con tope de tamaño;
- ``results``: los ``DataResult`` que respaldan la respuesta. De ahí salen las
  fuentes, los gráficos, el mapa y la verificación de las cifras citadas. Una
  herramienta que sólo orienta (buscar, describir) no devuelve ninguno: lo
  que no se leyó no se cita.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol

from app.application.answers.engine import ClarificationEvent, EngineRequest
from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.agent_llm import AgentTool

# Tope del texto que vuelve al modelo por herramienta. Una tabla entera no
# entra en el contexto ni hace falta: para eso están `calcular` y los filtros.
MAX_CONTENT_CHARS = 12_000
# Filas que ve el modelo de cada resultado. Las demás quedan en `results`
# (gráficos, fuentes) pero no en el prompt.
MAX_ROWS_FOR_MODEL = 60


class ToolInputError(ValueError):
    """Pedido inválido: el mensaje vuelve al modelo para que corrija."""


@dataclass
class ToolContext:
    deps: Any  # PipelineDeps
    req: EngineRequest
    # La búsqueda vectorial usa la sesión de SQLAlchemy de la request, que no
    # admite dos operaciones a la vez. El agente pide herramientas en
    # paralelo: sin esto, dos `buscar_datos` del mismo turno rompían la sesión
    # para el resto del turno (medido en staging el 01-oct).
    db_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class ToolOutcome:
    content: str
    results: list[DataResult] = field(default_factory=list)
    is_error: bool = False
    # Sólo `pedir_aclaracion`: el turno termina con una pregunta al usuario.
    clarification: ClarificationEvent | None = None
    # Lo que se hizo, para la lista de pasos que ve el usuario
    # ("Revisó «Estudio Nacional…» (82.327 filas)"). None = no se muestra.
    summary: str | None = None


class AgentToolImpl(Protocol):
    spec: AgentTool
    # Lo que ve el usuario mientras corre, si la herramienta no da algo más
    # concreto con `describe(args)` ("Consultando series de tiempo…").
    status: str

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome: ...


def quoted(text: Any, limit: int = 60) -> str:
    """Un texto del pedido, entre comillas y corto, para mostrarle al usuario."""
    value = " ".join(str(text or "").split())
    if len(value) > limit:
        value = value[: limit - 1].rstrip() + "…"
    return f"«{value}»"


def count(n: int | None, singular: str, plural: str) -> str:
    """ "1 fila", "82.327 filas" (con separador de miles argentino)."""
    if n is None:
        return plural
    word = singular if n == 1 else plural
    return f"{n:,}".replace(",", ".") + f" {word}"


def _default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def _dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=_default, separators=(",", ":"))


def to_json(payload: Any) -> str:
    """JSON compacto para el modelo, cortado a ``MAX_CONTENT_CHARS``."""
    text = _dumps(payload)
    if len(text) <= MAX_CONTENT_CHARS:
        return text
    return text[:MAX_CONTENT_CHARS] + '…(cortado: pedí menos filas o columnas, o usá calcular)"'


def plain_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filas con tipos de JSON: ``Decimal`` → float, fechas → ISO.

    Los gráficos y el `complete` del WS se serializan después; un ``Decimal``
    suelto rompía el armado de los gráficos.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        clean: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, Decimal):
                clean[k] = float(v)
            elif isinstance(v, datetime | date):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        out.append(clean)
    return out


def _rows_that_fit(head: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Las primeras filas que entran enteras en ``MAX_CONTENT_CHARS`` detrás de ``head``.

    Al menos una: sin filas el modelo no tendría nada que leer (si esa sola no
    entra, la corta ``to_json`` como antes).
    """
    room = MAX_CONTENT_CHARS - len(_dumps({**head, "filas": []}))
    shown: list[dict[str, Any]] = []
    for row in rows:
        size = len(_dumps(row)) + (1 if shown else 0)  # la coma entre filas
        if size > room:
            break
        room -= size
        shown.append(row)
    return shown or rows[:1]


def result_for_model(result: DataResult, **extra: Any) -> dict[str, Any]:
    """Un ``DataResult`` como lo ve el modelo: título, fuente, unidades y filas.

    Las filas van al final y enteras: si no entran en ``MAX_CONTENT_CHARS`` se
    sacan filas del final y la nota lo dice. Con las filas primero, el corte de
    ``to_json`` se comía la descripción y los avisos de ``extra``, y el modelo
    leía el último número a medias (revisión de #150: el aviso de una DDJJ
    excluida no llegaba con un ranking de 20).
    """
    records = result.records or []
    meta = result.metadata or {}
    payload: dict[str, Any] = {
        "titulo": result.dataset_title,
        "fuente": result.portal_name,
        "filas_totales": meta.get("total_records", len(records)),
    }
    if meta.get("units"):
        payload["unidades"] = meta["units"]
    if meta.get("description"):
        payload["descripcion"] = str(meta["description"])[:400]
    rows = records[:MAX_ROWS_FOR_MODEL]
    # La nota más larga posible reserva su lugar antes de medir las filas.
    nota_max = {"nota": f"Se muestran {len(rows)} de {len(records)} filas."}
    rows = _rows_that_fit({**payload, **nota_max, **extra}, rows)
    if len(rows) < len(records):
        payload["nota"] = f"Se muestran {len(rows)} de {len(records)} filas."
    payload.update(extra)
    payload["filas"] = rows
    return payload


def str_arg(
    args: dict[str, Any], name: str, *, required: bool = False, max_len: int = 300
) -> str | None:
    value = args.get(name)
    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise ToolInputError(f"Falta `{name}`.")
        return None
    if not isinstance(value, str | int | float):
        raise ToolInputError(f"`{name}` tiene que ser texto.")
    text = str(value).strip()
    if len(text) > max_len:
        raise ToolInputError(f"`{name}` es demasiado largo (máximo {max_len} caracteres).")
    return text


def int_arg(args: dict[str, Any], name: str, default: int, lo: int, hi: int) -> int:
    value = args.get(name, default)
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ToolInputError(f"`{name}` tiene que ser un número entero.") from None
    if not lo <= number <= hi:
        raise ToolInputError(f"`{name}` tiene que estar entre {lo} y {hi}.")
    return number
