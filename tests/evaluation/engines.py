"""Los motores que la batería sabe correr, detrás de una sola forma.

La batería no tiene que saber si del otro lado hay un grafo de LangGraph o un
agente con herramientas: le pasa la pregunta a ``EvalEngine.run`` y recibe un
``EngineOutput``. Así el mismo set de casos corre contra el sistema actual y
contra el agente, y la comparación es justa.

Hoy hay un solo motor, ``LegacyGraphEngine``, que arma el grafo igual que
``app/run.py``. Cuando exista la interfaz de producción (``AnswerEngine`` en
``app.application.answers.engine``, etapa 2 del plan), este módulo pasa a
envolver esa interfaz y el agente entra como un motor más.

**El costo se mide abajo, no se pide arriba.** El grafo sólo reporta los
tokens del analista (``tokens_used``); el planificador, el NL2SQL y los
reintentos no aparecen. Para saber cuánto cuesta de verdad una respuesta se
cuenta cada llamada a Bedrock en el adaptador, atribuida al caso que la
originó por una ``ContextVar``. La cifra que el motor dice de sí mismo queda
al lado (``tokens_reported``) para ver la diferencia.
"""

from __future__ import annotations

import contextlib
import math
import re
import statistics
import time
import unicodedata
from collections import Counter
from collections.abc import AsyncIterator, Iterator
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Protocol

# Precio por millón de tokens: (entrada, salida, escritura de caché, lectura de
# caché). Son los precios de lista de Anthropic; Bedrock factura aparte y
# puede diferir, así que el costo de la batería sirve para comparar motores
# entre sí, no para conciliar la factura de AWS. Se busca por substring del id
# del modelo, así cubre `us.anthropic.claude-haiku-4-5-20251001-v1:0` y
# `us.anthropic.claude-sonnet-4-6`.
PRICES_PER_MTOK: dict[str, tuple[float, float, float, float]] = {
    "claude-haiku-4-5": (1.00, 5.00, 1.25, 0.10),
    "claude-sonnet-4-6": (3.00, 15.00, 3.75, 0.30),
}


def price_for(model: str) -> tuple[float, float, float, float] | None:
    return next((p for key, p in PRICES_PER_MTOK.items() if key in (model or "")), None)


@dataclass
class ModelUsage:
    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass
class UsageMeter:
    """Lo que gastó un caso, separado por modelo."""

    by_model: dict[str, ModelUsage] = field(default_factory=dict)

    def add(
        self,
        model: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_read: int = 0,
        cache_write: int = 0,
    ) -> None:
        u = self.by_model.setdefault(model, ModelUsage())
        u.calls += 1
        u.input_tokens += int(input_tokens or 0)
        u.output_tokens += int(output_tokens or 0)
        u.cache_read_tokens += int(cache_read or 0)
        u.cache_write_tokens += int(cache_write or 0)

    @property
    def calls(self) -> int:
        return sum(u.calls for u in self.by_model.values())

    def totals(self) -> dict[str, int]:
        return {
            "input": sum(u.input_tokens for u in self.by_model.values()),
            "output": sum(u.output_tokens for u in self.by_model.values()),
            "cache_read": sum(u.cache_read_tokens for u in self.by_model.values()),
            "cache_write": sum(u.cache_write_tokens for u in self.by_model.values()),
        }

    def cost_usd(self) -> float | None:
        """None si algún modelo usado no tiene precio: mejor sin cifra que una baja."""
        total = 0.0
        for model, u in self.by_model.items():
            price = price_for(model)
            if price is None:
                return None
            p_in, p_out, p_cw, p_cr = price
            total += (
                u.input_tokens * p_in
                + u.output_tokens * p_out
                + u.cache_write_tokens * p_cw
                + u.cache_read_tokens * p_cr
            ) / 1_000_000
        return round(total, 6)

    def to_dict(self) -> dict[str, Any]:
        return {
            "llm_calls": self.calls,
            "tokens": self.totals(),
            "cost_usd": self.cost_usd(),
            "by_model": {m: vars(u) for m, u in self.by_model.items()},
        }


_meter: ContextVar[UsageMeter | None] = ContextVar("eval_usage_meter", default=None)


@contextlib.contextmanager
def metering() -> Iterator[UsageMeter]:
    """Cuenta las llamadas a Bedrock que ocurran adentro, en este contexto.

    Cada caso corre en su propia tarea de asyncio, y ``asyncio.to_thread``
    copia el contexto, así que dos casos en paralelo no se mezclan.
    """
    meter = UsageMeter()
    token = _meter.set(meter)
    try:
        yield meter
    finally:
        _meter.reset(token)


_installed = False


def install_usage_capture() -> None:
    """Envuelve el adaptador de Bedrock para que reporte cada llamada.

    Es un parche de la batería, no del código de producción: se instala una
    vez por proceso y sólo cuenta cuando hay un ``metering()`` activo.
    """
    global _installed
    if _installed:
        return
    from app.infrastructure.adapters.llm.bedrock_llm_adapter import BedrockLLMAdapter

    original_converse = BedrockLLMAdapter._converse_sync
    original_stream = BedrockLLMAdapter.chat_stream

    def _converse_sync(self: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        response = original_converse(self, *args, **kwargs)
        meter = _meter.get()
        if meter is not None:
            usage = response.get("usage", {}) or {}
            meter.add(
                self._model,
                usage.get("inputTokens", 0),
                usage.get("outputTokens", 0),
                usage.get("cacheReadInputTokens", 0),
                usage.get("cacheWriteInputTokens", 0),
            )
        return response

    async def chat_stream(
        self: Any,
        messages: Any,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        usage_out: dict[str, int] | None = None,
    ) -> AsyncIterator[str]:
        box: dict[str, int] = usage_out if usage_out is not None else {}
        async for chunk in original_stream(self, messages, temperature, max_tokens, usage_out=box):
            yield chunk
        meter = _meter.get()
        if meter is not None:
            meter.add(self._model, box.get("input_tokens", 0), box.get("output_tokens", 0))

    BedrockLLMAdapter._converse_sync = _converse_sync  # type: ignore[method-assign]
    BedrockLLMAdapter.chat_stream = chat_stream  # type: ignore[method-assign]
    _installed = True


# ── la forma común ─────────────────────────────────────────


@dataclass
class EngineOutput:
    answer: str
    sources: list[dict[str, Any]]
    latency_ms: int
    usage: dict[str, Any]
    # Lo que el motor dice que gastó. En el grafo actual es sólo el analista.
    tokens_reported: int = 0
    # Los datos que el motor tuvo a la vista, resumidos: es lo que el juez de
    # alucinación compara contra la respuesta.
    evidence: str = ""
    error: str | None = None
    # Diagnóstico propio de cada motor (plan, clasificación…). La batería no
    # lo puntúa salvo para la comparación con baselines viejos.
    diagnostics: dict[str, Any] = field(default_factory=dict)
    # Las advertencias que el canal le muestra al usuario ("Advertencias" en
    # el MCP): la batería las guarda para poder juzgarlas.
    warnings: list[str] = field(default_factory=list)
    # La evidencia fuente por fuente, con sus números: con esto se chequea
    # que cada fuente citada haya aportado alguna cifra (`fuente_sin_cifra`)
    # y se puede re-juzgar una corrida sin volver a gastar.
    evidence_items: list[dict[str, Any]] = field(default_factory=list)


class EvalEngine(Protocol):
    name: str

    async def start(self) -> None: ...

    async def run(
        self, question: str, *, case_id: str, mode: str, bypass_cache: bool
    ) -> EngineOutput: ...

    async def aclose(self) -> None: ...


# Tope del resumen de evidencia para el juez: alcanza para ver las cifras que
# se citaron sin mandarle al juez tablas enteras.
_EVIDENCE_MAX_CHARS = 20_000
# De una tabla larga se muestran el principio y, sobre todo, el final: en una
# serie el dato que se cita es el último. Con sólo las primeras 12 filas, el
# juez del 01-oct marcó como inventado el -6,28 % de julio de 2026, que era la
# última fila. Y el principio tiene que cubrir lo que ve el modelo de una
# tabla (`MAX_ROWS_FOR_MODEL`, 60 filas): con 5, el juez marcó como
# inventados los diputados del listado de ckan_004 (0,7-0,8), y desde que el
# juez vota eso desaprobaría una respuesta correcta.
_EVIDENCE_HEAD_ROWS = 60
_EVIDENCE_TAIL_ROWS = 25

# Desde qué largo de fila (la mediana) una fuente es de texto, como los
# fragmentos de las sesiones (~3.400 caracteres), y no una serie o una tabla
# (~90). Sólo de las de texto se eligen filas según la respuesta: en una serie
# los años de la respuesta coinciden con todas las filas y la cola, que es lo
# que se cita, quedaba afuera (neutralidad_005 del 07-oct perdía el 211,41 %
# de dic-2023, revisión de #172).
_TEXT_ROW_CHARS = 1_000
# Las palabras de la respuesta con las que se buscan, entre las filas, las que
# la respaldan: de 4 letras o más, o números como se escriben ("27.838", "3,4").
_TERM_RE = re.compile(r"\d+(?:[.,]\d+)+|\w+")
# Lo que ocupa una marca de filas salteadas ("… 11 filas sin mostrar …"), con
# su salto de línea y de sobra. Se cobra por marca, no por fila.
_GAP_RESERVE = 32
# Una fila que coincide con la respuesta y no entra entera entra como un
# pedazo alrededor de lo afirmado, si quedan al menos estos caracteres.
_MIN_WINDOW_CHARS = 500


def _fold(text: str) -> str:
    """Minúsculas y sin tildes, letra por letra: las posiciones no cambian."""
    return "".join(unicodedata.normalize("NFKD", ch)[0].lower()[:1] for ch in text)


def _answer_terms(answer: str) -> frozenset[str]:
    return frozenset(
        t
        for t in _TERM_RE.findall(_fold(answer or ""))
        if len(t) >= 4 or (len(t) >= 3 and any(c.isdigit() for c in t))
    )


def _row_order(lines: list[str], terms: frozenset[str]) -> tuple[list[int], int, dict[str, float]]:
    """En qué orden entran las filas cuando no entran todas.

    Devuelve el orden, cuántas de las primeras coinciden con la respuesta y
    el peso de cada palabra. Primero van las que tienen palabras distintivas
    de la respuesta: las que aparecen en a lo sumo la mitad de las filas, con
    más peso cuanto más raras ("Temu" en 1 de 12 pesa; "presupuesto", que
    está en todas, no). Después, el principio y el final alternados: el
    principio es lo que el modelo leyó de los fragmentos
    (``result_for_model``), el final lo que veía el juez antes de #172. Una
    afirmación inventada no coincide con ninguna fila, así que no trae
    evidencia: el juez ve lo mismo que sin respuesta. Sólo se usa para
    fuentes de texto (``_TEXT_ROW_CHARS``).
    """
    n = len(lines)
    present = [set(_TERM_RE.findall(_fold(line))) & terms for line in lines]
    df = Counter(t for row in present for t in row)
    weight = {t: math.log(n / c) for t, c in df.items() if c <= n // 2}
    score = [sum(weight.get(t, 0.0) for t in row) for row in present]
    matched = sorted((i for i in range(n) if score[i] > 0), key=lambda i: (-score[i], i))
    ends = (k for pair in zip(range(n), range(n - 1, -1, -1), strict=True) for k in pair)
    rest = [i for i in dict.fromkeys(ends) if score[i] <= 0]
    return matched + rest, len(matched), weight


def _skipped(k: int) -> str:
    return f"… {k} fila sin mostrar …" if k == 1 else f"… {k} filas sin mostrar …"


def _window(line: str, weight: dict[str, float], room: int) -> str:
    """El pedazo de una fila que no entra entera, alrededor de su palabra más rara de la respuesta."""
    folded = _fold(line)
    found = sorted((-w, folded.find(t)) for t, w in weight.items() if t in folded)
    center = found[0][1] if found else 0
    width = max(0, room - 2)
    start = max(0, min(center - width // 2, len(line) - width))
    end = start + width
    return ("…" if start > 0 else "") + line[start:end] + ("…" if end < len(line) else "")


def _rows_within(lines: list[str], terms: frozenset[str], room: int) -> str:
    """Las filas que entran en ``room``, en el orden de ``_row_order``.

    Van enteras; una que coincide con la respuesta y no entra, como un pedazo
    alrededor de lo afirmado. Se muestran en su orden original, con una marca
    donde se saltean filas. Cada marca se cobra una vez: elegir una fila
    agrega una sólo si parte un salto en dos, y saca una si lo cierra.
    """
    order, n_matched, weight = _row_order(lines, terms)
    n = len(lines)
    # Sin filas elegidas, todo es un salto: una marca.
    left = room - _GAP_RESERVE
    chosen: dict[int, str] = {}
    for rank, i in enumerate(order):
        gap_before = i > 0 and i - 1 not in chosen
        gap_after = i < n - 1 and i + 1 not in chosen
        marks = (int(gap_before) + int(gap_after) - 1) * _GAP_RESERVE
        cost = len(lines[i]) + 1 + marks
        if cost <= left:
            chosen[i] = lines[i]
            left -= cost
        elif rank < n_matched and left - 1 - marks >= _MIN_WINDOW_CHARS:
            chosen[i] = _window(lines[i], weight, left - 1 - marks)
            left -= len(chosen[i]) + 1 + marks
    if not chosen:
        return _window(lines[order[0]], weight, room)
    parts: list[str] = []
    prev = -1
    for i in sorted(chosen):
        if i > prev + 1:
            parts.append(_skipped(i - prev - 1))
        parts.append(chosen[i])
        prev = i
    if prev < len(lines) - 1:
        parts.append(_skipped(len(lines) - 1 - prev))
    return "\n".join(parts)


def _item_summary(r: Any, budget: int, terms: frozenset[str] = frozenset()) -> str:
    """Una fuente para el juez, dentro de ``budget`` caracteres.

    De una tabla larga, la cola entra siempre (es lo que se cita de una
    serie) y el principio, lo que quepa. De una corta que no entra, el final;
    si es de texto (``_TEXT_ROW_CHARS``), las filas que respaldan la
    respuesta (``terms``) y después el principio y el final (ver
    ``_row_order``).
    """
    title = getattr(r, "dataset_title", "") or ""
    portal = getattr(r, "portal_name", "") or ""
    meta = getattr(r, "metadata", {}) or {}
    records = getattr(r, "records", []) or []
    total = max(len(records), int(meta.get("total_records") or 0))
    header = f"## {title} — {portal} ({total} filas"
    header += f", unidades: {meta['units']})" if meta.get("units") else ")"
    if meta.get("generated_sql"):
        header += f"\nSQL: {str(meta['generated_sql'])[:400]}"
    if len(records) <= _EVIDENCE_HEAD_ROWS + _EVIDENCE_TAIL_ROWS:
        lines = [str(rec) for rec in records]
        body = "\n".join(lines)
        room = max(0, budget - len(header) - 1)
        if len(body) <= room:
            return header + "\n" + body
        if statistics.median(len(line) for line in lines) < _TEXT_ROW_CHARS:
            # Una serie o una tabla: el final, que es lo que se cita.
            return header + "\n…" + body[-room:]
        # De texto se mostraba también el final, cortado a ciegas.
        # sesiones_002 y sesiones_003 (07-oct): 12 fragmentos de ~3.400
        # caracteres, el modelo leyó los primeros (lo que entra en
        # `MAX_CONTENT_CHARS`), el juez vio los últimos cinco y medio, y marcó
        # como inventados "Temu, Shein o Alibaba" y la oferta de tropas para
        # Gaza, que estaban en los fragmentos que el juez no veía.
        return header + "\n" + _rows_within(lines, terms, room)
    tail = "\n".join(str(rec) for rec in records[-_EVIDENCE_TAIL_ROWS:])
    room = budget - len(header) - len(tail) - 40
    head_rows: list[str] = []
    for rec in records[:_EVIDENCE_HEAD_ROWS]:
        line = str(rec)
        if len(line) + 1 > room:
            break
        head_rows.append(line)
        room -= len(line) + 1
    skipped = len(records) - len(head_rows) - _EVIDENCE_TAIL_ROWS
    parts = [header, *head_rows, f"… {skipped} filas sin mostrar …", tail]
    text = "\n".join(parts)
    return (
        text if len(text) <= budget else header + "\n…" + tail[-max(0, budget - len(header) - 2) :]
    )


def summarize_evidence(results: list[Any], answer: str = "") -> str:
    """Lo que el motor tuvo a la vista, para el juez de alucinación.

    Cada fuente tiene su parte del tope: el 05-oct, dos series de 1.000 filas
    llenaban los 20.000 caracteres y la serie de donde salía el 49.700 de
    reservas (92.1) quedaba afuera, así que el juez marcaba la cifra como
    inventada (1,0). Los resultados repetidos (la misma llamada dos veces)
    se muestran una vez.

    Con ``answer``, de una fuente de texto que no entra entera se eligen
    primero las filas que contienen las palabras de la respuesta: el juez
    tiene que ver de dónde salió lo que se afirma, no el final de la
    evidencia. Las series y las tablas se muestran como antes. El tope no
    cambia, y lo que no está en ninguna fila sigue sin estar.
    """
    unique: list[Any] = []
    seen: set[tuple[str, str, int, str]] = set()
    for r in results or []:
        records = getattr(r, "records", []) or []
        key = (
            getattr(r, "dataset_title", "") or "",
            getattr(r, "portal_url", "") or "",
            len(records),
            str(records[-1]) if records else "",
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)
    if not unique:
        return ""
    budget = _EVIDENCE_MAX_CHARS // len(unique) - 2
    terms = _answer_terms(answer)
    return "\n\n".join(_item_summary(r, budget, terms) for r in unique)[:_EVIDENCE_MAX_CHARS]


# Las cuentas de filas que `calcular` le da al modelo junto a los grupos y
# guarda en los metadatos del resultado (`answers/tools/catalogo.py`): sobre
# cuántas filas se calculó, de todos los grupos o sólo de los mostrados, y
# cuántas tenían número. No están en `records`.
_CONTROL_COUNT_KEYS = ("filas_usadas", "filas_usadas_en_grupos_mostrados", "filas_con_valor")


def _control_counts(meta: dict[str, Any]) -> list[float]:
    return [
        float(v)
        for k in _CONTROL_COUNT_KEYS
        if isinstance(v := meta.get(k), int) and not isinstance(v, bool)
    ]


def evidence_items(results: list[Any]) -> list[dict[str, Any]]:
    """La evidencia como la cita el motor: título y URL de la fuente, y sus números.

    Las fuentes del agente salen de los mismos ``DataResult`` (título y
    ``portal_url``), así que la batería puede cruzar cada fuente citada con
    lo que esa fuente devolvió. ``derivadas`` son las variaciones que el
    modelo puede calcular de la cola de la serie (interanual sobre el
    índice, por ejemplo): también cuentan como aporte de la fuente.

    ``conteos_de_filas`` son las cuentas de filas de un cálculo
    (``_CONTROL_COUNT_KEYS``). Sin ellas, "la tabla tiene 52.367 registros"
    salía citada sin cifra aunque fuera el ``filas_usadas`` del conteo que
    se citaba (ckan_002, prueba de staging del 07-oct). Van aparte de
    ``numbers`` para no cambiar qué fuente "sólo trae conteos chicos".
    """
    from tests.evaluation.quality_checks import derived_variations, evidence_numbers

    out: list[dict[str, Any]] = []
    for r in results or []:
        meta = getattr(r, "metadata", {}) or {}
        records = getattr(r, "records", []) or []
        out.append(
            {
                "title": getattr(r, "dataset_title", "") or "",
                "url": getattr(r, "portal_url", "") or "",
                "portal": getattr(r, "portal_name", "") or "",
                "rows": len(records),
                "total_records": meta.get("total_records"),
                "last_date": next(
                    (
                        str(rec.get("fecha"))
                        for rec in reversed(records)
                        if isinstance(rec, dict) and rec.get("fecha")
                    ),
                    None,
                ),
                "numbers": evidence_numbers(records),
                "derivadas": derived_variations(records),
                "conteos_de_filas": _control_counts(meta),
            }
        )
    return out


class LegacyGraphEngine:
    """El pipeline de LangGraph que hoy contesta el chat, /ask y el MCP."""

    name = "legacy"

    def __init__(self) -> None:
        self._stack = contextlib.AsyncExitStack()
        self._graph: Any = None
        self._container: Any = None

    async def start(self) -> None:
        from dishka import Scope

        from app.application.pipeline.graph import build_pipeline_graph
        from app.application.pipeline.nodes import PipelineDeps, set_deps
        from app.setup.config.settings import AppSettings
        from app.setup.ioc.provider_registry import create_async_ioc_container, get_providers

        install_usage_capture()
        settings = AppSettings()
        self._container = create_async_ioc_container(providers=get_providers(), settings=settings)
        request_scope = await self._stack.enter_async_context(self._container(scope=Scope.REQUEST))
        deps = await request_scope.get(PipelineDeps)
        # Antes de lanzar las tareas: cada una hereda la ContextVar de acá.
        set_deps(deps)
        # Sin checkpointer: cada caso es una conversación nueva de un turno.
        self._graph = build_pipeline_graph(deps)

    async def run(
        self, question: str, *, case_id: str, mode: str, bypass_cache: bool
    ) -> EngineOutput:
        state: dict[str, Any] = {
            "question": question,
            "user_id": f"eval:{case_id}",
            # Sin conversation_id a propósito: la batería nunca escribe en el
            # historial de nadie.
            "conversation_id": "",
            "mode": mode,
            "replan_count": 0,
            # El modo profundo gastaría un turno preguntando en vez de
            # contestar. La batería mide el turno de búsqueda.
            "scoping_done": True,
            "bypass_cache": bypass_cache,
        }
        started = time.monotonic()
        error: str | None = None
        out: dict[str, Any] = {}
        with metering() as meter:
            try:
                out = await self._graph.ainvoke(state)
            except Exception as exc:  # noqa: BLE001 — un caso que explota ES el hallazgo
                error = f"{type(exc).__name__}: {exc}"[:300]
        latency_ms = int((time.monotonic() - started) * 1000)

        plan = out.get("plan")
        steps = getattr(plan, "steps", None) or []
        answer = str(out.get("clean_answer") or "")
        return EngineOutput(
            answer=answer,
            sources=[s for s in out.get("sources") or [] if isinstance(s, dict)],
            latency_ms=latency_ms,
            usage=meter.to_dict(),
            tokens_reported=int(out.get("tokens_used") or 0),
            evidence=summarize_evidence(out.get("data_results") or [], answer),
            error=error,
            diagnostics={
                "classification": out.get("classification"),
                "plan_actions": [getattr(s, "action", "") for s in steps],
            },
            warnings=[str(w) for w in out.get("warnings") or []],
            evidence_items=evidence_items(out.get("data_results") or []),
        )

    async def aclose(self) -> None:
        await self._stack.aclose()
        if self._container is not None:
            await self._container.close()


class _MeteredAgentLLM:
    """Envuelve el modelo del agente para contar cada vuelta en el medidor.

    El agente no pasa por el adaptador de Converse que parchea
    ``install_usage_capture``: habla con ``AsyncAnthropicBedrock``. Sin esto,
    su costo saldría de lo que el motor dice de sí mismo, que es justo lo que
    la batería no quiere creerle a nadie.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    @property
    def model(self) -> str:
        return str(self._inner.model)

    async def stream_turn(self, **kwargs: Any) -> Any:
        from app.domain.ports.llm.agent_llm import AgentTurn

        async for item in self._inner.stream_turn(**kwargs):
            if isinstance(item, AgentTurn):
                meter = _meter.get()
                if meter is not None:
                    u = item.usage
                    meter.add(
                        self.model,
                        u.input_tokens,
                        u.output_tokens,
                        u.cache_read_tokens,
                        u.cache_write_tokens,
                    )
            yield item


class AgentEngine:
    """El agente con herramientas (``ANSWERS_ENGINE=agent``), con un modelo fijo.

    Corre a través de ``EngineRunner``, igual que en producción: el runner
    descarta saludos e inyecciones, verifica las cifras y registra el turno.

    Cada caso abre su propio scope de request, como cada pedido real. El
    agente pide herramientas en paralelo y la búsqueda vectorial usa la sesión
    de la request: compartir una sola entre los casos concurrentes de la
    batería rompería la sesión, cosa que en producción no pasa.
    """

    def __init__(self, model: str, name: str) -> None:
        self.name = name
        self._model = model
        self._container: Any = None
        self._llm: Any = None

    async def start(self) -> None:
        from app.infrastructure.adapters.llm.anthropic_bedrock_agent_adapter import (
            AnthropicBedrockAgentAdapter,
        )
        from app.setup.config.settings import AppSettings
        from app.setup.ioc.provider_registry import create_async_ioc_container, get_providers

        install_usage_capture()
        settings = AppSettings()
        self._container = create_async_ioc_container(providers=get_providers(), settings=settings)
        self._llm = _MeteredAgentLLM(
            AnthropicBedrockAgentAdapter(region=settings.bedrock.REGION, model=self._model)
        )

    async def run(
        self, question: str, *, case_id: str, mode: str, bypass_cache: bool
    ) -> EngineOutput:
        from dishka import Scope

        from app.application.answers.agent_engine import AgentEngine as _Agent
        from app.application.answers.engine import (
            CompleteEvent,
            EngineRequest,
            StatusEvent,
        )
        from app.application.answers.runner import EngineRunner
        from app.application.pipeline.nodes import PipelineDeps

        started = time.monotonic()
        error: str | None = None
        result: Any = None
        tools: list[str] = []
        with metering() as meter:
            try:
                async with self._container(scope=Scope.REQUEST) as request_scope:
                    deps = await request_scope.get(PipelineDeps)
                    runner = EngineRunner(_Agent(self._llm, deps), deps)
                    req = EngineRequest(
                        question=question,
                        user_id=f"eval:{case_id}",
                        mode=mode,
                        bypass_cache=bypass_cache,
                    )
                    async with contextlib.aclosing(runner.stream(req)) as events:
                        async for event in events:
                            if isinstance(event, StatusEvent) and event.connector:
                                tools.append(event.connector)
                            elif isinstance(event, CompleteEvent):
                                result = event.result
            except Exception as exc:  # noqa: BLE001 — un caso que explota ES el hallazgo
                error = f"{type(exc).__name__}: {exc}"[:300]
        latency_ms = int((time.monotonic() - started) * 1000)
        if result is None and error is None:
            error = "EngineIncomplete: el motor terminó sin respuesta"
        answer = str(getattr(result, "answer", "") or "")
        return EngineOutput(
            answer=answer,
            sources=[s for s in getattr(result, "sources", None) or [] if isinstance(s, dict)],
            latency_ms=latency_ms,
            usage=meter.to_dict(),
            tokens_reported=int(getattr(result, "tokens_used", 0) or 0),
            evidence=summarize_evidence(getattr(result, "evidence", None) or [], answer),
            error=error,
            diagnostics={
                "classification": None,
                "plan_actions": tools,
                "intent": getattr(result, "intent", None),
                # Ni el intent del clasificador ni los pasos del pipeline viejo
                # existen en el agente: no se comparan.
                "routing_comparable": False,
            },
            warnings=[str(w) for w in getattr(result, "warnings", None) or []],
            evidence_items=evidence_items(getattr(result, "evidence", None) or []),
        )

    async def aclose(self) -> None:
        if self._container is not None:
            await self._container.close()


SONNET_4_6 = "us.anthropic.claude-sonnet-4-6"
HAIKU_4_5 = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

ENGINES: dict[str, Any] = {
    "legacy": LegacyGraphEngine,
    "agent-sonnet": lambda: AgentEngine(SONNET_4_6, "agent-sonnet"),
    "agent-haiku": lambda: AgentEngine(HAIKU_4_5, "agent-haiku"),
}
