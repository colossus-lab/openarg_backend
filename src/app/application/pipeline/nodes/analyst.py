"""LangGraph node: LLM analysis (the main 'analyst' call)."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.chart_builder import (
    build_deterministic_charts,
    extract_llm_charts,
    extract_meta,
)
from app.application.pipeline.context_builder import (
    build_capabilities_block,
    build_data_context,
)
from app.application.pipeline.state import OpenArgState
from app.domain.ports.llm.llm_provider import LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)

_MAX_MAP_FEATURES = 500

# FR-025a: soft character budget for the assembled analyst prompt.
# Claude Haiku 4.5 has a ~200k token context; at a conservative 4 chars
# per token that's ~800k chars available. We reserve roughly one quarter
# of that for the user-role prompt, leaving the rest for the system
# prompt, the LLM response, and future memory-context growth. If we
# swap models, bump this constant — that's the one place it lives.
ANALYST_PROMPT_MAX_CHARS = 50_000


def _build_map_data(results: list) -> dict[str, Any] | None:
    """Build a GeoJSON FeatureCollection from data results that contain geometry.

    Returns None if no results carry ``_geometry_geojson`` columns.
    Deterministic — no LLM involved.
    """
    features: list[dict[str, Any]] = []
    for r in results:
        for rec in r.records:
            geojson_str = rec.get("_geometry_geojson")
            if not geojson_str:
                continue
            try:
                geom = json.loads(geojson_str) if isinstance(geojson_str, str) else geojson_str
            except (json.JSONDecodeError, TypeError):
                continue
            props = {
                k: v for k, v in rec.items() if k not in ("_geometry_geojson", "_source_dataset_id")
            }
            features.append({"type": "Feature", "geometry": geom, "properties": props})
            if len(features) >= _MAX_MAP_FEATURES:
                break
        if len(features) >= _MAX_MAP_FEATURES:
            break
    if not features:
        return None
    return {"type": "FeatureCollection", "features": features}


# ── Pre-compiled regex patterns (avoid recompilation per call) ────

_RE_CHART_TAG = re.compile(r"<!--CHART:.*?-->", re.DOTALL)
_RE_CHART_TRUNC = re.compile(r"<!--CHART:.*", re.DOTALL)
_RE_META_TAG = re.compile(r"<!--META:.*?-->", re.DOTALL)
_RE_META_TRUNC = re.compile(r"<!--META:.*", re.DOTALL)
_RE_ANY_TAG = re.compile(r"<!--.*?-->", re.DOTALL)
_RE_ANY_TAG_TRUNC = re.compile(r"<!--.*", re.DOTALL)


def _strip_tags(text: str) -> str:
    """Strip <!--CHART:...--> tags (complete and truncated) from text."""
    text = _RE_CHART_TAG.sub("", text)
    return _RE_CHART_TRUNC.sub("", text).strip()


def _strip_meta(text: str) -> str:
    """Strip <!--META:...--> tags (complete and truncated) from text."""
    text = _RE_META_TAG.sub("", text)
    return _RE_META_TRUNC.sub("", text).strip()


# ── Internal-identifier scrub (FR-025e, FIX-011 fix) ─────────────

# Matches bare internal identifiers the analyst sometimes cites verbatim.
# The pattern is intentionally narrow: only tokens that start with one of
# the internal prefixes ``cache_`` / ``dataset_chunks`` / ``pgvector`` /
# ``query_cache`` / ``cached_datasets`` get scrubbed. The word boundary
# prevents partial matches inside legitimate prose.
_RE_INTERNAL_IDENTIFIER = re.compile(
    r"\b(?:cache_[\w]+|dataset_chunks|pgvector|query_cache|cached_datasets)\b",
    re.IGNORECASE,
)
# Matches a parenthetical or punctuation-wrapped citation like
# "(Fuente: cache_leyes_sancionadas)" so we remove the whole aside,
# not just the identifier leaving dangling "(Fuente: )" behind.
_RE_INTERNAL_CITATION = re.compile(
    r"[\s,;]*[\(\[][^)\]]*\b(?:cache_[\w]+|dataset_chunks|pgvector|query_cache|cached_datasets)\b[^)\]]*[\)\]]",
    re.IGNORECASE,
)


def _scrub_internal_identifiers(text: str) -> str:
    """Remove internal sandbox/infrastructure identifiers from analyst output.

    Closes FIX-011 — the analyst LLM sometimes cites ``cache_*`` table names
    verbatim (e.g. "(Fuente: cache_leyes_sancionadas)"), leaking internal
    infrastructure naming to the user. We first strip entire
    parenthetical/bracket citations that contain such a token, then strip
    any remaining bare tokens, and finally collapse the leftover
    whitespace so the prose reads cleanly.
    """
    if not text:
        return text
    scrubbed = _RE_INTERNAL_CITATION.sub("", text)
    scrubbed = _RE_INTERNAL_IDENTIFIER.sub("", scrubbed)
    # Collapse the whitespace runs left behind by the removals, but
    # preserve paragraph breaks.
    scrubbed = re.sub(r"[ \t]+", " ", scrubbed)
    scrubbed = re.sub(r" *\n *", "\n", scrubbed)
    scrubbed = re.sub(r"\n{3,}", "\n\n", scrubbed)
    return scrubbed.strip()


# ── Apologetic preface stripper (FR-025f, FIX-012 fix) ───────────

_APOLOGETIC_OPENERS = (
    "no tengo",
    "no cuento con",
    "no encontré",
    "no encontre",
    "no pude acceder",
    "no pude obtener",
    "no dispongo",
    "no hay datos",
    "no cuentan con",
    "aunque no tengo",
    "aunque no pude",
    "si bien no tengo",
    "si bien no pude",
    "lamentablemente no",
)

# Subordinate-clause openers can use either a comma or a full stop as
# the break ("Aunque no pude X, el dato..." or "Aunque no tengo X. El
# dato..."). The rest use a full sentence break only.
_COMMA_BREAK_OPENERS = ("aunque ", "si bien ", "lamentablemente ")

_RE_SENTENCE_BREAK = re.compile(r"([.!?]+[\*_\"'\)\]]*)(\s+|\s*$)")
_RE_CLAUSE_OR_SENTENCE_BREAK = re.compile(r"([,.!?]+[\*_\"'\)\]]*)(\s+|\s*$)")
_RE_NUMERIC = re.compile(r"\d")

# Mid-sentence ", pero/mas/aunque/sin embargo no tengo X" clauses.
# The LLM often writes "Tenemos X, pero no tengo Y específico. Sin
# embargo, tenemos Z" — we strip the negative sub-clause so the
# sentence becomes "Tenemos X. Sin embargo, tenemos Z".
_RE_MID_NEGATION_CLAUSE = re.compile(
    r",\s*(?:pero|mas|aunque|sin\s+embargo)\s+"
    r"(?:no\s+tengo|no\s+cuento\s+con|no\s+encontr[eé]|no\s+pude\s+(?:acceder|obtener)|no\s+dispongo)"
    r"[^.!?]*",
    re.IGNORECASE,
)

# Negation phrases that can appear anywhere inside the first sentence.
# Used to detect and drop the whole first sentence when the rest of the
# answer has real numeric data (e.g. "El problema es claro: no tengo X.
# Sin embargo, la inflación fue 2.88%" → drops the first sentence).
_NEGATION_PHRASES_LOOSE = (
    "no tengo",
    "no cuento con",
    "no encontré",
    "no encontre",
    "no pude acceder",
    "no pude obtener",
    "no dispongo",
    "no hay datos",
    "no cuentan con",
)

# Markdown heading prefix: "# Title\n\n" or "## Title\n\n" etc.
_RE_MD_HEADING_PREFIX = re.compile(r"^(?:\s*#{1,6}\s*[^\n]*\n+)*")


def _drop_apologetic_preface(text: str) -> str:
    """Remove apologetic prefaces while preserving real data.

    Closes FIX-012. Handles three distinct LLM patterns where the prompt
    rule against "No tengo X pero..." constructions is ignored:

    1. **Leading apologetic opener** — the whole first sentence starts
       with "No tengo/No encontré/Aunque no pude/..." and the data
       appears in the next sentence.
    2. **Mid-sentence negative clause** — the first sentence says
       "Tenemos X, pero no tengo Y específico" and we strip the
       ", pero no tengo Y..." sub-clause so the sentence reads
       "Tenemos X."
    3. **Loose negation in the first sentence** — the first sentence
       leads with a markdown heading or bold and then contains a
       negation phrase anywhere inside it ("El problema es claro: no
       tengo X"). We drop the whole first sentence, keeping the rest.

    All three strips are gated on the answer containing numeric data
    somewhere — honest "not found" responses (RENABAP, Adorni ejecutivo)
    are preserved because there is nothing below the disclaimer.
    """
    if not text:
        return text
    # Never touch a response without any numeric data — that is an
    # honest "no data" answer and the disclaimer is the entire payload.
    if not _RE_NUMERIC.search(text):
        return text

    # Pattern 2: strip ", pero no tengo X..." mid-sentence sub-clauses.
    # Applied across the whole text because these clauses can appear in
    # any sentence, not just the first one.
    text = _RE_MID_NEGATION_CLAUSE.sub("", text)

    stripped = text.lstrip()
    if not stripped:
        return text

    # Pattern 1: leading sentence literally starts with an apologetic
    # opener ("No tengo X pero ...", "Aunque no pude ...", etc).
    lower = stripped.lower()
    matched_opener = next((op for op in _APOLOGETIC_OPENERS if lower.startswith(op)), None)
    if matched_opener is not None:
        use_comma_break = any(lower.startswith(op) for op in _COMMA_BREAK_OPENERS)
        pattern = _RE_CLAUSE_OR_SENTENCE_BREAK if use_comma_break else _RE_SENTENCE_BREAK
        match = pattern.search(stripped)
        if match:
            remainder = stripped[match.end():].lstrip()
            if remainder and _RE_NUMERIC.search(remainder):
                stripped = remainder

    # Pattern 3: first sentence (possibly preceded by a markdown heading)
    # contains a loose negation phrase somewhere inside it. Drop the
    # whole first sentence so the user does not see "El problema es
    # claro: no tengo X" as the opener.
    heading_match = _RE_MD_HEADING_PREFIX.match(stripped)
    heading = stripped[:heading_match.end()] if heading_match else ""
    body = stripped[len(heading):]
    first_sentence_match = _RE_SENTENCE_BREAK.search(body)
    if first_sentence_match:
        first_sentence = body[:first_sentence_match.end()]
        first_lower = first_sentence.lower()
        if any(phrase in first_lower for phrase in _NEGATION_PHRASES_LOOSE):
            remainder = body[first_sentence_match.end():].lstrip()
            if remainder and _RE_NUMERIC.search(remainder):
                stripped = heading + remainder

    # Capitalize the first letter of the result if it is lowercase and
    # not inside a markdown heading line.
    if stripped and stripped[0].islower():
        stripped = stripped[0].upper() + stripped[1:]
    return stripped


# ── Prompt budget enforcement (FR-025a/b/c/d, DEBT-011 fix) ────────


def _truncate_segment(
    segment: str,
    over: int,
    *,
    label: str,
    keep_from: str = "tail",
    keep_floor: int = 0,
) -> tuple[str, int]:
    """Drop enough characters from ``segment`` to net-reduce its length by ``over``.

    ``keep_from="tail"`` keeps the last chars (for memory_ctx — the most
    recent conversation turns). ``keep_from="head"`` keeps the first
    chars (for data_context and errors_block — the highest-priority
    records/warnings come first). ``keep_floor`` is a minimum number of
    chars we refuse to drop below, so even a very-over-budget prompt
    still carries SOME signal from each segment when possible.

    Returns ``(new_segment, net_chars_removed)``. ``net_chars_removed``
    is the delta between the old and new segment length — the caller
    uses this to decrement its running "chars still to drop" budget.
    The returned segment includes an inline sentinel naming the label
    and the number of raw chars dropped from the original, per FR-025c.

    Subtlety: the sentinel itself adds chars back. If we naively dropped
    exactly ``over`` chars, the net reduction would be ``over -
    len(sentinel)``, leaving the caller under-reduced. To avoid that,
    we drop a larger raw count (``over + sentinel headroom``) so the net
    reduction is always at least ``over``.
    """
    if over <= 0 or not segment:
        return segment, 0
    available_to_drop = max(0, len(segment) - keep_floor)
    if available_to_drop == 0:
        return segment, 0

    # Upper bound on the sentinel's own length — the format is
    # "[... {label} truncated: {drop} chars dropped]\n" where label is
    # small and drop count is at most ~7 digits. 60 chars covers it
    # with headroom, and slightly over-dropping is strictly better than
    # under-dropping when the caller has a hard budget ceiling.
    sentinel_headroom = 60
    drop_target = over + sentinel_headroom
    drop = min(drop_target, available_to_drop)

    if drop >= len(segment):
        # Replace the whole segment with the sentinel.
        sentinel = f"[... {label} truncated: {len(segment)} chars dropped]"
        return sentinel, max(len(segment) - len(sentinel), 0)

    sentinel = f"[... {label} truncated: {drop} chars dropped]"
    keep = len(segment) - drop
    if keep_from == "tail":
        new_segment = f"{sentinel}\n{segment[-keep:]}"
    else:
        new_segment = f"{segment[:keep]}\n{sentinel}"

    net_removed = len(segment) - len(new_segment)
    if net_removed <= 0:
        # keep_floor + sentinel overhead means we cannot actually shrink
        # the segment without going below the floor. Return the
        # original untouched and report zero drops — the caller will
        # spill into the next priority segment.
        return segment, 0
    return new_segment, net_removed


def _enforce_prompt_budget(
    static_overhead: int,
    data_context: str,
    errors_block: str,
    memory_ctx: str,
) -> tuple[str, str, str]:
    """Trim the three truncatable segments to fit within the budget (FR-025b).

    Priority order (drop first → drop last): memory_ctx, data_context,
    errors_block. Static text (question, intent, instructions, skill
    block) is NEVER trimmed — if the static overhead alone is already
    over budget, we raise ``ValueError`` because that is a programming
    bug, not a runtime condition.

    When truncation fires, emits a single structured WARNING log with
    the final size, budget, and per-segment drop counts (FR-025d).
    """
    if static_overhead >= ANALYST_PROMPT_MAX_CHARS:
        raise ValueError(
            f"Analyst static prompt overhead is {static_overhead} chars, "
            f"over budget {ANALYST_PROMPT_MAX_CHARS}. This is a programming "
            "bug — adjust ANALYST_PROMPT_MAX_CHARS or trim the static text."
        )

    available = ANALYST_PROMPT_MAX_CHARS - static_overhead
    current = len(data_context) + len(errors_block) + len(memory_ctx)

    if current <= available:
        return data_context, errors_block, memory_ctx

    original_sizes = (len(data_context), len(errors_block), len(memory_ctx))
    over = current - available
    dropped_memory = dropped_data = dropped_errors = 0

    # Priority 1: memory_ctx — oldest, most dispensable.
    if over > 0 and memory_ctx:
        memory_ctx, dropped_memory = _truncate_segment(
            memory_ctx, over, label="memory_ctx", keep_from="tail"
        )
        over -= dropped_memory

    # Priority 2: data_context — keep the head (highest-priority sources).
    if over > 0 and data_context:
        data_context, dropped_data = _truncate_segment(
            data_context, over, label="data_context", keep_from="head", keep_floor=500
        )
        over -= dropped_data

    # Priority 3: errors_block — keep the head (headline warnings first).
    if over > 0 and errors_block:
        errors_block, dropped_errors = _truncate_segment(
            errors_block, over, label="errors_block", keep_from="head"
        )
        over -= dropped_errors

    final_size = static_overhead + len(data_context) + len(errors_block) + len(memory_ctx)

    logger.warning(
        "analyst prompt truncated: budget=%d final_size=%d "
        "memory_dropped=%d/%d data_dropped=%d/%d errors_dropped=%d/%d",
        ANALYST_PROMPT_MAX_CHARS,
        final_size,
        dropped_memory, original_sizes[2],
        dropped_data, original_sizes[0],
        dropped_errors, original_sizes[1],
    )

    if over > 0:
        # Every truncatable was exhausted and we are STILL over budget —
        # means the static text + minimum segment floors exceed the
        # budget. Defensive: raise instead of sending a prompt we know
        # the model will reject.
        raise ValueError(
            f"Analyst prompt still {final_size} chars after truncation, "
            f"over budget {ANALYST_PROMPT_MAX_CHARS}. Raise the budget or "
            "reduce the minimum keep_floor."
        )

    return data_context, errors_block, memory_ctx


# ── Analysis prompt builder (same logic as _build_analysis_prompt) ─


def _build_analysis_prompt(
    question: str,
    plan: Any,
    results: list,
    memory_ctx_analyst: str,
    all_warnings: list[str],
    skill_context: dict[str, str] | None = None,
) -> str:
    """Build the analyst LLM prompt from data context and warnings."""
    data_context = build_data_context(results)
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    errors_block = ""
    if all_warnings:
        errors_block = "\nERRORES EN LA RECOLECCIÓN:\n" + "\n".join(f"- {w}" for w in all_warnings)

    no_data_fallback = not results or not any(
        r.records or r.source.startswith("pgvector:") or r.source.startswith("ckan:")
        for r in results
    )

    if no_data_fallback:
        caps = build_capabilities_block()
        # Don't pass memory context in no-data mode — it causes the LLM to
        # hallucinate "we already discussed this" when no data was ever shown.
        return load_prompt(
            "analyst_no_data",
            question=question,
            today=today,
            memory_ctx_analyst="",
            caps=caps,
        )

    skill_block = ""
    if skill_context and skill_context.get("analyst"):
        skill_block = "\n\n--- FORMATO REQUERIDO POR SKILL ---\n" + skill_context["analyst"]

    def _assemble(dc: str, eb: str, mc: str) -> str:
        return (
            f'PREGUNTA DEL USUARIO: "{question}"\n'
            f"FECHA ACTUAL: {today}\n"
            f"INTENCIÓN: {plan.intent}\n\n"
            f"DATOS RECOLECTADOS:\n{dc}\n"
            f"{eb}"
            f"{mc}\n\n"
            "Si hay historial de conversación, tené en cuenta lo que ya se discutió "
            "para no repetir contexto innecesariamente y para mantener coherencia "
            "con respuestas anteriores.\n\n"
            "Respondé de forma breve y conversacional. Destacá el dato más importante, "
            "dá contexto mínimo, y sugerí preguntas de seguimiento para profundizar. "
            "Si los datos permiten un gráfico claro, incluilo con <!--CHART:{}-->."
            f"{skill_block}"
        )

    assembled = _assemble(data_context, errors_block, memory_ctx_analyst)

    # FR-025a/b/c/d: enforce the prompt budget. If the first-pass
    # assembly fits, return it unchanged. Otherwise trim the three
    # truncatable segments in priority order and re-assemble — the
    # static overhead is computed as the delta between the assembled
    # prompt and the sum of the truncatable segments, which keeps the
    # helper format-agnostic.
    if len(assembled) > ANALYST_PROMPT_MAX_CHARS:
        static_overhead = len(assembled) - (
            len(data_context) + len(errors_block) + len(memory_ctx_analyst)
        )
        data_context, errors_block, memory_ctx_analyst = _enforce_prompt_budget(
            static_overhead, data_context, errors_block, memory_ctx_analyst
        )
        assembled = _assemble(data_context, errors_block, memory_ctx_analyst)

    return assembled


async def analyst_node(state: OpenArgState) -> dict:
    """Call the LLM to analyse collected data and produce the user-facing answer.

    Builds the analysis prompt (identical to ``_build_analysis_prompt``
    in ``smart_query_service.py``), calls the LLM, extracts charts and
    META confidence/citations, and strips internal tags.
    """
    writer = get_stream_writer()
    deps = nodes_pkg.get_deps()
    replan_count = state.get("replan_count", 0)

    # If this is a second-pass analyst (after replan), tell the frontend
    # to replace the previous response instead of appending to it.
    if replan_count > 0:
        writer({"type": "clear_answer"})
        writer({"type": "status", "step": "generating", "detail": "Reintentando análisis..."})
    else:
        writer({"type": "status", "step": "generating", "detail": "Generando análisis..."})

    try:
        question = state["question"]
        plan = state.get("plan")
        results = state.get("data_results", [])
        memory_ctx_analyst = state.get("memory_ctx_analyst", "")
        all_warnings = list(state.get("step_warnings", []))

        # Build prompt
        skill_context = state.get("skill_context")
        analysis_prompt = _build_analysis_prompt(
            question,
            plan,
            results,
            memory_ctx_analyst,
            all_warnings,
            skill_context=skill_context,
        )

        # LLM streaming call — emit chunks as they arrive
        messages = [
            LLMMessage(role="system", content=load_prompt("analyst")),
            LLMMessage(role="user", content=analysis_prompt),
        ]

        full_text = ""
        stream_buf = ""
        usage_dict: dict[str, int] = {}
        try:
            async for chunk_text in deps.llm.chat_stream(
                messages=messages,
                temperature=0.2,
                max_tokens=8192,
                usage_out=usage_dict,
            ):
                full_text += chunk_text
                stream_buf += chunk_text

                # Buffer to avoid sending incomplete <!--CHART:--> or <!--META:--> tags
                if "<!--" in stream_buf and "-->" not in stream_buf:
                    continue  # Wait for tag to close

                # Strip any complete tags from the buffer before sending
                cleaned = _RE_ANY_TAG.sub("", stream_buf)
                if cleaned:
                    writer({"type": "chunk", "content": cleaned})
                stream_buf = ""

            # Flush remaining buffer
            if stream_buf:
                cleaned = _RE_ANY_TAG.sub("", stream_buf)
                cleaned = _RE_ANY_TAG_TRUNC.sub("", cleaned)
                if cleaned:
                    writer({"type": "chunk", "content": cleaned})
        except Exception:
            # Fallback to non-streaming if chat_stream fails
            logger.warning("chat_stream failed, falling back to chat()", exc_info=True)
            response = await deps.llm.chat(messages=messages, temperature=0.4, max_tokens=8192)
            full_text = response.content
            writer({"type": "chunk", "content": _strip_tags(full_text)})

        # Charts: prefer deterministic, fall back to LLM-generated
        det_charts = build_deterministic_charts(results)
        llm_charts = extract_llm_charts(full_text)
        charts = det_charts if det_charts else llm_charts

        # Maps: build GeoJSON FeatureCollection from geo results (deterministic)
        map_data = _build_map_data(results)
        # When map data is present, suppress charts (geo data doesn't chart well)
        if map_data:
            charts = None

        # Strip CHART tags from the answer text
        clean_answer = _strip_tags(full_text)

        # Parse META confidence/citations
        confidence, citations = extract_meta(clean_answer)
        clean_answer = _strip_meta(clean_answer)

        # FR-025e (FIX-011): scrub internal sandbox/infra identifiers the
        # analyst sometimes cites verbatim (``cache_*``, ``dataset_chunks``,
        # ``pgvector``, ``query_cache``, ``cached_datasets``).
        clean_answer = _scrub_internal_identifiers(clean_answer)
        # FR-025f (FIX-012): drop a leading "No tengo X pero..." sentence
        # when real data follows — the LLM sometimes opens with a
        # disclaimer even with useful data below. Honest "no data"
        # responses are preserved because the scrub requires numeric
        # content in the remainder.
        clean_answer = _drop_apologetic_preface(clean_answer)

        # Low confidence: just lower the confidence score, don't prepend text
        # (adding disclaimers triggers negative-phrase detection in E2E tests
        # and confuses users who see "Nota:" before every uncertain answer)

        # FIX-006: read token usage from usage_dict populated by the adapter.
        # If the adapter doesn't support it (or the non-streaming fallback was
        # used), tokens_used stays 0 — matches previous behavior for that path.
        tokens_used = usage_dict.get("total_tokens", 0)

        return {
            "analysis_prompt": analysis_prompt,
            "analysis_response": full_text,
            "clean_answer": clean_answer,
            "chart_data": charts if charts else None,
            "map_data": map_data,
            "confidence": confidence,
            "citations": citations,
            "tokens_used": tokens_used,
        }
    except Exception:
        logger.exception("analyst_node failed")
        return {
            "analysis_prompt": "",
            "analysis_response": "",
            "clean_answer": (
                "Ocurrió un error al analizar los datos. Probá reformulando tu consulta."
            ),
            "chart_data": None,
            "map_data": None,
            "confidence": 0.0,
            "citations": [],
            "tokens_used": 0,
            "error": "Analyst LLM call failed",
        }
