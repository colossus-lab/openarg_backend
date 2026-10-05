"""CLI runner for the OpenArg evaluation battery.

Corre el dataset contra un motor de respuestas y dice, caso por caso, si la
respuesta es buena. Hasta octubre de 2026 la batería medía si había respuesta
(palabras clave, título de la fuente) y aprobaba cosas como el PBI respondido
con el EMAE o las reservas en pesos. Ahora cada caso tiene un **veredicto**
que sale de ``quality_checks.assess``: valores esperados con tolerancia,
fuente por conector o portal, deflexión cuando corresponde y cuando no, y
errores internos filtrados al usuario.

**Repeticiones.** Con ``--n-runs 3`` cada caso corre tres veces. En cuatro
corridas del sistema actual, 26 de 35 respuestas cambiaron de una a otra: una
sola pasada no distingue una mejora de la suerte. El reporte da la tasa de
acierto por caso y cuántos casos son inestables.

**Todo queda en el reporte**: la respuesta completa de cada corrida, las
fuentes, las advertencias, la evidencia fuente por fuente, la latencia, los
tokens reales de todas las llamadas al modelo y su costo en USD. Con
``--judge`` además se agregan los jueces con Sonnet 4.6 (relevancia,
alucinación y, en los casos cebo, neutralidad), que cuestan aparte.

**Cifras calculadas, no fijas** (octubre de 2026). Los casos macro declaran
de dónde sale su cifra (``expected_values_from``, ver ``oracles.py``): IPC
mensual e interanual, acumulada, exportaciones, reservas y dólar del BCRA.
La batería las calcula al arrancar, desde la API de Series de Tiempo y la
v4 del BCRA, y las congela en el reporte (``expectativas``): ``--rescore``
usa lo congelado. Un oráculo que falla deja el caso "no evaluable", fuera
de la tasa y contado aparte, nunca aprobado.

**Los jueces votan.** Con ``--judge``, el de alucinación hace fallar el caso
por encima de ``--umbral-alucinacion`` y el de neutralidad (sólo casos con
``juez_neutralidad``) por debajo de ``--umbral-neutralidad``. El de
relevancia sigue siendo diagnóstico.

**Motor.** La batería no arma el pipeline: le pide la respuesta a un motor
(``engines.py``). Hoy hay uno, ``legacy``, que es el grafo actual. El agente
entra como otro motor y corre exactamente el mismo set.

**Intent.** El ``expected_intent`` del dataset mezcla cuatro valores que el
clasificador emite de verdad (``casual``, ``educational``, ``meta``,
``injection_blocked``) con etiquetas temáticas que nada produce; sólo se
puntúan las primeras, y el reporte dice sobre cuántos casos.

Usage::

    # línea base: el sistema actual, tres corridas, con jueces
    python -m tests.evaluation.run_eval --n-runs 3 --judge \\
        --output tests/evaluation/baselines/legacy_normal_x3.json

    # chequear regresiones contra un baseline
    python -m tests.evaluation.run_eval --compare tests/evaluation/baselines/normal.json

    # corregir una expectativa y recalcular sin volver a gastar
    python -m tests.evaluation.run_eval --rescore tests/evaluation/baselines/legacy_normal_x3.json

    # sólo validar el dataset, sin llamar a nada
    python -m tests.evaluation.run_eval --dry-run

Sale con código distinto de cero cuando ``--compare`` encuentra una regresión
dura o cuando se incumple una expectativa absoluta (fuente o frase prohibida).
El veredicto de calidad todavía no corta: la línea base del sistema actual
falla muchos casos a propósito, y eso es lo que hay que medir.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import statistics
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

from tests.evaluation.quality_checks import JudgeThresholds

logger = logging.getLogger(__name__)

DEFAULT_DATASET = Path(__file__).parent / "golden_dataset.json"

REQUIRED_FIELDS = {"id", "category", "question", "expected_intent"}

# The four dataset intents that map onto something `classify_request` really
# returns. Everything else in `expected_intent` is a topic label, not an
# intent, and is skipped when scoring intent accuracy.
INTENT_MAP = {
    "casual": "casual",
    "educational": "educational",
    "meta": "meta",
    "injection_blocked": "injection",
}

# A latency growth beyond this factor counts as a regression. Generous on
# purpose: these runs hit live portals and a live model, so run-to-run noise
# is large and a tight bound would cry wolf on every run.
LATENCY_REGRESSION_FACTOR = 2.0

# Y por debajo de esto no se mira la latencia: 43 ms → 163 ms es "más del
# doble" y no significa nada. Sin el piso, arreglar una compuerta para que un
# caso rebote en milisegundos se reporta como regresión.
LATENCY_FLOOR_MS = 1_000

# Below this, an answer is treated as "the pipeline produced nothing useful".
MIN_USEFUL_ANSWER_CHARS = 20

# El juez: Sonnet 4.6, más capaz que el Haiku que contesta hoy. Si el agente
# corre con Sonnet 4.6, se juzga a sí mismo; por eso la comparación final
# también lleva la revisión a ciegas de la etapa 4.
JUDGE_MODEL = "us.anthropic.claude-sonnet-4-6"


def load_golden_dataset(path: Path, categories: list[str] | None = None) -> list[dict]:
    """Load and optionally filter the golden dataset."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entries", [])
    if categories:
        entries = [e for e in entries if e["category"] in categories]
    return entries


# Campos de quality_checks y su tipo esperado: un typo en el dataset no puede
# dejar un chequeo sin correr en silencio.
_LIST_FIELDS = (
    "expected_answer_contains",
    "expected_sources",
    "expected_values",
    "forbidden_values",
    "expected_source_by",
    "expected_answer_patterns",
    "forbidden_answer_patterns",
    "forbidden_answer_contains",
    "forbidden_sources",
)


def validate_dataset(entries: list[dict]) -> list[str]:
    """Return a list of structural problems with the dataset."""
    from tests.evaluation.oracles import spec_problems
    from tests.evaluation.quality_checks import SOURCE_KINDS

    errors: list[str] = []
    for i, entry in enumerate(entries):
        eid = entry.get("id", i)
        for name in ("expected_values_from", "forbidden_values_from"):
            if name in entry and not isinstance(entry[name], list):
                errors.append(f"{eid}: {name} tiene que ser una lista")
                continue
            for spec in entry.get(name) or []:
                errors.extend(f"{eid}: {name}: {p}" for p in spec_problems(spec))
        if entry.get("expected_period_from") is not None:
            errors.extend(
                f"{eid}: expected_period_from: {p}"
                for p in spec_problems(entry["expected_period_from"])
            )
        for flag in ("juez_neutralidad", "rotular_no_oficial"):
            if flag in entry and not isinstance(entry[flag], bool):
                errors.append(f"{eid}: {flag} tiene que ser true o false")
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        missing = REQUIRED_FIELDS - set(entry)
        if missing:
            errors.append(f"entry {i}: missing {sorted(missing)}")
        eid = entry.get("id")
        if eid in seen:
            errors.append(f"entry {i}: duplicate id {eid!r}")
        seen.add(eid)
        for name in _LIST_FIELDS:
            if name in entry and not isinstance(entry[name], list):
                errors.append(f"{eid}: {name} tiene que ser una lista")
        for name in ("expected_values", "forbidden_values"):
            for spec in entry.get(name) or []:
                value = spec.get("value") if isinstance(spec, dict) else None
                values = value if isinstance(value, list) else [value]
                if not values or not all(
                    isinstance(v, int | float) and not isinstance(v, bool) for v in values
                ):
                    errors.append(f"{eid}: {name} sin 'value' numérico: {spec!r}")
        for group in entry.get("expected_source_by") or []:
            for kind in [group] if isinstance(group, str) else group:
                if kind not in SOURCE_KINDS:
                    errors.append(f"{eid}: tipo de fuente desconocido {kind!r}")
        if "must_deflect" in entry and not isinstance(entry["must_deflect"], bool | None):
            errors.append(f"{eid}: must_deflect tiene que ser true, false o null")
    return errors


def _forbidden_sources_hit(forbidden: list[str], sources: list[str]) -> list[str]:
    """Qué fuentes prohibidas aparecieron, por substring case-insensitive.

    Un caso puede declarar `forbidden_sources` cuando lo que importa no es a
    qué fuente llegó sino a cuál NO tiene que llegar. Ejemplo: una pregunta
    cuyo dato no tenemos no debe contestarse con una descarga en vivo del
    INDEC. La expectativa es negativa a propósito — si el pipeline encuentra
    el dataset real, la satisface igual, así que no se vuelve ruido.
    """
    hits: list[str] = []
    for pattern in forbidden:
        needle = pattern.lower()
        if any(needle in (name or "").lower() for name in sources):
            hits.append(pattern)
    return hits


def _forbidden_answer_hit(forbidden: list[str], answer: str) -> list[str]:
    """Qué frases prohibidas dijo la respuesta, por substring case-insensitive.

    Para lo que ninguna fuente expresa: que la respuesta no afirme algo falso.
    Caso Pinamar (30-sep-2026): el estudio del INDEC estaba en OpenArg y la
    respuesta dijo que "no está disponible".
    """
    text = (answer or "").lower()
    return [p for p in forbidden if p.lower() in text]


def check_absolute_expectations(report: dict) -> list[str]:
    """Expectativas que valen por sí solas, sin baseline contra qué comparar.

    Vive aparte de `compare_to_baseline` por una razón concreta: ese loop
    saltea las entradas que no están en el baseline (`if b is None:
    continue`), y los casos negativos son justamente entradas nuevas. Una
    condición escrita ahí adentro no se evaluaría nunca. Además, al no
    depender del baseline, esto no se desarma cuando el baseline envejece.
    """
    fallas: list[str] = []
    for r in report.get("results", []):
        for pattern in r.get("forbidden_sources_hit") or []:
            fallas.append(f"{r['id']}: fuente prohibida {pattern!r} en {r.get('sources')}")
        for phrase in r.get("forbidden_answer_hit") or []:
            fallas.append(f"{r['id']}: la respuesta dice {phrase!r}")
    return fallas


def _source_names(sources: list[dict] | None) -> list[str]:
    out: list[str] = []
    for s in sources or []:
        if isinstance(s, dict):
            out.append(str(s.get("name") or s.get("portal") or ""))
        else:
            out.append(str(s))
    return out


# ── una corrida de un caso ─────────────────────────────────


async def _judge_run(judge: Any, entry: dict, out: Any) -> dict[str, Any]:
    """Los jueces, con su gasto medido aparte del del motor."""
    from tests.evaluation.engines import metering
    from tests.evaluation.evaluator import (
        judge_answer_relevance,
        judge_hallucination,
        judge_neutrality,
    )

    if out.error or len(out.answer) < MIN_USEFUL_ANSWER_CHARS:
        return {
            "relevance": None,
            "hallucination": None,
            "neutrality": None,
            "reasons": {},
            "usage": None,
        }
    with metering() as meter:
        relevance = await judge_answer_relevance(judge, entry["question"], out.answer)
        # Sin datos a la vista (saludos, preguntas educativas) no hay contra
        # qué comparar: la alucinación no se juzga.
        hallucination = (
            await judge_hallucination(judge, entry["question"], out.answer, out.evidence)
            if out.evidence
            else None
        )
        neutrality = (
            await judge_neutrality(judge, entry["question"], out.answer)
            if entry.get("juez_neutralidad")
            else None
        )
    return {
        "relevance": relevance.score,
        "hallucination": hallucination.score if hallucination else None,
        "neutrality": neutrality.score if neutrality else None,
        "reasons": {
            "relevance": relevance.reason,
            "hallucination": hallucination.reason if hallucination else "",
            "neutrality": neutrality.reason if neutrality else "",
        },
        "usage": meter.to_dict(),
    }


def score_run(
    entry: dict,
    run: dict,
    resolved: dict[str, Any] | None,
    thresholds: JudgeThresholds | None,
) -> dict[str, Any]:
    """El veredicto de una corrida guardada: chequeos, oráculos y jueces.

    Es lo mismo al correr y al re-puntuar: una función pura sobre lo que
    quedó en el reporte. ``evidence_items`` ausente (reportes de antes de
    octubre de 2026) deja sin correr ``fuente_sin_cifra``.
    """
    from tests.evaluation.quality_checks import add_judge_checks, assess

    quality = assess(
        entry,
        run["answer"],
        run["sources"],
        run["error"],
        resolved=resolved,
        evidence_items=run.get("evidence_items"),
    )
    add_judge_checks(quality, run.get("judge"), entry, thresholds)
    return quality.to_dict()


async def evaluate_run(
    engine: Any,
    entry: dict,
    mode: str,
    run: int,
    use_cache: bool = False,
    judge: Any = None,
    resolved: dict[str, Any] | None = None,
    thresholds: JudgeThresholds | None = None,
) -> dict:
    """Una corrida de un caso: la respuesta completa y todo lo que se midió.

    El motor recibe ``user_id`` de batería y nunca un ``conversation_id``: la
    batería no escribe en el historial de nadie (ver ``engines.py``).
    """
    # Sin bypass la batería sólo sirve una vez: la segunda corrida mide el
    # caché que dejó la primera, y sus respuestas se les servirían a usuarios
    # reales.
    out = await engine.run(
        entry["question"], case_id=entry["id"], mode=mode, bypass_cache=not use_cache
    )
    record: dict[str, Any] = {
        "run": run,
        "error": out.error,
        "latency_ms": out.latency_ms,
        "answer": out.answer,
        "sources": out.sources,
        "warnings": out.warnings,
        "evidence_items": out.evidence_items,
        # Lo que vio el juez de alucinación: sin esto, una persona no puede
        # calificar la corrida a mano (ver calibracion_juez.py).
        "evidence": out.evidence,
        "usage": out.usage,
        "tokens_reported": out.tokens_reported,
        "diagnostics": out.diagnostics,
    }
    if judge is not None:
        record["judge"] = await _judge_run(judge, entry, out)
    record["quality"] = score_run(entry, record, resolved, thresholds)
    return record


# ── un caso, todas sus corridas ────────────────────────────


def _mean(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 4) if values else None


def aggregate_entry(entry: dict, runs: list[dict]) -> dict:
    """Junta las corridas de un caso.

    Los campos de siempre (``answered``, ``keyword_score``, ``connector_match``…)
    se mantienen para que ``compare_to_baseline`` siga leyendo baselines
    viejos. Con ``--n-runs 1`` valen lo mismo que antes. Con más corridas se
    eligen del lado pesimista: un caso "contesta" si contestó en todas.
    """
    from tests.evaluation.evaluator import compute_retrieval_precision
    from tests.evaluation.quality_checks import keyword_score

    first = runs[0]
    names = [_source_names(r["sources"]) for r in runs]
    errors = [r["error"] for r in runs if r["error"]]
    evaluable = [r for r in runs if r["quality"].get("evaluable", True)]
    passes = sum(1 for r in evaluable if r["quality"]["passed"])

    expected_intent = entry.get("expected_intent")
    mapped = INTENT_MAP.get(expected_intent or "")
    expected_connector = entry.get("expected_connector")

    failures = Counter(f for r in runs for f in r["quality"]["failures"])
    judges = [r.get("judge") or {} for r in runs]

    return {
        "id": entry["id"],
        "category": entry["category"],
        "question": entry["question"],
        "error": errors[0] if errors else None,
        "latency_ms": int(statistics.median(r["latency_ms"] for r in runs)),
        "answered": all(len(r["answer"]) >= MIN_USEFUL_ANSWER_CHARS for r in runs),
        "answer_chars": min(len(r["answer"]) for r in runs),
        "tokens_used": round(statistics.fmean(r["tokens_reported"] for r in runs)),
        "answer_head": first["answer"][:160],
        "classification": first["diagnostics"].get("classification"),
        "plan_actions": first["diagnostics"].get("plan_actions") or [],
        "sources": names[0],
        "keyword_score": round(
            statistics.fmean(
                keyword_score(r["answer"], entry.get("expected_answer_contains")) for r in runs
            ),
            3,
        ),
        "retrieval_precision": round(
            statistics.fmean(
                compute_retrieval_precision(entry.get("expected_sources") or [], n) for n in names
            ),
            3,
        ),
        "forbidden_sources_hit": sorted(
            {
                h
                for n in names
                for h in _forbidden_sources_hit(entry.get("forbidden_sources") or [], n)
            }
        ),
        "forbidden_answer_hit": sorted(
            {
                h
                for r in runs
                for h in _forbidden_answer_hit(
                    entry.get("forbidden_answer_contains") or [], r["answer"]
                )
            }
        ),
        "intent_scored": mapped is not None
        and first["diagnostics"].get("routing_comparable", True),
        "intent_match": bool(mapped)
        and all(r["diagnostics"].get("classification") == mapped for r in runs),
        # El conector esperado es un nombre de paso del pipeline viejo
        # (`query_series`…). El agente no tiene pasos: sus herramientas se
        # llaman distinto, y compararlas daría una "regresión" que no es tal.
        "connector_scored": expected_connector is not None
        and first["diagnostics"].get("routing_comparable", True),
        "connector_match": bool(expected_connector)
        and all(expected_connector in (r["diagnostics"].get("plan_actions") or []) for r in runs),
        # ── lo nuevo ──
        "n_runs": len(runs),
        "passes": passes,
        # Sobre las corridas evaluables: un oráculo caído no es un aprobado
        # ni un desaprobado. Sin ninguna evaluable, None.
        "pass_rate": round(passes / len(evaluable), 3) if evaluable else None,
        "runs_not_evaluable": len(runs) - len(evaluable),
        "failures": dict(failures.most_common()),
        "cost_usd": _mean(
            [r["usage"]["cost_usd"] for r in runs if r["usage"].get("cost_usd") is not None]
        ),
        "llm_tokens": round(statistics.fmean(sum(r["usage"]["tokens"].values()) for r in runs)),
        "judge_relevance": _mean(
            [j["relevance"] for j in judges if j.get("relevance") is not None]
        ),
        "judge_hallucination": _mean(
            [j["hallucination"] for j in judges if j.get("hallucination") is not None]
        ),
        "judge_neutrality": _mean(
            [j["neutrality"] for j in judges if j.get("neutrality") is not None]
        ),
        "runs": runs,
    }


# ── el resumen ─────────────────────────────────────────────


def _rate_over(pool: list[bool]) -> dict[str, Any]:
    return {"rate": round(sum(pool) / len(pool), 3) if pool else None, "scored_over": len(pool)}


def _quality_summary(results: list[dict]) -> dict[str, Any]:
    """Las tasas que deciden si un motor es mejor que otro.

    Todas se calculan sobre corridas, no sobre casos, y cada una dice sobre
    cuántas: una tasa sobre 4 corridas no pesa lo mismo que una sobre 159.
    """
    all_runs = [(r, run) for r in results for run in r.get("runs") or []]
    if not all_runs:
        return {}
    runs = [(r, run) for r, run in all_runs if run["quality"].get("evaluable", True)]

    def checks(run: dict, prefix: str) -> list[dict]:
        return [c for c in run["quality"]["checks"] if c["name"].startswith(prefix)]

    def rate_of(prefix: str) -> dict[str, Any]:
        return _rate_over(
            [all(c["ok"] for c in checks(run, prefix)) for _, run in runs if checks(run, prefix)]
        )

    numeric = [
        all(c["ok"] for c in checks(run, "valor:")) for _, run in runs if checks(run, "valor:")
    ]
    deflect = [
        all(c["ok"] for c in checks(run, "deflecta") + checks(run, "no_deflecta"))
        for _, run in runs
        if checks(run, "deflecta") or checks(run, "no_deflecta")
    ]
    sources = [
        all(c["ok"] for c in checks(run, "fuente_por_tipo"))
        for _, run in runs
        if checks(run, "fuente_por_tipo")
    ]
    leaks = sum(
        1 for _, run in runs if any(not c["ok"] for c in checks(run, "sin_errores_filtrados"))
    )
    by_cat: dict[str, list[bool]] = {}
    for r, run in runs:
        by_cat.setdefault(r["category"], []).append(run["quality"]["passed"])

    rates = [r.get("pass_rate") for r in results]
    return {
        "pass_rate": _rate_over([run["quality"]["passed"] for _, run in runs]),
        # Oráculos que no respondieron: fuera de la tasa, nunca aprobados.
        "runs_not_evaluable": len(all_runs) - len(runs),
        "cases_always_pass": sum(1 for p in rates if p == 1.0),
        "cases_never_pass": sum(1 for p in rates if p == 0.0),
        # Un caso que a veces pasa y a veces no: lo que una sola corrida no ve.
        "cases_flaky": sum(1 for p in rates if p is not None and 0.0 < p < 1.0),
        "numeric_accuracy": _rate_over(numeric),
        "date_accuracy": rate_of("fecha_del_dato"),
        "sourcing_accuracy": rate_of("fuente_sin_cifra"),
        "hallucination_judge_pass": rate_of("juez_alucinacion"),
        "neutrality_judge_pass": rate_of("juez_neutralidad"),
        "deflection_accuracy": _rate_over(deflect),
        "source_accuracy": _rate_over(sources),
        "runs_with_leaked_errors": leaks,
        "runs_with_internal_identifiers": sum(
            1 for _, run in runs if any("identificadores" in w for w in run["quality"]["warnings"])
        ),
        "by_category": {cat: _rate_over(v) for cat, v in sorted(by_cat.items())},
    }


def _cost_summary(results: list[dict]) -> dict[str, Any]:
    runs = [run for r in results for run in r.get("runs") or []]
    if not runs:
        return {}
    costs = [run["usage"]["cost_usd"] for run in runs if run["usage"].get("cost_usd") is not None]
    tokens: Counter[str] = Counter()
    for run in runs:
        tokens.update(run["usage"]["tokens"])
    judge_costs = [
        (run.get("judge") or {}).get("usage", {}) or {} for run in runs if run.get("judge")
    ]
    return {
        "runs": len(runs),
        "runs_without_price": len(runs) - len(costs),
        "total_usd": round(sum(costs), 4),
        "avg_per_answer_usd": round(statistics.fmean(costs), 5) if costs else None,
        "p95_per_answer_usd": (
            round(sorted(costs)[min(len(costs) - 1, int(len(costs) * 0.95))], 5) if costs else None
        ),
        "llm_calls_per_answer": round(
            statistics.fmean(run["usage"]["llm_calls"] for run in runs), 2
        ),
        "tokens": dict(tokens),
        "judge_total_usd": round(sum(j.get("cost_usd") or 0.0 for j in judge_costs), 4),
    }


def _judge_summary(results: list[dict]) -> dict[str, Any]:
    judged = [run["judge"] for r in results for run in r.get("runs") or [] if run.get("judge")]
    if not judged:
        return {}
    rel = [j["relevance"] for j in judged if j["relevance"] is not None]
    hal = [j["hallucination"] for j in judged if j["hallucination"] is not None]
    neu = [j["neutrality"] for j in judged if j.get("neutrality") is not None]
    return {
        "relevance_avg": _mean(rel),
        "relevance_scored_over": len(rel),
        "hallucination_avg": _mean(hal),
        "hallucination_scored_over": len(hal),
        "neutrality_avg": _mean(neu),
        "neutrality_scored_over": len(neu),
        # Corridas con respuesta que el juez no pudo puntuar: si crece, el
        # promedio deja de representar a la batería.
        "judge_failures": sum(
            1
            for j in judged
            if j["usage"]
            and (
                j["relevance"] is None
                or (j["hallucination"] is None and (j.get("reasons") or {}).get("hallucination"))
            )
        ),
    }


def summarise(results: list[dict], mode: str, engine: str = "legacy") -> dict:
    """Aggregate, keeping the denominators visible.

    Every rate here reports how many entries it was actually computed over,
    because several are scored on a subset and a bare percentage would hide
    that.
    """
    n = len(results)
    intent_pool = [r for r in results if r["intent_scored"]]
    conn_pool = [r for r in results if r["connector_scored"]]
    run_latencies = [run["latency_ms"] for r in results for run in r.get("runs") or []]
    lat = sorted(run_latencies or [r["latency_ms"] for r in results])
    m = len(lat)

    by_cat: dict[str, dict[str, Any]] = {}
    for r in results:
        c = by_cat.setdefault(r["category"], {"total": 0, "answered": 0, "errors": 0})
        c["total"] += 1
        c["answered"] += int(r["answered"])
        c["errors"] += int(bool(r["error"]))

    def rate(pool: list[dict], key: str) -> dict[str, Any]:
        return _rate_over([bool(r[key]) for r in pool])

    return {
        "engine": engine,
        "mode": mode,
        "n_runs": max((r.get("n_runs", 1) for r in results), default=1),
        "total": n,
        "errors": sum(1 for r in results if r["error"]),
        "answered": sum(1 for r in results if r["answered"]),
        "quality": _quality_summary(results),
        "cost": _cost_summary(results),
        "judges": _judge_summary(results),
        "tokens": {
            "total": sum(r["tokens_used"] for r in results),
            "avg_por_caso": round(sum(r["tokens_used"] for r in results) / n) if n else 0,
        },
        "avg_keyword_score": round(sum(r["keyword_score"] for r in results) / n, 3) if n else 0.0,
        "avg_retrieval_precision": (
            round(sum(r["retrieval_precision"] for r in results) / n, 3) if n else 0.0
        ),
        "intent_accuracy": rate(intent_pool, "intent_match"),
        "connector_accuracy": rate(conn_pool, "connector_match"),
        "latency_ms": {
            "avg": round(sum(lat) / m) if m else 0,
            "p50": lat[m // 2] if m else 0,
            "p95": lat[min(m - 1, int(m * 0.95))] if m else 0,
            "max": lat[-1] if m else 0,
        },
        "by_category": by_cat,
        "results": results,
    }


def compare_to_baseline(current: dict, baseline: dict) -> tuple[list[str], list[str]]:
    """Return ``(duras, blandas)``: lo que rompe el gate y lo que sólo avisa.

    La distinción no es cosmética, es lo que hace que el gate sirva. Las
    respuestas del modelo no son deterministas: `complex_004` bajó de 1.0 a 0.5
    entre dos corridas sin que nada del código lo tocara —la respuesta encabezó
    con la línea de pobreza en pesos en vez de la tasa— y un gate que falla por
    eso se desactiva a la semana.

    **Duras** (exit != 0): un caso que antes contestaba y ahora explota o se
    queda mudo, o que dejó de rutear al conector que acertaba. Ninguna de esas
    tres depende de cómo redactó el modelo.

    **Blandas** (sólo se imprimen): puntaje de keywords, tasa de acierto del
    veredicto y latencia. Son señales para mirar, no pruebas.
    """
    base = {r["id"]: r for r in baseline.get("results", [])}
    duras: list[str] = []
    blandas: list[str] = []

    # Comparar modos distintos y quejarse de la latencia es una falsa alarma
    # por diseño: el modo profundo TIENE que tardar más. Medido, produjo 11
    # "regresiones" de latencia que no eran nada. Entre modos se comparan las
    # respuestas; la latencia sólo contra un baseline del mismo modo.
    mismo_modo = current.get("mode") == baseline.get("mode")

    for r in current.get("results", []):
        b = base.get(r["id"])
        if b is None:
            continue  # entry is new to the dataset; nothing to compare against
        if r["error"] and not b["error"]:
            duras.append(f"{r['id']}: ahora falla — {r['error']}")
        if b["answered"] and not r["answered"]:
            duras.append(
                f"{r['id']}: antes contestaba ({b['answer_chars']} chars), ahora {r['answer_chars']}"
            )
        if r["keyword_score"] < b["keyword_score"] - 0.001:
            blandas.append(f"{r['id']}: keywords {b['keyword_score']} → {r['keyword_score']}")
        if (
            r.get("pass_rate") is not None
            and b.get("pass_rate") is not None
            and r["pass_rate"] < b["pass_rate"] - 0.001
        ):
            blandas.append(f"{r['id']}: veredicto {b['pass_rate']} → {r['pass_rate']}")
        if b["connector_scored"] and b["connector_match"] and not r["connector_match"]:
            duras.append(
                f"{r['id']}: dejó de rutear a {b['plan_actions']} (ahora {r['plan_actions']})"
            )
        if (
            mismo_modo
            and r["latency_ms"] >= LATENCY_FLOOR_MS
            and b["latency_ms"] > 0
            and r["latency_ms"] > b["latency_ms"] * LATENCY_REGRESSION_FACTOR
        ):
            blandas.append(
                f"{r['id']}: latencia {b['latency_ms']}ms → {r['latency_ms']}ms "
                f"(más de {LATENCY_REGRESSION_FACTOR}x)"
            )
    return duras, blandas


def _thresholds_from(report: dict, override: JudgeThresholds | None) -> JudgeThresholds:
    if override is not None:
        return override
    saved = report.get("umbrales_jueces") or {}
    return (
        JudgeThresholds(**{k: float(v) for k, v in saved.items()}) if saved else JudgeThresholds()
    )


def rescore(
    report: dict,
    entries: list[dict],
    *,
    thresholds: JudgeThresholds | None = None,
    expectativas: dict[str, Any] | None = None,
) -> dict:
    """Recalcula los veredictos de un reporte con las expectativas actuales.

    Los chequeos son funciones puras sobre la respuesta guardada, así que
    corregir una expectativa del dataset no obliga a volver a gastar Bedrock:
    se vuelven a aplicar sobre las mismas respuestas. Lo que viene del motor
    o del juez (latencia, costo, puntajes) queda como estaba. Los casos que no
    están en el reporte se ignoran.

    Las cifras de los oráculos salen de lo congelado en el reporte
    (``expectativas``), no de la fuente de hoy: el IPC del mes siguiente no
    puede desaprobar una respuesta que era correcta el día de la corrida. Un
    caso nuevo, o un reporte de antes de los oráculos, usa ``expectativas``
    (resuelto aparte, con la fecha de la corrida si se conoce).
    """
    frozen = report.get("expectativas") or {}
    extra = expectativas or {}
    casos = {**(extra.get("casos") or {}), **(frozen.get("casos") or {})}
    th = _thresholds_from(report, thresholds)

    by_id = {e["id"]: e for e in entries}
    results = []
    for old in report.get("results", []):
        entry = by_id.get(old["id"])
        if entry is None:
            continue
        resolved = casos.get(entry["id"])
        runs = [{**run, "quality": score_run(entry, run, resolved, th)} for run in old["runs"]]
        results.append(aggregate_entry(entry, runs))
    out = summarise(results, report.get("mode", "normal"), report.get("engine", "legacy"))
    out["expectativas"] = {
        "hoy": frozen.get("hoy") or extra.get("hoy"),
        "resuelto_en": frozen.get("resuelto_en") or extra.get("resuelto_en"),
        "casos": {e: casos[e] for e in casos if e in {r["id"] for r in results}},
    }
    out["umbrales_jueces"] = vars(th)
    for key in ("fecha", "commit"):
        if key in report:
            out[key] = report[key]
    return out


def _cases_needing_oracles(report: dict, entries: list[dict]) -> list[dict]:
    """Los casos con oráculos que el reporte no tiene congelados."""
    frozen = (report.get("expectativas") or {}).get("casos") or {}
    in_report = {r["id"] for r in report.get("results", [])}
    return [
        e
        for e in entries
        if e["id"] in in_report
        and e["id"] not in frozen
        and (
            e.get("expected_values_from")
            or e.get("forbidden_values_from")
            or e.get("expected_period_from")
        )
    ]


# ── la corrida entera ──────────────────────────────────────


def _make_judge() -> Any:
    from app.infrastructure.adapters.llm.bedrock_llm_adapter import BedrockLLMAdapter
    from app.setup.config.settings import AppSettings

    settings = AppSettings()
    return BedrockLLMAdapter(region=settings.bedrock.REGION, model=JUDGE_MODEL)


def resolve_expectations(entries: list[dict], hoy: date | None = None) -> dict[str, Any]:
    """Las cifras de los oráculos, calculadas ahora y con sello de tiempo."""
    from tests.evaluation.oracles import resolve_dataset

    resolved = resolve_dataset(entries, hoy=hoy)
    resolved["resuelto_en"] = datetime.now().isoformat(timespec="seconds")
    return resolved


async def run_evaluation(
    entries: list[dict],
    mode: str,
    concurrency: int,
    use_cache: bool = False,
    n_runs: int = 1,
    engine_name: str = "legacy",
    judge: bool = False,
    thresholds: JudgeThresholds | None = None,
    expectativas: dict[str, Any] | None = None,
) -> dict:
    """Arma el motor y corre cada caso ``n_runs`` veces.

    Las cifras de los oráculos se resuelven una vez, antes de la primera
    corrida, y todas las corridas se puntúan contra lo mismo.
    """
    from tests.evaluation.engines import ENGINES

    th = thresholds or JudgeThresholds()
    expectativas = expectativas if expectativas is not None else resolve_expectations(entries)
    casos = expectativas.get("casos") or {}
    for cid, res in casos.items():
        for err in res.get("errores") or []:
            print(f"  oráculo sin resolver en {cid}: {err}", file=sys.stderr)

    engine = ENGINES[engine_name]()
    await engine.start()
    judge_llm = _make_judge() if judge else None

    sem = asyncio.Semaphore(concurrency)
    total = len(entries) * n_runs
    done = 0

    async def one(entry: dict, run: int) -> dict:
        nonlocal done
        async with sem:
            rec = await evaluate_run(
                engine, entry, mode, run, use_cache, judge_llm, casos.get(entry["id"]), th
            )
        done += 1
        q = rec["quality"]
        flag = (
            "ERR "
            if rec["error"]
            else ("N/E " if not q.get("evaluable", True) else ("ok  " if q["passed"] else "MAL "))
        )
        cost = rec["usage"].get("cost_usd")
        print(
            f"  [{done:>3}/{total}] {flag}{entry['id']:<22} #{run} "
            f"{rec['latency_ms']:>6}ms  "
            f"{'$' + format(cost, '.4f') if cost is not None else '$?':>8}  "
            f"{'; '.join(q['failures'])[:90]}",
            file=sys.stderr,
            flush=True,
        )
        return rec

    try:
        # Primero todas las corridas #0, después las #1…: si se corta a la
        # mitad, lo que quedó cubre todos los casos al menos una vez.
        jobs = [(e, run) for run in range(n_runs) for e in entries]
        records = await asyncio.gather(*(one(e, run) for e, run in jobs))
    finally:
        await engine.aclose()

    by_id: dict[str, list[dict]] = {}
    for (e, _), rec in zip(jobs, records, strict=True):
        by_id.setdefault(e["id"], []).append(rec)
    results = [aggregate_entry(e, sorted(by_id[e["id"]], key=lambda r: r["run"])) for e in entries]
    report = summarise(results, mode, engine_name)
    report["fecha"] = datetime.now().isoformat(timespec="seconds")
    report["expectativas"] = expectativas
    report["umbrales_jueces"] = vars(th)
    return report


def _fmt_rate(r: dict[str, Any] | None) -> str:
    if not r or r.get("rate") is None:
        return "—"
    return f"{r['rate']:.0%} (sobre {r['scored_over']})"


def _print_report(rep: dict) -> None:
    lat = rep["latency_ms"]
    print(f"\n{'=' * 72}")
    print(
        f"  motor: {rep.get('engine', 'legacy')}   modo: {rep['mode']}   "
        f"casos: {rep['total']} × {rep.get('n_runs', 1)} corridas"
    )
    print(f"  respondidos: {rep['answered']}/{rep['total']}   errores: {rep['errors']}")
    q = rep.get("quality") or {}
    if q:
        print(f"  VEREDICTO:    {_fmt_rate(q['pass_rate'])} de las corridas aprueban")
        print(
            f"                {q['cases_always_pass']} casos siempre, "
            f"{q['cases_never_pass']} nunca, {q['cases_flaky']} a veces"
        )
        if q.get("runs_not_evaluable"):
            print(
                f"                {q['runs_not_evaluable']} corridas no evaluables "
                "(oráculo sin resolver): fuera de la tasa"
            )
        print(f"  números:      {_fmt_rate(q['numeric_accuracy'])}")
        print(f"  fecha dato:   {_fmt_rate(q.get('date_accuracy'))}")
        print(f"  fuente usada: {_fmt_rate(q.get('sourcing_accuracy'))}")
        print(f"  j.alucinac.:  {_fmt_rate(q.get('hallucination_judge_pass'))}")
        print(f"  j.neutral.:   {_fmt_rate(q.get('neutrality_judge_pass'))}")
        print(f"  deflexión:    {_fmt_rate(q['deflection_accuracy'])}")
        print(f"  fuente:       {_fmt_rate(q['source_accuracy'])}")
        print(
            f"  filtraciones: {q['runs_with_leaked_errors']} corridas con errores internos, "
            f"{q['runs_with_internal_identifiers']} con nombres de tablas"
        )
    c = rep.get("cost") or {}
    if c:
        avg = c["avg_per_answer_usd"]
        print(
            f"  costo:        US$ {c['total_usd']} en total, "
            f"{'US$ ' + format(avg, '.4f') if avg is not None else '—'} por respuesta "
            f"(p95 {c['p95_per_answer_usd']}), {c['llm_calls_per_answer']} llamadas por respuesta"
        )
        if c["runs_without_price"]:
            print(f"                {c['runs_without_price']} corridas con un modelo sin precio")
    j = rep.get("judges") or {}
    if j:
        print(
            f"  jueces:       relevancia {j['relevance_avg']} (sobre {j['relevance_scored_over']}), "
            f"alucinación {j['hallucination_avg']} (sobre {j['hallucination_scored_over']}), "
            f"{j['judge_failures']} fallas, US$ {c.get('judge_total_usd')}"
        )
    print(
        f"  keywords: {rep['avg_keyword_score']}   fuentes (título): {rep['avg_retrieval_precision']}"
    )
    ia, ca = rep["intent_accuracy"], rep["connector_accuracy"]
    print(f"  intent:   {_fmt_rate(ia)}   conector: {_fmt_rate(ca)}")
    print(
        f"  latencia: avg {lat['avg']}ms  p50 {lat['p50']}ms  p95 {lat['p95']}ms  max {lat['max']}ms"
    )
    print(f"{'=' * 72}")
    for cat, rate in (q.get("by_category") or {}).items():
        print(f"  {cat:<20} {_fmt_rate(rate)}")
    malos = [r for r in rep["results"] if (r.get("pass_rate") if "pass_rate" in r else 1.0) != 1.0]
    if malos:
        print(f"\n  {len(malos)} casos que no aprueban siempre:")
        for r in malos:
            top = "; ".join(f"{k} ×{v}" for k, v in list(r["failures"].items())[:3])
            ne = f" ({r['runs_not_evaluable']} N/E)" if r.get("runs_not_evaluable") else ""
            print(f"    {r['id']:<22} {r['passes']}/{r['n_runs']}{ne}  {top[:110]}")


def main() -> None:
    from tests.evaluation.engines import ENGINES

    p = argparse.ArgumentParser(description="Run the OpenArg evaluation battery.")
    p.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    p.add_argument("--engine", choices=sorted(ENGINES), default="legacy")
    p.add_argument("--mode", choices=["normal", "deep"], default="normal")
    p.add_argument("--categories", help="comma-separated subset of categories")
    p.add_argument("--ids", help="comma-separated subset of case ids")
    p.add_argument("--n-runs", type=int, default=1, help="corridas por caso")
    p.add_argument("--concurrency", type=int, default=3)
    p.add_argument(
        "--judge", action="store_true", help=f"agrega los jueces con {JUDGE_MODEL} (cuesta aparte)"
    )
    p.add_argument(
        "--umbral-alucinacion",
        type=float,
        default=None,
        help="el juez de alucinación desaprueba por encima de esto "
        f"(default {JudgeThresholds.alucinacion_max})",
    )
    p.add_argument(
        "--umbral-neutralidad",
        type=float,
        default=None,
        help="el juez de neutralidad desaprueba por debajo de esto "
        f"(default {JudgeThresholds.neutralidad_min})",
    )
    p.add_argument(
        "--hoy",
        type=date.fromisoformat,
        default=None,
        help="fecha (AAAA-MM-DD) para resolver los oráculos de un reporte que no los tiene "
        "congelados; por default, la fecha del reporte o la de hoy",
    )
    p.add_argument("--output", type=Path, help="write the report here")
    p.add_argument("--compare", type=Path, help="baseline report to check against")
    p.add_argument(
        "--use-cache",
        action="store_true",
        help="deja el caché semántico activo (mide el camino de producción, "
        "pero la corrida deja de ser repetible)",
    )
    p.add_argument(
        "--rescore",
        type=Path,
        help="recalcula los veredictos de un reporte guardado con el dataset actual, "
        "sin llamar al motor",
    )
    p.add_argument("--dry-run", action="store_true", help="validate the dataset only")
    args = p.parse_args()

    logging.basicConfig(level=logging.WARNING)
    # Una consola de Windows (cp1252) no puede imprimir "≤" ni "−" de las
    # etiquetas: que reemplace el carácter en vez de cortar la corrida.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(errors="replace")

    cats = args.categories.split(",") if args.categories else None
    entries = load_golden_dataset(args.dataset, cats)
    if args.ids:
        wanted = set(args.ids.split(","))
        entries = [e for e in entries if e["id"] in wanted]
    problems = validate_dataset(entries)
    if problems:
        print("dataset inválido:", file=sys.stderr)
        for e in problems:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(2)

    if args.dry_run:
        print(
            f"dataset OK: {len(entries)} casos, {len({e['category'] for e in entries})} categorías"
        )
        sys.exit(0)

    thresholds: JudgeThresholds | None = None
    if args.umbral_alucinacion is not None or args.umbral_neutralidad is not None:
        base = JudgeThresholds()
        thresholds = JudgeThresholds(
            alucinacion_max=(
                args.umbral_alucinacion
                if args.umbral_alucinacion is not None
                else base.alucinacion_max
            ),
            neutralidad_min=(
                args.umbral_neutralidad
                if args.umbral_neutralidad is not None
                else base.neutralidad_min
            ),
        )

    if args.rescore:
        old = json.loads(args.rescore.read_text(encoding="utf-8"))
        missing = _cases_needing_oracles(old, entries)
        extra = None
        if missing:
            hoy = args.hoy or (date.fromisoformat(old["fecha"][:10]) if old.get("fecha") else None)
            print(
                f"  {len(missing)} casos sin cifras congeladas: se resuelven con la fuente "
                f"al {hoy or 'día de hoy'}",
                file=sys.stderr,
            )
            extra = resolve_expectations(missing, hoy=hoy)
        report = rescore(old, entries, thresholds=thresholds, expectativas=extra)
    else:
        report = asyncio.run(
            run_evaluation(
                entries,
                args.mode,
                args.concurrency,
                args.use_cache,
                n_runs=max(1, args.n_runs),
                engine_name=args.engine,
                judge=args.judge,
                thresholds=thresholds,
                expectativas=resolve_expectations(entries, hoy=args.hoy),
            )
        )
    _print_report(report)

    if args.output:
        args.output.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"\nreporte escrito en {args.output}")

    # Se chequea siempre, con `--compare` o sin él: no depende del baseline.
    absolutas = check_absolute_expectations(report)
    if absolutas:
        print(f"\n{len(absolutas)} EXPECTATIVAS ABSOLUTAS INCUMPLIDAS:")
        for a in absolutas:
            print(f"    - {a}")

    if args.compare:
        baseline = json.loads(args.compare.read_text(encoding="utf-8"))
        duras, blandas = compare_to_baseline(report, baseline)
        print(f"\ncontra {args.compare.name} ({baseline.get('mode')}):")
        if baseline.get("mode") != report["mode"]:
            print("  (modos distintos: se comparan respuestas, no latencia)")
        if blandas:
            print(f"  {len(blandas)} avisos (no rompen el gate):")
            for w in blandas:
                print(f"    · {w}")
        if duras:
            print(f"  {len(duras)} REGRESIONES:")
            for d in duras:
                print(f"    - {d}")
            sys.exit(1)
        print("  sin regresiones")

    sys.exit(1 if absolutas else 0)


if __name__ == "__main__":
    main()
