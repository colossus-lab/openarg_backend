"""CLI runner for the OpenArg evaluation battery.

This is the runner the previous version promised and never was. The file
deleted on 2026-05-09 carried the note *"invoking the CLI will exit with
`import_failed` until it is migrated to drive the LangGraph pipeline
directly"* — and its scoring loop was a placeholder that built an empty
``EvalResult`` without calling anything. So the golden dataset has never
actually been executed against the pipeline.

This version drives the real graph: it builds the DI container the same way
``app/run.py`` does, compiles the pipeline, and runs every entry through it.

**What it measures, and what it deliberately does not.** The dataset's
``expected_intent`` mixes two different things: four values the classifier
really emits (``casual``, ``educational``, ``meta``, ``injection_blocked``)
and topical labels like ``inflacion`` or ``dolar`` that nothing in the
pipeline ever produces — for those the classifier returns ``None`` and the
question goes to the planner. Scoring the topical ones would manufacture a
guaranteed miss and make the accuracy number meaningless, so intent is scored
only over the entries where the expectation is comparable, and the report says
how many those were.

Usage::

    # write a baseline (do this on a known-good build)
    python -m tests.evaluation.run_eval --mode normal --output baseline_normal.json

    # later, check for regressions against it
    python -m tests.evaluation.run_eval --mode normal --compare baseline_normal.json

    # same battery through deep mode
    python -m tests.evaluation.run_eval --mode deep --output report_deep.json

    # cheap sanity check, no LLM calls
    python -m tests.evaluation.run_eval --dry-run

Exits non-zero when ``--compare`` finds a regression, so it can gate a deploy.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

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

# Below this, an answer is treated as "the pipeline produced nothing useful".
MIN_USEFUL_ANSWER_CHARS = 20


def load_golden_dataset(path: Path, categories: list[str] | None = None) -> list[dict]:
    """Load and optionally filter the golden dataset."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entries", [])
    if categories:
        entries = [e for e in entries if e["category"] in categories]
    return entries


def validate_dataset(entries: list[dict]) -> list[str]:
    """Return a list of structural problems with the dataset."""
    errors: list[str] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        missing = REQUIRED_FIELDS - set(entry)
        if missing:
            errors.append(f"entry {i}: missing {sorted(missing)}")
        eid = entry.get("id")
        if eid in seen:
            errors.append(f"entry {i}: duplicate id {eid!r}")
        seen.add(eid)
    return errors


def _plan_actions(plan: Any) -> list[str]:
    """The connector actions the planner chose, if it produced a plan."""
    steps = getattr(plan, "steps", None) or []
    return [getattr(s, "action", "") for s in steps]


def _source_names(sources: list[dict] | None) -> list[str]:
    out: list[str] = []
    for s in sources or []:
        if isinstance(s, dict):
            out.append(str(s.get("name") or s.get("portal") or ""))
        else:
            out.append(str(s))
    return out


async def evaluate_entry(graph: Any, entry: dict, mode: str, use_cache: bool = False) -> dict:
    """Run one dataset entry through the real pipeline and score it."""
    from tests.evaluation.evaluator import check_answer_contains, compute_retrieval_precision

    started = time.monotonic()
    state: dict[str, Any] = {
        "question": entry["question"],
        "user_id": f"eval:{entry['id']}",
        # No conversation_id on purpose: the battery must never write into
        # anyone's conversation history.
        "conversation_id": "",
        "mode": mode,
        "replan_count": 0,
        # Deep mode would otherwise spend a turn asking instead of answering,
        # and then every deep entry would score as "no answer". The battery
        # measures the search turn; the scoping turn has its own tests.
        "scoping_done": True,
        # Sin esto la batería sólo sirve una vez: la segunda corrida mide el
        # caché que dejó la primera. Y además evita que sus respuestas se le
        # sirvan a usuarios reales.
        "bypass_cache": not use_cache,
    }

    error: str | None = None
    out: dict[str, Any] = {}
    try:
        out = await graph.ainvoke(state)
    except Exception as exc:  # noqa: BLE001 — an entry that crashes IS the finding
        error = f"{type(exc).__name__}: {exc}"[:300]

    latency_ms = int((time.monotonic() - started) * 1000)
    answer = str(out.get("clean_answer") or "")
    actions = _plan_actions(out.get("plan"))
    sources = _source_names(out.get("sources"))

    expected_intent = entry.get("expected_intent")
    mapped = INTENT_MAP.get(expected_intent)
    intent_scored = mapped is not None
    intent_match = bool(mapped and out.get("classification") == mapped)

    expected_connector = entry.get("expected_connector")
    connector_scored = expected_connector is not None
    connector_match = bool(expected_connector and expected_connector in actions)

    return {
        "id": entry["id"],
        "category": entry["category"],
        "question": entry["question"],
        "error": error,
        "latency_ms": latency_ms,
        "answered": len(answer) >= MIN_USEFUL_ANSWER_CHARS,
        "answer_chars": len(answer),
        # Lo que cierra el §6.5 del plan con un número en vez de una promesa:
        # cuántos tokens cuesta de verdad cada modo sobre la MISMA batería.
        "tokens_used": int(out.get("tokens_used") or 0),
        "answer_head": answer[:160],
        "classification": out.get("classification"),
        "plan_actions": actions,
        "sources": sources,
        "keyword_score": round(
            check_answer_contains(answer, entry.get("expected_answer_contains") or []), 3
        ),
        "retrieval_precision": round(
            compute_retrieval_precision(entry.get("expected_sources") or [], sources), 3
        ),
        "intent_scored": intent_scored,
        "intent_match": intent_match,
        "connector_scored": connector_scored,
        "connector_match": connector_match,
    }


def summarise(results: list[dict], mode: str) -> dict:
    """Aggregate, keeping the denominators visible.

    Every rate here reports how many entries it was actually computed over,
    because three of the four are scored on a subset and a bare percentage
    would hide that.
    """
    n = len(results)
    intent_pool = [r for r in results if r["intent_scored"]]
    conn_pool = [r for r in results if r["connector_scored"]]
    lat = sorted(r["latency_ms"] for r in results)

    by_cat: dict[str, dict[str, Any]] = {}
    for r in results:
        c = by_cat.setdefault(r["category"], {"total": 0, "answered": 0, "errors": 0})
        c["total"] += 1
        c["answered"] += int(r["answered"])
        c["errors"] += int(bool(r["error"]))

    def rate(pool: list[dict], key: str) -> dict[str, Any]:
        return {
            "rate": round(sum(r[key] for r in pool) / len(pool), 3) if pool else None,
            "scored_over": len(pool),
        }

    return {
        "mode": mode,
        "total": n,
        "errors": sum(1 for r in results if r["error"]),
        "answered": sum(1 for r in results if r["answered"]),
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
            "avg": round(sum(lat) / n) if n else 0,
            "p50": lat[n // 2] if n else 0,
            "p95": lat[min(n - 1, int(n * 0.95))] if n else 0,
            "max": lat[-1] if n else 0,
        },
        "by_category": by_cat,
        "results": results,
    }


def compare_to_baseline(current: dict, baseline: dict) -> list[str]:
    """Return the regressions found. Empty list means clean.

    Only fires on things that are unambiguously worse for a user: an entry
    that used to answer and now errors or goes quiet, a keyword score that
    dropped, or a latency that more than doubled. Improvements are reported
    separately by the caller, never as failures.
    """
    base = {r["id"]: r for r in baseline.get("results", [])}
    problems: list[str] = []

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
            problems.append(f"{r['id']}: now errors — {r['error']}")
        if b["answered"] and not r["answered"]:
            problems.append(
                f"{r['id']}: answered before ({b['answer_chars']} chars), now {r['answer_chars']}"
            )
        if r["keyword_score"] < b["keyword_score"] - 0.001:
            problems.append(f"{r['id']}: keyword score {b['keyword_score']} → {r['keyword_score']}")
        if b["connector_scored"] and b["connector_match"] and not r["connector_match"]:
            problems.append(
                f"{r['id']}: no longer routes to {b['plan_actions']} (now {r['plan_actions']})"
            )
        if (
            mismo_modo
            and b["latency_ms"] > 0
            and r["latency_ms"] > b["latency_ms"] * LATENCY_REGRESSION_FACTOR
        ):
            problems.append(
                f"{r['id']}: latency {b['latency_ms']}ms → {r['latency_ms']}ms "
                f"(over {LATENCY_REGRESSION_FACTOR}x)"
            )
    return problems


async def run_evaluation(
    entries: list[dict], mode: str, concurrency: int, use_cache: bool = False
) -> dict:
    """Build the real pipeline and run the whole battery through it."""
    from dishka import Scope

    from app.application.pipeline.graph import build_pipeline_graph
    from app.application.pipeline.nodes import PipelineDeps, set_deps
    from app.setup.config.settings import AppSettings
    from app.setup.ioc.provider_registry import create_async_ioc_container, get_providers

    settings = AppSettings()
    container = create_async_ioc_container(providers=get_providers(), settings=settings)

    async with container(scope=Scope.REQUEST) as request_scope:
        deps = await request_scope.get(PipelineDeps)
        # Set before spawning: each task inherits the ContextVar from here.
        set_deps(deps)
        # No checkpointer — every entry is an independent single-turn run.
        graph = build_pipeline_graph(deps)

        sem = asyncio.Semaphore(concurrency)
        done = 0

        async def one(entry: dict) -> dict:
            nonlocal done
            async with sem:
                r = await evaluate_entry(graph, entry, mode, use_cache)
                done += 1
                flag = "ERR " if r["error"] else ("    " if r["answered"] else "SIN ")
                print(
                    f"  [{done:>2}/{len(entries)}] {flag}{r['id']:<22} "
                    f"{r['latency_ms']:>6}ms  kw={r['keyword_score']}",
                    file=sys.stderr,
                    flush=True,
                )
                return r

        results = await asyncio.gather(*(one(e) for e in entries))

    await container.close()
    return summarise(list(results), mode)


def _print_report(rep: dict) -> None:
    lat = rep["latency_ms"]
    print(f"\n{'=' * 64}")
    print(f"  modo: {rep['mode']}   casos: {rep['total']}")
    print(f"  respondidos: {rep['answered']}/{rep['total']}   errores: {rep['errors']}")
    print(f"  keywords: {rep['avg_keyword_score']}   fuentes: {rep['avg_retrieval_precision']}")
    ia, ca = rep["intent_accuracy"], rep["connector_accuracy"]
    print(f"  intent:    {ia['rate']}  (sobre {ia['scored_over']} casos comparables)")
    print(f"  conector:  {ca['rate']}  (sobre {ca['scored_over']} casos comparables)")
    print(
        f"  latencia:  avg {lat['avg']}ms  p50 {lat['p50']}ms  p95 {lat['p95']}ms  max {lat['max']}ms"
    )
    tk = rep["tokens"]
    print(f"  tokens:    {tk['total']} en total, {tk['avg_por_caso']} por caso")
    print(f"{'=' * 64}")
    for cat, c in sorted(rep["by_category"].items()):
        marca = "  <<< errores" if c["errors"] else ""
        print(f"  {cat:<20} {c['answered']}/{c['total']} respondidos{marca}")


def main() -> None:
    p = argparse.ArgumentParser(description="Run the OpenArg evaluation battery.")
    p.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    p.add_argument("--mode", choices=["normal", "deep"], default="normal")
    p.add_argument("--categories", help="comma-separated subset of categories")
    p.add_argument("--concurrency", type=int, default=3)
    p.add_argument("--output", type=Path, help="write the report here")
    p.add_argument("--compare", type=Path, help="baseline report to check against")
    p.add_argument(
        "--use-cache",
        action="store_true",
        help="deja el caché semántico activo (mide el camino de producción, "
        "pero la corrida deja de ser repetible)",
    )
    p.add_argument("--dry-run", action="store_true", help="validate the dataset only")
    args = p.parse_args()

    logging.basicConfig(level=logging.WARNING)

    cats = args.categories.split(",") if args.categories else None
    entries = load_golden_dataset(args.dataset, cats)
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

    report = asyncio.run(run_evaluation(entries, args.mode, args.concurrency, args.use_cache))
    _print_report(report)

    if args.output:
        args.output.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"\nreporte escrito en {args.output}")

    if args.compare:
        baseline = json.loads(args.compare.read_text(encoding="utf-8"))
        regressions = compare_to_baseline(report, baseline)
        print(f"\ncontra {args.compare.name} ({baseline.get('mode')}):")
        if baseline.get("mode") != report["mode"]:
            print("  (modos distintos: se comparan respuestas, no latencia)")
        if regressions:
            print(f"  {len(regressions)} REGRESIONES:")
            for r in regressions:
                print(f"    - {r}")
            sys.exit(1)
        print("  sin regresiones")

    sys.exit(0)


if __name__ == "__main__":
    main()
