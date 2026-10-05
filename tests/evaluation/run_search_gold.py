"""Gold set de búsqueda: ¿la búsqueda devuelve la tabla correcta? En sólo lectura.

El índice cambia de un día para otro (el salario mínimo se rompió entre el 03
y el 04-oct-2026) y staging no representa a prod (58 % de las filas listas
con ``row_count=0``), así que esto está pensado para correr **en prod**, de
noche, sin escribir nada:

- el embedding de la consulta sale de ``BedrockEmbeddingAdapter`` directo,
  sin ``CachedEmbeddingService`` (que escribe en Redis);
- todas las transacciones de las dos conexiones a la base empiezan con
  ``SET TRANSACTION READ ONLY``: un INSERT/UPDATE fallaría;
- no pasa por los endpoints, así que no descuenta cupo ni escribe
  ``api_usage``.

Mide los dos puntos de entrada, que arman resultados distintos sobre la misma
búsqueda:

- **mcp**: lo que devuelve ``GET /catalogo/buscar`` (``buscar_datasets`` del
  MCP). Reproduce el armado de ``catalogo_router.buscar`` con sus mismas
  constantes: ``search_datasets_ann`` → tablas con filas → agrupado por
  (título, URL). Si ese armado se mueve a una función compartida, este
  script tiene que llamarla en vez de reproducirlo.
- **agente**: ``BuscarDatos.run`` del agente, la herramienta real.

Uso, desde la raíz del repo::

    python tests/evaluation/run_search_gold.py --bundle > /tmp/gold_bundle.py
    ssh -i ~/.ssh/openarg.pem ec2-user@<host> \\
        'docker exec -i openarg_backend python - --json' < /tmp/gold_bundle.py > gold.json

``--bundle`` imprime este mismo script con el gold set adentro, para
mandarlo por stdin a un contenedor que no tiene ``tests/``. Sin ``--json``
imprime el resumen legible. El gold set está en ``search_gold.json``.

Criterios (RC11 del plan): hit@3 ≥ 90 % de las positivas; ≥ 90 % de las
negativas sin ningún resultado por encima de ``umbral_negativo``; p95 de
1,5 s o menos.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import statistics
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

GOLD_PATH = Path(__file__).with_name("search_gold.json") if "__file__" in globals() else None
# --bundle reemplaza esta línea por el gold set, para correr sin el repo.
_EMBEDDED_GOLD: str | None = None


# ── lo puro: matcheo y resumen ─────────────────────────────


def matches(spec: dict[str, Any], result: dict[str, Any], *, require_tables: bool = True) -> bool:
    """¿Un resultado (título, portal, tablas) es el dataset esperado?"""
    if not re.search(spec["titulo"], result.get("titulo") or "", re.IGNORECASE):
        return False
    portales = spec.get("portal")
    if portales and (result.get("portal") or "") not in portales:
        return False
    tablas = result.get("tablas") or []
    if require_tables and not tablas:
        return False
    if spec.get("tabla"):
        return any(re.search(spec["tabla"], t, re.IGNORECASE) for t in tablas)
    return True


def rank_of(case: dict[str, Any], results: list[dict[str, Any]]) -> int | None:
    """Posición (0 = primero) del primer resultado esperado, o None."""
    for i, r in enumerate(results):
        if any(matches(spec, r) for spec in case.get("esperado") or []):
            return i
    return None


def negative_ok(results: list[dict[str, Any]], umbral: float) -> bool:
    """Una negativa pasa si nada de lo devuelto supera el umbral."""
    return all((r.get("score") or 0.0) < umbral for r in results)


def evaluate_case(
    case: dict[str, Any], results: list[dict[str, Any]], umbral: float
) -> dict[str, Any]:
    if case.get("negativo"):
        return {"ok": negative_ok(results, umbral), "rank": None}
    rank = rank_of(case, results)
    return {"ok": rank is not None and rank < 3, "rank": rank}


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * q))]


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for entry in ("mcp", "agente"):
        pos = [r for r in rows if not r["negativo"] and entry in r]
        neg = [r for r in rows if r["negativo"] and entry in r]
        ranks = [r[entry]["rank"] for r in pos]

        def hit(k: int, ranks: list[int | None] = ranks) -> float | None:
            return (
                round(sum(1 for x in ranks if x is not None and x < k) / len(ranks), 3)
                if ranks
                else None
            )

        out[entry] = {
            "positivas": len(pos),
            "hit@1": hit(1),
            "hit@3": hit(3),
            "hit@10": hit(10),
            "negativas": len(neg),
            "negativas_ok": round(sum(1 for r in neg if r[entry]["ok"]) / len(neg), 3)
            if neg
            else None,
            "fallan": [r["id"] for r in pos + neg if not r[entry]["ok"]],
        }
    lat = [r["ms"] for r in rows if r.get("ms") is not None]
    top_pos = [r["top_score"] for r in rows if not r["negativo"] and r.get("top_score") is not None]
    top_neg = [r["top_score"] for r in rows if r["negativo"] and r.get("top_score") is not None]
    out["latencia_ms"] = {
        "p50": _pct(lat, 0.5),
        "p95": _pct(lat, 0.95),
        "max": max(lat) if lat else None,
    }
    out["puntaje_top"] = {
        "positivas_mediana": round(statistics.median(top_pos), 3) if top_pos else None,
        "positivas_min": round(min(top_pos), 3) if top_pos else None,
        "negativas_mediana": round(statistics.median(top_neg), 3) if top_neg else None,
        "negativas_max": round(max(top_neg), 3) if top_neg else None,
    }
    return out


def load_gold() -> dict[str, Any]:
    if _EMBEDDED_GOLD is not None:
        data: dict[str, Any] = json.loads(_EMBEDDED_GOLD)
        return data
    if GOLD_PATH is None:
        raise SystemExit("sin gold set: corré el bundle (--bundle) o desde el repo")
    data = json.loads(GOLD_PATH.read_text(encoding="utf-8"))
    return data


def bundle_source() -> str:
    """Este script con el gold set adentro, para mandarlo por stdin."""
    if GOLD_PATH is None:
        raise SystemExit("--bundle se corre desde el repo")
    source = Path(__file__).read_text(encoding="utf-8")
    gold = json.dumps(json.loads(GOLD_PATH.read_text(encoding="utf-8")), ensure_ascii=True)
    marker = "_EMBEDDED_GOLD: str | None = None"
    if marker not in source:
        raise SystemExit("no encuentro el marcador del gold set")
    return source.replace(marker, f"_EMBEDDED_GOLD: str | None = {gold!r}", 1)


# ── lo que toca la base (sólo lectura) ─────────────────────


def _read_only(sync_engine: Any) -> None:
    """Toda transacción de este engine arranca en sólo lectura."""
    from sqlalchemy import event

    @event.listens_for(sync_engine, "begin")
    def _ro(conn: Any) -> None:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")


async def _run(gold: dict[str, Any], limite: int) -> list[dict[str, Any]]:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from app.application.answers.tools.base import ToolContext
    from app.application.answers.tools.catalogo import BuscarDatos
    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import BedrockEmbeddingAdapter
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter
    from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter
    from app.presentation.http.controllers.public_api import catalogo_router as router
    from app.setup.config.settings import AppSettings

    settings = AppSettings()
    embedding = BedrockEmbeddingAdapter(
        region=settings.bedrock.REGION,
        model=settings.bedrock.EMBEDDING_MODEL,
        dimensions=settings.agents.EMBEDDING_DIMENSIONS,
    )
    engine = create_async_engine(os.environ["DATABASE_URL"], pool_size=1, max_overflow=0)
    _read_only(engine.sync_engine)
    sandbox = PgSandboxAdapter()
    _read_only(sandbox._get_engine())
    umbral = float(gold.get("umbral_negativo", 0.55))

    rows: list[dict[str, Any]] = []
    async with AsyncSession(engine) as session:
        vector_search = PgVectorSearchAdapter(session)  # type: ignore[arg-type]
        deps = SimpleNamespace(embedding=embedding, vector_search=vector_search, sandbox=sandbox)
        for case in gold["casos"]:
            started = time.monotonic()
            vector = await embedding.embed(case["q"])
            hits = await vector_search.search_datasets_ann(
                query_embedding=vector,
                limit=limite * 2,
                portal_filter=None,
                min_similarity=router._MIN_SIMILARITY,
            )
            tables: dict[str, list[str]] = {}
            for t in await sandbox.find_tables(dataset_ids=[str(h.dataset_id) for h in hits]):
                if t.dataset_id and t.row_count != 0:
                    tables.setdefault(str(t.dataset_id), []).append(t.table_name)
            merged: dict[tuple[str, str], dict[str, Any]] = {}
            for h in hits:
                key = (h.title.strip().lower(), (h.download_url or "").strip())
                found = tables.get(str(h.dataset_id), [])
                if key in merged:
                    merged[key]["tablas"] += [t for t in found if t not in merged[key]["tablas"]]
                    continue
                merged[key] = {
                    "titulo": h.title,
                    "portal": h.portal,
                    "score": round(float(h.score), 4),
                    "tablas": list(found),
                }
            mcp = list(merged.values())[:limite]
            ms = int((time.monotonic() - started) * 1000)
            await session.rollback()

            score_by_title = {r["titulo"].strip().lower(): r["score"] for r in merged.values()}
            ctx = ToolContext(deps=deps, req=None)  # type: ignore[arg-type]
            outcome = await BuscarDatos().run({"texto": case["q"]}, ctx)
            await session.rollback()
            try:
                payload = json.loads(outcome.content)
            except ValueError:
                payload = {}
            agente = [
                {
                    "titulo": d.get("titulo"),
                    "portal": d.get("portal"),
                    "score": score_by_title.get(str(d.get("titulo") or "").strip().lower()),
                    "tablas": [t.get("tabla") for t in d.get("tablas") or []],
                }
                for d in payload.get("datasets") or []
            ]
            marts = [
                {
                    "titulo": m.get("descripcion"),
                    "tabla": m.get("tabla"),
                    "score": m.get("similitud"),
                }
                for m in payload.get("tablas_curadas") or []
            ]
            row = {
                "id": case["id"],
                "q": case["q"],
                "negativo": bool(case.get("negativo")),
                "ms": ms,
                "top_score": mcp[0]["score"] if mcp else None,
                "mcp": {**evaluate_case(case, mcp, umbral), "top": mcp[:5]},
                "agente": {
                    **evaluate_case(
                        case, agente + marts if case.get("negativo") else agente, umbral
                    ),
                    "top": agente[:5],
                    "marts": marts[:3],
                },
            }
            rows.append(row)
            print(
                f"  {row['id']:<9} mcp={'ok ' if row['mcp']['ok'] else 'MAL'} "
                f"agente={'ok ' if row['agente']['ok'] else 'MAL'} top={row['top_score']} "
                f"{ms}ms  {case['q'][:60]}",
                file=sys.stderr,
                flush=True,
            )
    await engine.dispose()
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Gold set de búsqueda, en sólo lectura.")
    p.add_argument(
        "--bundle", action="store_true", help="imprime el script con el gold set adentro"
    )
    p.add_argument("--json", action="store_true", help="salida JSON completa por stdout")
    p.add_argument("--limite", type=int, default=10, help="resultados por consulta (el del MCP)")
    p.add_argument("--ids", help="sólo estos casos (separados por coma)")
    args = p.parse_args()
    if args.bundle:
        # En bytes UTF-8: una consola de Windows (cp1252) no puede con "→".
        sys.stdout.buffer.write(bundle_source().encode("utf-8"))
        return
    gold = load_gold()
    if args.ids:
        wanted = set(args.ids.split(","))
        gold = {**gold, "casos": [c for c in gold["casos"] if c["id"] in wanted]}
    rows = asyncio.run(_run(gold, args.limite))
    report = {
        "umbral_negativo": gold.get("umbral_negativo"),
        "resumen": summarize(rows),
        "casos": rows,
    }
    if args.json:
        json.dump(report, sys.stdout, ensure_ascii=False, indent=1)
        print()
        return
    print(json.dumps(report["resumen"], ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
