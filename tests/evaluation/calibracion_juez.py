"""Calibrar un juez LLM contra etiquetas humanas, en 20 corridas.

Desde octubre de 2026 dos jueces votan en el veredicto de la batería: el de
alucinación (todas las corridas con datos) y el de neutralidad (los casos
cebo de causalidad). Un juez que vota tiene que estar calibrado: el de
relevancia, sin calibrar, castigaba respuestas correctas con su propio
calendario ("el PBI de 2025 no podría estar publicado"), y el de alucinación
marcaba como inventados los diputados que no veía en un resumen recortado.

**Procedimiento** (una persona, unas 2 horas por juez):

1. **La rúbrica se escribe antes de mirar.** Está en ``evaluator.py``
   (``_NEUTRALITY_RUBRIC``, ``_HALLUCINATION_RUBRIC``). Quien etiqueta la lee
   y etiqueta con esa vara, no con la propia.

2. **Correr la batería con jueces** sobre los casos a calibrar, en staging::

       python -m tests.evaluation.run_eval --engine agent-sonnet --judge \\
           --categories neutralidad --n-runs 2 --output reporte_neutralidad.json

   Para alucinación alcanza con cualquier reporte con ``--judge`` y
   evidencia guardada (``evidence`` en cada corrida, desde octubre de 2026).

3. **Exportar 20 corridas a ciegas**::

       python -m tests.evaluation.calibracion_juez exportar \\
           --reporte reporte_neutralidad.json --juez neutralidad --n 20 \\
           --salida etiquetas_neutralidad.csv

   La muestra pone primero las corridas donde el juez y los chequeos
   determinísticos no coinciden (ahí se aprende algo; 20 corridas al azar
   serían casi todas acuerdos fáciles) y completa al azar, repartido por
   caso. El CSV no trae ni el puntaje del juez ni el motor.

4. **Etiquetar a mano** la columna ``etiqueta``: ``1`` = neutral (o "todo
   sale de los datos", para alucinación), ``0`` = no. ``nota`` es libre. Sin
   abrir el reporte mientras tanto.

5. **Medir el acuerdo**::

       python -m tests.evaluation.calibracion_juez medir \\
           --reporte reporte_neutralidad.json --etiquetas etiquetas_neutralidad.csv \\
           --juez neutralidad

   Da el porcentaje de acuerdo, el kappa de Cohen, la matriz de confusión y
   cada desacuerdo con el motivo del juez.

6. **Decidir.** Con acuerdo ≥ 80 % y kappa ≥ 0,6 el juez queda votando con
   ese umbral (``--umbral-neutralidad`` / ``--umbral-alucinacion``). Si no:
   reescribir la rúbrica mirando los desacuerdos, y repetir desde el paso 2
   con **corridas nuevas** (las etiquetadas ya enseñaron la rúbrica: medir
   sobre ellas es circular). Si el umbral es lo que falla y no la rúbrica,
   ``medir --umbral`` permite probar otros sin volver a gastar.

7. **Guardar las etiquetas** en ``tests/evaluation/labels/AAAA-MM-<juez>.csv``
   junto con el reporte que juzgaron: son la referencia para la próxima vez
   que cambie la rúbrica o el modelo juez.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from pathlib import Path
from typing import Any

JUECES = {
    # (clave del puntaje en run["judge"], 1 = bueno cuando el puntaje es…)
    "neutralidad": ("neutrality", "alto"),
    "alucinacion": ("hallucination", "bajo"),
}
DEFAULT_UMBRAL = {"neutralidad": 0.5, "alucinacion": 0.5}
_COLUMNS = ["id", "caso", "pregunta", "respuesta", "evidencia", "etiqueta", "nota"]


def judge_says_good(score: float, juez: str, umbral: float) -> bool:
    _, sentido = JUECES[juez]
    return score >= umbral if sentido == "alto" else score <= umbral


def _runs_with_score(report: dict[str, Any], juez: str) -> list[tuple[dict, dict]]:
    key, _ = JUECES[juez]
    return [
        (r, run)
        for r in report.get("results", [])
        for run in r.get("runs") or []
        if (run.get("judge") or {}).get(key) is not None
    ]


def _deterministic_says_good(run: dict[str, Any], juez: str) -> bool:
    """Lo que dicen los chequeos de código sobre lo mismo que mira el juez."""
    checks = (run.get("quality") or {}).get("checks") or []
    names: tuple[str, ...]
    if juez == "neutralidad":
        names = ("patron_prohibido",)
    else:
        names = ("valor:", "valor_prohibido:", "fuente_sin_cifra")
    relevant = [c for c in checks if c["name"].startswith(names)]
    return all(c["ok"] for c in relevant)


def run_id(case_id: str, run: dict[str, Any]) -> str:
    return f"{case_id}#{run.get('run', 0)}"


def select_runs(
    report: dict[str, Any], juez: str, n: int = 20, seed: int = 0, umbral: float | None = None
) -> list[tuple[dict, dict]]:
    """Primero los desacuerdos juez-código, después al azar repartido por caso."""
    key, _ = JUECES[juez]
    th = DEFAULT_UMBRAL[juez] if umbral is None else umbral
    pool = _runs_with_score(report, juez)
    discrepant = [
        (r, run)
        for r, run in pool
        if judge_says_good(float(run["judge"][key]), juez, th)
        != _deterministic_says_good(run, juez)
    ]
    chosen = discrepant[:n]
    rng = random.Random(seed)
    rest = [x for x in pool if x not in chosen]
    rng.shuffle(rest)
    # Repartido por caso: una vuelta por cada caso antes de repetir uno.
    by_case: dict[str, list[tuple[dict, dict]]] = {}
    for item in rest:
        by_case.setdefault(item[0]["id"], []).append(item)
    while len(chosen) < n and any(by_case.values()):
        for cid in sorted(by_case):
            if by_case[cid] and len(chosen) < n:
                chosen.append(by_case[cid].pop())
    rng.shuffle(chosen)  # que el orden no delate cuáles eran desacuerdos
    return chosen


def export_rows(selected: list[tuple[dict, dict]], juez: str) -> list[dict[str, str]]:
    """Las filas a etiquetar: sin puntaje del juez ni motor."""
    return [
        {
            "id": run_id(r["id"], run),
            "caso": r["id"],
            "pregunta": r.get("question", ""),
            "respuesta": run.get("answer", ""),
            "evidencia": run.get("evidence", "") if juez == "alucinacion" else "",
            "etiqueta": "",
            "nota": "",
        }
        for r, run in selected
    ]


def cohen_kappa(a: list[int], b: list[int]) -> float | None:
    """Kappa de Cohen para dos etiquetadores binarios. None si no se puede calcular."""
    if not a or len(a) != len(b):
        return None
    n = len(a)
    observed = sum(1 for x, y in zip(a, b, strict=True) if x == y) / n
    pa, pb = sum(a) / n, sum(b) / n
    expected = pa * pb + (1 - pa) * (1 - pb)
    if expected == 1.0:
        return None
    return (observed - expected) / (1 - expected)


def agreement(
    report: dict[str, Any], labels: dict[str, int], juez: str, umbral: float | None = None
) -> dict[str, Any]:
    """Acuerdo entre las etiquetas humanas y el juez, con los desacuerdos."""
    key, _ = JUECES[juez]
    th = DEFAULT_UMBRAL[juez] if umbral is None else umbral
    human: list[int] = []
    judge: list[int] = []
    desacuerdos: list[dict[str, Any]] = []
    for r, run in _runs_with_score(report, juez):
        rid = run_id(r["id"], run)
        if rid not in labels:
            continue
        score = float(run["judge"][key])
        j = int(judge_says_good(score, juez, th))
        human.append(labels[rid])
        judge.append(j)
        if j != labels[rid]:
            desacuerdos.append(
                {
                    "id": rid,
                    "humano": labels[rid],
                    "juez": score,
                    "motivo": ((run["judge"].get("reasons") or {}).get(key) or "")[:300],
                }
            )
    n = len(human)
    matriz = {
        "ambos_bueno": sum(1 for h, j in zip(human, judge, strict=True) if h and j),
        "ambos_malo": sum(1 for h, j in zip(human, judge, strict=True) if not h and not j),
        "humano_bueno_juez_malo": sum(1 for h, j in zip(human, judge, strict=True) if h and not j),
        "humano_malo_juez_bueno": sum(1 for h, j in zip(human, judge, strict=True) if not h and j),
    }
    acuerdo = (matriz["ambos_bueno"] + matriz["ambos_malo"]) / n if n else None
    kappa = cohen_kappa(human, judge)
    return {
        "juez": juez,
        "umbral": th,
        "etiquetadas": n,
        "acuerdo": round(acuerdo, 3) if acuerdo is not None else None,
        "kappa": round(kappa, 3) if kappa is not None else None,
        "matriz": matriz,
        "aprobado": bool(acuerdo is not None and acuerdo >= 0.8 and (kappa or 0) >= 0.6),
        "desacuerdos": desacuerdos,
    }


def read_labels(path: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            value = (row.get("etiqueta") or "").strip()
            if value in ("0", "1"):
                out[row["id"]] = int(value)
    return out


def main() -> None:
    p = argparse.ArgumentParser(
        description="Calibrar un juez de la batería contra etiquetas humanas."
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    ex = sub.add_parser("exportar", help="arma la muestra a ciegas para etiquetar")
    ex.add_argument("--reporte", type=Path, required=True)
    ex.add_argument("--juez", choices=sorted(JUECES), required=True)
    ex.add_argument("--n", type=int, default=20)
    ex.add_argument("--seed", type=int, default=0)
    ex.add_argument("--salida", type=Path, required=True)
    me = sub.add_parser("medir", help="acuerdo entre las etiquetas y el juez")
    me.add_argument("--reporte", type=Path, required=True)
    me.add_argument("--etiquetas", type=Path, required=True)
    me.add_argument("--juez", choices=sorted(JUECES), required=True)
    me.add_argument("--umbral", type=float, default=None)
    args = p.parse_args()

    report = json.loads(args.reporte.read_text(encoding="utf-8"))
    if args.cmd == "exportar":
        rows = export_rows(select_runs(report, args.juez, args.n, args.seed), args.juez)
        with args.salida.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"{len(rows)} corridas para etiquetar en {args.salida}")
        return
    result = agreement(report, read_labels(args.etiquetas), args.juez, args.umbral)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()
