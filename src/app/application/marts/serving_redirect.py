"""Redirección de marts bloqueados: si el mejor mart para una pregunta está
fuera de servicio, se usa el que su bloqueo indica.

Un mart con `serving.blocked` se saca del ruteo, y con eso también se perdía
la información de que la pregunta era de ese tema. El 29-sep en prod, "gasto
público por ministerio del último ejercicio" tenía como mejor candidato a
`presupuesto_nacional_ejecutado` (bloqueado: mezcla unidades y cubre 37 de 51
jurisdicciones). Al filtrarlo, el ruteo cayó en
`presupuesto_servicios_administrativos`, un catálogo de organismos sin montos,
aunque el propio motivo del bloqueo decía "Usar presupuesto_consolidado".

Convención: el `blocked_reason` del YAML del mart nombra su reemplazo con
"Usar <mart_id>". `tests/unit/test_serving_redirect.py` verifica que todo
reemplazo nombrado existe y no está bloqueado.
"""

from __future__ import annotations

import re

_REDIRECT_RE = re.compile(r"\bUsar\s+`?([a-z][a-z0-9_]*)`?", re.IGNORECASE)

# Postgres `substring(x from pattern)` devuelve el primer grupo: la misma regla
# que `redirect_target`, del lado de la base.
REDIRECT_SQL_PATTERN = "[Uu]sar `?([a-z][a-z0-9_]*)"


def redirect_target(reason: str | None) -> str | None:
    """El mart_id que el motivo del bloqueo indica usar en su lugar, si hay."""
    if not reason:
        return None
    match = _REDIRECT_RE.search(reason)
    return match.group(1).lower() if match else None


# Los 3 marts más parecidos a la pregunta —bloqueados incluidos— y, para cada
# bloqueado, su reemplazo. Un bloqueado sin reemplazo válido (no existe, está
# también bloqueado o no tiene filas) desaparece, como antes. Si el reemplazo
# ya estaba entre los 3, queda una sola vez con el mejor puntaje.
#
# Columnas: mart_id, mart_schema, mart_view_name, domain, last_row_count,
# base_score, sample_max_sim, redirected_from.
MART_CANDIDATES_SQL = f"""
WITH ranked AS (
  SELECT md.mart_id,
         COALESCE(md.serving_blocked, FALSE) AS blocked,
         substring(md.serving_blocked_reason from '{REDIRECT_SQL_PATTERN}') AS redirect_id,
         1 - (md.embedding <=> CAST(:emb AS vector)) AS base_score
  FROM mart_definitions md
  WHERE md.embedding IS NOT NULL
    AND COALESCE(md.last_row_count, 0) > 0
  ORDER BY md.embedding <=> CAST(:emb AS vector)
  LIMIT 3
),
effective AS (
  SELECT t.mart_id, t.mart_schema, t.mart_view_name, t.domain, t.last_row_count,
         MAX(r.base_score) AS base_score,
         MAX(CASE WHEN r.blocked THEN r.mart_id END) AS redirected_from
  FROM ranked r
  JOIN mart_definitions t
    ON t.mart_id = CASE WHEN r.blocked THEN r.redirect_id ELSE r.mart_id END
  WHERE COALESCE(t.last_row_count, 0) > 0
    AND NOT COALESCE(t.serving_blocked, FALSE)
  GROUP BY t.mart_id, t.mart_schema, t.mart_view_name, t.domain, t.last_row_count
)
SELECT e.mart_id, e.mart_schema, e.mart_view_name, e.domain, e.last_row_count,
       e.base_score, e.redirected_from,
       COALESCE((
         SELECT MAX(1 - (msq.embedding <=> CAST(:emb AS vector)))
         FROM public.mart_sample_queries msq
         WHERE msq.mart_id = e.mart_id
       ), 0) AS sample_max_sim
FROM effective e
ORDER BY e.base_score DESC
"""
