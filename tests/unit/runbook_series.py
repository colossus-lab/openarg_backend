"""La vuelta atrás de una tabla de series, como la escribe `docs/runbook.md` (§9).

Los tests la leen del runbook en vez de copiarla: prueban lo que va a pegar el
operador, y no una versión propia que puede quedar distinta del documento.
"""

from __future__ import annotations

import re
from pathlib import Path

RUNBOOK = Path(__file__).resolve().parents[2] / "docs" / "runbook.md"


def receta_volver_atras(clave: str) -> list[str]:
    """Las sentencias del paso 2 de §9 para `cache_series_<clave>`, sin BEGIN/COMMIT."""
    seccion = RUNBOOK.read_text(encoding="utf-8").split("\n## 9. ", 1)[1].split("\n## ", 1)[0]
    paso = seccion.split("\n2. ", 1)[1].split("\n3. ", 1)[0]
    bloque = re.search(r"```sql\n(.*?)\n\s*```", paso, re.S)
    assert bloque, "el paso 2 de §9 del runbook no tiene un bloque ```sql"
    sql = " ".join(linea.strip() for linea in bloque.group(1).splitlines())
    sentencias = [s.strip().replace("<clave>", clave) for s in sql.split(";")]
    return [s for s in sentencias if s and s.upper() not in ("BEGIN", "COMMIT")]
