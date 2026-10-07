"""Canario del recall de la búsqueda: el índice HNSW contra la búsqueda exacta.

El índice cambia de un día para otro: el SMVM aparecía primero el 03-oct y el
04-oct ya no (los chunks de ese dataset se habían re-embebido esa tarde), y
nadie se enteró hasta que una auditoría externa lo buscó a mano. Con
``ef_search=200`` el recall@10 contra la exacta llegó a 0 en varias consultas.
``search_datasets_ann`` recorre el índice y cae a la exacta cuando duda, pero
eso tapa una degradación del índice en vez de avisarla.

Esto la mide todas las noches: unas 20 consultas fijas, el top 10 del índice
solo (``search_datasets_hnsw``) contra el de la exacta, y un aviso si el
promedio baja de 0,95.

Tres detalles que deciden si el canario mide algo:

- **Que el índice sea el índice.** Con ``ef_search=1000`` el planificador
  dejaba el HNSW y recorría la tabla entera, y el canario comparaba la exacta
  contra la exacta: 1,0 siempre (revisión del 05-oct, H208). Ahora
  ``search_datasets_hnsw`` recorre el grafo con el seq scan apagado, así que
  lo que se mide es el grafo. Con el índice forzado, esas 20 consultas daban
  0,915 el 05-oct: el aviso sale hasta que se reconstruya el índice.

- **Embeddings de consulta reales.** Cohere embebe la consulta como
  ``search_query`` y los documentos como ``search_document``. Con el vector de
  un chunk como consulta el índice encuentra todo (auto-recall de 0,99-1,00 en
  prod) y el canario daría verde siempre: la falla es propia de las consultas.
- **Empates.** Hay datasets con vectores idénticos (30 "Listado de agentes"
  de Córdoba, 0,663 todos). El top 10 de la exacta elige 10 cualesquiera de
  ese empate y el índice otros 10: por identificador el recall daba 0,6 sin que
  faltara nada. Acá un resultado del índice cuenta si su puntaje llega al del
  décimo de la exacta.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from app.domain.ports.search.vector_search import SearchResult

RECALL_FLOOR = 0.95
K = 10

# Las 14 consultas de la auditoría (4.4) y otras que cubren lo que más se
# pregunta. Fijas a propósito: un canario que cambia de preguntas no compara
# una noche con la otra.
CANARY_QUERIES: tuple[str, ...] = (
    "tasa de desempleo",
    "inflación mensual",
    "pobreza e indigencia",
    "jubilación mínima",
    "salario mínimo vital y móvil",
    "homicidios dolosos por provincia",
    "dólar oficial",
    "presupuesto universidades nacionales 2026",
    "votaciones diputados 2025",
    "deuda pública nacional",
    "coparticipación federal transferencias a provincias",
    "tarifas de electricidad",
    "exportaciones de soja",
    "cantidad de empleados públicos nacionales",
    "reservas internacionales del BCRA",
    "votaciones nominales",
    "presupuesto de la Ciudad de Buenos Aires",
    "resultados de elecciones legislativas",
    "homicidios en Córdoba",
    "matrícula escolar por provincia",
)


def recall_band(mean_recall: float) -> int:
    """La banda de 5 puntos del recall, para la identidad de la alerta.

    0,93 → 90; 0,949 → 90; 0,85 → 85. Con la banda en la clave, el mismo
    índice degradado en el mismo grado es la misma alerta (``notify`` la
    reabre a la 3ª, 10ª y 30ª vez), y si empeora es una alerta nueva.
    """
    return max(0, min(100, int(round(mean_recall * 100, 6)) // 5 * 5))


def tie_aware_recall(
    ann: Sequence[SearchResult], exact: Sequence[SearchResult], k: int = K
) -> float:
    """Qué parte del top ``k`` de la exacta alcanza el índice, contando empates."""
    top = list(exact[:k])
    if not top:
        return 1.0
    threshold = top[-1].score - 1e-6
    return sum(1 for h in ann[:k] if h.score >= threshold) / len(top)


@dataclass(frozen=True)
class QueryRecall:
    query: str
    recall: float
    hnsw_top: float
    exact_top: float
    hnsw_ms: float
    exact_ms: float


@dataclass(frozen=True)
class CanaryReport:
    results: tuple[QueryRecall, ...]
    floor: float = RECALL_FLOOR

    @property
    def mean_recall(self) -> float:
        if not self.results:
            return 1.0
        return sum(r.recall for r in self.results) / len(self.results)

    @property
    def degraded(self) -> bool:
        return bool(self.results) and self.mean_recall < self.floor

    def worst(self, n: int = 3) -> list[QueryRecall]:
        return sorted(self.results, key=lambda r: (r.recall, r.query))[:n]

    def detail_es(self) -> str:
        peores = ", ".join(
            f"«{r.query}» {r.recall:.1f} ({r.hnsw_top:.3f} vs {r.exact_top:.3f})"
            for r in self.worst()
            if r.recall < 1.0
        )
        return (
            f"Peores: {peores}. "
            "El buscador cae a la búsqueda exacta cuando el índice trae poco o con puntaje "
            "bajo, pero esto mide el índice solo: si sigue bajo, el grafo HNSW se degradó "
            "(churn de embeddings, REINDEX pendiente)."
        )
