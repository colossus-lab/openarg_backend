"""Lo que el planner alcanza a ver cuando elige un conector.

Capturado en staging el 2026-09-08, para "Tasa de desempleo en Argentina":

    MARTS DISPONIBLES (vistas semánticas curadas, preferí estas):
      - empleo_formal_argentina
      … (8 marts)

    TABLAS CACHEADAS RELEVANTES (datos reales descargados, usar query_sandbox):
      - empleo_registrado_argentina — 0 filas [relevancia: 0.82]
      - salarios_por_sector_argentina — 0 filas [relevancia: 0.76]
      - empleo_formal_argentina — 0 filas [relevancia: 0.63]

Dos cosas mal, las dos de la misma familia que el resto de este mes:

1. **"0 filas" sobre marts con datos.** `empleo_registrado_argentina` tiene
   18.404 filas y `empleo_formal_argentina` 625.824. El conteo se buscaba en
   `matches`, que sale de `table_catalog`; los marts vienen por otro camino y
   nunca están ahí, así que salían todos en cero. Le estábamos diciendo al
   planner que las vistas curadas estaban vacías.

2. **Ni un solo recurso raw**, de 30.542 que existen. Dos filtros
   encadenados: los 8 marts llenaban el cupo y `catalog_resources` no se
   consultaba, y aunque se consultara, el schema se infería del texto de
   `materialized_table_name` — que está 100 % pelado — así que todo quedaba
   marcado `cache_legacy` y el bloque lo descartaba.
"""

from __future__ import annotations

from pathlib import Path

_SANDBOX = Path("src/app/application/pipeline/connectors/sandbox.py").read_text(encoding="utf-8")
_ADAPTER = Path("src/app/infrastructure/adapters/serving/legacy_serving_adapter.py").read_text(
    encoding="utf-8"
)


# ── el conteo de filas de los marts ────────────────────────


def test_la_consulta_de_marts_trae_el_conteo_de_filas() -> None:
    i = _SANDBOX.index("WITH ranked AS (")
    bloque = _SANDBOX[i : i + 900]

    assert "md.last_row_count" in bloque, (
        "sin traer `last_row_count`, el bloque de hints no tiene con qué "
        "contradecir el 0 por defecto"
    )


def test_el_conteo_de_un_mart_no_se_busca_en_table_catalog() -> None:
    i = _SANDBOX.index("base_match = next(")
    bloque = _SANDBOX[i : i + 900]

    assert "mart_row_counts" in bloque, (
        "los marts no están en `matches`: buscar ahí su conteo devuelve "
        "siempre 0 y el planner los lee como vacíos"
    )


# ── la capa de un recurso ──────────────────────────────────


def test_el_schema_no_se_infiere_del_texto_del_nombre() -> None:
    i = _ADAPTER.index("SELECT cr.resource_identity")
    bloque = _ADAPTER[i : i + 1200]

    assert "rtv.schema_name" in bloque, (
        "`materialized_table_name` convive en tres formas y hoy está pelado: "
        "el schema tiene que salir de `raw_table_versions`, no del string"
    )


def test_el_registro_se_lee_calificado() -> None:
    """Vía PgBouncer, una referencia sin schema cae en la copia obsoleta."""
    i = _ADAPTER.index("SELECT cr.resource_identity")
    bloque = _ADAPTER[i : i + 1200]

    assert "public.raw_table_versions" in bloque


# ── el cupo ────────────────────────────────────────────────


def test_los_marts_no_pueden_quedarse_con_todo_el_cupo() -> None:
    from app.infrastructure.adapters.serving.legacy_serving_adapter import _raw_slot_reserve

    # Con el cupo que usa el planner (8), tiene que quedar lugar para raws.
    assert _raw_slot_reserve(8) >= 1
    assert _raw_slot_reserve(5) >= 1
    # Y la reserva nunca puede comerse más de la mitad: los marts siguen
    # siendo la superficie preferida.
    for limite in (3, 5, 8, 12, 20):
        assert _raw_slot_reserve(limite) <= limite // 2


def test_con_un_cupo_minimo_no_se_reserva_nada() -> None:
    """Con 1 o 2 lugares, reservar dejaría al planner sin marts."""
    from app.infrastructure.adapters.serving.legacy_serving_adapter import _raw_slot_reserve

    assert _raw_slot_reserve(1) == 0
    assert _raw_slot_reserve(2) == 0


def test_la_reserva_se_puede_apagar_por_env(monkeypatch) -> None:
    """`OPENARG_RAW_SLOT_RESERVE=0` restaura el comportamiento anterior."""
    from app.infrastructure.adapters.serving.legacy_serving_adapter import _raw_slot_reserve

    monkeypatch.setenv("OPENARG_RAW_SLOT_RESERVE", "0")
    assert _raw_slot_reserve(8) == 0


def test_la_reserva_nunca_deja_al_planner_sin_marts(monkeypatch) -> None:
    """Aunque alguien pida una reserva absurda, queda lugar para un mart."""
    from app.infrastructure.adapters.serving.legacy_serving_adapter import _raw_slot_reserve

    monkeypatch.setenv("OPENARG_RAW_SLOT_RESERVE", "99")
    assert _raw_slot_reserve(8) == 7
