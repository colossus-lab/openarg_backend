"""Los datasets de DDJJ que el colector genérico ya no baja.

Las declaraciones de la Oficina Anticorrupción (en su portal, `justicia`, y en
su copia de `datos_gob_ar`), las de actividades anteriores y posteriores a la
función pública y las de la Ciudad de Buenos Aires las carga `ddjj_tasks` en
`raw.cache_ddjj_*`. Por el colector genérico llegaban rotas:

- ~60 tablas para un mismo dataset;
- años cortados en 500.000 filas;
- los ZIP con Deflate64 salteados.

Este módulo nombra los datasets por (portal, título). No hace falta mirar la
organización: el mismo dataset aparece con " Ministerio de Justicia" y con
"Ministerio de Justicia". Lo usan el despacho del colector, `collect_dataset` y
el retiro de las tablas viejas. El dataset de ARSAT ("Declaraciones Juradas
Patrimoniales", otra planilla) no está acá y se sigue bajando.
"""

from __future__ import annotations

REEMPLAZADOS: frozenset[tuple[str, str]] = frozenset(
    {
        ("justicia", "Declaraciones Juradas Patrimoniales Integrales de carácter público"),
        ("datos_gob_ar", "Declaraciones Juradas Patrimoniales Integrales de carácter público"),
        ("caba", "Declaraciones Juradas"),
        (
            "justicia",
            "Declaraciones Juradas de actividades anteriores y posteriores a la función pública",
        ),
        (
            "datos_gob_ar",
            "Declaraciones Juradas de actividades anteriores y posteriores a la función pública",
        ),
    }
)


def es_reemplazado(portal: str | None, titulo: str | None) -> bool:
    return ((portal or "").strip(), (titulo or "").strip()) in REEMPLAZADOS


def _literal(valor: str) -> str:
    return "'" + valor.replace("'", "''") + "'"


def sql_sin_reemplazados(alias: str = "d") -> str:
    """Condición SQL (empieza con AND) que deja afuera a los datasets reemplazados."""
    pares = ", ".join(f"({_literal(p)}, {_literal(t)})" for p, t in sorted(REEMPLAZADOS))
    return f" AND ({alias}.portal, trim({alias}.title)) NOT IN ({pares}) "
