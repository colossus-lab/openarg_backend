"""
Gobernadores — ETL de gobernadores provinciales desde Wikidata.

Consulta Wikidata SPARQL para obtener los gobernadores actuales
de las 23 provincias argentinas + CABA y los cachea en PostgreSQL.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset

logger = logging.getLogger(__name__)

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"

# Q414 = Argentina, P150 = subdivisions, P6 = head of government
# P102 = party, P580 = start date
GOVERNORS_QUERY = """
SELECT ?provinceLabel ?governorLabel ?partyLabel ?startDate WHERE {
  wd:Q414 wdt:P150 ?province.
  ?province wdt:P6 ?governor.
  OPTIONAL { ?governor wdt:P102 ?party. }
  OPTIONAL {
    ?province p:P6 ?stmt.
    ?stmt ps:P6 ?governor.
    ?stmt pq:P580 ?startDate.
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "es,en". }
}
ORDER BY ?provinceLabel
"""


def _register_dataset(engine, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    source_id = "gobernadores-provincias"
    portal = "gobernaciones"
    title = "Gobernadores de Provincias Argentinas"
    columns_json = json.dumps(list(df.columns))
    now = datetime.now(UTC)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, '', 'json', :cols, :tags,
                     :now, false, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = false, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id,
                "title": title,
                "desc": (
                    "Gobernadores actuales de las 23 provincias argentinas y "
                    "Jefe de Gobierno de CABA."
                ),
                "org": "Gobernaciones Provinciales",
                "portal": portal,
                "url": "https://www.argentina.gob.ar/interior/gobernadores",
                "cols": columns_json,
                "tags": "gobernadores,provincias,gobierno,autoridades",
                "now": now,
                "rows": len(df),
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal},
        ).fetchone()
        dataset_id = dataset_row[0] if dataset_row else None

        if dataset_id:
            finalized = _finalize_cached_dataset(
                engine,
                dataset_id=dataset_id,
                portal=portal,
                source_id=source_id,
                table_name=table_name,
                row_count=len(df),
                columns=list(df.columns),
                declared_format="json",
                download_url=WIKIDATA_SPARQL_URL,
                now=now,
            )
            if not finalized["ok"]:
                return None

    return dataset_id


@celery_app.task(
    name="openarg.scrape_gobernadores",
    bind=True,
    max_retries=3,
    soft_time_limit=120,
    time_limit=180,
)
def scrape_gobernadores(self):
    """Fetch current Argentine province governors from Wikidata SPARQL."""
    import httpx

    engine = get_sync_engine()

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(
                WIKIDATA_SPARQL_URL,
                params={"query": GOVERNORS_QUERY},
                headers={"Accept": "application/json", "User-Agent": "OpenArg/1.0"},
            )
            resp.raise_for_status()

        data = resp.json()
        bindings = data.get("results", {}).get("bindings", [])

        if not bindings:
            logger.warning("Wikidata: no governors returned")
            return {"error": "empty_response"}

        records = []
        for b in bindings:
            provincia = b.get("provinceLabel", {}).get("value", "")
            # Clean "Provincia de " prefix
            provincia = provincia.replace("Provincia de ", "").replace("Provincia del ", "")
            if "Ciudad Autónoma" in provincia:
                provincia = "CABA"

            records.append(
                {
                    "provincia": provincia,
                    "gobernador": b.get("governorLabel", {}).get("value", ""),
                    "partido": b.get("partyLabel", {}).get("value", ""),
                    "inicio_mandato": b.get("startDate", {}).get("value", ""),
                }
            )

        df = pd.DataFrame(records)
        # Deduplicate (Wikidata may return multiple party entries)
        df = df.drop_duplicates(subset=["provincia", "gobernador"], keep="first")

        table_name = "cache_gobernadores"
        df.to_sql(table_name, engine, if_exists="replace", index=False)

        dataset_id = _register_dataset(engine, table_name, df)
        logger.info("Gobernadores: %d provinces cached → %s", len(df), table_name)

        if dataset_id:
            from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

            index_dataset_embedding.delay(dataset_id)

        return {"table": table_name, "rows": len(df)}

    except SoftTimeLimitExceeded:
        logger.error("Gobernadores scrape timed out")
        raise
    except Exception as exc:
        logger.exception("Gobernadores scrape failed")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()
