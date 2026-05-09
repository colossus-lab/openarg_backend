"""Set up `mart_sample_queries` table + populate with 35 hand-curated rows.

Architecture choice (vs the embed-concat shortcut from
`enrich_mart_embeddings.py`): a separate row per (mart_id, query_text)
lets the ranker compute MAX similarity over samples instead of an
average-of-concat. With concat-embed, "elecciones PASO" looks vaguely
similar to every mart's broadened embedding because all 7 marts now
contain enough Spanish-government tokens to score >0.5 against any
gov-domain query. With max-over-rows, the ranker can answer "does ANY
sample for this mart actually match the query?" — which is the
discriminator we lacked.

After this script runs, the ranker uses:

    mart_score = base_similarity(query, mart.embedding) + (
        +0.17 if max_similarity(query, mart_sample_queries[mart_id]) >= 0.45
        else 0.0
    )

That gates the boost: only marts with a sample within cosine 0.45 of
the user's query get the lift. Negative controls (queries with no mart
relevance, e.g. "resultados electorales PASO" — none of the 7 marts has
an electoral sample) get NO boost, so raw wins as expected.

This script is a one-shot. The proper sprint-level packaging is an
alembic migration that creates the table + a yaml field
`sample_queries:` that `build_mart` upserts into the table.
"""

from __future__ import annotations

import asyncio
import os
import sys

from sqlalchemy import create_engine, text

# Same set as `enrich_mart_embeddings.py` — keep in sync until both are
# replaced by the proper YAML-driven backfill.
SAMPLE_QUERIES: dict[str, list[str]] = {
    "salud_establecimientos": [
        "cuántos hospitales hay en Argentina",
        "establecimientos de salud por provincia",
        "centros de salud en Buenos Aires",
        "listado de hospitales públicos",
        "clínicas privadas por departamento",
        "consultorios por localidad",
        "salas de internación por región",
    ],
    "presupuesto_consolidado": [
        "presupuesto vigente por jurisdicción",
        "ejecución presupuestaria 2024",
        "crédito devengado del Ministerio de Salud",
        "ejecución del presupuesto de educación",
        "gasto público por programa",
        "cuánto se ejecutó del presupuesto",
        "presupuesto nacional consolidado",
    ],
    "escuelas_argentina": [
        "cantidad de escuelas en CABA",
        "establecimientos educativos por provincia",
        "escuelas primarias y secundarias",
        "matrícula educativa por departamento",
        "padrón de escuelas argentinas",
        "colegios públicos por región",
        "infraestructura educativa",
    ],
    "staff_estado": [
        "cantidad de empleados públicos por organismo",
        "dotación de personal del Estado",
        "planta permanente del Ministerio",
        "personal del Senado",
        "empleados de Diputados",
        "empleo público por jurisdicción",
        "staff del estado argentino",
    ],
    "legislatura_actividad": [
        "sesiones del Senado en 2024",
        "proyectos de ley en Diputados",
        "actividad legislativa del Congreso",
        "votaciones del Congreso",
        "leyes sancionadas",
        "diarios de sesiones",
    ],
    "series_economicas": [
        "inflación mensual del IPC",
        "tipo de cambio BCRA histórico",
        "tasa de política monetaria",
        "reservas internacionales",
        "agregados monetarios M2 M3",
        "tasas de interés históricas",
    ],
    # ---------- Marts shipped 2026-05-09 (analytics-driven sprint) ----------
    "bcra_principales_indicadores": [
        "mostrame la evolución de las reservas del BCRA",
        "cómo vienen las reservas en el último semestre",
        "reservas internacionales Argentina",
        "tasas de interés del Banco Central",
        "tasa BADLAR y plazo fijo",
        "tasa de política monetaria",
        "pasivos monetarios BCRA",
        "tasas activas para préstamos personales hipotecarios",
        "letras de liquidez BCRA",
        "cómo el banco central ha imprimido dinero",
    ],
    "comercio_exterior_argentina": [
        "exportaciones argentinas por sector",
        "balance comercial Argentina",
        "importaciones de bienes por actividad",
        "valor FOB de exportaciones mensuales",
        "valor CIF de importaciones",
        "comercio exterior por clae",
        "exportaciones por rubro",
        "importaciones por sector económico",
        "saldo comercial mensual",
        "alícuotas derechos de exportación",
    ],
    "empleo_formal_argentina": [
        "empleo formal por provincia",
        "trabajadores registrados Argentina",
        "remuneración media por departamento",
        "salarios formales por género",
        "cantidad de empleados formales por municipio",
        "afiliados al sistema previsional por departamento",
        "brecha salarial entre hombres y mujeres",
        "empleo registrado por radio censal",
        "ocupación formal en el conurbano",
        "trabajadores por jurisdicción",
    ],
    "magyp_cultivos_principales": [
        "producción de soja por provincia",
        "rendimiento del maíz",
        "trigo siembra cosecha y producción",
        "cultivos principales Argentina",
        "estimaciones agrícolas MAGyP",
        "girasol producción por campaña",
        "superficie sembrada por departamento",
        "cebada avena centeno producción",
        "rendimiento kg por hectárea",
        "producción agrícola por campaña",
    ],
    "magyp_senasa_movimientos_pecuarios": [
        "movimientos de ganado por provincia",
        "exportación de hacienda bovina",
        "ganado vacuno por departamento",
        "movimientos pecuarios SENASA",
        "porcinos cerda lechón faena",
        "aves pollos parrilleros movimientos",
        "ovinos cordero carnero borrego",
        "equinos caballo yegua",
        "vacas vaquillonas novillos terneros",
        "tránsito animal entre provincias",
    ],
    "mendoza_ejecucion_acumulada_fondo": [
        "ejecución presupuestaria del fondo Mendoza",
        "presupuesto provincial Mendoza acumulado",
        "gastos votado devengado pagado Mendoza",
        "ejecución mensual fondo provincial",
        "presupuesto Mendoza por mes",
        "concepto presupuestario Mendoza",
    ],
    "presupuesto_clasificador_economico": [
        "clasificador económico del presupuesto nacional",
        "incisos del gasto público Argentina",
        "gasto en personal por inciso",
        "transferencias corrientes presupuesto",
        "bienes de capital ejecutado",
        "partidas parciales subparciales",
        "clasificación económica del gasto",
    ],
    "presupuesto_servicios_administrativos": [
        "servicios administrativos del presupuesto",
        "catálogo de servicios PEN",
        "ministerios y secretarías presupuesto",
        "servicios de la administración pública nacional",
        "código de servicio administrativo",
        "lookup servicio presupuestario",
    ],
    "presupuesto_finalidad_funcion": [
        "finalidad y función del presupuesto",
        "gasto en educación nacional por finalidad",
        "gasto en salud presupuesto función",
        "presupuesto por finalidad",
        "función del gasto público",
        "clasificación funcional del presupuesto",
        "defensa seguridad servicios sociales presupuesto",
    ],
    "demo_energia_pozos": [
        "producción de petróleo por empresa",
        "pozos de gas y petróleo Argentina",
        "producción mensual de hidrocarburos",
        "Vaca Muerta producción petróleo",
        "producción m3 petróleo gas por provincia",
        "empresas operadoras de yacimientos",
        "rendimiento de pozos petroleros",
    ],
    "pobreza_indec_aglomerados": [
        "indicadores de pobreza INDEC",
        "tasa de pobreza Argentina por semestre",
        "indigencia 31 aglomerados urbanos",
        "incidencia de la pobreza",
        "pobreza Buenos Aires Córdoba Rosario",
        "personas hogares debajo de la línea de pobreza",
        "evolución pobreza e indigencia",
        "informe de pobreza INDEC",
    ],
    "pami_compras_publicas": [
        "compras y contrataciones de PAMI",
        "licitaciones PAMI",
        "contrataciones del Instituto Nacional de Servicios Sociales",
        "PAMI adjudicaciones",
        "expedientes de compras PAMI",
        "obra social jubilados compras",
        "objeto licitatorio PAMI",
    ],
    # ---------- Marts agregados en paralelo (52 totales) ----------
    "mediaciones_prejudiciales": [
        "mediaciones prejudiciales Argentina",
        "mediación previa civil",
        "padrón de mediadores prejudiciales",
        "estadísticas de mediación judicial",
        "expedientes de mediación obligatoria",
        "mediadores por jurisdicción",
        "centros de mediación",
    ],
    "nombres_personas_fisicas": [
        "padrón de personas físicas Argentina",
        "registro de nombres y apellidos",
        "base de datos de personas",
        "padrón nacional",
        "identidad de personas físicas",
    ],
    "registro_automotor_transferencias": [
        "transferencias de autos",
        "registro automotor Argentina",
        "patentes de autos transferidos",
        "compraventa de vehículos usados",
        "operaciones registro automotor",
        "transferencias de motos",
    ],
    "flujo_vehicular_peajes_caba": [
        "flujo vehicular peajes AUSA",
        "tránsito por autopistas CABA",
        "movimiento de autos en peajes",
        "autopistas urbanas Buenos Aires",
        "tráfico vehicular Buenos Aires",
    ],
    "suaci_atencion_ciudadana_caba": [
        "atención al ciudadano CABA",
        "SUACI Buenos Aires",
        "reclamos de vecinos CABA",
        "trámites del ciudadano CABA",
        "consultas y reclamos al gobierno de la Ciudad",
    ],
    "registro_igj_entidades": [
        "Inspección General de Justicia entidades",
        "registro IGJ",
        "sociedades inscriptas Argentina",
        "personas jurídicas registradas",
        "entidades civiles inscriptas",
        "asociaciones civiles IGJ",
    ],
    "subte_viajes_molinetes": [
        "viajes en subte CABA",
        "molinetes subte Buenos Aires",
        "afluencia de pasajeros subte",
        "estaciones de subte estadísticas",
        "uso del subte por estación",
        "viajes en metro CABA",
    ],
    "ministerios_publicos_casos": [
        "casos de ministerio público",
        "fiscalías Argentina",
        "estadísticas judiciales fiscales",
        "casos penales por jurisdicción",
        "expedientes ministerios públicos",
    ],
    "ddjj_funcionarios_federales": [
        "declaraciones juradas funcionarios",
        "DDJJ funcionarios federales",
        "patrimonio de funcionarios públicos",
        "transparencia patrimonial",
        "bienes declarados funcionarios",
        "ley de ética pública DDJJ",
    ],
    "archivos_judiciales_recibidos": [
        "archivos judiciales recibidos",
        "expedientes archivados poder judicial",
        "archivo del fuero judicial",
        "ingresos al archivo judicial",
        "documentación judicial recibida",
        "archivos del Consejo de la Magistratura",
    ],
    "salarios_departamento_clae2": [
        "salarios por departamento y actividad",
        "remuneraciones por sector económico",
        "salarios por clae provincial",
        "ingresos formales por rama de actividad",
        "sueldos por departamento Argentina",
        "salarios por sector y municipio",
    ],
}


async def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.stderr.write("ERROR: DATABASE_URL required.\n")
        return 2
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    engine = create_engine(db_url)

    # 1) Create the table if missing. Idempotent — safe to re-run.
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS mart_sample_queries (
                id BIGSERIAL PRIMARY KEY,
                mart_id TEXT NOT NULL,
                sample_text TEXT NOT NULL,
                embedding vector(1024),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(mart_id, sample_text)
            )
            """
        ))
        # No HNSW: only ~50 rows expected, sequential scan is faster.
        # Add an index on mart_id for the GROUP BY in the ranker.
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_mart_sample_queries_mart_id "
            "ON mart_sample_queries(mart_id)"
        ))

    # 2) Embed each sample, upsert into the table.
    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import (
        BedrockEmbeddingAdapter,
    )
    region = os.environ.get("AWS_REGION", "us-east-1")
    embedder = BedrockEmbeddingAdapter(region=region)

    inserted = 0
    skipped = 0
    for mart_id, samples in SAMPLE_QUERIES.items():
        for sample in samples:
            emb = await embedder.embed(sample)
            emb_str = "[" + ",".join(str(x) for x in emb) + "]"
            with engine.begin() as conn:
                result = conn.execute(text(
                    """
                    INSERT INTO mart_sample_queries (mart_id, sample_text, embedding)
                    VALUES (:mid, :st, CAST(:emb AS vector))
                    ON CONFLICT (mart_id, sample_text) DO UPDATE
                    SET embedding = EXCLUDED.embedding,
                        created_at = NOW()
                    RETURNING id
                    """
                ), {"mid": mart_id, "st": sample, "emb": emb_str})
                if result.first():
                    inserted += 1
                else:
                    skipped += 1
        print(f"  ✓ {mart_id}: {len(samples)} samples")

    print(f"\nDone. Inserted/updated: {inserted}, skipped: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
