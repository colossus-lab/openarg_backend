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
        # FX-only — el mart cubre solo cotizaciones cambiarias BCRA (~39 monedas
        # vs peso argentino, snapshot diario). Reservas / tasas / agregados
        # monetarios viven en `bcra_principales_indicadores`, IPC en
        # `inflacion_argentina`. Mantener los samples FX-específicos previene
        # que el boost +0.17 dispare para queries que el mart NO puede contestar.
        "tipo de cambio BCRA",
        "cotización del dólar oficial",
        "cotización del euro en Argentina",
        "cotización del real brasilero",
        "tipo de cambio peso argentino",
        "cotización de monedas extranjeras BCRA",
        "valor del dólar BCRA hoy",
        "cambio peso a otras monedas",
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
    "sube_uso_transporte_publico": [
        "cuántos viajes en colectivo por mes",
        "transacciones SUBE por día",
        "uso del transporte público en AMBA",
        "boletos pagos en colectivo Buenos Aires",
        "viajes en tren por empresa",
        "ranking de empresas de colectivo por pasajeros",
        "demanda de transporte público por provincia",
        "evolución del uso del subte y colectivo",
        "operaciones SUBE en Argentina",
        "viajes diarios en transporte público",
        "qué línea de colectivo transporta más pasajeros",
        "uso del SUBE por municipio",
    ],
    "subsidios_transporte_publico": [
        "subsidios al transporte público",
        "cuánto subsidia el Estado al transporte",
        "FCT Fondo Compensación Tarifaria",
        "transferencias a empresas de colectivos",
        "subsidio por provincia transporte",
        "monto de subsidios al colectivo",
        "subsidios AMBA vs interior",
        "qué empresa recibe más subsidio",
        "compensación tarifaria por mes",
        "gasto en subsidios al transporte de pasajeros",
        "subsidios al gasoil del transporte",
        "transferencias nacionales al transporte público",
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
    # ── batch 2026-05-09 evening: cobertura de marts huérfanos ──
    "caba_cobertura_vegetal": [
        "cobertura vegetal en CABA",
        "árboles por barrio Buenos Aires",
        "espacios verdes Ciudad Buenos Aires",
        "vegetación urbana CABA",
        "censo de arbolado CABA",
        "biodiversidad Ciudad de Buenos Aires",
        "metros cuadrados de parques por habitante CABA",
    ],
    "sociedades_registro_nacional": [
        "sociedades registradas en Argentina",
        "Inspección General de Justicia sociedades",
        "registro de empresas Argentina",
        "inscripciones de sociedades anónimas",
        "SRL inscriptas en IGJ",
        "personas jurídicas registradas",
        "creación de empresas por año",
    ],
    "energia_precios_combustibles": [
        "precios de combustibles en Argentina",
        "precio de la nafta por provincia",
        "precio del gasoil estaciones de servicio",
        "evolución del precio de YPF",
        "diferencia de precios combustibles entre provincias",
        "precios EESS Argentina",
        "precio premium vs súper",
        "precio histórico nafta",
    ],
    "energia_refinacion_combustibles": [
        "refinación de combustibles Argentina",
        "producción de naftas refinerías",
        "capacidad de refinación nacional",
        "refinerías argentinas",
        "elaboración de gasoil",
        "destilación petróleo refinerías",
    ],
    "inscripciones_iniciales_autos": [
        "patentamientos de autos cero kilómetro",
        "inscripciones iniciales de vehículos",
        "venta de 0km Argentina",
        "marcas más patentadas Argentina",
        "patentamientos por provincia",
        "ranking de autos vendidos Argentina",
        "registro nacional automotor inscripciones",
    ],
    "energia_petroleo_gas_produccion": [
        "producción de petróleo Argentina",
        "producción de gas natural Argentina",
        "Vaca Muerta producción",
        "petróleo por cuenca",
        "producción no convencional petróleo",
        "yacimientos de gas Argentina",
        "evolución producción hidrocarburos",
    ],
    "salarios_por_sector_argentina": [
        "salarios por sector económico Argentina",
        "remuneraciones por rama de actividad",
        "sueldo promedio por industria",
        "ingresos formales por sector",
        "salarios construcción comercio industria",
        "evolución salarial por sector",
    ],
    "caba_presupuesto_ejecutado": [
        "presupuesto ejecutado CABA",
        "gasto del gobierno de la Ciudad",
        "ejecución presupuestaria Buenos Aires",
        "obras ejecutadas CABA presupuesto",
        "partidas ejecutadas Ciudad",
        "devengado presupuesto CABA",
    ],
    "estadistica_mediaciones": [
        "estadísticas de mediaciones",
        "mediaciones por jurisdicción",
        "casos de mediación civil",
        "registro de mediadores Argentina",
        "resoluciones por mediación",
        "actuaciones de mediación",
    ],
    "empleo_registrado_argentina": [
        "empleo registrado Argentina",
        "trabajadores registrados por provincia",
        "evolución del empleo formal",
        "empleo asalariado por sector",
        "puestos de trabajo registrados SIPA",
        "asalariados privados Argentina",
    ],
    "causas_no_penales_judiciales": [
        "causas judiciales no penales",
        "litigios civiles Argentina",
        "expedientes civiles y comerciales",
        "causas de familia tribunales",
        "estadísticas judiciales no penales",
        "causas laborales cantidad",
    ],
    "indicadores_sectoriales_provincia": [
        "indicadores sectoriales por provincia",
        "actividad económica provincial",
        "PBG por provincia",
        "indicadores económicos provinciales",
        "producción provincial Argentina",
        "industria por provincia",
    ],
    "caba_departamentos_en_venta": [
        "departamentos en venta CABA",
        "precio del metro cuadrado Buenos Aires",
        "ofertas inmobiliarias Ciudad",
        "valores propiedad CABA",
        "mercado inmobiliario Buenos Aires",
        "departamentos por barrio precios",
    ],
    "compras_publicas_bac": [
        "compras públicas Buenos Aires Compras",
        "licitaciones BAC",
        "contrataciones del gobierno de la Ciudad",
        "BAC Buenos Aires Compras procesos",
        "proveedores del Estado CABA",
        "adjudicaciones Ciudad de Buenos Aires",
    ],
    "caba_presupuesto_sancionado": [
        "presupuesto sancionado CABA",
        "ley de presupuesto Ciudad",
        "presupuesto aprobado Legislatura porteña",
        "asignaciones presupuesto CABA",
        "crédito original presupuesto Ciudad",
        "presupuesto sancionado Buenos Aires",
    ],
    "exportaciones_argentina": [
        "exportaciones argentinas",
        "comercio exterior exportador",
        "destinos de exportación Argentina",
        "principales productos exportados",
        "exportaciones por país destino",
        "complejo agroexportador",
        "balance comercial exportaciones",
    ],
    "caba_pauta_publicitaria": [
        "pauta publicitaria CABA",
        "publicidad oficial Ciudad de Buenos Aires",
        "gasto en pauta gobierno porteño",
        "medios contratados CABA",
        "pauta oficial publicidad",
        "transparencia publicidad oficial",
    ],
    "mendoza_ejecucion_administracion_central": [
        "ejecución presupuestaria Mendoza",
        "gasto provincia de Mendoza",
        "presupuesto ejecutado Mendoza",
        "administración central Mendoza",
        "partidas ejecutadas gobierno Mendoza",
    ],
    "caba_transportes_autorizados": [
        "transportes autorizados CABA",
        "habilitaciones taxi Buenos Aires",
        "transportes escolares Ciudad",
        "remises autorizados CABA",
        "registro transporte público CABA",
        "vehículos habilitados Ciudad",
    ],
    "presupuesto_nacional_ejecutado": [
        "presupuesto nacional ejecutado",
        "gasto público nacional",
        "ejecución presupuestaria Estado nacional",
        "devengado presupuesto Argentina",
        "ejecución partidas nación",
        "gasto por jurisdicción presupuesto",
    ],
    "inflacion_argentina": [
        "inflación Argentina",
        "IPC Argentina",
        "evolución inflación mensual",
        "índice de precios al consumidor",
        "inflación interanual Argentina",
        "núcleo inflacionario INDEC",
        "rubros de mayor inflación",
    ],
    "caba_terrenos_oferta": [
        "terrenos en venta CABA",
        "oferta de lotes Buenos Aires",
        "valor del suelo Ciudad",
        "mercado de terrenos CABA",
        "terrenos urbanos en venta",
    ],
    "innovacion_industrial": [
        "innovación industrial Argentina",
        "I+D en industria",
        "encuesta nacional de dinámica industrial",
        "actividad innovativa empresas",
        "patentes industriales Argentina",
        "gasto en innovación empresas",
    ],
    "decretos_presidenciales": [
        "decretos presidenciales Argentina",
        "DNU decretos de necesidad y urgencia",
        "decretos firmados por presidente",
        "boletín oficial decretos",
        "decretos del Poder Ejecutivo",
        "DNU vigentes Argentina",
    ],
    "neuquen_ejecucion_presupuestaria": [
        "ejecución presupuestaria Neuquén",
        "presupuesto provincia de Neuquén",
        "gasto Neuquén ejecutado",
        "regalías hidrocarburos Neuquén",
        "presupuesto provincial Neuquén",
    ],
    "geografia_administrativa": [
        "división administrativa Argentina",
        "provincias y departamentos Argentina",
        "geografía política Argentina",
        "límites administrativos",
        "departamentos por provincia",
        "georef datos administrativos",
    ],
    "autoridades_pen": [
        "autoridades del Poder Ejecutivo Nacional",
        "ministros y secretarios PEN",
        "funcionarios del gobierno nacional",
        "directorio del Poder Ejecutivo",
        "estructura del gobierno nacional",
        "altos cargos del PEN",
    ],
    "demografia_caba": [
        "demografía CABA",
        "población Ciudad de Buenos Aires",
        "habitantes por barrio CABA",
        "censo Buenos Aires",
        "estructura poblacional CABA",
        "densidad demográfica Ciudad",
    ],
    "energia_balance_nacional": [
        "balance energético Argentina",
        "matriz energética nacional",
        "consumo energético por sector",
        "oferta y demanda de energía",
        "energías renovables matriz",
        "porcentaje gas natural energía",
    ],
    "legisladores_argentina": [
        "legisladores argentinos",
        "diputados y senadores",
        "padrón legislativo nacional",
        "miembros del Congreso",
        "bloques parlamentarios Argentina",
        "legisladores por provincia",
    ],
    "diputados_ejecucion_presupuestaria": [
        "ejecución presupuestaria Cámara de Diputados",
        "gasto del Congreso ejecutado",
        "presupuesto Diputados",
        "partidas ejecutadas Diputados nación",
        "presupuesto poder legislativo",
    ],
    "indicadores_provinciales": [
        "indicadores provinciales Argentina",
        "ranking de provincias",
        "IDH provincial",
        "comparativa entre provincias",
        "indicadores socioeconómicos provincias",
        "qué provincia tiene mejor empleo",
    ],
    "mujer_centros_atencion": [
        "centros de atención a la mujer",
        "Centros Integrales de la Mujer CIM",
        "violencia de género dónde denunciar",
        "asistencia mujeres víctimas violencia",
        "CIM por barrio CABA",
        "centros mujer GCBA",
    ],
    # ── batch 2026-05-09 night: 4 marts nuevos (RUAGI/ACUMAR/delitos) ──
    "audiencias_gestion_intereses": [
        "audiencias de funcionarios públicos",
        "RUAGI registro de audiencias",
        "lobby al gobierno argentino",
        "reuniones del jefe de gabinete",
        "audiencias del ministro de Economía",
        "qué empresas se reunieron con el gobierno",
        "gestión de intereses Decreto 1172",
        "transparencia audiencias funcionarios",
        "lobbyistas en el Estado nacional",
        "objeto de las audiencias oficiales",
    ],
    "acumar_agentes_contaminantes": [
        "empresas contaminantes Riachuelo",
        "agentes contaminantes Cuenca Matanza",
        "industrias contaminantes Lanús La Matanza",
        "ACUMAR registro contaminantes",
        "polución Riachuelo establecimientos",
        "contaminación industrial AMBA Sur",
        "razones sociales contaminantes Cuenca",
        "fallo Mendoza Beatriz contaminantes",
        "establecimientos vigilados ACUMAR",
        "subcuencas contaminadas Matanza Riachuelo",
    ],
    "delitos_caba": [
        "robos en CABA",
        "delitos por barrio Buenos Aires",
        "homicidios en la Ciudad de Buenos Aires",
        "mapa del delito CABA",
        "uso de armas en delitos Buenos Aires",
        "robos con moto en CABA",
        "delitos por comuna Ciudad",
        "hurtos en Palermo Caballito",
        "franja horaria con más delitos CABA",
        "criminalidad en barrios porteños",
    ],
    "delitos_argentina_snic": [
        "delitos por provincia Argentina",
        "homicidios dolosos por provincia",
        "tasa de criminalidad nacional",
        "víctimas mujeres por departamento",
        "robos por provincia Argentina",
        "SNIC estadísticas criminales",
        "ranking de provincias por delito",
        "evolución del delito en Argentina",
        "delitos por departamento partido",
        "tasa de delitos cada 100 mil habitantes",
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
