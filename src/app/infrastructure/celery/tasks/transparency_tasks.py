"""
Transparency Worker — Tareas de análisis de transparencia y calidad de datos.

Feature 1: Índice de Salud de Datos Abiertos (scoring por dataset/portal)
Feature 2: Monitor de Datos Fantasma (freshness/staleness)
Feature 3: Detector de Anomalías Patrimoniales (DDJJ)
Feature 4: Análisis Temático de Sesiones Parlamentarias
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Feature 1 & 2: Dataset Health Index + Ghost Data Monitor
# ---------------------------------------------------------------------------

# Formats considered machine-readable
_MACHINE_READABLE_FORMATS = {"csv", "json", "geojson", "xml", "xlsx", "xls", "parquet", "shp"}


def _is_meaningful(value) -> bool:
    """Check if a metadata field has meaningful content (not empty/null/bare JSON)."""
    if not value:
        return False
    s = str(value).strip()
    return bool(s) and s not in ("[]", "{}", "null", "None", "")


def _score_single_dataset(row) -> dict:
    """Score a single dataset on 6 quality dimensions (0.0 to 1.0 each)."""
    now = datetime.now(UTC)

    # 1. Freshness: how recently updated vs expected frequency
    freshness = 0.0
    freshness_status = "unknown"
    ts = row.last_updated_at
    if ts:
        age_days = (now - ts.replace(tzinfo=UTC if ts.tzinfo is None else ts.tzinfo)).days
        # Default expected frequency: 90 days
        expected = 90
        if age_days <= expected:
            freshness = 1.0
            freshness_status = "FRESH"
        elif age_days <= expected * 2:
            freshness = max(0.3, 1.0 - (age_days - expected) / expected)
            freshness_status = "STALE"
        else:
            freshness = 0.0
            freshness_status = "ABANDONED"
    else:
        freshness_status = "unknown"

    # 2. Accessibility: dataset has a download URL
    accessibility = 1.0 if row.download_url else 0.0

    # 3. Machine readability: format is machine-readable
    fmt = (row.format or "").lower().strip()
    machine_readability = 1.0 if fmt in _MACHINE_READABLE_FORMATS else 0.0

    # 4. Completeness: how many metadata fields have meaningful content
    fields = [row.title, row.description, row.organization, row.tags, row.columns]
    filled = sum(1 for f in fields if _is_meaningful(f))
    completeness = filled / len(fields)

    # 5. License: open license present (we can't check from datasets table directly,
    #    but all CKAN gov portals are public data — score based on portal being gov)
    license_score = 1.0  # All 10 portals are government open data

    # 6. Contactability: organization info present
    contactability = 1.0 if row.organization and str(row.organization).strip() else 0.0

    # Overall: weighted average (freshness matters most for transparency)
    overall = (
        freshness * 0.30
        + accessibility * 0.15
        + machine_readability * 0.15
        + completeness * 0.20
        + license_score * 0.05
        + contactability * 0.15
    )

    return {
        "freshness_score": round(freshness, 3),
        "accessibility_score": round(accessibility, 3),
        "machine_readability_score": round(machine_readability, 3),
        "completeness_score": round(completeness, 3),
        "license_score": round(license_score, 3),
        "contactability_score": round(contactability, 3),
        "overall_score": round(overall, 3),
        "freshness_status": freshness_status,
    }


@celery_app.task(name="openarg.score_portal_health", bind=True, max_retries=2, soft_time_limit=300, time_limit=360)
def score_portal_health(self, portal: str | None = None):
    """
    Score dataset health for all datasets (or a specific portal).
    Replaces previous scores for the same batch.
    """
    engine = get_sync_engine()
    try:
        query = """
            SELECT CAST(id AS text) AS id, title, description, organization,
                   portal, download_url, format, columns, tags, last_updated_at
            FROM datasets
        """
        params: dict = {}
        if portal:
            query += " WHERE portal = :portal"
            params["portal"] = portal

        with engine.begin() as conn:
            rows = conn.execute(text(query), params).fetchall()

        if not rows:
            logger.info("No datasets to score%s", f" for {portal}" if portal else "")
            return {"scored": 0}

        scored = 0
        portal_stats: dict[str, list[float]] = defaultdict(list)

        with engine.begin() as conn:
            # Clear previous scores for this batch
            if portal:
                conn.execute(
                    text("DELETE FROM dataset_health_scores WHERE portal = :p"),
                    {"p": portal},
                )
            else:
                conn.execute(text("DELETE FROM dataset_health_scores"))

            for row in rows:
                scores = _score_single_dataset(row)
                conn.execute(
                    text("""
                        INSERT INTO dataset_health_scores
                            (dataset_id, portal, freshness_score, accessibility_score,
                             machine_readability_score, completeness_score, license_score,
                             contactability_score, overall_score, freshness_status)
                        VALUES
                            (CAST(:did AS uuid), :portal, :fs, :as_, :mr, :cs, :ls, :co, :os, :fst)
                    """),
                    {
                        "did": row.id, "portal": row.portal,
                        "fs": scores["freshness_score"],
                        "as_": scores["accessibility_score"],
                        "mr": scores["machine_readability_score"],
                        "cs": scores["completeness_score"],
                        "ls": scores["license_score"],
                        "co": scores["contactability_score"],
                        "os": scores["overall_score"],
                        "fst": scores["freshness_status"],
                    },
                )
                portal_stats[row.portal].append(scores["overall_score"])
                scored += 1

        # Log portal averages
        for p, scores_list in sorted(portal_stats.items()):
            avg = sum(scores_list) / len(scores_list) if scores_list else 0
            abandoned = sum(1 for s in scores_list if s < 0.3)
            logger.info(
                "Portal %s: avg_score=%.2f, datasets=%d, abandoned=%d",
                p, avg, len(scores_list), abandoned,
            )

        logger.info("Scored %d datasets across %d portals", scored, len(portal_stats))
        return {"scored": scored, "portals": len(portal_stats)}

    except SoftTimeLimitExceeded:
        logger.error("Health scoring timed out")
        raise
    except Exception as exc:
        logger.exception("Health scoring failed")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Feature 3: DDJJ Anomaly Detector (ajustado por inflación y tipo de cambio)
# ---------------------------------------------------------------------------

_DDJJ_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "ddjj_dataset.json"

# Constantes económicas Argentina 2024
TC_PROMEDIO_2024 = 900.0        # ARS/USD promedio anual 2024
INFLACION_ANUAL_2024 = 2.11     # 211% inflación acumulada 2024
UMBRAL_VARIACION_USD = 50_000   # Variaciones reales < USD 50K no son significativas
UMBRAL_BRECHA_USD = 100_000     # Brecha inexplicable mínima para flaggear


@celery_app.task(name="openarg.detect_ddjj_anomalies", bind=True, max_retries=2, soft_time_limit=300, time_limit=360)
def detect_ddjj_anomalies(self):
    """
    Analiza las DDJJ patrimoniales con métricas ajustadas a la realidad argentina:
    - Variación real descontando inflación
    - Conversión a USD para evaluar magnitud real
    - Brecha inexplicable (variación real - ingreso neto)
    - Percentil de patrimonio entre pares
    """
    engine = get_sync_engine()
    try:
        raw = _DDJJ_PATH.read_text(encoding="utf-8")
        dataset = json.loads(raw)
        logger.info("Loaded %d DDJJ records for anomaly analysis", len(dataset))
    except Exception:
        logger.exception("Failed to load DDJJ dataset from %s", _DDJJ_PATH)
        return {"error": "Could not load DDJJ data"}

    try:
        # Pre-compute percentiles for patrimonio_cierre across all records
        patrimonios = [
            float(r.get("patrimonioCierre", 0) or 0) for r in dataset
        ]
        patrimonios_sorted = sorted(patrimonios)
        n_total = len(patrimonios_sorted)

        def _percentil(valor: float) -> float:
            """Percentile rank of valor within all patrimonio_cierre values."""
            if n_total == 0:
                return 0.0
            count_below = sum(1 for v in patrimonios_sorted if v < valor)
            return round((count_below / max(n_total - 1, 1)) * 100, 1)

        anomalies = []

        for record in dataset:
            bienes_inicio = float(record.get("bienesInicio", 0) or 0)
            bienes_cierre = float(record.get("bienesCierre", 0) or 0)
            deudas_inicio = float(record.get("deudasInicio", 0) or 0)
            deudas_cierre = float(record.get("deudasCierre", 0) or 0)
            patrimonio_cierre = float(record.get("patrimonioCierre", 0) or 0)
            ingresos = float(record.get("ingresosTrabajoNeto", 0) or 0)
            gastos = float(record.get("gastosPersonales", 0) or 0)

            # --- Métricas ajustadas ---

            # 1. Variación real (descontando inflación)
            variacion_nominal = bienes_cierre - bienes_inicio
            variacion_por_inflacion = bienes_inicio * INFLACION_ANUAL_2024
            variacion_real = variacion_nominal - variacion_por_inflacion

            # 2. Conversión a USD
            variacion_real_usd = variacion_real / TC_PROMEDIO_2024

            # 3. Brecha real inexplicable
            ingreso_neto = ingresos - gastos
            brecha_real = variacion_real - ingreso_neto
            brecha_real_usd = brecha_real / TC_PROMEDIO_2024

            # 4. Ratio real (solo si hay ingresos y variación real positiva)
            ratio_real = (variacion_real / ingresos) if (ingresos > 0 and variacion_real > 0) else 0.0

            # 5. Percentil de patrimonio entre pares
            percentil = _percentil(patrimonio_cierre)

            # --- Clasificación ---
            # Variación bruta (para DB, compatibilidad)
            variacion = variacion_nominal
            brecha = variacion - ingreso_neto
            ratio = variacion / ingresos if ingresos > 0 else 0.0

            is_anomalous = False
            anomaly_type = None
            severity = None

            # Filtro previo: si la variación real es <= 0 o < USD 50K, no es anomalía
            if variacion_real > 0 and variacion_real_usd >= UMBRAL_VARIACION_USD:
                # HIGH
                if ratio_real > 10 and brecha_real_usd > 500_000:
                    is_anomalous = True
                    anomaly_type = "crecimiento_excesivo"
                    severity = "high"
                elif ingresos <= 0 and variacion_real_usd > 100_000:
                    is_anomalous = True
                    anomaly_type = "crecimiento_sin_ingresos"
                    severity = "high"
                # MEDIUM
                elif ratio_real > 5 and brecha_real_usd > UMBRAL_BRECHA_USD:
                    is_anomalous = True
                    anomaly_type = "crecimiento_excesivo"
                    severity = "medium"
                elif (
                    bienes_inicio == 0
                    and (patrimonio_cierre / TC_PROMEDIO_2024) > 200_000
                    and ratio_real > 3
                ):
                    is_anomalous = True
                    anomaly_type = "patrimonio_aparece"
                    severity = "medium"
                # LOW
                elif ratio_real > 3 and brecha_real_usd > UMBRAL_VARIACION_USD:
                    is_anomalous = True
                    anomaly_type = "crecimiento_elevado"
                    severity = "low"

            details = {
                "deudas_inicio": deudas_inicio,
                "deudas_cierre": deudas_cierre,
                "ingreso_neto": ingreso_neto,
                "bienes_count": len(record.get("bienes") or []),
                "variacion_real": round(variacion_real, 2),
                "variacion_real_usd": round(variacion_real_usd, 2),
                "brecha_real": round(brecha_real, 2),
                "brecha_real_usd": round(brecha_real_usd, 2),
                "ratio_real": round(ratio_real, 2),
                "percentil_patrimonio": percentil,
                "tc_usado": TC_PROMEDIO_2024,
                "inflacion_usada": INFLACION_ANUAL_2024,
            }

            anomalies.append({
                "cuit": record.get("cuit", ""),
                "nombre": record.get("nombre", ""),
                "anio": str(record.get("anioDeclaracion", "")),
                "patrimonio_cierre": patrimonio_cierre,
                "bienes_inicio": bienes_inicio,
                "bienes_cierre": bienes_cierre,
                "ingresos": ingresos,
                "gastos": gastos,
                "variacion": variacion,
                "brecha": brecha,
                "ratio": round(ratio, 2),
                "is_anomalous": is_anomalous,
                "anomaly_type": anomaly_type,
                "severity": severity,
                "details": details,
            })

        # Store in DB
        flagged = 0
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM ddjj_anomalies"))

            for a in anomalies:
                conn.execute(
                    text("""
                        INSERT INTO ddjj_anomalies
                            (cuit, nombre, anio_declaracion, patrimonio_cierre,
                             bienes_inicio, bienes_cierre, ingresos_trabajo_neto,
                             gastos_personales, variacion_patrimonial,
                             brecha_inexplicable, ratio_crecimiento,
                             is_anomalous, anomaly_type, severity, details_json)
                        VALUES
                            (:cuit, :nombre, :anio, :pat, :bi, :bc, :ing, :gas,
                             :var, :brecha, :ratio, :anom, :tipo, :sev, :det)
                    """),
                    {
                        "cuit": a["cuit"], "nombre": a["nombre"], "anio": a["anio"],
                        "pat": a["patrimonio_cierre"], "bi": a["bienes_inicio"],
                        "bc": a["bienes_cierre"], "ing": a["ingresos"],
                        "gas": a["gastos"], "var": a["variacion"],
                        "brecha": a["brecha"], "ratio": a["ratio"],
                        "anom": a["is_anomalous"], "tipo": a["anomaly_type"],
                        "sev": a["severity"],
                        "det": json.dumps(a["details"], ensure_ascii=False),
                    },
                )
                if a["is_anomalous"]:
                    flagged += 1

        logger.info(
            "DDJJ anomaly analysis complete: %d records, %d flagged "
            "(inflation=%.0f%%, tc=%.0f ARS/USD)",
            len(anomalies), flagged, INFLACION_ANUAL_2024 * 100, TC_PROMEDIO_2024,
        )
        return {"total": len(anomalies), "flagged": flagged}

    except SoftTimeLimitExceeded:
        logger.error("DDJJ anomaly detection timed out")
        raise
    except Exception as exc:
        logger.exception("DDJJ anomaly detection failed")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Feature 4: Session Topic Analysis
# ---------------------------------------------------------------------------

# Taxonomía de temas parlamentarios argentinos (keywords normalizados)
TOPIC_TAXONOMY: dict[str, list[str]] = {
    "economia": [
        "presupuesto", "gasto público", "deuda", "política fiscal", "impuesto",
        "inflación", "dólar", "bcra", "banco central", "subsidio",
        "tarifa", "precio", "exportación", "importación", "pbi",
        "recaudación", "déficit", "superávit", "financiamiento",
        "inversión", "economía", "económic", "tributari",
    ],
    "seguridad": [
        "seguridad", "policía", "policial", "delito", "crimen",
        "narcotráfico", "droga", "fuerzas armadas", "defensa",
        "penitenciar", "cárcel", "prisión", "femicidio",
        "violencia", "arma", "terrorismo",
    ],
    "educacion": [
        "educación", "escuela", "universidad", "docente", "maestr",
        "alumno", "estudiant", "pedagóg", "curricular", "beca",
        "investigación científica", "conicet", "ciencia",
        "tecnología", "innovación",
    ],
    "salud": [
        "salud", "hospital", "médic", "sanitari", "vacuna",
        "epidemia", "pandemia", "medicamento", "obra social",
        "prepaga", "enfermedad", "mortalidad", "natalidad",
        "discapacidad", "mental",
    ],
    "trabajo": [
        "trabajo", "empleo", "desempleo", "salar", "sindicat",
        "convenio colectivo", "jubilación", "pensión", "anses",
        "previsional", "retiro", "laboral", "trabajador",
        "paritaria", "gremial",
    ],
    "justicia": [
        "justicia", "judicial", "juez", "jueza", "ministerio público fiscal",
        "tribunal", "corte suprema", "constitución", "constitucional",
        "amparo", "recurso", "penal", "civil", "procesal",
        "reforma judicial", "ministerio público",
    ],
    "derechos_humanos": [
        "derechos humanos", "género", "diversidad", "igualdad",
        "discriminación", "pueblos originarios", "indígena",
        "migrante", "refugiad", "identidad de género",
        "interrupción", "aborto", "memoria", "dictadura",
    ],
    "medio_ambiente": [
        "ambiente", "ambiental", "cambio climático", "contaminación",
        "residuo", "reciclaje", "minería", "hidrocarburo",
        "energía renovable", "deforestación", "biodiversidad",
        "humedal", "agua", "río",
    ],
    "infraestructura": [
        "obra pública", "infraestructura", "transporte", "ruta",
        "autopista", "ferrocarril", "tren", "aeropuerto",
        "puerto", "vivienda", "urbanismo", "vialidad",
        "construcción", "licitación",
    ],
    "politica_institucional": [
        "elección", "electoral", "voto", "partido",
        "congreso", "senado", "diputado", "legislat",
        "decreto", "reglamento", "comisión", "reforma",
        "federalismo", "coparticipación", "autonomía",
        "república", "democracia",
    ],
    "relaciones_exteriores": [
        "relaciones exteriores", "cancillería", "tratado",
        "mercosur", "internacional", "embajad", "diplomátic",
        "soberanía", "malvinas", "fmi", "naciones unidas",
    ],
    "agro": [
        "agropecuari", "campo", "rural", "agrícola", "ganader",
        "cosecha", "soja", "trigo", "maíz", "retenciones",
        "inta", "senasa", "fitosanitar",
    ],
}


def _classify_text(text_content: str) -> dict[str, int]:
    """Classify text by counting keyword matches per topic category."""
    text_lower = text_content.lower()
    counts: dict[str, int] = {}
    for category, keywords in TOPIC_TAXONOMY.items():
        count = 0
        for kw in keywords:
            count += len(re.findall(re.escape(kw), text_lower))
        if count > 0:
            counts[category] = count
    return counts


@celery_app.task(name="openarg.analyze_session_topics", bind=True, max_retries=2, soft_time_limit=300, time_limit=360)
def analyze_session_topics(self):
    """
    Analiza los chunks de sesiones parlamentarias por tema.
    Agrupa por sesión (periodo + reunion) y genera distribución temática.
    """
    chunks_dir = Path(__file__).resolve().parent.parent.parent / "data" / "chunks"
    engine = get_sync_engine()

    try:
        # Load all session chunks from JSON files
        chunk_files = sorted(chunks_dir.glob("*.json"))
        if not chunk_files:
            logger.warning("No session chunk files found in %s", chunks_dir)
            return {"error": "No chunk files found"}

        all_chunks: list[dict] = []
        for f in chunk_files:
            data = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(data, list):
                all_chunks.extend(data)
            else:
                all_chunks.append(data)

        logger.info("Loaded %d session chunks from %d files", len(all_chunks), len(chunk_files))

        # Group chunks by session (periodo + reunion)
        sessions: dict[tuple[int, int], list[dict]] = defaultdict(list)
        for chunk in all_chunks:
            key = (chunk.get("periodo", 0), chunk.get("reunion", 0))
            sessions[key].append(chunk)

        results = []
        for (periodo, reunion), chunks in sessions.items():
            # Session metadata from first chunk
            meta = chunks[0]
            fecha = meta.get("fecha", "")
            tipo_sesion = meta.get("tipoSesion", "")

            # Aggregate topic counts across all chunks in this session
            session_topics: dict[str, int] = Counter()
            speaker_topics: dict[str, dict[str, int]] = defaultdict(Counter)
            topic_excerpts: dict[str, list[str]] = defaultdict(list)

            for chunk in chunks:
                chunk_text = chunk.get("text") or ""
                speaker = chunk.get("speaker") or "Desconocido"
                if not chunk_text:
                    continue
                counts = _classify_text(chunk_text)

                for cat, count in counts.items():
                    session_topics[cat] += count
                    speaker_topics[cat][speaker] += count
                    # Keep a short excerpt for context
                    if len(topic_excerpts[cat]) < 3:
                        excerpt = chunk_text[:200].strip()
                        if excerpt:
                            topic_excerpts[cat].append(excerpt)

            # Compute relevance scores (normalized by total mentions)
            total_mentions = sum(session_topics.values()) or 1

            _TOPIC_DISPLAY_NAMES: dict[str, str] = {
                "economia": "Economía",
                "seguridad": "Seguridad",
                "educacion": "Educación",
                "salud": "Salud",
                "trabajo": "Trabajo",
                "justicia": "Justicia",
                "derechos_humanos": "Derechos Humanos",
                "medio_ambiente": "Medio Ambiente",
                "infraestructura": "Infraestructura",
                "politica_institucional": "Política Institucional",
                "relaciones_exteriores": "Relaciones Exteriores",
                "agro": "Agro",
            }

            for cat, count in session_topics.items():
                top_speakers = dict(
                    sorted(speaker_topics[cat].items(), key=lambda x: x[1], reverse=True)[:5]
                )
                results.append({
                    "periodo": periodo,
                    "reunion": reunion,
                    "fecha": fecha,
                    "tipo_sesion": tipo_sesion,
                    "topic_name": _TOPIC_DISPLAY_NAMES.get(cat, cat.replace("_", " ").title()),
                    "topic_category": cat,
                    "mention_count": count,
                    "relevance_score": round(count / total_mentions, 4),
                    "top_speakers": top_speakers,
                    "excerpts": topic_excerpts.get(cat, []),
                })

        # Store in DB
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM session_topics"))

            for r in results:
                conn.execute(
                    text("""
                        INSERT INTO session_topics
                            (periodo, reunion, fecha, tipo_sesion, topic_name,
                             topic_category, mention_count, relevance_score,
                             top_speakers_json, sample_excerpts_json)
                        VALUES
                            (:per, :reu, :fecha, :tipo, :tn, :tc, :mc, :rs, :ts, :ex)
                    """),
                    {
                        "per": r["periodo"], "reu": r["reunion"],
                        "fecha": r["fecha"], "tipo": r["tipo_sesion"],
                        "tn": r["topic_name"], "tc": r["topic_category"],
                        "mc": r["mention_count"],
                        "rs": r["relevance_score"],
                        "ts": json.dumps(r["top_speakers"], ensure_ascii=False),
                        "ex": json.dumps(r["excerpts"], ensure_ascii=False),
                    },
                )

        logger.info(
            "Session topic analysis complete: %d sessions, %d topic entries",
            len(sessions), len(results),
        )
        return {"sessions_analyzed": len(sessions), "topic_entries": len(results)}

    except SoftTimeLimitExceeded:
        logger.error("Session topic analysis timed out")
        raise
    except Exception as exc:
        logger.exception("Session topic analysis failed")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()
