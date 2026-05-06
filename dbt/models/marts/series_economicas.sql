-- MASTERPLAN Fase 5 — series_economicas mart (dbt-managed example).
--
-- This is the dbt counterpart of `config/marts/series_economicas.yaml`.
-- Both can coexist: pick one or the other depending on whether you
-- want dbt's lineage/tests on top, or stick with the YAML-driven
-- Celery task. Don't run both at the same time on the same target
-- table or you'll race materializations.

{{ config(
    materialized='table',
    schema='mart',
    unique_key='serie_id || fecha::text',
    indexes=[
        {'columns': ['serie_id', 'fecha'], 'unique': true},
        {'columns': ['fecha']},
    ]
) }}

SELECT
    -- canonical column names match `config/marts/series_economicas.yaml`
    'bcra:' || COALESCE(s.serie_id, 'unknown') AS serie_id,
    s.fecha::date                              AS fecha,
    s.valor::float                             AS valor,
    'porcentaje'::text                         AS unidad,
    'BCRA'::text                               AS fuente
FROM {{ source('staging', 'series_economicas__bcra') }} AS s
WHERE s.fecha IS NOT NULL
  AND s.valor IS NOT NULL
