-- Parse-quality diagnostic snapshot.
--
-- Run on staging or prod to quantify parser-related bugs in already-ingested
-- tables. Pair with `specs/021-parser-hardening/` plan.
--
-- Usage (staging):
--   ssh ec2-user@<staging-ip>
--   docker cp scripts/parse_quality_diag.sql openarg_pgbouncer:/tmp/diag.sql
--   docker exec openarg_pgbouncer psql 'postgresql://openarg:<pwd>@<rds>:5432/openarg_staging' -P pager=off -f /tmp/diag.sql
--
-- The 7 metrics map 1:1 to the bugs the parser hardening plan targets.

\echo === A. col_N placeholders (header detection failed → cols got auto-named col_1, col_2, ...) ===
SELECT table_schema,
       COUNT(DISTINCT table_name) AS tables_affected,
       COUNT(*)                   AS placeholder_columns
FROM information_schema.columns
WHERE table_schema IN ('raw','public')
  AND column_name ~ '^col_[0-9]+$'
GROUP BY 1
ORDER BY 1;

\echo === B. Mega-wide tables (>= 200 columns, likely uncollapsed pivot) ===
SELECT table_schema, COUNT(*) AS tables
FROM (
  SELECT table_schema, table_name, COUNT(*) AS n
  FROM information_schema.columns
  WHERE table_schema IN ('raw','public')
  GROUP BY 1, 2
  HAVING COUNT(*) >= 200
) sub
GROUP BY 1
ORDER BY 1;

\echo === C. Minimal tables (<= 3 columns, pure parse fail) ===
SELECT table_schema, COUNT(*) AS tables_minimal
FROM (
  SELECT table_schema, table_name, COUNT(*) AS n
  FROM information_schema.columns
  WHERE table_schema IN ('raw','public')
    AND (table_name LIKE 'cache_%' OR table_name LIKE '%\_\_%' ESCAPE '\')
  GROUP BY 1, 2
  HAVING COUNT(*) <= 3
) sub
GROUP BY 1
ORDER BY 1;

\echo === D. error_category distribution (84% should NOT be unknown) ===
SELECT COALESCE(error_category, 'NULL') AS error_category, COUNT(*) AS n
FROM cached_datasets
GROUP BY 1
ORDER BY 2 DESC
LIMIT 12;

\echo === E. Time-pivot signals (column names look like months/years) ===
SELECT table_schema, COUNT(DISTINCT table_name) AS tables
FROM information_schema.columns
WHERE table_schema IN ('raw','public')
  AND (
    column_name ~* '^(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)'
    OR column_name ~  '^[12][0-9]{3}(-[0-9]{2})?$'
    OR column_name ~* '^(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)[ _-][0-9]{2,4}$'
  )
GROUP BY 1
ORDER BY 1;

\echo === F. Garbage column-name prefixes (titles, URLs, "Páginas:" from PDFs) ===
SELECT
  CASE
    WHEN column_name ~* '^p[áa]ginas?:'  THEN 'paginas:'
    WHEN column_name ~* '^https?:'       THEN 'http_url'
    WHEN column_name ~* '^cuadro'        THEN 'cuadro_titulo'
    ELSE 'other'
  END AS pattern,
  COUNT(*)                  AS columns,
  COUNT(DISTINCT table_name) AS tables
FROM information_schema.columns
WHERE table_schema IN ('raw','public')
  AND (column_name ~* '^p[áa]ginas?:|^https?:|^cuadro')
GROUP BY 1
ORDER BY 2 DESC;

\echo === G. Summary line ===
SELECT
  (SELECT COUNT(DISTINCT table_name) FROM information_schema.columns
   WHERE table_schema IN ('raw','public') AND column_name ~ '^col_[0-9]+$') AS col_n_tables,
  (SELECT COUNT(*) FROM (SELECT table_name FROM information_schema.columns
   WHERE table_schema IN ('raw','public')
   GROUP BY table_schema, table_name HAVING COUNT(*) <= 3) s) AS minimal_tables,
  (SELECT COUNT(*) FROM cached_datasets WHERE error_category = 'unknown') AS unknown_errors,
  NOW() AS captured_at;
