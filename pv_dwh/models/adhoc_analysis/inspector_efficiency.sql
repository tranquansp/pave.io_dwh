{{ config(
    materialized = 'table'
) }}

SELECT
  di.NAME AS inspector_name,
  COUNT( DISTINCT inspection_id) AS total_inspections,
  COUNT(DISTINCT fi.inspection_datetime::date) AS work_days,
  ROUND(COUNT(*)::NUMERIC / NULLIF(COUNT(DISTINCT fi.inspection_datetime::date), 0), 2) AS inspections_per_day
FROM {{ source('marts', 'fact_inspection_damage') }} fi
JOIN {{ source('marts', 'dim_inspector') }} di ON fi.inspector_id = di.inspector_id
GROUP BY di.NAME
ORDER BY inspections_per_day DESC

