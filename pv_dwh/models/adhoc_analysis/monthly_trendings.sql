{{ config(
    materialized = 'table'
) }}

SELECT
  DATE_TRUNC('month', fi.inspection_datetime) AS inspection_month,
  COUNT( DISTINCT inspection_id) AS total_inspections,
  SUM(fi.estimated_cost) AS total_estimated_cost,
  AVG(fi.estimated_cost) AS avg_estimated_cost
FROM {{ source('marts', 'fact_inspection_damage') }} fi
GROUP BY inspection_month
ORDER BY inspection_month
