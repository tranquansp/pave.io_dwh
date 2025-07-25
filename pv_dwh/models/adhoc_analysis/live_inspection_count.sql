{{ config(
    materialized = 'table'
) }}

SELECT
  COUNT(*) AS inspection_count_last_hour
FROM {{ source('marts', 'fact_inspection') }}
WHERE inspection_datetime >= NOW() - INTERVAL '1 hour'