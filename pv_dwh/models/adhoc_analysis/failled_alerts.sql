{{ config(
    materialized = 'table'
) }}

SELECT
  di.NAME,
  count(DISTINCT fi.inspection_id) as total_ins
FROM {{ source('marts', 'fact_inspection') }} fi
JOIN {{ source('marts', 'dim_inspector') }} di ON fi.sk_inspector_id = di.sk_inspector_id
WHERE fi.status = 'failed' AND fi.inspection_datetime >= NOW() - INTERVAL '1 hour'
GROUP BY di.NAME
HAVING count(DISTINCT fi.inspection_id) > 10