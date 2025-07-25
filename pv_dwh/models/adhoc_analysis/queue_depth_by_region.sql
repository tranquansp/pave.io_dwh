{{ config(
    materialized = 'table'
) }}

SELECT
  di.region,
  COUNT(*) AS pending_inspections
FROM {{ source('marts', 'fact_inspection') }} fi
JOIN  {{ source('marts', 'dim_inspector') }} di ON fi.sk_inspector_id = di.sk_inspector_id
WHERE fi.status = 'pending_review' AND fi.inspection_datetime >= NOW() - INTERVAL '1 day'
GROUP BY di.region
ORDER BY pending_inspections DESC