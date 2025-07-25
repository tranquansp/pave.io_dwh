{{ config(
    materialized = 'table'
) }}

SELECT
  dr.state,
  dr.city_or_county,
  dr.lat,
  dr.lon,
  COUNT( DISTINCT inspection_id) AS inspection_count,
  AVG(fi.estimated_cost) AS avg_estimated_cost,
  MAX(fi.estimated_cost) AS max_estimated_cost,
  MIN(fi.estimated_cost) AS min_estimated_cost
FROM {{ source('marts', 'fact_inspection_damage') }} fi
JOIN {{ source('marts', 'dim_region') }} dr ON fi.location_id = dr.location_id
GROUP BY dr.state, dr.city_or_county, dr.lat, dr.lon
HAVING COUNT(*) > 5
ORDER BY avg_estimated_cost DESC