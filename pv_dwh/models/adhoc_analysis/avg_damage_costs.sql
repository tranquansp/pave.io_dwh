{{ config(
    materialized = 'table'
) }}

SELECT
  fid.damage_type,
  AVG(estimated_cost) AS avg_damage_cost
FROM {{ source('marts', 'fact_inspection_damage') }} fid
GROUP BY fid.damage_type