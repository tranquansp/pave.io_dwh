{{ config(
    materialized = 'table'
) }}

SELECT
  dv.make AS vehicle_make,
  fid.damage_type,
  COUNT(*) AS damage_count
FROM {{ source('marts', 'fact_inspection_damage') }} fid
JOIN {{ source('marts', 'dim_vehicle') }} dv ON fid.vehicle_vin = dv.vehicle_vin
GROUP BY dv.make, fid.damage_type
ORDER BY damage_count DESC
LIMIT 10