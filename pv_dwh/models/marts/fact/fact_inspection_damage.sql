{{ config(
  materialized = 'table'
) }}

SELECT
  dr.report_id,
  dr.inspection_id,
  dr.damage_type,
  dr.severity,
  dr.estimated_cost,
  dr.part_affected,
  dr.image_count,
  i.vehicle_vin,
  i.inspector_id,
  i.sk_inspector_id,
  i.location_id,
  i.location_lat,
  i.location_lon,
  i.inspection_datetime,
  i.date_key
FROM
  {{ source(
    'raws',
    'damage_reports'
  ) }}
  dr
  JOIN {{ ref('fact_inspection') }}
  i
  ON i.inspection_id = dr.inspection_id
