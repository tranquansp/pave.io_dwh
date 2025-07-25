{{ config(
   materialized = 'table'
) }}

SELECT
   *
FROM
   {{ source(
      'raws',
      'inspections'
   ) }}
WHERE
   inspection_date IS NULL
   OR vehicle_vin IS NULL
   OR inspector_id IS NULL
   OR location_lat IS NULL
   OR location_lon IS NULL
   OR status IS NULL
   OR duration_minutes IS NULL
