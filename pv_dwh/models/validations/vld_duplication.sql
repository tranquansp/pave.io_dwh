{{ config(
   materialized = 'table'
) }}

SELECT
   vehicle_vin,
   inspection_date,
   COUNT(*) AS COUNT
FROM
   {{ source(
      'raws',
      'inspections'
   ) }}
GROUP BY
   vehicle_vin,
   inspection_date
HAVING
   COUNT(*) > 1
