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
   duration_minutes IS NULL
   OR duration_minutes < 5
   OR duration_minutes > 120
