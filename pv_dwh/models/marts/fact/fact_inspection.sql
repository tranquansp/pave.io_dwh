{{ config(
  materialized = 'table'
) }}

SELECT
  i.inspection_id,
  d.sk_inspector_id,
  i.vehicle_vin,
  i.inspector_id,
  CAST(
    i.inspection_date AS TIMESTAMP
  ) AS inspection_datetime,
  to_char(CAST(i.inspection_date AS TIMESTAMP), 'YYYYMMDD') :: INT AS date_key,
  i.status,
  i.duration_minutes,
  i.location_lat,
  i.location_lon,
  r.location_id
FROM
  {{ source(
    'raws',
    'inspections'
  ) }}
  i
  LEFT JOIN {{ ref('dim_inspector') }}
  d
  ON i.inspector_id = d.inspector_id
  AND CAST(
    i.inspection_date AS TIMESTAMP
  ) >= d.valid_from
  AND CAST(
    i.inspection_date AS TIMESTAMP
  ) < d.valid_to
  LEFT JOIN {{ ref('dim_region') }}
  r
  ON ROUND(
    i.location_lat :: numeric,
    1
  ) :: numeric(
    4,
    1
  ) = r.lat :: numeric(
    4,
    1
  )
  AND ROUND(
    i.location_lon :: numeric,
    1
  ) :: numeric(
    4,
    1
  ) = r.lon :: numeric(
    4,
    1
  )
