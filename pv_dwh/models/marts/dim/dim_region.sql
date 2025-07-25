{{ config(
    materialized = 'table'
) }}

SELECT
    DISTINCT MD5(
        city_or_county || state
    ) AS location_id,
    lat,
    lon,
    city_or_county,
    state
FROM
    {{ source(
        'raws',
        'geocode_city'
    ) }}
    gc
