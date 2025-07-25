{{ config(
    materialized = 'table'
) }}

SELECT
    DISTINCT vin as vehicle_vin,
    make,
    model,
    YEAR as year,
    body_type,
    current_mileage,
    market_value
FROM
    {{ source(
        'raws',
        'vehicles'
    ) }}
WHERE
    vin IS NOT NULL
