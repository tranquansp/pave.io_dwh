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
    vehicle_vin IS NULL
    OR LENGTH(vehicle_vin) != 17
    OR vehicle_vin ~ '[^A-Z0-9]'
