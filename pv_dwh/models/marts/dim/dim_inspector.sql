{{ config(
    materialized = 'table'
) }}

SELECT
    *
FROM
    {{ source(
        'transitives',
        'inspector_scd'
    ) }}
