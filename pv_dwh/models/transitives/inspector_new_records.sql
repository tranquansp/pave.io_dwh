{{ config(
    materialized = 'table'
) }}

{% set scd_exists = adapter.get_relation(
    database = target.database,
    schema = target.schema,
    identifier = 'dim_inspector_scd'
) is not none %}
WITH snapshot AS (

    SELECT
        *
    FROM
        {{ source(
            'raws',
            'inspectors'
        ) }}
) {% if scd_exists %},
    CURRENT AS (
        SELECT
            *
        FROM
            {{ ref('inspector_scd') }}
    )
{% else %},
    CURRENT AS (
        SELECT
            NULL AS inspector_id
        LIMIT
            0
    )
{% endif %}
SELECT
    s.inspector_id,
    s.name,
    s.region,
    s.experience_level,
    CAST(
        s.certification_date AS DATE
    ) AS certification_date,
    s.inspections_completed,
    '2000-01-01' :: TIMESTAMP AS valid_from,
    '3000-12-31' :: TIMESTAMP AS valid_to,
    CURRENT_TIMESTAMP AS last_data_changed_time
FROM
    snapshot s
    LEFT JOIN CURRENT C
    ON s.inspector_id = C.inspector_id
WHERE
    C.inspector_id IS NULL
