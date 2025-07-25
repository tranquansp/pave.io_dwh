{{ config(
    materialized = 'table'
) }}

{% set scd_exists = adapter.get_relation(
    database = target.database,
    schema = target.schema,
    identifier = 'dim_inspector_scd'
) is not none %}
WITH CURRENT AS ({% if scd_exists %}

    SELECT
        *
    FROM
        {{ ref('inspector_scd') }}
    WHERE
        valid_to = '3000-12-31'
    {% else %}
    SELECT
        NULL :: text AS inspector_id, NULL :: text AS NAME, NULL :: text AS region, NULL :: text AS experience_level, NULL :: DATE AS certification_date, NULL :: INTEGER AS inspections_completed, NULL :: TIMESTAMP AS valid_from, NULL :: TIMESTAMP AS valid_to, NULL :: TIMESTAMP AS last_data_changed_time
    WHERE
        FALSE
    {% endif %}),
    raw_data AS (
        SELECT
            *
        FROM
            {{ source(
                'raws',
                'inspectors'
            ) }}
    )
SELECT
    r.inspector_id,
    r.name,
    r.region,
    r.experience_level,
    CAST(
        r.certification_date AS DATE
    ) AS certification_date,
    r.inspections_completed,
    CURRENT_TIMESTAMP AS valid_from,
    '3000-12-31' :: TIMESTAMP AS valid_to,
    CURRENT_TIMESTAMP AS last_data_changed_time
FROM
    raw_data r
    JOIN CURRENT f
    ON r.inspector_id = f.inspector_id
WHERE
    r.name IS DISTINCT
FROM
    f.name
    OR r.region IS DISTINCT
FROM
    f.region
    OR r.experience_level IS DISTINCT
FROM
    f.experience_level
    OR CAST(
        r.certification_date AS DATE
    ) IS DISTINCT
FROM
    f.certification_date
    OR r.inspections_completed IS DISTINCT
FROM
    f.inspections_completed
