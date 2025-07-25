{{ config(
    materialized = 'view'
) }}

WITH ordered_versions AS (

    SELECT
        *,
        LEAD(valid_from) over (
            PARTITION BY inspector_id
            ORDER BY
                valid_from ASC
        ) AS next_valid_from
    FROM
        {{ ref('inspector_full') }}
)
SELECT
    inspector_id || '---' || MD5(
        inspector_id || '|' || NAME || '|' || region || '|' || experience_level || '|' || certification_date || '|' || inspections_completed || '|' || valid_from
    ) AS sk_inspector_id,
    inspector_id,
    NAME,
    region,
    experience_level,
    certification_date,
    inspections_completed,
    valid_from,
    COALESCE(
        next_valid_from,
        '9999-12-31' :: TIMESTAMP
    ) AS valid_to,
    last_data_changed_time
FROM
    ordered_versions
