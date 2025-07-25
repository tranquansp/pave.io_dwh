{{ config(
    materialized = 'incremental',
    unique_key = 'inspector_id',
    alias = 'inspector_full'
) }}

SELECT
    inspector_id,
    NAME,
    region,
    experience_level,
    certification_date,
    inspections_completed,
    valid_from,
    valid_to,
    last_data_changed_time
FROM
    {{ ref('inspector_new_records') }}
UNION ALL
SELECT
    inspector_id,
    NAME,
    region,
    experience_level,
    certification_date,
    inspections_completed,
    valid_from,
    valid_to,
    last_data_changed_time
FROM
    {{ ref('inspector_update_changes') }}
