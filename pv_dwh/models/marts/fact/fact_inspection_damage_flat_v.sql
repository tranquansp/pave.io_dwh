{{ config(
    materialized = 'view'
) }}

SELECT
    dr.report_id,
    dr.inspection_id,
    dr.damage_type,
    dr.severity,
    dr.estimated_cost,
    dr.part_affected,
    dr.image_count,
    i.vehicle_vin,
    i.inspector_id,
    i.sk_inspector_id,
    i.location_id,
    i.location_lat,
    i.location_lon,
    i.duration_minutes,
    i.inspection_datetime,
    i.date_key,
    dre.state,
    dre.city_or_county,
    di.NAME,
    di.region,
    di.experience_level,
    di.certification_date,
    di.inspections_completed,
    dv.make,
    dv.model,
    dv.YEAR,
    dv.body_type,
    dv.current_mileage,
    dv.market_value
FROM
    {{ source(
        'raws',
        'damage_reports'
    ) }}
    dr
    JOIN {{ ref('fact_inspection') }} i
    ON i.inspection_id = dr.inspection_id
    LEFT JOIN {{ ref('dim_inspector') }} di
    ON i.sk_inspector_id = di.sk_inspector_id
    LEFT JOIN {{ ref('dim_region') }} dre
    ON i.location_id = dre.location_id
    LEFT JOIN {{ ref('dim_vehicle') }} dv
    ON i.vehicle_vin = dv.vehicle_vin
