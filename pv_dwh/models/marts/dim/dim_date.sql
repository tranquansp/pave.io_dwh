{{ config(
    materialized = 'table'
) }}

WITH date_range AS (

    SELECT
        generate_series(
            DATE '2020-01-01',
            DATE '2027-01-01',
            INTERVAL '1 day'
        ) :: DATE AS DATE
),
dim_date AS (
    SELECT
        to_char(
            DATE,
            'YYYYMMDD'
        ) AS date_key,
        DATE,
        EXTRACT(
            YEAR
            FROM
                DATE
        ) :: INT AS YEAR,
        EXTRACT(
            quarter
            FROM
                DATE
        ) :: INT AS quarter,
        EXTRACT(
            MONTH
            FROM
                DATE
        ) :: INT AS MONTH,
        EXTRACT(
            DAY
            FROM
                DATE
        ) :: INT AS DAY,
        EXTRACT(
            week
            FROM
                DATE
        ) :: INT AS week_num,
        DATE_TRUNC(
            'week',
            DATE
        ) :: DATE AS start_of_week,
        (DATE_TRUNC('week', DATE) + INTERVAL '6 days') :: DATE AS end_of_week,
        EXTRACT(
            dow
            FROM
                DATE
        ) :: INT + 1 AS day_of_week,
        -- 1 = Monday
        to_char(
            DATE,
            'YYYY "W"IW'
        ) AS week_of_year,
        CASE
            WHEN to_char(
                DATE,
                'IYYY-IW'
            ) = to_char(
                CURRENT_DATE,
                'IYYY-IW'
            ) THEN 'CURRENT_WEEK'
            WHEN to_char(
                DATE,
                'IYYY-IW'
            ) = to_char(
                CURRENT_DATE - INTERVAL '7 day',
                'IYYY-IW'
            ) THEN 'LAST_WEEK'
            ELSE to_char(
                DATE,
                '"W"IW-YYYY'
            )
        END AS current_week_condition,
        CASE
            WHEN to_char(
                DATE,
                'YYYYMM'
            ) = to_char(
                CURRENT_DATE,
                'YYYYMM'
            ) THEN 'CURRENT_MONTH'
            WHEN to_char(
                DATE,
                'YYYYMM'
            ) = to_char(
                CURRENT_DATE - INTERVAL '1 month',
                'YYYYMM'
            ) THEN 'LAST_MONTH'
            ELSE to_char(
                DATE,
                'MM-YYYY'
            )
        END AS current_month_condition,
        CASE
            WHEN EXTRACT(
                YEAR
                FROM
                    DATE
            ) = EXTRACT(
                YEAR
                FROM
                    CURRENT_DATE
            ) THEN 'CURRENT_YEAR'
            WHEN EXTRACT(
                YEAR
                FROM
                    DATE
            ) = EXTRACT(
                YEAR
                FROM
                    CURRENT_DATE
            ) - 1 THEN 'LAST_YEAR'
            ELSE to_char(
                DATE,
                'YYYY'
            )
        END AS current_year_condition,
        CASE
            WHEN EXTRACT(
                YEAR
                FROM
                    DATE
            ) = EXTRACT(
                YEAR
                FROM
                    CURRENT_DATE
            )
            AND DATE <= (
                DATE_TRUNC(
                    'month',
                    CURRENT_DATE + INTERVAL '1 month'
                ) + INTERVAL '1 month - 1 day'
            ) :: DATE THEN 'Y'
            ELSE 'N'
        END AS ytd_next_month,
        CASE
            WHEN EXTRACT(
                YEAR
                FROM
                    DATE
            ) = EXTRACT(
                YEAR
                FROM
                    CURRENT_DATE
            )
            AND DATE <= (
                DATE_TRUNC(
                    'month',
                    CURRENT_DATE + INTERVAL '2 month'
                ) + INTERVAL '1 month - 1 day'
            ) :: DATE THEN 'Y'
            ELSE 'N'
        END AS ytd_next_2_months,
        CASE
            WHEN EXTRACT(
                YEAR
                FROM
                    DATE
            ) = EXTRACT(
                YEAR
                FROM
                    CURRENT_DATE
            )
            AND DATE <= (
                DATE_TRUNC(
                    'month',
                    CURRENT_DATE + INTERVAL '3 month'
                ) + INTERVAL '1 month - 1 day'
            ) :: DATE THEN 'Y'
            ELSE 'N'
        END AS ytd_next_3_months,
        CASE
            WHEN EXTRACT(
                dow
                FROM
                    DATE
            ) IN (
                0,
                6
            ) THEN 'Y'
            ELSE 'N'
        END AS is_weekend,
        CASE
            WHEN DATE = (DATE_TRUNC('month', DATE) + INTERVAL '1 month - 1 day') :: DATE THEN 'Y'
            ELSE 'N'
        END AS is_last_day_of_month,
        'N' AS is_holiday -- Placeholder, bạn có thể cập nhật sau bằng cách JOIN với dim_holiday
    FROM
        date_range
)
SELECT
    *
FROM
    dim_date
