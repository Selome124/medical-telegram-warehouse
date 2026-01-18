{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH date_range AS (
    SELECT 
        MIN(DATE(message_date)) AS start_date,
        MAX(DATE(message_date)) AS end_date
    FROM {{ ref('stg_telegram_messages') }}
),

all_dates AS (
    SELECT 
        generate_series(
            (SELECT start_date FROM date_range),
            (SELECT end_date FROM date_range),
            '1 day'::interval
        )::date AS full_date
)

SELECT 
    -- Surrogate key (YYYYMMDD format)
    TO_CHAR(full_date, 'YYYYMMDD')::integer AS date_key,
    
    -- Date
    full_date,
    
    -- Day information
    EXTRACT(DAY FROM full_date) AS day_of_month,
    EXTRACT(DOW FROM full_date) AS day_of_week,
    INITCAP(TO_CHAR(full_date, 'Day')) AS day_name,
    
    -- Week information
    EXTRACT(WEEK FROM full_date) AS week_of_year,
    EXTRACT(ISOYEAR FROM full_date) || '-W' || LPAD(EXTRACT(WEEK FROM full_date)::text, 2, '0') AS week_key,
    
    -- Month information
    EXTRACT(MONTH FROM full_date) AS month_number,
    INITCAP(TO_CHAR(full_date, 'Month')) AS month_name,
    TO_CHAR(full_date, 'Mon') AS month_abbr,
    
    -- Quarter information
    EXTRACT(QUARTER FROM full_date) AS quarter_number,
    'Q' || EXTRACT(QUARTER FROM full_date) AS quarter_name,
    
    -- Year information
    EXTRACT(YEAR FROM full_date) AS year,
    
    -- Flags
    CASE 
        WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend,
    
    CASE 
        WHEN full_date = CURRENT_DATE THEN TRUE 
        ELSE FALSE 
    END AS is_current_day,
    
    -- Additional useful fields
    TO_CHAR(full_date, 'YYYY-MM') AS year_month,
    TO_CHAR(full_date, 'YYYY-Q') || EXTRACT(QUARTER FROM full_date) AS year_quarter,
    
    CURRENT_TIMESTAMP AS loaded_at
    
FROM all_dates
ORDER BY full_date
