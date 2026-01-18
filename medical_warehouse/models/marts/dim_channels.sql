{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH channel_stats AS (
    SELECT 
        channel_name,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*) AS total_posts,
        AVG(views) AS avg_views,
        AVG(forwards) AS avg_forwards,
        SUM(CASE WHEN has_image THEN 1 ELSE 0 END) AS total_images,
        COUNT(DISTINCT DATE(message_date)) AS active_days
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
),

channel_enriched AS (
    SELECT 
        channel_name,
        first_post_date,
        last_post_date,
        total_posts,
        ROUND(avg_views::numeric, 2) AS avg_views,
        ROUND(avg_forwards::numeric, 2) AS avg_forwards,
        total_images,
        active_days,
        CASE 
            WHEN channel_name ILIKE '%pharma%' THEN 'Pharmaceutical'
            WHEN channel_name ILIKE '%cosmetic%' THEN 'Cosmetics'
            WHEN channel_name ILIKE '%med%' OR channel_name ILIKE '%health%' THEN 'Medical'
            ELSE 'Other'
        END AS channel_type,
        ROUND(total_posts::numeric / NULLIF(active_days, 0), 2) AS avg_posts_per_day
    FROM channel_stats
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} AS channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views,
    avg_forwards,
    total_images,
    active_days,
    avg_posts_per_day,
    CURRENT_TIMESTAMP AS loaded_at
FROM channel_enriched
