{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH messages_with_keys AS (
    SELECT 
        -- Fact table primary key (message_id is unique per channel)
        stg.message_id,
        
        -- Foreign keys
        dc.channel_key,
        dd.date_key,
        
        -- Message content
        stg.message_text,
        stg.message_length,
        stg.message_length_category,
        
        -- Engagement metrics
        stg.views AS view_count,
        stg.forwards AS forward_count,
        
        -- Media information
        stg.has_media,
        stg.has_image,
        
        -- Engagement level
        stg.engagement_level,
        
        -- Original dates (for reference)
        stg.message_date,
        
        -- Audit columns
        stg.loaded_at,
        stg.transformed_at
        
    FROM {{ ref('stg_telegram_messages') }} stg
    LEFT JOIN {{ ref('dim_channels') }} dc 
        ON stg.channel_name = dc.channel_name
    LEFT JOIN {{ ref('dim_dates') }} dd 
        ON DATE(stg.message_date) = dd.full_date
)

SELECT 
    -- Composite primary key
    {{ dbt_utils.generate_surrogate_key(['message_id', 'channel_key']) }} AS message_key,
    
    -- Business keys
    message_id,
    channel_key,
    date_key,
    
    -- Attributes
    message_text,
    message_length,
    message_length_category,
    
    -- Measures
    view_count,
    forward_count,
    
    -- Flags
    has_media,
    has_image,
    engagement_level,
    
    -- Date references
    message_date,
    
    -- Audit trail
    loaded_at,
    transformed_at,
    CURRENT_TIMESTAMP AS fact_loaded_at
    
FROM messages_with_keys
