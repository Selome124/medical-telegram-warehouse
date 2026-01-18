-- Simple staging model for testing
SELECT 
    message_id,
    channel_name,
    message_date::timestamp,
    message_text,
    COALESCE(views, 0) as views,
    COALESCE(forwards, 0) as forwards,
    has_media,
    CASE 
        WHEN image_path IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as has_image
FROM raw.telegram_messages
WHERE message_text IS NOT NULL
  AND message_date IS NOT NULL
