-- TASK 2: Create Star Schema Manually
-- This creates all the tables without needing dbt

-- Step 1: Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Step 2: Create staging view
DROP VIEW IF EXISTS staging.stg_telegram_messages CASCADE;
CREATE OR REPLACE VIEW staging.stg_telegram_messages AS
SELECT 
    message_id,
    channel_name,
    message_date::timestamp as message_date,
    DATE(message_date) as message_date_date,
    message_text,
    LENGTH(message_text) as message_length,
    COALESCE(views, 0) as views,
    COALESCE(forwards, 0) as forwards,
    has_media,
    CASE 
        WHEN image_path IS NOT NULL AND image_path != '' THEN TRUE 
        ELSE FALSE 
    END as has_image,
    CURRENT_TIMESTAMP as transformed_at
FROM raw.telegram_messages
WHERE message_text IS NOT NULL 
  AND message_text != ''
  AND message_date IS NOT NULL;

-- Step 3: Create dim_channels table
DROP TABLE IF EXISTS marts.dim_channels CASCADE;
CREATE TABLE marts.dim_channels (
    channel_key SERIAL PRIMARY KEY,
    channel_name VARCHAR(100) UNIQUE NOT NULL,
    channel_type VARCHAR(50),
    first_post_date TIMESTAMP,
    last_post_date TIMESTAMP,
    total_posts INTEGER,
    avg_views DECIMAL(10,2),
    avg_forwards DECIMAL(10,2),
    total_images INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO marts.dim_channels (
    channel_name, channel_type, first_post_date, last_post_date, 
    total_posts, avg_views, avg_forwards, total_images
)
SELECT 
    channel_name,
    CASE 
        WHEN channel_name ILIKE '%pharma%' THEN 'Pharmaceutical'
        WHEN channel_name ILIKE '%cosmetic%' THEN 'Cosmetics'
        ELSE 'Medical'
    END as channel_type,
    MIN(message_date) as first_post_date,
    MAX(message_date) as last_post_date,
    COUNT(*) as total_posts,
    ROUND(AVG(views)::numeric, 2) as avg_views,
    ROUND(AVG(forwards)::numeric, 2) as avg_forwards,
    SUM(CASE WHEN has_image THEN 1 ELSE 0 END) as total_images
FROM staging.stg_telegram_messages
GROUP BY channel_name;

-- Step 4: Create dim_dates table
DROP TABLE IF EXISTS marts.dim_dates CASCADE;
CREATE TABLE marts.dim_dates (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generate dates from data
INSERT INTO marts.dim_dates (
    date_key, full_date, day_of_week, day_name, week_of_year,
    month, month_name, quarter, year, is_weekend
)
SELECT 
    TO_CHAR(dates.full_date, 'YYYYMMDD')::integer as date_key,
    dates.full_date,
    EXTRACT(DOW FROM dates.full_date) as day_of_week,
    TO_CHAR(dates.full_date, 'Day') as day_name,
    EXTRACT(WEEK FROM dates.full_date) as week_of_year,
    EXTRACT(MONTH FROM dates.full_date) as month,
    TO_CHAR(dates.full_date, 'Month') as month_name,
    EXTRACT(QUARTER FROM dates.full_date) as quarter,
    EXTRACT(YEAR FROM dates.full_date) as year,
    CASE WHEN EXTRACT(DOW FROM dates.full_date) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
FROM (
    SELECT DISTINCT message_date_date as full_date
    FROM staging.stg_telegram_messages
    UNION 
    SELECT CURRENT_DATE  -- Add today if not in data
) dates;

-- Step 5: Create fct_messages table
DROP TABLE IF EXISTS marts.fct_messages CASCADE;
CREATE TABLE marts.fct_messages (
    message_key VARCHAR(100) PRIMARY KEY,
    message_id INTEGER NOT NULL,
    channel_key INTEGER REFERENCES marts.dim_channels(channel_key),
    date_key INTEGER REFERENCES marts.dim_dates(date_key),
    message_text TEXT,
    message_length INTEGER,
    view_count INTEGER,
    forward_count INTEGER,
    has_image BOOLEAN,
    message_date TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO marts.fct_messages (
    message_key, message_id, channel_key, date_key, 
    message_text, message_length, view_count, forward_count,
    has_image, message_date
)
SELECT 
    CONCAT(stg.message_id, '-', dc.channel_key) as message_key,
    stg.message_id,
    dc.channel_key,
    dd.date_key,
    stg.message_text,
    stg.message_length,
    stg.views as view_count,
    stg.forwards as forward_count,
    stg.has_image,
    stg.message_date
FROM staging.stg_telegram_messages stg
LEFT JOIN marts.dim_channels dc ON stg.channel_name = dc.channel_name
LEFT JOIN marts.dim_dates dd ON stg.message_date_date = dd.full_date;

-- Step 6: Create indexes for performance
CREATE INDEX idx_fct_messages_channel ON marts.fct_messages(channel_key);
CREATE INDEX idx_fct_messages_date ON marts.fct_messages(date_key);
CREATE INDEX idx_fct_messages_has_image ON marts.fct_messages(has_image);

-- Step 7: Show results
SELECT '✅ Star schema created successfully!' as status;

SELECT 
    'stg_telegram_messages' as table_name,
    COUNT(*) as row_count
FROM staging.stg_telegram_messages
UNION ALL
SELECT 'dim_channels', COUNT(*) FROM marts.dim_channels
UNION ALL
SELECT 'dim_dates', COUNT(*) FROM marts.dim_dates
UNION ALL
SELECT 'fct_messages', COUNT(*) FROM marts.fct_messages
ORDER BY table_name;
