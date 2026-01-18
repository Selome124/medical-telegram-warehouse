import psycopg2
import time

def create_star_schema():
    print("=" * 60)
    print("TASK 2: Creating Star Schema")
    print("=" * 60)
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="telegram_warehouse",
                user="postgres",
                password="postgres",
                port="5432"
            )
            print("✅ Connected to PostgreSQL")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                print("Waiting 5 seconds before retry...")
                time.sleep(5)
            else:
                print(f"❌ Failed to connect after {max_retries} attempts: {e}")
                print("\n💡 Please make sure:")
                print("1. Run: docker-compose up -d")
                print("2. Wait 30 seconds for PostgreSQL to start")
                return
    
    try:
        conn.autocommit = True
        cur = conn.cursor()
        
        print("\n📋 Step 1: Creating schemas...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS marts;")
        print("✅ Schemas created")
        
        print("\n📋 Step 2: Creating staging view...")
        staging_sql = '''
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
            END as has_image
        FROM raw.telegram_messages
        WHERE message_text IS NOT NULL 
          AND message_text != ''
          AND message_date IS NOT NULL;
        '''
        cur.execute(staging_sql)
        print("✅ Staging view created")
        
        print("\n📋 Step 3: Creating dim_channels...")
        dim_channels_sql = '''
        CREATE TABLE IF NOT EXISTS marts.dim_channels (
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
        GROUP BY channel_name
        ON CONFLICT (channel_name) DO UPDATE SET
            last_post_date = EXCLUDED.last_post_date,
            total_posts = EXCLUDED.total_posts,
            avg_views = EXCLUDED.avg_views,
            avg_forwards = EXCLUDED.avg_forwards,
            total_images = EXCLUDED.total_images,
            loaded_at = CURRENT_TIMESTAMP;
        '''
        cur.execute(dim_channels_sql)
        print("✅ dim_channels created")
        
        print("\n📋 Step 4: Creating dim_dates...")
        dim_dates_sql = '''
        CREATE TABLE IF NOT EXISTS marts.dim_dates (
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
            SELECT CURRENT_DATE
        ) dates
        ON CONFLICT (date_key) DO NOTHING;
        '''
        cur.execute(dim_dates_sql)
        print("✅ dim_dates created")
        
        print("\n📋 Step 5: Creating fct_messages...")
        fct_messages_sql = '''
        CREATE TABLE IF NOT EXISTS marts.fct_messages (
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
            CONCAT(stg.message_id::text, '-', dc.channel_key::text) as message_key,
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
        LEFT JOIN marts.dim_dates dd ON stg.message_date_date = dd.full_date
        ON CONFLICT (message_key) DO NOTHING;
        '''
        cur.execute(fct_messages_sql)
        print("✅ fct_messages created")
        
        print("\n📋 Step 6: Creating indexes...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fct_channel ON marts.fct_messages(channel_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fct_date ON marts.fct_messages(date_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fct_image ON marts.fct_messages(has_image);")
        print("✅ Indexes created")
        
        # Show results
        print("\n" + "=" * 60)
        print("📊 RESULTS:")
        print("=" * 60)
        
        cur.execute("SELECT 'stg_telegram_messages' as table, COUNT(*) FROM staging.stg_telegram_messages")
        print(f"  staging.stg_telegram_messages: {cur.fetchone()[1]} rows")
        
        cur.execute("SELECT 'dim_channels' as table, COUNT(*) FROM marts.dim_channels")
        print(f"  marts.dim_channels: {cur.fetchone()[1]} rows")
        
        cur.execute("SELECT 'dim_dates' as table, COUNT(*) FROM marts.dim_dates")
        print(f"  marts.dim_dates: {cur.fetchone()[1]} rows")
        
        cur.execute("SELECT 'fct_messages' as table, COUNT(*) FROM marts.fct_messages")
        print(f"  marts.fct_messages: {cur.fetchone()[1]} rows")
        
        print("\n" + "=" * 60)
        print("🎉 STAR SCHEMA CREATED SUCCESSFULLY!")
        print("=" * 60)
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating star schema: {e}")

if __name__ == "__main__":
    create_star_schema()
