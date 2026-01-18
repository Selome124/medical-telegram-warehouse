"""
Task 2: Load raw JSON data into PostgreSQL
Loads Telegram messages from data lake into database
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="telegram_warehouse",
            user="postgres",
            password="postgres",
            port="5432"
        )
        print("✅ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def create_raw_schema(conn):
    """Create raw schema and table"""
    with conn.cursor() as cur:
        # Create raw schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        
        # Create raw.telegram_messages table
        create_table_sql = '''
        DROP TABLE IF EXISTS raw.telegram_messages;
        CREATE TABLE raw.telegram_messages (
            id SERIAL PRIMARY KEY,
            message_id INTEGER,
            channel_name VARCHAR(100),
            message_date TIMESTAMP,
            message_text TEXT,
            views INTEGER,
            forwards INTEGER,
            has_media BOOLEAN,
            image_path VARCHAR(500),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file VARCHAR(255)
        );
        '''
        cur.execute(create_table_sql)
        print("✅ Created raw.telegram_messages table")
        
        conn.commit()

def find_json_files():
    """Find all JSON files in data lake"""
    json_files = []
    base_path = Path("data/raw/telegram_messages")
    
    if not base_path.exists():
        print("❌ No data found in data lake. Run Task 1 scraper first.")
        return []
    
    # Find all JSON files
    for json_file in base_path.rglob("*.json"):
        json_files.append(json_file)
    
    print(f"📁 Found {len(json_files)} JSON files")
    return json_files

def load_json_file(conn, json_file):
    """Load a single JSON file into database"""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            print(f"  ⚠ Empty file: {json_file}")
            return 0
        
        # Prepare data for insertion
        records = []
        for item in data:
            # Parse date if exists
            message_date = None
            if item.get('message_date'):
                try:
                    # Try different date formats
                    date_str = item['message_date']
                    if 'T' in date_str:
                        message_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    else:
                        message_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                except:
                    message_date = None
            
            record = (
                item.get('message_id'),
                item.get('channel_name', 'unknown'),
                message_date,
                item.get('message_text', ''),
                item.get('views', 0),
                item.get('forwards', 0),
                item.get('has_media', False),
                item.get('image_path'),
                str(json_file)  # source_file
            )
            records.append(record)
        
        # Insert data
        with conn.cursor() as cur:
            insert_sql = '''
            INSERT INTO raw.telegram_messages 
            (message_id, channel_name, message_date, message_text, views, forwards, has_media, image_path, source_file)
            VALUES %s
            '''
            execute_values(cur, insert_sql, records)
        
        print(f"  ✅ Loaded {len(records)} records from {json_file.name}")
        return len(records)
        
    except Exception as e:
        print(f"  ❌ Error loading {json_file}: {e}")
        return 0

def main():
    """Main function to load data"""
    print("=" * 60)
    print("TASK 2: Load Raw Data to PostgreSQL")
    print("=" * 60)
    
    # Connect to database
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # Create schema and table
        create_raw_schema(conn)
        
        # Find JSON files
        json_files = find_json_files()
        if not json_files:
            print("\n💡 Creating sample data for testing...")
            create_sample_data()
            json_files = find_json_files()
        
        # Load each file
        total_records = 0
        print(f"\n📤 Loading {len(json_files)} files into database...")
        
        for json_file in json_files:
            records_loaded = load_json_file(conn, json_file)
            total_records += records_loaded
        
        # Commit transaction
        conn.commit()
        
        # Show summary
        print(f"\n📊 LOADING COMPLETE!")
        print(f"   Total files: {len(json_files)}")
        print(f"   Total records: {total_records}")
        
        # Verify data
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
            count = cur.fetchone()[0]
            print(f"   Records in database: {count}")
            
            cur.execute("SELECT DISTINCT channel_name FROM raw.telegram_messages;")
            channels = cur.fetchall()
            print(f"   Channels loaded: {', '.join([c[0] for c in channels])}")
        
        print("\n✅ Raw data successfully loaded to PostgreSQL!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("Database connection closed")

def create_sample_data():
    """Create sample data if no real data exists"""
    sample_dir = Path("data/raw/telegram_messages/sample")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    sample_data = [
        {
            "message_id": 101,
            "channel_name": "CheMed123",
            "message_date": "2026-01-18T10:30:00",
            "message_text": "Medical products available: Paracetamol, Ibuprofen",
            "views": 150,
            "forwards": 5,
            "has_media": True,
            "image_path": "data/raw/images/CheMed123/101.jpg"
        },
        {
            "message_id": 102,
            "channel_name": "CheMed123",
            "message_date": "2026-01-18T11:45:00",
            "message_text": "New shipment of antibiotics arriving tomorrow",
            "views": 89,
            "forwards": 3,
            "has_media": False
        },
        {
            "message_id": 201,
            "channel_name": "lobelia4cosmetics",
            "message_date": "2026-01-18T09:15:00",
            "message_text": "Skincare products 50% off this week",
            "views": 200,
            "forwards": 12,
            "has_media": True,
            "image_path": "data/raw/images/lobelia4cosmetics/201.jpg"
        }
    ]
    
    sample_file = sample_dir / "sample_messages.json"
    with open(sample_file, 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, indent=2)
    
    print(f"  Created sample data: {sample_file}")

if __name__ == "__main__":
    main()
