import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="telegram_warehouse", 
        user="postgres",
        password="postgres",
        port="5432"
    )
    
    with conn.cursor() as cur:
        # Check raw table
        cur.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
        raw_count = cur.fetchone()[0]
        print(f"📋 Raw messages in database: {raw_count}")
        
        # Show sample
        if raw_count > 0:
            cur.execute("""
                SELECT channel_name, COUNT(*) as count
                FROM raw.telegram_messages 
                GROUP BY channel_name 
                ORDER BY count DESC;
            """)
            print("\n📈 Messages by channel:")
            for row in cur.fetchall():
                print(f"  • {row[0]}: {row[1]} messages")
        
    conn.close()
    
except Exception as e:
    print(f"❌ Error checking database: {e}")
    print("Make sure PostgreSQL is running: docker-compose up -d")
