"""
Run SQL script to create star schema
"""
import psycopg2
import sys

def run_sql_file(filename):
    """Execute SQL from file"""
    print("=" * 60)
    print("TASK 2: Creating Star Schema Manually")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host="localhost",
            database="telegram_warehouse",
            user="postgres",
            password="postgres",
            port="5432"
        )
        conn.autocommit = True  # Allow DDL commands
        
        # Read and execute SQL
        with open(filename, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        with conn.cursor() as cur:
            # Split by semicolons but be careful with $$
            commands = []
            current_command = ""
            in_dollar_quote = False
            
            for line in sql.split('\n'):
                if '$$' in line:
                    in_dollar_quote = not in_dollar_quote
                current_command += line + '\n'
                if not in_dollar_quote and line.strip().endswith(';'):
                    commands.append(current_command.strip())
                    current_command = ""
            
            # Execute each command
            for i, cmd in enumerate(commands, 1):
                if cmd.strip():
                    try:
                        cur.execute(cmd)
                        print(f"✅ Step {i} executed")
                    except Exception as e:
                        print(f"⚠️  Step {i}: {e}")
        
        # Show final results
        print("\n" + "=" * 60)
        print("📊 FINAL RESULTS:")
        print("=" * 60)
        
        with conn.cursor() as cur:
            # Get table counts
            cur.execute("""
                SELECT 'stg_telegram_messages' as table, COUNT(*) as rows 
                FROM staging.stg_telegram_messages
                UNION ALL
                SELECT 'dim_channels', COUNT(*) FROM marts.dim_channels
                UNION ALL
                SELECT 'dim_dates', COUNT(*) FROM marts.dim_dates
                UNION ALL
                SELECT 'fct_messages', COUNT(*) FROM marts.fct_messages
                ORDER BY table;
            """)
            
            for table, count in cur.fetchall():
                print(f"  {table}: {count} rows")
            
            # Show sample from each table
            print("\n🔍 Sample data from each table:")
            
            print("\n1. dim_channels:")
            cur.execute("SELECT channel_name, channel_type, total_posts FROM marts.dim_channels LIMIT 3;")
            for row in cur.fetchall():
                print(f"   • {row[0]} ({row[1]}): {row[2]} posts")
            
            print("\n2. dim_dates:")
            cur.execute("SELECT full_date, day_name, is_weekend FROM marts.dim_dates ORDER BY full_date LIMIT 3;")
            for row in cur.fetchall():
                print(f"   • {row[0]} ({row[1]}): Weekend={row[2]}")
            
            print("\n3. fct_messages:")
            cur.execute("""
                SELECT m.message_id, c.channel_name, m.view_count, m.has_image 
                FROM marts.fct_messages m
                JOIN marts.dim_channels c ON m.channel_key = c.channel_key
                LIMIT 3;
            """)
            for row in cur.fetchall():
                print(f"   • Message {row[0]} in {row[1]}: {row[2]} views, Has image={row[3]}")
        
        conn.close()
        print("\n" + "=" * 60)
        print("🎉 STAR SCHEMA CREATED SUCCESSFULLY!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Make sure:")
        print("1. PostgreSQL is running: docker-compose up -d")
        print("2. Database 'telegram_warehouse' exists")
        print("3. Raw data is loaded (run scripts/load_to_postgres.py)")

if __name__ == "__main__":
    run_sql_file("scripts/create_star_schema_simple.sql")
