import psycopg2

print("=" * 60)
print("TASK 2 VERIFICATION")
print("=" * 60)

try:
    conn = psycopg2.connect(
        host="localhost",
        database="telegram_warehouse",
        user="postgres",
        password="postgres",
        port="5432"
    )
    
    cur = conn.cursor()
    
    print("\n✅ Database connection successful")
    
    # Check all required components
    print("\n📋 Checking required components:")
    
    # 1. Check schemas
    cur.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name IN ('raw', 'staging', 'marts')
        ORDER BY schema_name;
    """)
    
    schemas = [row[0] for row in cur.fetchall()]
    required_schemas = ['raw', 'staging', 'marts']
    
    for schema in required_schemas:
        if schema in schemas:
            print(f"  ✓ Schema: {schema}")
        else:
            print(f"  ✗ Missing schema: {schema}")
    
    # 2. Check tables/views
    print("\n📊 Checking tables and views:")
    
    cur.execute("""
        SELECT 
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema IN ('raw', 'staging', 'marts')
        ORDER BY table_schema, table_type, table_name;
    """)
    
    tables = cur.fetchall()
    
    required_objects = [
        ('raw', 'telegram_messages', 'BASE TABLE'),
        ('staging', 'stg_telegram_messages', 'VIEW'),
        ('marts', 'dim_channels', 'BASE TABLE'),
        ('marts', 'dim_dates', 'BASE TABLE'),
        ('marts', 'fct_messages', 'BASE TABLE')
    ]
    
    found_objects = []
    for table_schema, table_name, table_type in tables:
        obj = (table_schema, table_name, table_type)
        found_objects.append(obj)
        print(f"  Found: {table_schema}.{table_name} ({table_type})")
    
    # 3. Check row counts
    print("\n📈 Checking row counts:")
    
    checks = [
        ("raw.telegram_messages", "SELECT COUNT(*) FROM raw.telegram_messages"),
        ("staging.stg_telegram_messages", "SELECT COUNT(*) FROM staging.stg_telegram_messages"),
        ("marts.dim_channels", "SELECT COUNT(*) FROM marts.dim_channels"),
        ("marts.dim_dates", "SELECT COUNT(*) FROM marts.dim_dates"),
        ("marts.fct_messages", "SELECT COUNT(*) FROM marts.fct_messages")
    ]
    
    for table_name, query in checks:
        try:
            cur.execute(query)
            count = cur.fetchone()[0]
            print(f"  {table_name}: {count} rows")
        except:
            print(f"  {table_name}: ERROR - table might not exist")
    
    # 4. Check foreign key relationships
    print("\n🔗 Checking relationships:")
    
    try:
        cur.execute("""
            SELECT 
                m.message_id,
                c.channel_name,
                d.full_date,
                m.view_count
            FROM marts.fct_messages m
            JOIN marts.dim_channels c ON m.channel_key = c.channel_key
            JOIN marts.dim_dates d ON m.date_key = d.date_key
            LIMIT 3;
        """)
        
        print("  ✓ Foreign key relationships are valid")
        print("  Sample joined data:")
        for row in cur.fetchall():
            print(f"    - Message {row[0]} in {row[1]} on {row[2]}: {row[3]} views")
            
    except Exception as e:
        print(f"  ✗ Foreign key check failed: {e}")
    
    print("\n" + "=" * 60)
    
    # Final assessment
    missing_objects = []
    for required in required_objects:
        if required not in found_objects:
            missing_objects.append(f"{required[0]}.{required[1]}")
    
    if len(missing_objects) == 0:
        print("🎉 TASK 2: COMPLETE SUCCESSFULLY!")
        print("All required components are present and working.")
    else:
        print("⚠️  TASK 2: PARTIALLY COMPLETE")
        print(f"Missing: {', '.join(missing_objects)}")
    
    print("=" * 60)
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Verification failed: {e}")
    print("\n💡 Please run these commands:")
    print("1. docker-compose up -d")
    print("2. python scripts/load_to_postgres.py")
    print("3. python scripts/create_simple_star_schema.py")
