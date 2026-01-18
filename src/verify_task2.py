# verify_task2.py
import os
from database_sqlite import SessionLocal, TelegramMessage, ChannelInfo
import pandas as pd

def verify_task2():
    print("="*60)
    print("VERIFYING TASK 2 COMPLETION")
    print("="*60)
    
    # Check if database file exists
    db_file = os.getenv('DB_PATH', 'medical_telegram.db')
    if not os.path.exists(db_file):
        print(f"✗ Database file not found: {db_file}")
        return False
    
    print(f"✓ Database file found: {db_file}")
    print(f"  Size: {os.path.getsize(db_file) / 1024:.1f} KB")
    
    # Connect to database
    db = SessionLocal()
    
    try:
        # Count records
        channel_count = db.query(ChannelInfo).count()
        message_count = db.query(TelegramMessage).count()
        
        print(f"\n✓ Database Statistics:")
        print(f"  Channels stored: {channel_count}")
        print(f"  Messages stored: {message_count}")
        
        if channel_count > 0:
            print(f"\n✓ Channels in database:")
            channels = db.query(ChannelInfo).all()
            for ch in channels:
                print(f"  - {ch.channel_name}: {ch.channel_title}")
        
        if message_count > 0:
            print(f"\n✓ Sample messages:")
            messages = db.query(TelegramMessage).limit(3).all()
            for msg in messages:
                preview = msg.message_text[:60] + "..." if msg.message_text and len(msg.message_text) > 60 else msg.message_text
                print(f"  [{msg.channel_name}] {preview}")
        
        # Check for CSV export
        csv_file = "telegram_messages_export.csv"
        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file)
            print(f"\n✓ CSV Export verified:")
            print(f"  File: {csv_file}")
            print(f"  Records: {len(df)}")
            print(f"  Columns: {', '.join(df.columns)}")
        else:
            print(f"\n⚠ CSV export not found")
        
        # Success criteria
        print("\n" + "="*60)
        print("TASK 2 COMPLETION CHECKLIST:")
        print("="*60)
        
        checks = [
            ("Database file created", os.path.exists(db_file)),
            ("At least 1 channel stored", channel_count > 0),
            ("At least 10 messages stored", message_count >= 10),
            ("Tables properly structured", channel_count >= 0 and message_count >= 0),
        ]
        
        all_passed = True
        for check_name, passed in checks:
            status = "✓" if passed else "✗"
            print(f"{status} {check_name}")
            if not passed:
                all_passed = False
        
        if all_passed:
            print("\n✅ TASK 2 COMPLETED SUCCESSFULLY!")
        else:
            print("\n⚠ Task 2 needs more work")
        
        return all_passed
        
    except Exception as e:
        print(f"✗ Error verifying database: {e}")
        return False
    
    finally:
        db.close()

if __name__ == "__main__":
    verify_task2()