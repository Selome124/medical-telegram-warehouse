"""
SIMPLE TASK 1 TEST - Minimal version
"""
import asyncio
from telethon import TelegramClient
import os
from dotenv import load_dotenv

async def test_connection():
    """Test if we can connect to Telegram"""
    print("Testing Telegram connection...")
    
    load_dotenv()
    api_id = int(os.getenv('TELEGRAM_API_ID'))
    api_hash = os.getenv('TELEGRAM_API_HASH')
    
    print(f"API ID: {api_id}")
    print(f"API Hash: {api_hash[:8]}...")
    
    # Try to connect
    client = TelegramClient('test_session', api_id, api_hash)
    
    try:
        print("\n Starting connection...")
        print("You will need to:")
        print("1. Enter your phone number (+251XXXXXXXXX)")
        print("2. Enter the code from Telegram")
        print("3. Enter your 2FA password (if you have one)")
        print()
        
        await client.start(
            phone=lambda: input("Phone number: "),
            code_callback=lambda: input("Code: "),
            password=lambda: input("Password (press Enter if none): ") or None
        )
        
        print("\n SUCCESS! Connected to Telegram")
        
        # Test scraping one channel
        print("\nTesting channel access...")
        try:
            channel = await client.get_entity('CheMed123')
            print(f" Can access channel: {channel.title}")
            
            # Get a few messages
            count = 0
            async for msg in client.iter_messages(channel, limit=5):
                print(f"  Message {msg.id}: {msg.text[:50]}..." if msg.text else "  [No text]")
                count += 1
            
            print(f"\n Successfully read {count} messages")
            
        except Exception as e:
            print(f" Channel access failed: {e}")
        
        await client.disconnect()
        print("\n Test completed successfully!")
        print("You can now run the full scraper.")
        
    except Exception as e:
        print(f"\n Connection failed: {e}")
        print("\n Troubleshooting:")
        print("1. Check your internet connection")
        print("2. Make sure API credentials are correct")
        print("3. Enter correct 2FA password")
        print("4. Try again in a few minutes")

if __name__ == "__main__":
    asyncio.run(test_connection())
