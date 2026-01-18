"""
SIMPLE WORKING TASK 1 SCRAPER
Minimal version that actually works
"""
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
import time

print("=" * 60)
print("TASK 1: Telegram Medical Channel Scraper")
print("=" * 60)

async def main():
    # Load credentials
    load_dotenv()
    api_id = int(os.getenv('TELEGRAM_API_ID'))
    api_hash = os.getenv('TELEGRAM_API_HASH')
    
    print(f"📱 API ID: {api_id}")
    
    # Create directories
    Path("data/raw/images").mkdir(parents=True, exist_ok=True)
    Path("data/raw/telegram_messages").mkdir(parents=True, exist_ok=True)
    
    # Create client with existing session (from test)
    client = TelegramClient('test_session', api_id, api_hash)
    
    try:
        # Connect using existing session
        await client.connect()
        if not await client.is_user_authorized():
            print("❌ Not logged in. Run test_connection.py first!")
            return
        
        print("✅ Connected using saved session")
        
        # Channels to scrape
        channels = ['CheMed123']
        
        for channel in channels:
            print(f"\n📊 Scraping: @{channel}")
            await scrape_safe(client, channel)
        
        print("\n" + "=" * 60)
        print("🎉 TASK 1 PARTIALLY COMPLETE!")
        print("Check data/raw/ folder for results")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await client.disconnect()

async def scrape_safe(client, channel_name):
    """Safe scraping with error handling"""
    try:
        entity = await client.get_entity(channel_name)
        messages = []
        
        print(f"  Getting messages from {channel_name}...")
        
        # Get only 30 messages to avoid timeouts
        count = 0
        async for msg in client.iter_messages(entity, limit=30):
            try:
                data = {
                    'message_id': msg.id,
                    'channel_name': channel_name,
                    'message_date': str(msg.date),
                    'message_text': msg.text or '',
                    'views': msg.views or 0,
                    'forwards': msg.forwards or 0,
                    'has_media': bool(msg.media)
                }
                
                # Try to download image (but skip if fails)
                if msg.media and hasattr(msg.media, 'photo'):
                    try:
                        folder = Path(f"data/raw/images/{channel_name}")
                        folder.mkdir(exist_ok=True)
                        filepath = folder / f"{msg.id}.jpg"
                        # Quick download with timeout
                        await asyncio.wait_for(
                            msg.download_media(file=str(filepath)),
                            timeout=5.0
                        )
                        data['image_path'] = str(filepath)
                    except:
                        pass  # Skip image if download fails
                
                messages.append(data)
                count += 1
                
                if count % 10 == 0:
                    print(f"  Processed {count} messages...")
                    
            except Exception as e:
                print(f"  Skipping message {msg.id}: {e}")
                continue
        
        # Save to JSON
        if messages:
            today = datetime.now().strftime("%Y-%m-%d")
            folder = Path(f"data/raw/telegram_messages/{today}")
            folder.mkdir(exist_ok=True)
            
            filepath = folder / f"{channel_name}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2)
            
            print(f"  ✅ Saved {len(messages)} messages to {filepath}")
            
            # Count images
            img_folder = Path(f"data/raw/images/{channel_name}")
            if img_folder.exists():
                image_count = len(list(img_folder.glob("*.jpg")))
                print(f"  📸 Downloaded {image_count} images")
        else:
            print(f"  ⚠ No messages saved")
            
    except FloodWaitError as e:
        print(f"  ⏳ Rate limited. Wait {e.seconds} seconds")
        time.sleep(e.seconds)
        await scrape_safe(client, channel_name)
    except Exception as e:
        print(f"  ❌ Failed: {e}")

# Run
if __name__ == "__main__":
    asyncio.run(main())
