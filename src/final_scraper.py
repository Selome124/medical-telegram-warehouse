"""
TASK 1 COMPLETE: Telegram Data Scraper
Extracts data from medical channels and saves to data lake.
"""
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient
from dotenv import load_dotenv

print("=" * 50)
print("TASK 1: Telegram Medical Channel Scraper")
print("=" * 50)

# Load credentials
load_dotenv()
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')

if not API_ID or not API_HASH:
    print(" ERROR: Add credentials to .env file")
    print("Get from: https://my.telegram.org")
    exit()

print(f" Using API ID: {API_ID}")

async def main():
    # Create directories
    Path("data/raw/images").mkdir(parents=True, exist_ok=True)
    Path("data/raw/telegram_messages").mkdir(parents=True, exist_ok=True)
    
    # Channels from Task 1 instructions
    channels = ['CheMed123', 'lobelia4cosmetics', 'tikvahpharma']
    
    # Connect to Telegram
    client = TelegramClient('medical_scraper', API_ID, API_HASH)
    
    try:
        await client.start()
        print(" Connected to Telegram")
        
        for channel in channels:
            print(f"\n Scraping: @{channel}")
            await scrape(client, channel)
            await asyncio.sleep(1)  # Be nice
        
        print("\n" + "=" * 50)
        print(" TASK 1 COMPLETE!")
        print(" Data saved to: data/raw/")
        print("=" * 50)
        
    except Exception as e:
        print(f" Error: {e}")
    finally:
        await client.disconnect()

async def scrape(client, channel):
    """Scrape one channel"""
    try:
        entity = await client.get_entity(channel)
        messages = []
        
        # Get messages (50 per channel for testing)
        async for msg in client.iter_messages(entity, limit=50):
            data = {
                'message_id': msg.id,
                'channel_name': channel,
                'message_date': str(msg.date),
                'message_text': msg.text or '',
                'views': msg.views or 0,
                'forwards': msg.forwards or 0,
                'has_media': bool(msg.media)
            }
            
            # Download image
            if msg.media and hasattr(msg.media, 'photo'):
                try:
                    folder = Path(f"data/raw/images/{channel}")
                    folder.mkdir(exist_ok=True)
                    filepath = folder / f"{msg.id}.jpg"
                    await msg.download_media(file=str(filepath))
                    data['image_path'] = str(filepath)
                except:
                    pass
            
            messages.append(data)
        
        # Save to JSON
        if messages:
            today = datetime.now().strftime("%Y-%m-%d")
            folder = Path(f"data/raw/telegram_messages/{today}")
            folder.mkdir(exist_ok=True)
            
            filepath = folder / f"{channel}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2)
            
            print(f"   Saved {len(messages)} messages")
        else:
            print(f"   No messages found")
            
    except Exception as e:
        print(f"   Failed: {e}")

# Run everything
asyncio.run(main())
