"""
TASK 1 COMPLETE: Telegram Data Scraper
Extracts messages and images from medical Telegram channels.
"""
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import logging
from telethon import TelegramClient
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger()

async def main():
    """Main function to run Task 1"""
    print("=" * 60)
    print("TASK 1: Telegram Medical Channel Scraper")
    print("=" * 60)
    
    # Load credentials
    load_dotenv()
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    
    print(f" API ID: {api_id}")
    print(f" API Hash: {api_hash[:8]}...")
    print()
    
    # Create directories
    Path("data/raw/images").mkdir(parents=True, exist_ok=True)
    Path("data/raw/telegram_messages").mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    # Channels from Task 1 instructions
    channels = [
        'CheMed123',           # CheMed Telegram Channel
        'lobelia4cosmetics',   # Lobelia Cosmetics
        'tikvahpharma',        # Tikvah Pharma
    ]
    
    # Connect to Telegram
    client = TelegramClient('medical_scraper', int(api_id), api_hash)
    
    try:
        await client.start()
        print(" Connected to Telegram API")
        print()
        
        total_messages = 0
        total_images = 0
        
        # Scrape each channel
        for channel in channels:
            print(f" Scraping: @{channel}")
            messages, images = await scrape_channel(client, channel)
            total_messages += messages
            total_images += images
            await asyncio.sleep(2)  # Be nice to API
        
        # Summary
        print("\n" + "=" * 60)
        print(" TASK 1 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f" Total Messages Scraped: {total_messages}")
        print(f"  Total Images Downloaded: {total_images}")
        print(f" Data saved in: data/raw/")
        print("=" * 60)
        
    except Exception as e:
        print(f" Error: {e}")
    finally:
        await client.disconnect()
        print("Disconnected from Telegram")

async def scrape_channel(client, channel_name):
    """Scrape messages from a single channel"""
    try:
        # Get channel entity
        entity = await client.get_entity(channel_name)
        
        messages_data = []
        image_count = 0
        
        # Get messages (limit to 100 for speed)
        async for message in client.iter_messages(entity, limit=100):
            # Extract data as per Task 1 requirements
            message_data = {
                'message_id': message.id,
                'channel_name': channel_name,
                'message_date': message.date.isoformat() if message.date else None,
                'message_text': message.text or '',
                'views': message.views or 0,
                'forwards': message.forwards or 0,
                'has_media': bool(message.media)
            }
            
            # Download image if exists (Task 1 requirement)
            if message.media and hasattr(message.media, 'photo'):
                image_path = await download_image(message, channel_name)
                if image_path:
                    message_data['image_path'] = str(image_path)
                    image_count += 1
            
            messages_data.append(message_data)
        
        # Save to JSON file (Task 1 requirement)
        if messages_data:
            save_to_json(channel_name, messages_data)
            print(f"   {len(messages_data)} messages, {image_count} images")
        else:
            print(f"   No messages found")
        
        return len(messages_data), image_count
        
    except Exception as e:
        print(f"   Failed to scrape {channel_name}: {e}")
        return 0, 0

async def download_image(message, channel_name):
    """Download and save image as per Task 1 folder structure"""
    try:
        # Create channel-specific directory
        image_dir = Path(f"data/raw/images/{channel_name}")
        image_dir.mkdir(exist_ok=True)
        
        # Save image with message_id as filename
        image_path = image_dir / f"{message.id}.jpg"
        await message.download_media(file=str(image_path))
        
        return image_path
    except Exception as e:
        return None

def save_to_json(channel_name, messages):
    """Save messages to JSON file with date partitioning"""
    # Create date directory (YYYY-MM-DD)
    today = datetime.now().strftime("%Y-%m-%d")
    date_dir = Path(f"data/raw/telegram_messages/{today}")
    date_dir.mkdir(exist_ok=True)
    
    # Save JSON file
    file_path = date_dir / f"{channel_name}.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(messages, f, indent=2, ensure_ascii=False)
    
    print(f"   Saved to: {file_path}")

# Run the scraper
if __name__ == "__main__":
    asyncio.run(main())
