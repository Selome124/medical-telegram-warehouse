# scraper.py - UPDATED VERSION
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, UsernameNotOccupiedError
from dotenv import load_dotenv

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleTelegramScraper:
    def __init__(self):
        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        
        if not self.api_id or not self.api_hash:
            raise ValueError("TELEGRAM_API_ID or TELEGRAM_API_HASH not found in environment variables")
        
        # Setup directories
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Session file
        self.session_file = "telegram_session.session"
        
        # Initialize client
        self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
    
    async def start(self):
        """Start the Telegram client"""
        await self.client.start()
        logger.info("Telegram client started successfully")
    
    async def test_channel(self, channel_username):
        """Test if a channel exists and is accessible"""
        try:
            # Remove @ if present
            username = channel_username.replace('@', '')
            
            # Try to get the entity
            entity = await self.client.get_entity(username)
            
            logger.info(f"✓ Channel '{username}' found!")
            logger.info(f"  Title: {getattr(entity, 'title', 'N/A')}")
            logger.info(f"  ID: {entity.id}")
            
            return True
            
        except UsernameNotOccupiedError:
            logger.error(f"✗ Channel '{channel_username}' doesn't exist")
            return False
        except ValueError as e:
            if "Cannot find any entity corresponding to" in str(e):
                logger.error(f"✗ Channel '{channel_username}' not found")
            else:
                logger.error(f"✗ Error accessing '{channel_username}': {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Error with '{channel_username}': {e}")
            return False
    
    async def scrape_channel(self, channel_username, limit=100):
        """Scrape messages from a channel"""
        try:
            # Get entity (channel/group)
            username = channel_username.replace('@', '')
            entity = await self.client.get_entity(username)
            
            logger.info(f"Starting to scrape from '{username}'...")
            
            messages = []
            async for message in self.client.iter_messages(entity, limit=limit):
                message_data = {
                    'id': message.id,
                    'date': message.date.isoformat() if message.date else None,
                    'message': message.text,
                    'sender_id': message.sender_id,
                    'views': message.views,
                    'forwards': message.forwards
                }
                messages.append(message_data)
                
                if len(messages) % 10 == 0:
                    logger.info(f"  Scraped {len(messages)} messages...")
            
            # Save to file
            filename = f"{username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = self.data_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✓ Saved {len(messages)} messages to {filepath}")
            return messages
            
        except Exception as e:
            logger.error(f"Error scraping channel {channel_username}: {e}")
            return []
    
    async def disconnect(self):
        """Disconnect the client"""
        await self.client.disconnect()
        logger.info("Disconnected from Telegram")

async def main():
    """Main function to run the scraper"""
    scraper = SimpleTelegramScraper()
    
    try:
        await scraper.start()
        
        # First, test some channels to find working ones
        logger.info("\n" + "="*50)
        logger.info("Testing channel availability...")
        logger.info("="*50)
        
        # List of potential medical channels to try
        test_channels = [
            "medicine_channel",
            "medicalupdates", 
            "healthtipsglobal",
            "medical_education",
            "doctors_world",
            "healthcare_news",
            "medicinestudy",
            "pharmacology_notes"
        ]
        
        working_channels = []
        for channel in test_channels:
            if await scraper.test_channel(channel):
                working_channels.append(channel)
            await asyncio.sleep(1)  # Small delay between checks
        
        logger.info("\n" + "="*50)
        logger.info(f"Found {len(working_channels)} working channels")
        logger.info("="*50)
        
        # Now scrape from working channels
        if working_channels:
            for channel in working_channels:
                logger.info(f"\nScraping channel: {channel}")
                messages = await scraper.scrape_channel(channel, limit=20)  # Start with 20 messages
                logger.info(f"Scraped {len(messages)} messages from {channel}")
                
                # Small delay between channels
                await asyncio.sleep(3)
        else:
            logger.warning("No working channels found to scrape!")
            logger.info("Try finding public medical channels and add their usernames to the list.")
        
    except SessionPasswordNeededError:
        logger.error("Two-factor authentication required. Please check your Telegram account.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    finally:
        await scraper.disconnect()

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())