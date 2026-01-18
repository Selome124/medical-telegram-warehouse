# task2_scraper.py - Complete Task 2 with SQLite
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, UsernameNotOccupiedError
from dotenv import load_dotenv
import sys

# Add current directory to path
sys.path.append('.')
from database_sqlite import SessionLocal, TelegramMessage, ChannelInfo, create_tables

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class MedicalTelegramScraper:
    def __init__(self):
        # Telegram API credentials
        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        
        if not self.api_id or not self.api_hash:
            raise ValueError("Telegram API credentials not found in .env file")
        
        # Setup directories
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Telegram client
        self.session_file = "telegram_session.session"
        self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
        
        # Database
        self.db = SessionLocal()
    
    async def start(self):
        """Start Telegram client"""
        await self.client.start()
        logger.info("✓ Telegram client started")
    
    async def find_medical_channels(self):
        """Find and save medical channels to database"""
        logger.info("\n" + "="*50)
        logger.info("SEARCHING FOR MEDICAL CHANNELS")
        logger.info("="*50)
        
        # List of medical channels to try (mix of English and other languages)
        medical_channels = [
            "medical_education",        # Ultimate Doctors (found earlier)
            "healthcare_news",          # 의료경영/보건의료 뉴스 스크랩
            "medicinestudy",            # دراسات وابحاث علمية وطبية
            "pharmacology_notes",       # Pharmacology Notes
            "medicaldoctors",           # Medical Doctors
            "health_today",             # Health Today
            "medscape",                 # Medscape (medical news)
            "who",                      # World Health Organization
            "cdcgov",                   # CDC
            "nih",                      # NIH
            "lancet",                   # The Lancet
            "nejm",                     # New England Journal of Medicine
            "bmj_latest",               # BMJ Latest
            "naturemedicine",           # Nature Medicine
            "sciencemagazine",          # Science Magazine
        ]
        
        working_channels = []
        
        for channel in medical_channels:
            try:
                logger.info(f"Checking: {channel}")
                entity = await self.client.get_entity(channel)
                
                # Save channel to database if not exists
                existing = self.db.query(ChannelInfo).filter_by(channel_name=channel).first()
                if not existing:
                    channel_info = ChannelInfo(
                        channel_name=channel,
                        channel_title=getattr(entity, 'title', ''),
                        telegram_id=entity.id,
                        last_scraped=datetime.utcnow()
                    )
                    self.db.add(channel_info)
                    self.db.commit()
                    logger.info(f"  ✓ Added to database: {getattr(entity, 'title', channel)}")
                else:
                    logger.info(f"  ✓ Already in database")
                
                working_channels.append((channel, entity))
                
            except Exception as e:
                logger.info(f"  ✗ Not found or error: {str(e)[:50]}...")
            
            await asyncio.sleep(1)  # Be polite to Telegram API
        
        return working_channels
    
    async def scrape_channel_messages(self, channel_name, entity, message_limit=100):
        """Scrape messages from a channel and save to database"""
        try:
            logger.info(f"\nScraping: {channel_name}")
            
            new_message_count = 0
            total_count = 0
            
            async for message in self.client.iter_messages(entity, limit=message_limit):
                total_count += 1
                
                # Check if message already exists
                existing = self.db.query(TelegramMessage).filter_by(
                    message_id=message.id,
                    channel_name=channel_name
                ).first()
                
                if existing:
                    continue  # Skip if already saved
                
                # Save new message
                telegram_msg = TelegramMessage(
                    message_id=message.id,
                    channel_name=channel_name,
                    channel_title=getattr(entity, 'title', ''),
                    message_text=message.text,
                    sender_id=message.sender_id,
                    views=message.views,
                    forwards=message.forwards,
                    date=message.date
                )
                
                self.db.add(telegram_msg)
                new_message_count += 1
                
                # Progress indicator
                if total_count % 20 == 0:
                    logger.info(f"  Processed {total_count} messages...")
            
            # Commit to database
            self.db.commit()
            
            # Update channel's last_scraped time
            channel = self.db.query(ChannelInfo).filter_by(channel_name=channel_name).first()
            if channel:
                channel.last_scraped = datetime.utcnow()
                self.db.commit()
            
            logger.info(f"✓ Saved {new_message_count} new messages (Total processed: {total_count})")
            
            # Also save backup to JSON
            await self.save_backup_json(channel_name, entity, min(50, message_limit))
            
            return new_message_count
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"✗ Error scraping {channel_name}: {e}")
            return 0
    
    async def save_backup_json(self, channel_name, entity, limit=50):
        """Save backup to JSON file"""
        try:
            messages = []
            async for message in self.client.iter_messages(entity, limit=limit):
                messages.append({
                    'id': message.id,
                    'date': message.date.isoformat() if message.date else None,
                    'message': message.text,
                    'sender_id': message.sender_id,
                    'views': message.views,
                    'forwards': message.forwards
                })
            
            filename = f"{channel_name}_backup_{datetime.now().strftime('%Y%m%d')}.json"
            filepath = self.data_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            
            logger.info(f"  Backup saved: {filename}")
            
        except Exception as e:
            logger.error(f"  Backup failed: {e}")
    
    def show_database_stats(self):
        """Display database statistics"""
        try:
            total_messages = self.db.query(TelegramMessage).count()
            total_channels = self.db.query(ChannelInfo).count()
            
            # Messages per channel
            from sqlalchemy import func
            channel_stats = self.db.query(
                TelegramMessage.channel_name,
                func.count(TelegramMessage.id).label('count')
            ).group_by(TelegramMessage.channel_name).all()
            
            logger.info("\n" + "="*50)
            logger.info("DATABASE STATISTICS")
            logger.info("="*50)
            logger.info(f"Total Channels: {total_channels}")
            logger.info(f"Total Messages: {total_messages}")
            
            if channel_stats:
                logger.info("\nMessages per Channel:")
                for channel, count in channel_stats:
                    logger.info(f"  {channel}: {count} messages")
            
            # Latest messages
            latest = self.db.query(TelegramMessage).order_by(TelegramMessage.date.desc()).limit(3).all()
            if latest:
                logger.info("\nLatest Messages:")
                for msg in latest:
                    preview = msg.message_text[:80] + "..." if msg.message_text and len(msg.message_text) > 80 else msg.message_text
                    logger.info(f"  [{msg.channel_name}] {preview}")
            
            return {
                'channels': total_channels,
                'messages': total_messages,
                'channel_stats': dict(channel_stats)
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    def export_to_csv(self):
        """Export data to CSV for analysis"""
        try:
            import pandas as pd
            
            # Query all messages
            messages = self.db.query(TelegramMessage).all()
            
            if not messages:
                logger.warning("No messages to export")
                return
            
            # Convert to DataFrame
            data = []
            for msg in messages:
                data.append({
                    'message_id': msg.message_id,
                    'channel': msg.channel_name,
                    'date': msg.date,
                    'message': msg.message_text,
                    'views': msg.views,
                    'forwards': msg.forwards,
                    'scraped_at': msg.scraped_at
                })
            
            df = pd.DataFrame(data)
            
            # Export to CSV
            csv_file = "telegram_messages_export.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"✓ Data exported to {csv_file}")
            logger.info(f"  Total records: {len(df)}")
            logger.info(f"  Columns: {', '.join(df.columns)}")
            
            # Show sample
            logger.info("\nSample of exported data:")
            print(df.head(3).to_string())
            
            return csv_file
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return None
    
    async def disconnect(self):
        """Cleanup"""
        await self.client.disconnect()
        self.db.close()
        logger.info("✓ Disconnected")

async def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("MEDICAL TELEGRAM SCRAPER - TASK 2: DATA STORAGE")
    logger.info("="*60)
    
    # Initialize database
    create_tables()
    
    # Create scraper instance
    scraper = MedicalTelegramScraper()
    
    try:
        # Start Telegram client
        await scraper.start()
        
        # Step 1: Find and save medical channels
        working_channels = await scraper.find_medical_channels()
        
        if not working_channels:
            logger.error("No channels found! Exiting.")
            return
        
        # Step 2: Scrape messages from each channel
        logger.info("\n" + "="*50)
        logger.info("SCRAPING MESSAGES")
        logger.info("="*50)
        
        total_new_messages = 0
        for channel_name, entity in working_channels:
            new_messages = await scraper.scrape_channel_messages(channel_name, entity, message_limit=50)
            total_new_messages += new_messages
            await asyncio.sleep(2)  # Delay between channels
        
        # Step 3: Show statistics
        scraper.show_database_stats()
        
        # Step 4: Export to CSV
        logger.info("\n" + "="*50)
        logger.info("EXPORTING DATA")
        logger.info("="*50)
        scraper.export_to_csv()
        
        logger.info("\n" + "="*60)
        logger.info(f"TASK 2 COMPLETED SUCCESSFULLY!")
        logger.info(f"Total new messages saved: {total_new_messages}")
        logger.info("="*60)
        
    except SessionPasswordNeededError:
        logger.error("Two-factor authentication required!")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    finally:
        await scraper.disconnect()

if __name__ == "__main__":
    asyncio.run(main())