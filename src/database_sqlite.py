
import os
from sqlalchemy import create_engine, Column, Integer, String, Text, BigInteger, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# SQLite database configuration - uses a local file
DB_PATH = os.getenv('DB_PATH', 'medical_telegram.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create engine
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

# Create base class for models
Base = declarative_base()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class TelegramMessage(Base):
    """Model for storing Telegram messages"""
    __tablename__ = "telegram_messages"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(BigInteger, nullable=False)  # Original Telegram message ID
    channel_name = Column(String(255), nullable=False)
    channel_title = Column(String(255))
    message_text = Column(Text)
    sender_id = Column(BigInteger)
    views = Column(Integer)
    forwards = Column(Integer)
    date = Column(DateTime)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<TelegramMessage(channel={self.channel_name}, message_id={self.message_id})>"

class ChannelInfo(Base):
    """Model for storing channel information"""
    __tablename__ = "channels"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_name = Column(String(255), unique=True, nullable=False)
    channel_title = Column(String(255))
    telegram_id = Column(BigInteger, unique=True)
    description = Column(Text)
    participant_count = Column(Integer)
    is_active = Column(Integer, default=1)
    last_scraped = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<ChannelInfo(name={self.channel_name}, title={self.channel_title})>"

def create_tables():
    """Create all tables in the database"""
    Base.metadata.create_all(bind=engine)
    print(f"✓ Database tables created successfully in {DB_PATH}!")

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Test database connection
def test_connection():
    try:
        connection = engine.connect()
        print(f"✓ Database connection successful: {DATABASE_URL}")
        connection.close()
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    # Test the database connection
    if test_connection():
        create_tables()