# api/main.py - SIMPLIFIED VERSION
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
import os
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/medical_warehouse")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for analyzing medical Telegram channel data",
    version="1.0.0"
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "Medical Telegram Analytics API"}

@app.get("/api/reports/top-products")
def get_top_products(limit: int = 10):
    """Get most frequently mentioned terms/products"""
    with SessionLocal() as db:
        query = text("""
            SELECT 'paracetamol' as product_name, 15 as mention_count
            UNION ALL
            SELECT 'aspirin', 10
            UNION ALL
            SELECT 'vitamin c', 8
            LIMIT :limit
        """)
        
        result = db.execute(query, {"limit": limit}).fetchall()
        products = [{"product_name": row[0], "mention_count": row[1]} for row in result]
        
        return {
            "success": True,
            "data": {"products": products}
        }

@app.get("/api/channels/{channel_name}/activity")
def get_channel_activity(channel_name: str):
    """Get posting activity for specific channel"""
    with SessionLocal() as db:
        # Sample data for demo
        return {
            "success": True,
            "data": {
                "channel": channel_name,
                "activity": [
                    {"date": "2026-01-18", "message_count": 5},
                    {"date": "2026-01-17", "message_count": 3}
                ]
            }
        }

@app.get("/api/search/messages")
def search_messages(query: str, limit: int = 20):
    """Search for messages containing keyword"""
    with SessionLocal() as db:
        # Sample data for demo
        return {
            "success": True,
            "data": {
                "query": query,
                "results": [
                    {
                        "message_id": 1,
                        "channel_name": "medical_education",
                        "message_text": f"Discussion about {query} in medical field...",
                        "date": "2026-01-18",
                        "views": 150
                    }
                ]
            }
        }

@app.get("/api/reports/visual-content")
def get_visual_content_stats():
    """Get statistics about image usage"""
    with SessionLocal() as db:
        # Sample data for demo
        return {
            "success": True,
            "data": {
                "visual_content": [
                    {
                        "channel_name": "medical_education",
                        "total_images": 15,
                        "promotional_count": 3,
                        "product_display_count": 8
                    }
                ]
            }
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)