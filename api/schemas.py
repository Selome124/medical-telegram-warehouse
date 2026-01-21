# api/schemas.py
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ProductMention(BaseModel):
    product_name: str
    mention_count: int
    
class ChannelActivity(BaseModel):
    date: str
    message_count: int
    
class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    date: datetime
    views: int
    
class VisualContentStats(BaseModel):
    channel_name: str
    total_images: int
    promotional_count: int
    product_display_count: int
    
class APIResponse(BaseModel):
    success: bool
    data: Optional[dict] = None
    message: Optional[str] = None