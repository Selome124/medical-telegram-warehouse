from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class MessageBase(BaseModel):
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str
    has_media: bool
    views: int
    forwards: int

class MessageCreate(MessageBase):
    pass

class Message(MessageBase):
    id: int
    created_at: datetime
    
    class Config:
        orm_mode = True

class TopProductResponse(BaseModel):
    product_name: str
    mention_count: int
    channels: list[str]
    last_mentioned: datetime

class ChannelActivityResponse(BaseModel):
    channel_name: str
    post_count: int
    avg_views: float
    avg_forwards: float
    date_range: str
