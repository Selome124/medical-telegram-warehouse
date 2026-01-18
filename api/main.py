from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database import engine, Base

app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="API for accessing insights from Telegram medical channels",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Medical Telegram Warehouse API", "status": "running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
