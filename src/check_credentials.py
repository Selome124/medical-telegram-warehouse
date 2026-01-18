print(" Telegram Credentials Check")
print("=" * 50)

import os
from dotenv import load_dotenv

load_dotenv()

api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    print(" ERROR: Missing credentials in .env")
    print("Make sure .env has TELEGRAM_API_ID and TELEGRAM_API_HASH")
    exit(1)

print(f" Found API ID: {api_id}")
print(f" Found API Hash: {api_hash[:8]}...")

# Check if they're still the example values
if api_id == "12345678" or api_hash == "abcdef1234567890":
    print("\n CRITICAL ERROR ")
    print("You still have EXAMPLE values in .env!")
    print("You MUST replace them with YOUR REAL credentials.")
    print("\nGet them from: https://my.telegram.org")
    print("\nYour .env should look like this (but with YOUR numbers):")
    print("TELEGRAM_API_ID=28463712")
    print('TELEGRAM_API_HASH="7a3b9c8d5e1f2a4b6c8d9e0f1a2b3c4d"')
    exit(1)

print("\n Credentials look good!")
print("You're ready to run the scraper.")
