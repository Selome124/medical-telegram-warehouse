# test_api.py
import requests
import json

BASE_URL = "http://localhost:8000"

def test_api():
    print("Testing Medical Telegram API...\n")
    
    # Test 1: Root endpoint
    print("1. Testing root endpoint:")
    response = requests.get(f"{BASE_URL}/")
    print(f"   Response: {response.json()}\n")
    
    # Test 2: Top products
    print("2. Testing top products endpoint:")
    response = requests.get(f"{BASE_URL}/api/reports/top-products?limit=5")
    print(f"   Response: {json.dumps(response.json(), indent=2)}\n")
    
    # Test 3: Channel activity
    print("3. Testing channel activity:")
    response = requests.get(f"{BASE_URL}/api/channels/medical_education/activity")
    print(f"   Response: {json.dumps(response.json(), indent=2)}\n")
    
    # Test 4: Search messages
    print("4. Testing message search:")
    response = requests.get(f"{BASE_URL}/api/search/messages?query=paracetamol")
    print(f"   Response: {json.dumps(response.json(), indent=2)}\n")
    
    # Test 5: Visual content
    print("5. Testing visual content stats:")
    response = requests.get(f"{BASE_URL}/api/reports/visual-content")
    print(f"   Response: {json.dumps(response.json(), indent=2)}\n")

if __name__ == "__main__":
    test_api()