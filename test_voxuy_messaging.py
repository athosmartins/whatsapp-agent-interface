#!/usr/bin/env python3
"""
Test script to verify Voxuy API messaging works locally
"""

import sys
import os
sys.path.insert(0, '.')

from services.voxuy_api import send_whatsapp_message, get_voxuy_api_token

def test_voxuy_connection():
    """Test Voxuy API connection and token loading"""
    print("🔧 Testing Voxuy API Configuration...")
    
    # Test token loading
    token = get_voxuy_api_token()
    print(f"✅ API Token loaded: {token[:8]}..." if len(token) > 8 else f"❌ Token: {token}")
    
    if token == "REPLACE_WITH_YOUR_VOXUY_API_TOKEN":
        print("❌ ERROR: Still using placeholder token. Please update .streamlit/secrets.toml")
        return False
        
    print("✅ Token configuration looks good!")
    return True

def test_send_message():
    """Test sending a WhatsApp message"""
    print("\n📱 Testing WhatsApp Message Sending...")
    
    # Use a test phone number (you can change this to your own number)
    test_phone = input("Enter your phone number for testing (with country code, e.g., 5511999999999): ")
    
    if not test_phone:
        print("❌ No phone number provided. Skipping message test.")
        return
    
    # Test message
    test_message = "🤖 Test message from your WhatsApp conversation processor! This confirms the API is working correctly."
    
    print(f"📤 Sending test message to {test_phone}...")
    
    try:
        result = send_whatsapp_message(
            phone_number=test_phone,
            message_content=test_message,
            client_name="Test User",
            client_address="Test Address",
            client_district="Test District"
        )
        
        print(f"\n📋 API Response:")
        print(f"   Success: {result['success']}")
        print(f"   Status Code: {result['status_code']}")
        print(f"   API Message: {result.get('api_message', 'N/A')}")
        
        if result['success']:
            print("✅ Message sent successfully! Check your WhatsApp.")
        else:
            print("❌ Message failed to send.")
            print(f"   Error: {result.get('error', 'Unknown error')}")
            print(f"   API Response: {result.get('api_response', 'N/A')}")
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")

def main():
    """Main test function"""
    print("🚀 Voxuy API Local Testing")
    print("=" * 40)
    
    # Test 1: Token configuration
    if not test_voxuy_connection():
        return
    
    # Test 2: Message sending (optional)
    send_test = input("\nDo you want to send a test WhatsApp message? (y/N): ").lower().strip()
    
    if send_test in ['y', 'yes']:
        test_send_message()
    else:
        print("📝 Skipping message test. Configuration verified successfully!")
    
    print("\n✅ Testing complete!")

if __name__ == "__main__":
    main()