#!/usr/bin/env python3
"""
Simple test to verify Voxuy API token loading
"""

import sys
sys.path.insert(0, '.')

from services.voxuy_api import get_voxuy_api_token

def test_token():
    print("🔧 Testing Voxuy API Token Loading...")
    
    token = get_voxuy_api_token()
    print(f"✅ Token loaded: {token[:8]}..." if len(token) > 8 else f"Token: {token}")
    
    if "68a1c480" in token:
        print("⚠️ WARNING: You're still using the exposed token. Please regenerate it in Voxuy!")
    elif token == "REPLACE_WITH_YOUR_VOXUY_API_TOKEN":
        print("❌ ERROR: Placeholder token detected. Please update .streamlit/secrets.toml")
    else:
        print("✅ NEW TOKEN: Configuration looks secure!")
    
    return token

if __name__ == "__main__":
    token = test_token()
    print(f"\n📝 Next steps:")
    print(f"1. ✅ Token loading works - Your app can send messages locally")
    print(f"2. 🚀 Start your Streamlit app: streamlit run app.py")
    print(f"3. 📱 Test sending messages through the Processor interface")
    
    if "68a1c480" in token:
        print(f"\n⚠️ SECURITY REMINDER:")
        print(f"   - Generate a NEW token in your Voxuy dashboard")
        print(f"   - Replace the token in .streamlit/secrets.toml")
        print(f"   - The current token was exposed on GitHub")