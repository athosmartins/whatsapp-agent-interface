"""Voxuy API service for sending WhatsApp messages."""

import requests
import json
import os
import streamlit as st
from typing import Dict, Any

# Voxuy API configuration - Load from environment variables or Streamlit secrets
def get_voxuy_api_token():
    """Get Voxuy API token from environment variables or Streamlit secrets."""
    # Try environment variable first
    token = os.getenv('VOXUY_API_TOKEN')
    if token:
        return token
    
    # Try loading directly from .streamlit/secrets.toml for non-Streamlit contexts
    try:
        import toml
        secrets_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.streamlit', 'secrets.toml')
        if os.path.exists(secrets_path):
            with open(secrets_path, 'r') as f:
                secrets = toml.load(f)
                if 'voxuy' in secrets and 'api_token' in secrets['voxuy']:
                    return secrets['voxuy']['api_token']
    except Exception as e:
        print(f"Could not load from secrets.toml: {e}")
    
    # Try Streamlit secrets (when running in Streamlit context)
    try:
        if hasattr(st, 'secrets') and 'voxuy' in st.secrets:
            return st.secrets['voxuy']['api_token']
    except Exception:
        pass
    
    # Final fallback - no secrets configured
    print("❌ ERROR: No Voxuy API token found! Please set VOXUY_API_TOKEN environment variable or configure Streamlit secrets.")
    raise Exception("Voxuy API token not configured. Please set VOXUY_API_TOKEN environment variable or add token to .streamlit/secrets.toml")

# Load token at runtime instead of module import time
VOXUY_API_TOKEN = None  # Will be loaded when needed
VOXUY_CUSTOM_MESSAGE_ID = "48800cc8-e0fb-41ff-bfdd-a8b1fe"
VOXUY_WEBHOOK_URL = "https://sistema.voxuy.com/api/65294302-8497-476a-872e-1d1b778aede1/webhooks/voxuy/transaction"

def send_whatsapp_message(
    phone_number: str,
    message_content: str,
    client_name: str = "",
    client_address: str = "",
    client_district: str = "",
    client_document: str = ""
) -> Dict[str, Any]:
    """
    Send a WhatsApp message via Voxuy API.
    
    Args:
        phone_number: Phone number to send message to
        message_content: The message content to send
        client_name: Optional client name
        client_address: Optional client address
        client_district: Optional client district
        client_document: Optional client document
    
    Returns:
        Dict containing success status and response details
    """
    # Load token at runtime to ensure Streamlit context is available
    global VOXUY_API_TOKEN
    if VOXUY_API_TOKEN is None:
        try:
            VOXUY_API_TOKEN = get_voxuy_api_token()
            print(f"🔑 Voxuy token loaded at runtime: {VOXUY_API_TOKEN[:8]}...")
        except Exception as e:
            print(f"❌ Failed to load Voxuy token at runtime: {e}")
            return {
                "success": False,
                "status_code": None,
                "api_response": None,
                "api_success": None,
                "api_message": f"Token loading error: {str(e)}",
                "api_errors": None,
                "error": f"Token loading error: {str(e)}"
            }
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    # Clean phone number (remove @ and domain if present)
    clean_phone = phone_number.split('@')[0] if '@' in phone_number else phone_number
    
    client_data = {
        "apiToken": VOXUY_API_TOKEN,
        "planId": VOXUY_CUSTOM_MESSAGE_ID,
        "paymentType": 99,  # Custom event
        "status": 0,        # Custom event
        "clientName": client_name,
        "clientPhoneNumber": clean_phone,
        "customEvent": "40419",  # Custom event ID
        "metadata": {
            "mensagem_customizada": message_content,  # Message content goes here as a variable
        }
    }
    
    try:
        # Log the data being sent for debugging
        print(f"Sending to Voxuy API: {json.dumps(client_data, indent=2)}")
        
        response = requests.post(
            VOXUY_WEBHOOK_URL,
            headers=headers,
            data=json.dumps(client_data),
            timeout=30
        )
        
        status_code = response.status_code
        print(f"Voxuy API Response Status: {status_code}")
        print(f"Voxuy API Response Text: {response.text}")
        
        # Special debug for invalid token
        if status_code == 400 and 'inválido' in response.text.lower():
            print(f"🚨 DEBUG: Invalid token error detected!")
            print(f"🔑 Token sent: '{VOXUY_API_TOKEN}'")
            print(f"📏 Token length: {len(VOXUY_API_TOKEN)}")
            print(f"🌐 Endpoint: {VOXUY_WEBHOOK_URL}")
            print(f"📋 Full request data keys: {list(client_data.keys())}")
        
        # Try to parse the response as JSON
        try:
            response_data = response.json()
        except Exception as json_error:
            print(f"Failed to parse JSON response: {json_error}")
            response_data = {"raw_response": response.text}
        
        # Extract pertinent fields from the API response if available
        success_val = response_data.get("success") if isinstance(response_data, dict) else None
        message_val = response_data.get("message") if isinstance(response_data, dict) else None
        errors_val = response_data.get("errors") if isinstance(response_data, dict) else None
        
        return {
            "success": status_code == 200 and success_val is not False,
            "status_code": status_code,
            "api_response": response.text,
            "api_success": success_val,
            "api_message": message_val,
            "api_errors": errors_val,
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "status_code": None,
            "api_response": None,
            "api_success": None,
            "api_message": str(e),
            "api_errors": None,
            "error": str(e)
        }