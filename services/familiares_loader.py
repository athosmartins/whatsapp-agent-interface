"""
Familiares data loader service for family member information.
Loads family relationship data from the dedicated familiares spreadsheet.
"""

import pandas as pd
import streamlit as st
from typing import Dict, List, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Familiares spreadsheet configuration
FAMILIARES_SPREADSHEET_ID = "1m4vrMjZvAW7KS1T9jtsFC5tpOlRDKNagfQilTeBQ5gE"
CREDENTIALS_FILE = "credentials.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

@st.cache_data(ttl=3600, max_entries=1)  # Cache for 1 hour
def get_familiares_data() -> pd.DataFrame:
    """
    Load familiares data from the dedicated Google Spreadsheet.
    
    Returns:
        pd.DataFrame: DataFrame containing family relationship data
    """
    try:
        # Initialize Google Sheets service
        service = get_familiares_sheets_service()
        if not service:
            print("⚠️ Could not initialize Google Sheets service for familiares")
            return pd.DataFrame()
        
        # Try to read from the first sheet (assuming main data is there)
        # You may need to adjust the range based on the actual structure
        range_name = "A:Z"  # Read all data from columns A to Z
        
        result = service.spreadsheets().values().get(
            spreadsheetId=FAMILIARES_SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            print("⚠️ No data found in familiares spreadsheet")
            return pd.DataFrame()
        
        # Convert to DataFrame
        if len(values) > 1:
            headers = values[0]
            data = values[1:]
            df = pd.DataFrame(data, columns=headers)
            
            print(f"✅ Loaded {len(df)} rows from familiares spreadsheet")
            print(f"📊 Columns: {list(df.columns)}")
            
            return df
        else:
            print("⚠️ Familiares spreadsheet has headers but no data")
            return pd.DataFrame()
    
    except Exception as e:
        print(f"❌ Error loading familiares data: {e}")
        return pd.DataFrame()

def get_familiares_sheets_service():
    """Initialize and return Google Sheets API service for familiares."""
    try:
        # Try to load from Streamlit secrets first (for production)
        credentials_info = None
        
        try:
            if hasattr(st, 'secrets') and 'google_sheets' in st.secrets:
                credentials_info = dict(st.secrets['google_sheets'])
                credentials = Credentials.from_service_account_info(
                    credentials_info, scopes=SCOPES
                )
                print("Using Google Sheets credentials from Streamlit secrets for familiares")
            else:
                raise Exception("Streamlit secrets not available")
        except:
            # Fallback to local credentials.json file (for development)
            import os
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"Error: {CREDENTIALS_FILE} not found and Streamlit secrets not configured for familiares.")
                return None
            
            credentials = Credentials.from_service_account_file(
                CREDENTIALS_FILE, scopes=SCOPES
            )
            print("Using Google Sheets credentials from local file for familiares")
        
        service = build('sheets', 'v4', credentials=credentials)
        return service
    
    except Exception as e:
        print(f"Error initializing Google Sheets service for familiares: {e}")
        return None

def get_familiares_by_cpf(cpf: str) -> str:
    """
    Get familiares information for a specific CPF from the familiares spreadsheet.
    
    Args:
        cpf (str): The CPF to look up
        
    Returns:
        str: Formatted familiares string with relationships, or empty string if not found
    """
    try:
        if not cpf or pd.isna(cpf):
            return ""
        
        # Clean CPF for matching (remove dots, dashes, spaces) and pad with zeros if needed
        clean_cpf = str(cpf).strip().replace(".", "").replace("-", "").replace(" ", "")
        if not clean_cpf:
            return ""
        
        # Pad with leading zeros to 11 digits if needed
        clean_cpf = clean_cpf.zfill(11)
        
        # Load familiares data
        familiares_df = get_familiares_data()
        if familiares_df.empty:
            return ""
        
        # The spreadsheet structure:
        # CPF_RELACIONADO: The person we're looking up
        # NOME_RELACIONADO: Name of the person we're looking up  
        # RELACAO: Relationship type (FILHO(A), MAE, PAI, etc.)
        # DOCUMENTO: CPF of the family member
        # NOME: Name of the family member
        
        # Clean the CPF_RELACIONADO column for matching
        familiares_df['clean_cpf_relacionado'] = familiares_df['CPF_RELACIONADO'].astype(str).str.replace(".", "").str.replace("-", "").str.replace(" ", "").str.zfill(11)
        
        # Find all matching rows for this CPF
        matching_rows = familiares_df[familiares_df['clean_cpf_relacionado'] == clean_cpf]
        
        if matching_rows.empty:
            return ""
        
        # Group family members by relationship type
        familiares_info = []
        relationship_groups = {}
        
        for _, row in matching_rows.iterrows():
            relacao = str(row.get('RELACAO', '')).strip()
            nome_familiar = str(row.get('NOME', '')).strip()
            
            if relacao and nome_familiar:
                # Normalize relationship names
                relacao_clean = relacao.upper()
                if relacao_clean == 'FILHO(A)':
                    relacao_clean = 'FILHO'
                elif relacao_clean == 'FILHA':
                    relacao_clean = 'FILHO'  # Group children together
                
                if relacao_clean not in relationship_groups:
                    relationship_groups[relacao_clean] = []
                relationship_groups[relacao_clean].append(nome_familiar)
        
        # Format the familiares string
        for relacao, nomes in relationship_groups.items():
            if len(nomes) == 1:
                familiares_info.append(f"{relacao}: {nomes[0]}")
            else:
                familiares_info.append(f"{relacao}: {', '.join(nomes)}")
        
        return ", ".join(familiares_info) if familiares_info else ""
    
    except Exception as e:
        print(f"❌ Error getting familiares for CPF {cpf}: {e}")
        import traceback
        traceback.print_exc()
        return ""

def get_familiares_by_phone(phone: str) -> str:
    """
    Get familiares information by phone number (if phone column exists in familiares sheet).
    
    Args:
        phone (str): The phone number to look up
        
    Returns:
        str: Formatted familiares string or empty string if not found
    """
    try:
        if not phone or pd.isna(phone):
            return ""
        
        # Clean phone for matching
        clean_phone = str(phone).strip().replace("+", "").replace("-", "").replace(" ", "").replace("(", "").replace(")", "")
        if not clean_phone:
            return ""
        
        # Load familiares data
        familiares_df = get_familiares_data()
        if familiares_df.empty:
            return ""
        
        # Try to find phone columns
        phone_columns = [col for col in familiares_df.columns 
                        if any(term in col.lower() for term in ['phone', 'telefone', 'celular', 'fone'])]
        
        if not phone_columns:
            return ""  # No phone column available
        
        phone_col = phone_columns[0]  # Use first phone column found
        
        # Clean phone column for matching
        familiares_df['clean_phone'] = familiares_df[phone_col].astype(str).str.replace("+", "").str.replace("-", "").str.replace(" ", "").str.replace("(", "").str.replace(")", "")
        
        # Find matching row
        matching_rows = familiares_df[familiares_df['clean_phone'].str.contains(clean_phone[-8:], na=False)]  # Match last 8 digits
        
        if matching_rows.empty:
            return ""
        
        # Extract familiares information from the first matching row
        row = matching_rows.iloc[0]
        
        # Try to find familiares columns
        familiares_columns = [col for col in familiares_df.columns 
                             if any(term in col.lower() for term in 
                                   ['familia', 'family', 'parente', 'relacionado', 'dependente',
                                    'conjuge', 'esposa', 'marido', 'filho', 'filha', 'pai', 'mae', 'familiar'])]
        
        if familiares_columns:
            familiares_info = []
            for col in familiares_columns:
                value = row.get(col, "")
                if pd.notna(value) and str(value).strip():
                    familiares_info.append(str(value).strip())
            
            return ", ".join(familiares_info) if familiares_info else ""
        
        return ""
    
    except Exception as e:
        print(f"❌ Error getting familiares for phone {phone}: {e}")
        return ""