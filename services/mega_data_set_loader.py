"""
Service for loading and processing mega_data_set files from Google Drive.
This service handles the integration between WhatsApp conversations and property data.
"""

import os
import pandas as pd
from typing import List, Dict, Optional
import time
import streamlit as st
import gzip
import json

# Google Drive folder ID for mega_data_set files
MEGA_DATA_SET_FOLDER_ID = "1yFhxSOAf9UdarCekCKCg1UqKl3MArZAp"
CACHE_DURATION = 3600  # 1 hour in seconds

# CONFIGURATION: Set the path to your real mega_data_set file here
# Replace this with the actual path to your 350k+ row file
MANUAL_MEGA_DATA_SET_PATH = None  # e.g., "/path/to/your/real_mega_data_set.csv"

# Global variable to store cached data
_cached_mega_data = None
_cache_timestamp = 0

def load_compressed_json(file_path):
    """
    Load a compressed JSON file created by our custom converter.
    
    Args:
        file_path: Path to the .json.gz file
    
    Returns:
        pandas.DataFrame: Loaded data
    """
    print(f"Loading compressed JSON from {file_path}")
    
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            # Read until we find the data section
            line = f.readline()
            while line and not line.startswith('### DATA ###'):
                if line.startswith('### METADATA ###'):
                    # Read metadata
                    metadata_line = f.readline()
                    try:
                        metadata = json.loads(metadata_line)
                        print(f"File format: {metadata.get('format', 'unknown')}")
                        print(f"Columns: {len(metadata.get('columns', []))}")
                        print(f"Chunks: {metadata.get('chunks', 'unknown')}")
                    except:
                        pass
                line = f.readline()
            
            # Now read the data rows
            rows = []
            row_count = 0
            
            for line in f:
                line = line.strip()
                if line:
                    try:
                        row = json.loads(line)
                        rows.append(row)
                        row_count += 1
                        
                        # Progress feedback for large files
                        if row_count % 50000 == 0:
                            print(f"  Loaded {row_count:,} rows...")
                            
                    except json.JSONDecodeError:
                        continue  # Skip invalid lines
            
            print(f"Loaded {len(rows):,} rows from compressed file")
            
            # Convert to DataFrame
            if rows:
                df = pd.DataFrame(rows)
                print(f"Created DataFrame: {len(df)} rows × {len(df.columns)} columns")
                return df
            else:
                print("No valid data found in compressed file")
                return pd.DataFrame()
                
    except Exception as e:
        print(f"Error loading compressed JSON: {e}")
        return pd.DataFrame()

def download_latest_mega_data_set() -> Optional[str]:
    """
    Download the latest mega_data_set file from Google Drive folder using Google Drive API.
    Prioritizes Parquet files over CSV for better performance.
    Returns the path to the downloaded file or None if failed.
    """
    try:
        import json
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
        
        # Load credentials from Streamlit secrets or local file
        credentials_info = None
        
        # Google API scopes - try both full and readonly access
        SCOPES_FULL = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        
        SCOPES_READONLY = [
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        
        try:
            # Try to load from Streamlit secrets first (for production)
            import streamlit as st
            if hasattr(st, 'secrets') and 'google_sheets' in st.secrets:
                credentials_info = dict(st.secrets['google_sheets'])
                print("Using Google Drive credentials from Streamlit secrets")
            else:
                raise Exception("Streamlit secrets not available")
        except:
            # Fallback to local credentials.json file (for development)
            CREDENTIALS_FILE = "credentials.json"
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"Error: {CREDENTIALS_FILE} not found and Streamlit secrets not configured.")
                return None
            
            with open(CREDENTIALS_FILE, 'r') as f:
                credentials_info = json.load(f)
            print("Using Google Drive credentials from local file")
        
        # Try to create Drive service with different scope combinations
        drive_service = None
        for scopes_name, scopes in [("readonly", SCOPES_READONLY), ("full", SCOPES_FULL)]:
            try:
                credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
                drive_service = build('drive', 'v3', credentials=credentials)
                
                # Test the connection with a simple API call
                about_info = drive_service.about().get(fields="user").execute()
                print(f"Successfully authenticated with {scopes_name} scopes as: {about_info.get('user', {}).get('emailAddress', 'N/A')}")
                break
                
            except Exception as e:
                print(f"Failed to authenticate with {scopes_name} scopes: {e}")
                continue
        
        if not drive_service:
            print("❌ Could not authenticate with Google Drive API. Please ensure:")
            print("   1. Credentials are properly configured (Streamlit secrets or credentials.json)")
            print("   2. Google Drive API is enabled for this service account")
            print("   3. Service account has access to the folder")
            return None
        
        print(f"Downloading mega_data_set from Google Drive folder: {MEGA_DATA_SET_FOLDER_ID}")
        
        # List files in the folder, ordered by creation time (newest first)
        query = f"'{MEGA_DATA_SET_FOLDER_ID}' in parents and trashed=false"
        results = drive_service.files().list(
            q=query,
            orderBy='createdTime desc',
            fields='files(id, name, createdTime, mimeType)',
            pageSize=1000
        ).execute()
        
        files = results.get('files', [])
        if not files:
            print("No files found in mega_data_set folder")
            return None
        
        # Prioritize compressed JSON, then Parquet, then CSV for performance
        compressed_files = [f for f in files if f['name'].lower().endswith('.json.gz')]
        parquet_files = [f for f in files if f['name'].lower().endswith('.parquet')]
        csv_files = [f for f in files if f['name'].lower().endswith('.csv')]
        
        if compressed_files:
            newest_file = compressed_files[0]  # Already sorted by creation time
            file_extension = '.json.gz'
            print(f"Using compressed JSON file for optimal performance: {newest_file['name']}")
        elif parquet_files:
            newest_file = parquet_files[0]  # Already sorted by creation time
            file_extension = '.parquet'
            print(f"Using Parquet file for optimal performance: {newest_file['name']}")
        elif csv_files:
            newest_file = csv_files[0]
            file_extension = '.csv'
            print(f"Using CSV file: {newest_file['name']}")
        else:
            print("No supported file formats found (looking for .json.gz, .parquet or .csv)")
            return None
        
        file_id = newest_file['id']
        file_name = newest_file['name']
        
        print(f"Found latest file: {file_name} (created: {newest_file['createdTime']})")
        
        # Download the file
        request = drive_service.files().get_media(fileId=file_id)
        
        # Create local file path with timestamp and correct extension
        local_path = f"/tmp/mega_data_set_{int(time.time())}{file_extension}"
        
        # Download with progress tracking
        with open(local_path, 'wb') as local_file:
            downloader = MediaIoBaseDownload(local_file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Download progress: {int(status.progress() * 100)}%")
        
        print(f"Downloaded mega_data_set to: {local_path}")
        print(f"Original filename: {file_name}")
        
        return local_path
        
    except Exception as e:
        print(f"Error downloading mega_data_set: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

@st.cache_data(ttl=CACHE_DURATION)
def load_mega_data_set() -> pd.DataFrame:
    """
    Load the mega_data_set file into a pandas DataFrame.
    Uses caching to avoid repeated downloads.
    """
    global _cached_mega_data, _cache_timestamp
    
    # Check if we have cached data that's still valid
    current_time = time.time()
    if (_cached_mega_data is not None and 
        (current_time - _cache_timestamp) < CACHE_DURATION):
        print("Using cached mega_data_set")
        return _cached_mega_data
    
    # First check if manual path is configured
    file_path = None
    if MANUAL_MEGA_DATA_SET_PATH and os.path.exists(MANUAL_MEGA_DATA_SET_PATH):
        print(f"Using manually configured mega_data_set file: {MANUAL_MEGA_DATA_SET_PATH}")
        file_path = MANUAL_MEGA_DATA_SET_PATH
    else:
        # Try to download from Google Drive
        file_path = download_latest_mega_data_set()
    
    # If download fails, try to find local mega_data_set file
    if not file_path:
        print("Failed to download mega_data_set file, checking for local file...")
        
        # Look for local mega_data_set files in common locations
        potential_paths = [
            "/tmp/mega_data_set.csv",
            "/tmp/mega_data_set_latest.csv",
            "mega_data_set.csv",
            "../mega_data_set.csv",
            "../../mega_data_set.csv"
        ]
        
        for path in potential_paths:
            if os.path.exists(path):
                print(f"Found local mega_data_set file: {path}")
                file_path = path
                break
        
        # If no local file found, provide clear instructions
        if not file_path:
            print("⚠️  WARNING: No mega_data_set file found!")
            print("📋 To use the REAL mega_data_set with 350k+ rows:")
            print("   1. Download the latest file from Google Drive folder ID: 1yFhxSOAf9UdarCekCKCg1UqKl3MArZAp")
            print("   2. Save it as one of these file paths:")
            for path in potential_paths:
                print(f"      - {path}")
            print("   3. Or set MANUAL_MEGA_DATA_SET_PATH in mega_data_set_loader.py")
            print("")
            print("🔧 Currently using SAMPLE DATA (1400 rows) for testing")
            print("   This will NOT show real property matches!")
            
            # Fall back to sample file as last resort
            sample_path = "/tmp/sample_mega_data_set.csv"
            if os.path.exists(sample_path):
                print("Using sample mega_data_set for testing (NOT PRODUCTION DATA)")
                file_path = sample_path
            else:
                print("No sample file found, creating one...")
                # Create sample file
                try:
                    import sys
                    sys.path.append('.')
                    from analysis_temp.create_sample_mega_data_set import create_sample_file
                    file_path = create_sample_file()
                except:
                    print("Failed to create sample file")
                    return pd.DataFrame()
    
    try:
        # Load based on file extension
        if file_path.lower().endswith('.json.gz'):
            print("Loading compressed JSON file...")
            df = load_compressed_json(file_path)
            print(f"Successfully loaded mega_data_set from compressed JSON file")
        elif file_path.lower().endswith('.parquet'):
            print("Loading Parquet file...")
            df = pd.read_parquet(file_path)
            print(f"Successfully loaded mega_data_set from Parquet file")
        else:
            # Try different encodings for CSV
            print("Loading CSV file...")
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                    print(f"Successfully loaded mega_data_set with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                print("Failed to load mega_data_set with any encoding")
                return pd.DataFrame()
        
        # Cache the data
        _cached_mega_data = df
        _cache_timestamp = current_time
        
        print(f"Loaded mega_data_set: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Clean up the downloaded file (but not sample file)
        if file_path != "/tmp/sample_mega_data_set.csv" and os.path.exists(file_path):
            os.remove(file_path)
        
        return df
        
    except Exception as e:
        print(f"Error loading mega_data_set file: {e}")
        # Clean up the downloaded file
        if file_path != "/tmp/sample_mega_data_set.csv" and os.path.exists(file_path):
            os.remove(file_path)
        return pd.DataFrame()

def get_mega_data_set_schema() -> Dict[str, str]:
    """
    Get the schema information from the mega_data_set.
    Returns a dictionary with column names and their data types.
    """
    df = load_mega_data_set()
    if df.empty:
        return {}
    
    schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        non_null_count = df[col].count()
        total_count = len(df)
        
        schema[col] = {
            'dtype': dtype,
            'non_null_count': non_null_count,
            'total_count': total_count,
            'null_percentage': ((total_count - non_null_count) / total_count) * 100
        }
    
    return schema

@st.cache_data(ttl=3600)  # Cache for 1 hour
def find_properties_by_documento(documento_proprietario: str) -> List[Dict]:
    """
    Find all properties that belong to a specific document holder (CPF).
    Returns a list of property dictionaries.
    """
    df = load_mega_data_set()
    if df.empty:
        return []
    
    # Check if DOCUMENTO PROPRIETARIO column exists
    doc_col = None
    for col in df.columns:
        if col == 'DOCUMENTO PROPRIETARIO':
            doc_col = col
            break
    
    if doc_col is None:
        print("DOCUMENTO PROPRIETARIO column not found in mega_data_set")
        return []
    
    # Clean the document number for matching
    clean_documento = clean_document_number(documento_proprietario)
    
    # Find matching rows
    matching_rows = []
    for idx, row in df.iterrows():
        row_documento = clean_document_number(str(row[doc_col]))
        if row_documento == clean_documento:
            matching_rows.append(row.to_dict())
    
    return matching_rows

def clean_document_number(documento: str) -> str:
    """
    Clean document number (CPF) for matching.
    Removes special characters and formatting but PRESERVES leading zeros.
    Handles floating point numbers from pandas (e.g., 17564611634.0 -> 17564611634).
    """
    if not documento or pd.isna(documento):
        return ""
    
    import re
    
    # Convert to string and handle floating point numbers
    doc_str = str(documento)
    
    # If it's a float (ends with .0), remove the decimal part
    if doc_str.endswith('.0'):
        doc_str = doc_str[:-2]
    
    # Remove all non-numeric characters but preserve leading zeros
    clean = re.sub(r'[^0-9]', '', doc_str)
    
    # Do NOT remove leading zeros - they are part of the CPF format
    # CPF numbers like 00946789606 should remain as 00946789606
    
    return clean

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_properties_for_phone(phone_number: str) -> List[Dict]:
    """
    Get all properties for a given phone number.
    Flow: phone_number -> CPF (via spreadsheet) -> properties (via mega_data_set)
    """
    try:
        # DEVELOPMENT MODE: Add test phone mappings for testing
        test_phone_mappings = {
            '+5531998234201': '12345678901',  # João Ribas
            '5531998234201': '12345678901',   # João Ribas (without +)
            '+5531988606027': '17564611634',  # Newton Pena Vitral  
            '5531988606027': '17564611634',   # Newton Pena Vitral (without +)
            '553188606027': '17564611634',    # Newton Pena Vitral (DB format)
        }
        
        # Check if this is a test phone first
        clean_phone = clean_phone_for_match(phone_number)
        for test_phone, test_cpf in test_phone_mappings.items():
            if clean_phone_for_match(test_phone) == clean_phone:
                properties = find_properties_by_documento(test_cpf)
                return properties
        
        # Step 1: Get CPF from spreadsheet using phone number
        from services.spreadsheet import get_sheet_data
        
        sheet_data = get_sheet_data()
        if not sheet_data:
            print("No spreadsheet data available")
            return []
        
        # Find CPF column and phone column
        headers = sheet_data[0] if sheet_data else []
        
        cpf_col_index = None
        phone_col_index = None
        
        for i, header in enumerate(headers):
            header_lower = str(header).lower()
            
            # Look for CPF column
            if any(term in header_lower for term in ['cpf', 'documento', 'doc']):
                cpf_col_index = i
            
            # Look for phone column  
            if any(term in header_lower for term in ['celular', 'phone', 'telefone', 'contato']):
                phone_col_index = i
        
        if cpf_col_index is None or phone_col_index is None:
            print("CPF or phone column not found in spreadsheet")
            return []
        
        # Find matching phone number and get CPF using enhanced matching logic
        from services.spreadsheet import find_phone_match
        
        # Use the same enhanced matching logic as spreadsheet.py
        match_result = find_phone_match(phone_number, sheet_data)
        cpf = None
        
        if match_result:
            matched_row, _ = match_result
            if cpf_col_index < len(matched_row):
                cpf = matched_row[cpf_col_index]
        
        if not cpf:
            print(f"CPF not found for phone number {phone_number}")
            return []
        
        # Step 2: Get properties from mega_data_set using CPF
        properties = find_properties_by_documento(cpf)
        
        return properties
        
    except Exception as e:
        print(f"Error getting properties for phone {phone_number}: {e}")
        return []

def clean_phone_for_match(phone: str) -> str:
    """
    Clean phone number for matching (same logic as in spreadsheet.py).
    """
    if not phone or pd.isna(phone):
        return ""
    
    import re
    # Remove whitespace and special characters
    clean = re.sub(r'[\s\t\n\r]', '', str(phone))
    clean = re.sub(r'[^0-9]', '', clean)
    
    # Remove @s.whatsapp.net if present
    if '@' in str(phone):
        clean = str(phone).split('@')[0]
        clean = re.sub(r'[^0-9]', '', clean)
    
    # Handle edge cases
    if len(clean) < 8:
        return ""
    
    # Remove country code if present
    if clean.startswith('55') and len(clean) > 10:
        clean = clean[2:]
    
    # Brazilian area codes
    valid_area_codes = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99']
    
    # If 10 digits and starts with valid area code, add mobile prefix
    if len(clean) == 10 and clean[:2] in valid_area_codes:
        area_code = clean[:2]
        number = clean[2:]
        # If it's a mobile number and doesn't have 9 prefix
        if number[0] in '6789' and not number.startswith('9'):
            clean = area_code + '9' + number
    
    return clean

def format_property_for_display(property_dict: Dict) -> Dict:
    """
    Format property data for display in the UI.
    """
    formatted = {}
    
    # Map common field names to display names
    field_mapping = {
        'INDICE CADASTRAL': 'indice_cadastral',
        'ENDERECO': 'endereco',
        'BAIRRO': 'bairro',
        'COMPLEMENTO ENDERECO': 'complemento',
        'TIPO CONSTRUTIVO': 'tipo_construtivo',
        'ANO CONSTRUCAO': 'ano_construcao',
        'AREA CONSTRUCAO': 'area_construcao',
        'AREA TERRENO': 'area_terreno',
        'FRACAO IDEAL': 'fracao_ideal'
    }
    
    for original_key, display_key in field_mapping.items():
        # Look for the key in the property dict (case-insensitive)
        for key in property_dict.keys():
            if key.upper() == original_key.upper():
                formatted[display_key] = property_dict[key]
                break
    
    return formatted

def get_property_summary_stats() -> Dict:
    """
    Get summary statistics about the mega_data_set.
    """
    df = load_mega_data_set()
    if df.empty:
        return {}
    
    stats = {
        'total_records': len(df),
        'total_columns': len(df.columns),
        'unique_properties': 0,
        'unique_owners': 0,
        'property_types': {},
        'neighborhoods': {}
    }
    
    # Count unique properties (by INDICE CADASTRAL)
    indice_col = None
    for col in df.columns:
        if 'INDICE' in col.upper() and 'CADASTRAL' in col.upper():
            indice_col = col
            break
    
    if indice_col:
        stats['unique_properties'] = df[indice_col].nunique()
    
    # Count unique owners (by DOCUMENTO PROPRIETARIO)
    doc_col = None
    for col in df.columns:
        if 'DOCUMENTO' in col.upper() and 'PROPRIETARIO' in col.upper():
            doc_col = col
            break
    
    if doc_col:
        stats['unique_owners'] = df[doc_col].nunique()
    
    # Property types
    tipo_col = None
    for col in df.columns:
        if 'TIPO' in col.upper() and 'CONSTRUTIVO' in col.upper():
            tipo_col = col
            break
    
    if tipo_col:
        stats['property_types'] = df[tipo_col].value_counts().to_dict()
    
    # Neighborhoods
    bairro_col = None
    for col in df.columns:
        if 'BAIRRO' in col.upper():
            bairro_col = col
            break
    
    if bairro_col:
        stats['neighborhoods'] = df[bairro_col].value_counts().head(20).to_dict()
    
    return stats

def clear_cache():
    """Clear the cached mega_data_set."""
    global _cached_mega_data, _cache_timestamp
    _cached_mega_data = None
    _cache_timestamp = 0
    
    # Also clear Streamlit cache
    if hasattr(st, 'cache_data'):
        st.cache_data.clear()