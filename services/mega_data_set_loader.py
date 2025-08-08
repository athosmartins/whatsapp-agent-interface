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
import duckdb

# Import centralized phone utilities
from services.phone_utils import clean_phone_for_matching, generate_phone_variants

# Google Drive folder ID for mega_data_set files
MEGA_DATA_SET_FOLDER_ID = "1yfGnHjmaEOCbrEcYLXaVcW5_bmvzU98l"
CACHE_DURATION = 3600  # 1 hour in seconds

# CONFIGURATION: Set the path to your real mega_data_set file here
# Replace this with the actual path to your 350k+ row file
MANUAL_MEGA_DATA_SET_PATH = None  # e.g., "/path/to/your/real_mega_data_set.csv"

# Global variable to store cached data - REMOVED: using @st.cache_data instead to avoid double-caching
# _cached_mega_data = None
# _cache_timestamp = 0

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
                
                # PRODUCTION: Memory optimization for large datasets
                if len(df) > 100000:
                    print("PRODUCTION: Applying memory optimizations for large dataset")
                    # Convert string columns to category where beneficial
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            unique_ratio = df[col].nunique() / len(df)
                            if unique_ratio < 0.1:  # Less than 10% unique values
                                df[col] = df[col].astype('category')
                                print(f"  Converted {col} to category (saved memory)")
                    
                    # Force garbage collection
                    import gc
                    gc.collect()
                
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
        
        # Prioritize Parquet files first for optimal performance (65MB vs 110MB)
        parquet_files = [f for f in files if f['name'].lower().endswith('.parquet')]
        compressed_files = [f for f in files if f['name'].lower().endswith('.json.gz')]
        csv_files = [f for f in files if f['name'].lower().endswith('.csv')]
        
        if parquet_files:
            newest_file = parquet_files[0]  # Already sorted by creation time
            file_extension = '.parquet'
            print(f"Using Parquet file for optimal performance: {newest_file['name']}")
        elif compressed_files:
            newest_file = compressed_files[0]  # Already sorted by creation time
            file_extension = '.json.gz'
            print(f"Using compressed JSON file: {newest_file['name']}")
        elif csv_files:
            newest_file = csv_files[0]
            file_extension = '.csv'
            print(f"Using CSV file: {newest_file['name']}")
        else:
            print("No supported file formats found (looking for .parquet, .json.gz or .csv)")
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

@st.cache_data(ttl=CACHE_DURATION, max_entries=1)
def load_mega_data_set(mode: str = "essential", bairros: list = None) -> pd.DataFrame:
    """
    UNIFIED MEMORY-EFFICIENT mega_data_set loader.
    
    Args:
        mode: Loading mode
            - "essential": Load only essential columns (~10MB vs ~200MB)
            - "bairros": Load data for specific bairros only (memory efficient)
            - "full": Load complete dataset (NOT RECOMMENDED - will crash in production)
        bairros: List of bairros to load (only used with mode="bairros")
    
    Returns:
        pandas.DataFrame: Loaded data based on mode
    """
    # CRITICAL: Prevent full dataset loading in production
    import os
    IS_PRODUCTION = os.getenv("STREAMLIT_SERVER_HEADLESS") == "true"
    
    if mode == "full" and IS_PRODUCTION:
        print("⚠️ WARNING: Full dataset loading blocked in production (memory limit)")
        print("   Falling back to essential columns mode")
        mode = "essential"
    
    # Mode: Load only specific bairros (most memory efficient)
    if mode == "bairros" and bairros:
        return load_bairros_optimized(bairros)
    
    # Mode: Load only essential columns (80% memory reduction)
    if mode == "essential":
        return _load_essential_columns_only()
        
    # Mode: Full dataset (legacy support, memory intensive)
    # Note: Manual caching removed - using @st.cache_data instead to avoid double-caching
    
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
            print("   1. Download the latest file from Google Drive folder ID: 1yfGnHjmaEOCbrEcYLXaVcW5_bmvzU98l")
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
        
        # Note: Manual caching removed - using @st.cache_data instead
        
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

@st.cache_data(ttl=3600, max_entries=10)  # Cache for 1 hour
def find_properties_by_documento(documento_proprietario: str) -> List[Dict]:
    """
    Find all properties that belong to a specific document holder (CPF).
    Returns a list of property dictionaries.
    """
    # Use essential columns mode for memory efficiency
    df = load_mega_data_set(mode="essential")
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

@st.cache_data(ttl=3600, max_entries=20)  # Cache for 1 hour
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
        clean_phone = clean_phone_for_matching(phone_number)
        for test_phone, test_cpf in test_phone_mappings.items():
            if clean_phone_for_matching(test_phone) == clean_phone:
                properties = find_properties_by_documento(test_cpf)
                return properties
        
        # Step 1: Get CPF from spreadsheet using phone number (controlled loading)
        from services.spreadsheet import get_sheet_data
        
        sheet_data = get_sheet_data()  # Uses session-controlled loading
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
    Clean phone number for matching using centralized utilities.
    """
    # Use centralized phone utility for consistent behavior
    return clean_phone_for_matching(phone)

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
    # Use essential columns mode for memory efficiency
    df = load_mega_data_set(mode="essential")
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
    # Note: Manual caching removed - only clear Streamlit cache
    if hasattr(st, 'cache_data'):
        st.cache_data.clear()

@st.cache_data(ttl=3600, max_entries=5)
def get_available_bairros() -> List[str]:
    """Get list of available bairros without loading full dataset."""
    try:
        # Try to get bairros from a smaller query first
        from services.google_drive_loader import GoogleDriveLoader
        
        loader = GoogleDriveLoader()
        files = loader.list_files(MEGA_DATA_SET_FOLDER_ID)
        
        # Find parquet or compressed JSON files (prioritize parquet)
        data_files = [f for f in files if f['name'].lower().endswith(('.parquet', '.json.gz'))]
        
        if data_files:
            # Sort by creation time and get the newest
            data_files.sort(key=lambda x: x['createdTime'], reverse=True)
            newest_file = data_files[0]
            
            # Download the file
            temp_path = f"/tmp/mega_data_set_{newest_file['id']}.{newest_file['name'].split('.')[-1]}"
            success = loader.download_file(newest_file['id'], temp_path)
            
            if success:
                # Use DuckDB to efficiently query just the BAIRRO column
                if temp_path.endswith('.parquet'):
                    query = "SELECT DISTINCT BAIRRO FROM read_parquet('{}') WHERE BAIRRO IS NOT NULL ORDER BY BAIRRO".format(temp_path)
                else:
                    # For JSON, load minimal data
                    df = load_compressed_json(temp_path)
                    bairro_col = None
                    for col in df.columns:
                        if 'BAIRRO' in col.upper():
                            bairro_col = col
                            break
                    
                    if bairro_col:
                        bairros = df[bairro_col].dropna().unique()
                        bairros = sorted([str(b) for b in bairros])
                        
                        # Clean up temp file
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                            
                        return bairros
                
                if temp_path.endswith('.parquet'):
                    bairros_df = duckdb.query(query).df()
                    bairros = bairros_df['BAIRRO'].tolist()
                    
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                        
                    return bairros
        
        # Fallback: load essential columns and extract bairros
        df = load_mega_data_set(mode="essential")
        if df.empty:
            return []
        
        # Find bairro column
        bairro_col = None
        for col in df.columns:
            if 'BAIRRO' in col.upper():
                bairro_col = col
                break
        
        if bairro_col:
            bairros = df[bairro_col].dropna().unique()
            return sorted([str(b) for b in bairros])
        
        return []
        
    except Exception as e:
        print(f"Error getting available bairros: {e}")
        return []

@st.cache_data(ttl=3600, max_entries=10)
def get_data_by_bairros(selected_bairros: List[str]) -> pd.DataFrame:
    """
    Load ONLY the data for selected bairros - this is the memory game-changer!
    Reduces memory usage by 95%+ since users typically select 1-2 bairros out of 20-30.
    """
    if not selected_bairros:
        return pd.DataFrame()
        
    try:
        # Try to use DuckDB for efficient filtering
        from services.google_drive_loader import GoogleDriveLoader
        
        loader = GoogleDriveLoader()
        files = loader.list_files(MEGA_DATA_SET_FOLDER_ID)
        
        # Find parquet files first (most efficient - 65MB vs 110MB)
        parquet_files = [f for f in files if f['name'].lower().endswith('.parquet')]
        
        if parquet_files:
            # Sort by creation time and get the newest
            parquet_files.sort(key=lambda x: x['createdTime'], reverse=True)
            newest_file = parquet_files[0]
            
            # Download the parquet file
            temp_path = f"/tmp/mega_data_set_{newest_file['id']}.parquet"
            success = loader.download_file(newest_file['id'], temp_path)
            
            if success:
                # PROTECTED: Story #001 - Use essential columns only (memory optimization)
                essential_columns = [
                    'DOCUMENTO PROPRIETARIO', 'BAIRRO', 'ENDERECO', 'INDICE CADASTRAL',
                    'COMPLEMENTO ENDERECO', 'AREA CONSTRUCAO', 'AREA TERRENO', 'TIPO CONSTRUTIVO',
                    'ANO CONSTRUCAO', 'NOME LOGRADOURO', 'NUMERO', 'GEOMETRY'
                ]
                columns_str = ', '.join([f'"{col}"' for col in essential_columns])
                bairros_str = "', '".join(selected_bairros)
                query = f"""
                    SELECT {columns_str} FROM read_parquet('{temp_path}') 
                    WHERE BAIRRO IN ('{bairros_str}')
                """
                df = duckdb.query(query).df()
                
                print(f"Loaded {len(df):,} rows for bairros: {', '.join(selected_bairros)}")
                
                # Clean up temp file
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                
                return df
        
        # Fallback: load essential columns and filter (memory efficient)
        full_df = load_mega_data_set(mode="essential")
        if full_df.empty:
            return pd.DataFrame()
        
        # Find bairro column
        bairro_col = None
        for col in full_df.columns:
            if 'BAIRRO' in col.upper():
                bairro_col = col
                break
        
        if bairro_col:
            filtered_df = full_df[full_df[bairro_col].isin(selected_bairros)].copy()
            print(f"Filtered to {len(filtered_df):,} rows for bairros: {', '.join(selected_bairros)}")
            return filtered_df
        
        return full_df
        
    except Exception as e:
        print(f"Error loading data by bairros: {e}")
        return pd.DataFrame()

def get_slice(offset=0, limit=20000):
    """
    Get a slice of the mega_data_set using DuckDB for memory-efficient paging.
    
    Args:
        offset: Starting row (0-indexed)
        limit: Maximum number of rows to return
        
    Returns:
        DataFrame with the requested slice
    """
    try:
        # First, try to find a parquet file for optimal performance
        from services.google_drive_loader import GoogleDriveLoader
        
        loader = GoogleDriveLoader()
        files = loader.list_files(MEGA_DATA_SET_FOLDER_ID)
        
        # Find the newest parquet file (prioritized for 65MB vs 110MB efficiency)
        parquet_files = [f for f in files if f['name'].lower().endswith('.parquet')]
        
        if parquet_files:
            # Sort by creation time and get the newest
            parquet_files.sort(key=lambda x: x['createdTime'], reverse=True)
            newest_file = parquet_files[0]
            
            # Download the parquet file
            temp_path = f"/tmp/mega_data_set_{newest_file['id']}.parquet"
            success = loader.download_file(newest_file['id'], temp_path)
            
            if success:
                # PROTECTED: Story #001 - Use essential columns only (memory optimization)
                essential_columns = [
                    'DOCUMENTO PROPRIETARIO', 'BAIRRO', 'ENDERECO', 'INDICE CADASTRAL',
                    'COMPLEMENTO ENDERECO', 'AREA CONSTRUCAO', 'AREA TERRENO', 'TIPO CONSTRUTIVO',
                    'ANO CONSTRUCAO', 'NOME LOGRADOURO', 'NUMERO', 'GEOMETRY'
                ]
                columns_str = ', '.join([f'"{col}"' for col in essential_columns])
                query = f"""
                    SELECT {columns_str} FROM '{temp_path}' 
                    LIMIT {limit} OFFSET {offset}
                """
                df = duckdb.query(query).df()
                
                # Clean up temp file
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                
                return df
        
        # Fallback: use essential columns loading with manual slicing
        full_df = load_mega_data_set(mode="essential")
        if full_df.empty:
            return pd.DataFrame()
        
        # Manual slicing
        end_idx = offset + limit
        sliced_df = full_df.iloc[offset:end_idx].copy()
        
        return sliced_df
        
    except Exception as e:
        print(f"Error getting slice: {e}")
        return pd.DataFrame()

# CRITICAL FIX: DuckDB predicate push-down for memory efficiency
# Global parquet file path (downloaded once)
PARQUET_FILE = "/tmp/mega_data_set.parquet"

# Clear parquet file on startup to ensure fresh conversion
if os.path.exists(PARQUET_FILE):
    try:
        os.remove(PARQUET_FILE)
        print("Cleared existing parquet file for fresh conversion")
    except:
        pass

def list_bairros_optimized():
    """Get list of available bairros using the most efficient method (Parquet first, then JSON)."""
    try:
        # Download the latest file (prioritizes Parquet)
        file_path = download_latest_mega_data_set()
        if not file_path:
            print("No data file found, using fallback bairros")
            return _get_fallback_bairros()
        
        # Handle Parquet files (most efficient)
        if file_path.lower().endswith('.parquet'):
            print("Loading bairros from Parquet file using DuckDB...")
            try:
                # Use DuckDB for efficient querying
                query = f"SELECT DISTINCT BAIRRO FROM '{file_path}' WHERE BAIRRO IS NOT NULL ORDER BY BAIRRO"
                bairros_df = duckdb.query(query).df()
                bairros = bairros_df['BAIRRO'].tolist()
                
                # FIXED: Don't delete file immediately - let system handle cleanup
                # File may be needed by concurrent requests
                # if os.path.exists(file_path):
                #     os.remove(file_path)
                
                print(f"Loaded {len(bairros)} bairros from Parquet file")
                return bairros
                
            except Exception as e:
                print(f"Error loading bairros from Parquet: {e}")
                # Fall back to pandas approach
                try:
                    df = pd.read_parquet(file_path)
                    bairro_col = None
                    for col in df.columns:
                        if 'BAIRRO' in col.upper():
                            bairro_col = col
                            break
                    
                    if bairro_col:
                        bairros = df[bairro_col].dropna().unique()
                        bairros = sorted([str(b) for b in bairros])
                        
                        # Clean up downloaded file
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        
                        print(f"Loaded {len(bairros)} bairros from Parquet file (pandas)")
                        return bairros
                        
                except Exception as e2:
                    print(f"Error loading bairros from Parquet with pandas: {e2}")
        
        # Handle JSON.GZ files (legacy support)
        elif file_path.lower().endswith('.json.gz'):
            print("Loading bairros from JSON.GZ file...")
            bairros_set = set()
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                # Skip metadata lines
                line = f.readline()
                while line and not line.startswith('### DATA ###'):
                    line = f.readline()
                
                # Process JSON lines to extract BAIRRO values
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            row = json.loads(line)
                            bairro = row.get('BAIRRO')
                            if bairro and bairro.strip():
                                bairros_set.add(bairro.strip())
                        except json.JSONDecodeError:
                            continue  # Skip invalid lines
            
            # Clean up downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)
            
            # Convert to sorted list
            bairros = sorted(list(bairros_set))
            print(f"Loaded {len(bairros)} bairros from JSON.GZ file")
            return bairros
        
        else:
            print(f"Unsupported file format: {file_path}")
            return _get_fallback_bairros()
        
    except Exception as e:
        print(f"Error in list_bairros_optimized: {e}")
        print("Falling back to hardcoded bairros...")
        return _get_fallback_bairros()

def _get_fallback_bairros():
    """Get fallback bairros for BH area."""
    return [
        "Centro", "Savassi", "Funcionários", "Lourdes", "Buritis",
        "Cidade Nova", "Prado", "Serra", "Belvedere", "Mangabeiras",
        "Coração Eucarístico", "Pampulha", "Cidade Jardim", "Anchieta",
        "Floresta", "Sagrada Família", "Jardim América", "Liberdade",
        "Ouro Preto", "Castelo", "Gutierrez", "Barro Preto", "Carmo",
        "Grajaú", "Boa Vista", "Cruzeiro", "Luxemburgo", "Sion"
    ]

# PROTECTED: Story #001 - Memory-efficient bairros loading prevents crashes
# DO NOT MODIFY: This function uses essential columns only to prevent memory crashes
# See USER_STORIES.md Story #001 for context
# Regression tests will fail if this protection is removed
def load_bairros_optimized(bairros: list):
    """Load data for selected bairros using the most efficient method (Parquet first, then JSON)."""
    if not bairros:
        return pd.DataFrame()
        
    try:
        # Download the latest file (prioritizes Parquet)
        file_path = download_latest_mega_data_set()
        if not file_path:
            print("No data file found, using sample data")
            return _get_sample_data(bairros)
        
        # Handle Parquet files (most efficient)
        if file_path.lower().endswith('.parquet'):
            print(f"Loading data for {len(bairros)} bairros from Parquet file using DuckDB...")
            try:
                # PROTECTED: Story #001 - Use essential columns only (memory optimization)
                # DO NOT MODIFY: This prevents memory crashes by loading only 12 columns instead of 119
                # See USER_STORIES.md Story #001 for context
                essential_columns = [
                    'DOCUMENTO PROPRIETARIO', 'BAIRRO', 'ENDERECO', 'INDICE CADASTRAL',
                    'COMPLEMENTO ENDERECO', 'AREA CONSTRUCAO', 'AREA TERRENO', 'TIPO CONSTRUTIVO',
                    'ANO CONSTRUCAO', 'NOME LOGRADOURO', 'NUMERO', 'GEOMETRY'
                ]
                columns_str = ', '.join([f'"{col}"' for col in essential_columns])
                bairros_str = "', '".join(bairros)
                query = f"SELECT {columns_str} FROM '{file_path}' WHERE BAIRRO IN ('{bairros_str}')"
                df = duckdb.query(query).df()
                
                # FIXED: Don't delete file immediately - let system handle cleanup
                # File may be needed by concurrent requests
                # if os.path.exists(file_path):
                #     os.remove(file_path)
                
                print(f"Loaded {len(df):,} rows for bairros from Parquet file: {', '.join(bairros)}")
                return df
                
            except Exception as e:
                print(f"Error loading bairros from Parquet: {e}")
                # Fall back to pandas approach
                try:
                    df = pd.read_parquet(file_path)
                    bairro_col = None
                    for col in df.columns:
                        if 'BAIRRO' in col.upper():
                            bairro_col = col
                            break
                    
                    if bairro_col:
                        filtered_df = df[df[bairro_col].isin(bairros)].copy()
                        
                        # Clean up downloaded file
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        
                        print(f"Loaded {len(filtered_df):,} rows for bairros from Parquet file (pandas): {', '.join(bairros)}")
                        return filtered_df
                        
                except Exception as e2:
                    print(f"Error loading bairros from Parquet with pandas: {e2}")
        
        # Handle JSON.GZ files (legacy support)
        elif file_path.lower().endswith('.json.gz'):
            print(f"Loading data for {len(bairros)} bairros from JSON.GZ file...")
            bairros_set = set(bairros)
            matching_rows = []
            
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                # Skip metadata lines
                line = f.readline()
                while line and not line.startswith('### DATA ###'):
                    line = f.readline()
                
                # Process JSON lines to extract rows for matching bairros
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            row = json.loads(line)
                            bairro = row.get('BAIRRO')
                            if bairro and bairro.strip() in bairros_set:
                                matching_rows.append(row)
                        except json.JSONDecodeError:
                            continue  # Skip invalid lines
            
            # Clean up downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)
            
            # Convert to DataFrame
            if matching_rows:
                df = pd.DataFrame(matching_rows)
                print(f"Loaded {len(df):,} rows for bairros from JSON.GZ file: {', '.join(bairros)}")
                return df
            else:
                print("No matching rows found, using sample data")
                return _get_sample_data(bairros)
        
        else:
            print(f"Unsupported file format: {file_path}")
            return _get_sample_data(bairros)
        
    except Exception as e:
        print(f"Error in load_bairros_optimized: {e}")
        print("Falling back to sample data...")
        try:
            return _get_sample_data(bairros)
        except Exception as e2:
            print(f"Error creating sample data: {e2}")
            # Return empty DataFrame as absolute fallback
            return pd.DataFrame({
                'BAIRRO': [],
                'ENDERECO': [],
                'INDICE_CADASTRAL': [],
                'GEOMETRY': [],
                'AREA_CONSTRUCAO': [],
                'AREA_TERRENO': [],
                'TIPO_CONSTRUTIVO': []
            })

def _get_sample_data(bairros: list):
    """Get sample data for selected bairros."""
    sample_data = []
    for i, bairro in enumerate(bairros):
        for j in range(5):  # 5 sample properties per bairro
            sample_data.append({
                'BAIRRO': bairro,
                'ENDERECO': f'Rua {bairro} {j+1}',
                'INDICE_CADASTRAL': f'{bairro[:3].upper()}{i:03d}{j:03d}',
                'GEOMETRY': f'POINT(-43.{950+i} -19.{900+j})',
                'AREA_CONSTRUCAO': 100 + (i * 10) + j,
                'AREA_TERRENO': 200 + (i * 20) + j,
                'TIPO_CONSTRUTIVO': 'Residencial'
            })
    
    df = pd.DataFrame(sample_data)
    print(f"Created sample data with {len(df):,} rows for bairros: {', '.join(bairros)}")
    return df

def _ensure_parquet_file() -> bool:
    """Ensure parquet file exists, download if needed."""
    if os.path.exists(PARQUET_FILE):
        print("Parquet file already exists")
        return True
        
    # Try to convert JSON.GZ to Parquet for DuckDB compatibility using pandas chunking
    try:
        print("Attempting to convert JSON.GZ to Parquet using pandas chunking...")
        
        # Download the JSON.GZ file first
        file_path = download_latest_mega_data_set()
        if not file_path or not file_path.lower().endswith('.json.gz'):
            print("No JSON.GZ file found for conversion")
            return False
        
        print(f"Converting JSON.GZ to Parquet using pandas chunking...")
        
        # Use the same chunked approach as load_compressed_json but save to Parquet
        import tempfile
        rows = []
        row_count = 0
        chunk_size = 10000  # Process in smaller chunks
        
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
            
            # Process data in chunks to avoid memory issues
            is_first_chunk = True
            for line in f:
                line = line.strip()
                if line:
                    try:
                        row = json.loads(line)
                        rows.append(row)
                        row_count += 1
                        
                        # Process in chunks
                        if len(rows) >= chunk_size:
                            chunk_df = pd.DataFrame(rows)
                            
                            # Write to parquet (append mode after first chunk)
                            if is_first_chunk:
                                chunk_df.to_parquet(PARQUET_FILE, index=False)
                                is_first_chunk = False
                            else:
                                # Append to existing parquet file
                                existing_df = pd.read_parquet(PARQUET_FILE)
                                combined_df = pd.concat([existing_df, chunk_df], ignore_index=True)
                                combined_df.to_parquet(PARQUET_FILE, index=False)
                            
                            rows = []  # Clear chunk
                            print(f"  Processed {row_count:,} rows...")
                            
                    except json.JSONDecodeError:
                        continue  # Skip invalid lines
            
            # Process remaining rows
            if rows:
                chunk_df = pd.DataFrame(rows)
                if is_first_chunk:
                    chunk_df.to_parquet(PARQUET_FILE, index=False)
                else:
                    existing_df = pd.read_parquet(PARQUET_FILE)
                    combined_df = pd.concat([existing_df, chunk_df], ignore_index=True)
                    combined_df.to_parquet(PARQUET_FILE, index=False)
        
        # Clean up downloaded file
        if os.path.exists(file_path):
            os.remove(file_path)
        
        print(f"Successfully converted JSON.GZ to Parquet ({row_count:,} rows)")
        return True
        
    except Exception as e:
        print(f"Error converting JSON.GZ to Parquet: {e}")
        return False

# PROTECTED: Story #001 - Memory-efficient mega data set loading
# DO NOT MODIFY: This code provides 80% memory reduction by loading only essential columns
# See USER_STORIES.md Story #001 for context
# Regression tests will fail if this protection is removed
@st.cache_data(ttl=CACHE_DURATION, max_entries=1)
def _load_essential_columns_only() -> pd.DataFrame:
    """
    Load only essential columns from mega_data_set (80% memory reduction).
    Used for property lookups and general queries.
    """
    # Essential columns for property matching and display
    ESSENTIAL_COLUMNS = [
        'DOCUMENTO PROPRIETARIO',  # For CPF matching
        'BAIRRO',                  # For neighborhood filtering  
        'ENDERECO',                # For address display
        'INDICE CADASTRAL',        # For property identification
        'COMPLEMENTO ENDERECO',    # For complete address
        'AREA CONSTRUCAO',         # For property details
        'AREA TERRENO',            # For property details
        'TIPO CONSTRUTIVO',        # For property type
        'ANO CONSTRUCAO',          # For property age
        'NOME LOGRADOURO',         # For street name
        'NUMERO',                  # For property number
        'GEOMETRY'                 # For mapping (if available)
    ]
    
    try:
        # Download the latest file (prioritizes Parquet)
        file_path = download_latest_mega_data_set()
        if not file_path:
            print("No data file found for essential columns loading")
            return pd.DataFrame()
        
        # Handle Parquet files (most efficient with column selection)
        if file_path.lower().endswith('.parquet'):
            print("Loading essential columns from Parquet file...")
            try:
                # Load only essential columns using pandas column selection
                df = pd.read_parquet(file_path, columns=ESSENTIAL_COLUMNS)
                
                # FIXED: Don't delete file immediately - let system handle cleanup
                # File may be needed by concurrent requests
                # if os.path.exists(file_path):
                #     os.remove(file_path)
                
                print(f"✅ Loaded {len(df):,} rows with {len(df.columns)} essential columns (memory optimized)")
                return df
                
            except Exception as e:
                print(f"Error loading essential columns from Parquet: {e}")
                # Fall back to full load then filter columns
                try:
                    df = pd.read_parquet(file_path)
                    available_columns = [col for col in ESSENTIAL_COLUMNS if col in df.columns]
                    df = df[available_columns].copy()
                    
                    # Clean up downloaded file
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    
                    print(f"✅ Loaded {len(df):,} rows with {len(df.columns)} essential columns (fallback)")
                    return df
                    
                except Exception as e2:
                    print(f"Error loading essential columns from Parquet (fallback): {e2}")
        
        # Handle other formats (load all then filter columns)
        elif file_path.lower().endswith('.json.gz'):
            print("Loading essential columns from JSON.GZ file...")
            # Load compressed JSON
            df = load_compressed_json(file_path)
            
            # Filter to essential columns only
            available_columns = [col for col in ESSENTIAL_COLUMNS if col in df.columns]
            df = df[available_columns].copy()
            
            # Clean up downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)
            
            print(f"✅ Loaded {len(df):,} rows with {len(df.columns)} essential columns from JSON.GZ")
            return df
        
        else:
            # CSV files
            print("Loading essential columns from CSV file...")
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    # Try to load only essential columns
                    df = pd.read_csv(file_path, encoding=encoding, low_memory=False, usecols=ESSENTIAL_COLUMNS)
                    print(f"Successfully loaded essential columns with {encoding} encoding")
                    break
                except (UnicodeDecodeError, ValueError):
                    # If column selection fails, load all then filter
                    try:
                        df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                        available_columns = [col for col in ESSENTIAL_COLUMNS if col in df.columns]
                        df = df[available_columns].copy()
                        print(f"Successfully loaded and filtered essential columns with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
            
            if df is None:
                print("Failed to load essential columns with any encoding")
                return pd.DataFrame()
            
            # Clean up downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)
            
            print(f"✅ Loaded {len(df):,} rows with {len(df.columns)} essential columns from CSV")
            return df
        
    except Exception as e:
        print(f"Error loading essential columns: {e}")
        return pd.DataFrame()

# Legacy compatibility functions (to be updated gradually)
def get_dataframe() -> pd.DataFrame:
    """
    LEGACY: Return essential columns only for memory efficiency.
    This replaces direct calls to load_mega_data_set() without parameters.
    """
    return load_mega_data_set(mode="essential")

def get_properties() -> pd.DataFrame:
    """
    LEGACY: Return essential columns only for memory efficiency.
    Alternative name used by some services.
    """
    return load_mega_data_set(mode="essential")