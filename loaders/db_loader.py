# db_loader.py  ─ one-path loader
import os
import sqlite3
import pandas as pd
import time
import streamlit as st
import duckdb

# Import centralized phone utilities
from services.phone_utils import clean_phone_for_matching, generate_phone_variants

# Import debug logging
from services.debug_logger import debug_log

GDRIVE_FILE_ID   = "1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw"   # snapshot folder/file ID
LOCAL_DB_PATH    = "whatsapp_conversations.db"           # always the same name
TABLE            = "deepseek_results"                    # what the UI expects

# Global variable to store database info for debug mode
_last_db_info = None

def _download_from_drive(dest: str):
    """Download only the newest database file from Google Drive using Google Drive API."""
    try:
        import json
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
        
        # Load credentials from Streamlit secrets or local file
        credentials_info = None
        
        # Google API scopes
        SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
        
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
                _download_from_drive_fallback(dest)
                return
            
            # Load credentials
            with open(CREDENTIALS_FILE, 'r') as f:
                credentials_info = json.load(f)
            print("Using Google Drive credentials from local file")
        
        credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=credentials)
        
        print("Finding newest database file on Google Drive...")
        
        # List files in the folder, ordered by modification time (newest first)
        folder_id = "1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw"
        query = f"'{folder_id}' in parents and trashed=false and name contains '.db'"
        results = drive_service.files().list(
            q=query,
            orderBy='modifiedTime desc',
            fields='files(id, name, createdTime, modifiedTime)',
            pageSize=10  # Only get top 10 files
        ).execute()
        
        files = results.get('files', [])
        if not files:
            print("No database files found in Google Drive folder")
            _download_from_drive_fallback(dest)
            return
        
        # Get the newest file (first in the list due to orderBy='createdTime desc')
        newest_file = files[0]
        file_id = newest_file['id']
        file_name = newest_file['name']
        
        print(f"Downloading newest file: {file_name} (created: {newest_file['createdTime']}, modified: {newest_file.get('modifiedTime', 'N/A')})")
        
        # Download only this specific file
        request = drive_service.files().get_media(fileId=file_id)
        
        # Download with progress tracking
        with open(dest, 'wb') as local_file:
            downloader = MediaIoBaseDownload(local_file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Download progress: {int(status.progress() * 100)}%")
        
        print(f"Successfully downloaded: {file_name}")
        
        # Store original filename and modification time for debug info
        global _last_db_info
        _last_db_info = {
            'original_filename': file_name,
            'last_modified': os.path.getmtime(dest)
        }
        
    except Exception as e:
        print(f"Error downloading with Google Drive API: {e}")
        print("Falling back to gdown method...")
        _download_from_drive_fallback(dest)

def _download_from_drive_fallback(dest: str):
    """Fallback method using direct file download (no longer downloads entire folder)."""
    import gdown
    
    # Since we can't use the Drive API, we'll need to use a direct file ID
    # This is a limitation - fallback should ideally use the newest file ID
    # For now, we'll have to use a known recent file ID or implement folder listing via gdown
    
    print("⚠️ Fallback method: Cannot determine newest file without Drive API")
    print("⚠️ Consider configuring Google Drive API credentials for optimal performance")
    
    # Try to get file listing using gdown folder method but exit early to avoid full download
    import tempfile
    import pathlib
    import shutil
    import os
    import requests
    
    try:
        # Alternative: Try to get the folder contents via requests to find newest file
        folder_url = f"https://drive.google.com/drive/folders/{GDRIVE_FILE_ID}"
        
        # This is a simplified fallback - in practice, you might want to implement
        # a more sophisticated method to determine the latest file ID
        print("📥 Using fallback download method...")
        
        # For now, create a temporary directory and download folder, but be more selective
        tmp_dir = tempfile.mkdtemp()
        try:
            gdown.download_folder(
                id="1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw",
                output=tmp_dir,
                quiet=True,  # Reduce output noise
                use_cookies=False
            )
            
            # Find all .db files and get the newest one
            db_files = list(pathlib.Path(tmp_dir).glob("*.db"))
            if not db_files:
                raise Exception("No .db files found in the downloaded folder")
            
            # Get the newest file by modification time
            newest = max(db_files, key=os.path.getmtime)
            print(f"📦 Selected newest file from folder: {newest.name}")
            
            # Store original filename and modification time for debug info
            global _last_db_info
            _last_db_info = {
                'original_filename': newest.name,
                'last_modified': os.path.getmtime(newest)
            }
            
            # Move only the newest file to destination
            shutil.move(str(newest), dest)
            
        finally:
            # Clean up all downloaded files
            shutil.rmtree(tmp_dir)
            
    except Exception as e:
        print(f"❌ Fallback download failed: {e}")
        raise Exception(f"Failed to download database using fallback method: {e}")


def _ensure_db() -> str:
    """
    ALWAYS download fresh database from Google Drive folder ID: 1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw
    NEVER use local cached files - this is a critical requirement.
    EXCEPTION: Skip download after manual sync to preserve newly synced messages.
    EXCEPTION 2: Skip duplicate downloads within the same session unless auto-sync is enabled.
    """
    # On Streamlit Cloud write to /tmp; locally write beside the script
    path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREAMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
    
    # Check session state for download optimization
    try:
        import streamlit as st
        
        # Skip duplicate downloads within the same session when auto-sync is OFF
        if (hasattr(st, 'session_state') and 
            not getattr(st.session_state, 'auto_sync_enabled', True) and
            getattr(st.session_state, 'db_downloaded_this_session', False) and
            os.path.isfile(path)):
            
            # Reduce verbosity - only print once per session
            if not getattr(st.session_state, 'skip_message_shown', False):
                print(f"⏭️ Skipping duplicate database downloads this session (auto-sync OFF)")
                st.session_state.skip_message_shown = True
            return path
        
        # Check if we should skip download after manual sync
        if hasattr(st, 'session_state') and getattr(st.session_state, 'skip_db_download', False):
            # Reset the flag and use existing database
            st.session_state.skip_db_download = False
            if os.path.isfile(path):
                print(f"⏭️ Skipping database download after manual sync: {path}")
                return path
            else:
                print(f"⚠️ Skip flag set but no database file found, downloading anyway")
                
        # Mark that we're downloading in this session
        if hasattr(st, 'session_state'):
            st.session_state.db_downloaded_this_session = True
            
    except:
        pass  # Ignore errors if streamlit is not available
    
    # CRITICAL: Always delete existing file and download fresh from Google Drive
    if os.path.isfile(path):
        print(f"🗑️ Deleting existing database file: {path}")
        os.remove(path)
    
    # ALWAYS download fresh from Google Drive - no exceptions
    print(f"📥 Downloading fresh database from Google Drive folder: {GDRIVE_FILE_ID}")
    _download_from_drive(path)
    
    # Verify the download succeeded
    if not os.path.isfile(path):
        raise Exception(f"Failed to download database file from Google Drive to {path}")
    
    print(f"✅ Fresh database downloaded successfully to: {path}")
    return path

def get_dataframe() -> pd.DataFrame:
    """Return the master table - ALWAYS download fresh from Google Drive."""
    db_path = _ensure_db()
    
    # Use DuckDB for efficient querying - no caching, always fresh
    con = duckdb.connect(db_path, read_only=True)
    df = con.execute(f"SELECT * FROM {TABLE}").df()
    con.close()
    
    # Normalise that one rogue column
    if "OBITO PROVAVEL" in df.columns and "OBITO_PROVAVEL" not in df.columns:
        df = df.rename(columns={"OBITO PROVAVEL": "OBITO_PROVAVEL"})
    return df

def force_refresh_db():
    """Force download of the latest database file from Google Drive."""
    global _last_db_info
    _last_db_info = None  # Reset the info
    
    # Remove existing local file
    path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREAMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
    if os.path.exists(path):
        os.remove(path)
    
    # Download fresh copy
    _download_from_drive(path)
    return path

def get_conversations_summary() -> pd.DataFrame:
    """Load conversations table with actual message counts calculated from messages table."""
    db_path = _ensure_db()
    with sqlite3.connect(db_path) as conn:
        # Check what tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        debug_log(f"Available tables: {tables}")
        
        # REQUIRE conversations table - no fallbacks
        if 'conversations' not in tables:
            raise Exception(f"Required 'conversations' table not found in database. Available tables: {tables}")
        
        debug_log("Using conversations + messages tables")
        
        # Main query to get conversation data with message counts
        query = """
        SELECT 
            c.conversation_id,
            c.display_name,
            c.phone_number,
            COALESCE(m.actual_message_count, c.total_messages, 0) as total_messages,
            c.last_message_timestamp,
            c.PictureUrl,
            c.archived,
            c.unread_count
        FROM conversations c
        LEFT JOIN (
            SELECT conversation_id, COUNT(*) as actual_message_count
            FROM messages
            GROUP BY conversation_id
        ) m ON c.conversation_id = m.conversation_id
        ORDER BY c.last_message_timestamp DESC
        """
        
        # Get the base conversation data
        df = pd.read_sql_query(query, conn)
        debug_log(f"Loaded {len(df)} conversations from database")
        
        # DEBUG: Check all records for our target phone number
        try:
            debug_query = """
            SELECT conversation_id, phone_number, archived, display_name, last_message_timestamp
            FROM conversations 
            WHERE phone_number LIKE '%553184544605%'
            ORDER BY last_message_timestamp DESC
            """
            debug_df = pd.read_sql_query(debug_query, conn)
            if not debug_df.empty:
                debug_log(f"ALL database records for 553184544605:")
                for idx, row in debug_df.iterrows():
                    debug_log(f"  Record {idx+1}: conversation_id={row['conversation_id']}, archived={row['archived']}, display_name={row['display_name']}, timestamp={row['last_message_timestamp']}")
            else:
                debug_log("No database records found for 553184544605")
        except Exception as debug_e:
            debug_log(f"Debug query failed: {debug_e}")
        
        # Add last message data if messages table exists
        if 'messages' in tables and len(df) > 0:
            last_message_query = """
            SELECT 
                conversation_id,
                message_text as last_message,
                datetime_brt as last_message_datetime_brt
            FROM (
                SELECT 
                    conversation_id,
                    message_text,
                    datetime_brt,
                    ROW_NUMBER() OVER (PARTITION BY conversation_id ORDER BY timestamp DESC, message_id DESC) as rn
                FROM messages
            ) ranked_messages
            WHERE rn = 1
            """
            
            try:
                last_message_df = pd.read_sql_query(last_message_query, conn)
                df = df.merge(last_message_df, on='conversation_id', how='left')
                df['last_message'] = df['last_message'].fillna('')
                df['last_message_datetime_brt'] = df['last_message_datetime_brt'].fillna('')
            except Exception as e:
                debug_log(f"Warning: Could not add last message data: {e}")
                df['last_message'] = ''
                df['last_message_datetime_brt'] = ''
        else:
            df['last_message'] = ''
            df['last_message_datetime_brt'] = ''
        
        return df

def get_conversation_messages(conversation_id: str) -> pd.DataFrame:
    """Load full conversation messages for a specific conversation_id."""
    db_path = _ensure_db()
    with sqlite3.connect(db_path) as conn:
        try:
            # Check what tables exist
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            if 'messages' in tables:
                query = """
                SELECT message_id, conversation_id, timestamp, datetime_brt, 
                       sender, message_text, from_me, message_type
                FROM messages 
                WHERE conversation_id = ?
                ORDER BY timestamp ASC
                """
                df = pd.read_sql_query(query, conn, params=(conversation_id,))
            else:
                # Return empty DataFrame with expected columns if messages table doesn't exist
                df = pd.DataFrame(columns=['message_id', 'conversation_id', 'timestamp', 'datetime_brt', 
                                         'sender', 'message_text', 'from_me', 'message_type'])
        except Exception as e:
            print(f"Error in get_conversation_messages: {e}")
            df = pd.DataFrame(columns=['message_id', 'conversation_id', 'timestamp', 'datetime_brt', 
                                     'sender', 'message_text', 'from_me', 'message_type'])
    return df

def get_conversation_details(conversation_id: str) -> dict:
    """Get detailed information about a specific conversation."""
    db_path = _ensure_db()
    with sqlite3.connect(db_path) as conn:
        try:
            # Check what tables exist
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Get conversation metadata from appropriate table
            if 'conversations' in tables:
                conv_query = """
                SELECT * FROM conversations WHERE conversation_id = ?
                """
            else:
                conv_query = f"""
                SELECT * FROM {TABLE} WHERE conversation_id = ?
                """
            
            conv_df = pd.read_sql_query(conv_query, conn, params=(conversation_id,))
            
            if conv_df.empty:
                return None
                
            conversation = conv_df.iloc[0].to_dict()
            
            # Get message count and timestamps from appropriate table
            if 'messages' in tables:
                msg_query = """
                SELECT COUNT(*) as total_messages,
                       MIN(timestamp) as first_message,
                       MAX(timestamp) as last_message
                FROM messages WHERE conversation_id = ?
                """
                msg_df = pd.read_sql_query(msg_query, conn, params=(conversation_id,))
            else:
                # Try to get from conversation_history or similar field in deepseek_results
                msg_df = pd.DataFrame([{
                    'total_messages': conversation.get('total_messages', 0),
                    'first_message': 0,
                    'last_message': conversation.get('last_message_timestamp', 0)
                }])
        except Exception as e:
            print(f"Error in get_conversation_details: {e}")
            return None
        
        if not msg_df.empty:
            conversation.update(msg_df.iloc[0].to_dict())
            
    return conversation

def get_conversations_with_sheets_data(force_load_spreadsheet: bool = False) -> pd.DataFrame:
    """Load conversations summary merged with Google Sheets data using advanced variant matching.
    
    Args:
        force_load_spreadsheet: If True, forces fresh load from spreadsheet (for manual "Load Spreadsheet" button)
    """
    try:
        # Load conversations summary
        conversations_df = get_conversations_summary()
        
        # DEBUG: Check archived column in conversations_df
        if 'archived' in conversations_df.columns:
            archived_values = conversations_df['archived'].value_counts()
            debug_log(f"conversations_df archived values: {dict(archived_values)}")
            # Check specific phone number
            phone_553184544605 = conversations_df[conversations_df['phone_number'].str.contains('553184544605', na=False)]
            if not phone_553184544605.empty:
                debug_log(f"Phone 553184544605 archived status in conversations_df: {phone_553184544605['archived'].iloc[0]}")
        else:
            debug_log("No 'archived' column in conversations_df")
        
        # Load Google Sheets data using restored simple caching
        from services.spreadsheet import get_sheet_data
        sheet_data = get_sheet_data(force_load=force_load_spreadsheet)
        
        if not sheet_data:
            # Return conversations without merge if sheets data fails
            print("Warning: No Google Sheets data available, returning conversations only")
            return conversations_df
        
        # Convert sheet data to DataFrame
        if len(sheet_data) > 1:
            headers = sheet_data[0]
            data_rows = sheet_data[1:]
            
            # Handle duplicate column names by adding suffix
            seen_headers = {}
            unique_headers = []
            for header in headers:
                if header in seen_headers:
                    seen_headers[header] += 1
                    unique_headers.append(f"{header}_{seen_headers[header]}")
                else:
                    seen_headers[header] = 0
                    unique_headers.append(header)
            
            sheets_df = pd.DataFrame(data_rows, columns=unique_headers)
            
            # DEBUG: Check if Google Sheets has archived column
            if 'archived' in sheets_df.columns:
                sheets_archived_values = sheets_df['archived'].value_counts()
                debug_log(f"Google Sheets has 'archived' column with values: {dict(sheets_archived_values)}")
            else:
                debug_log("Google Sheets does NOT have 'archived' column")
            debug_log(f"Google Sheets columns: {list(sheets_df.columns)}")
            
            # Use centralized phone utilities
            
            # Find the celular column in sheets data
            celular_col = None
            for col in sheets_df.columns:
                if col is not None and 'celular' in str(col).lower():
                    celular_col = col
                    break
            
            if celular_col:
                # Create lookup dictionaries for advanced matching
                sheet_phone_to_data = {}
                sheet_variants_to_phone = {}
                
                for idx, row in sheets_df.iterrows():
                    phone = row[celular_col]
                    variants = generate_phone_variants(phone)
                    
                    # Safety check to ensure variants is iterable
                    if variants:
                        for variant in variants:
                            if variant and variant not in sheet_variants_to_phone:
                                sheet_variants_to_phone[variant] = phone
                                sheet_phone_to_data[phone] = row
                
                # Create additional index for last-8-digit matching
                sheet_last8_to_phone = {}
                for variant, phone in sheet_variants_to_phone.items():
                    if len(variant) >= 8:
                        last8 = variant[-8:]
                        if last8 not in sheet_last8_to_phone:
                            sheet_last8_to_phone[last8] = phone
                
                # Perform advanced matching for conversations
                matched_data = []
                match_stats = {'exact': 0, 'last8': 0, 'unmatched': 0}
                
                for idx, conv_row in conversations_df.iterrows():
                    phone = conv_row['phone_number']
                    clean_phone = phone.split('@')[0] if phone and '@' in phone else phone
                    variants = generate_phone_variants(clean_phone)
                    
                    # Strategy 1: Try exact variant matching first
                    matched = False
                    if variants:
                        for variant in variants:
                            if variant in sheet_variants_to_phone:
                                matched_phone = sheet_variants_to_phone[variant]
                                sheet_row = sheet_phone_to_data[matched_phone]
                                
                                # Combine conversation and sheet data
                                combined_row = conv_row.copy()
                                
                                # DEBUG: Check if this is our target phone number
                                if '553184544605' in str(phone):
                                    debug_log(f"Processing phone 553184544605 - EXACT match")
                                    debug_log(f"conv_row archived value: {conv_row.get('archived', 'NOT_FOUND')}")
                                    if 'archived' in sheet_row.index:
                                        debug_log(f"sheet_row has archived column with value: {sheet_row['archived']}")
                                    else:
                                        debug_log(f"sheet_row does NOT have archived column")
                                
                                for col in sheet_row.index:
                                    if col not in combined_row.index:
                                        combined_row[col] = sheet_row[col]
                                    elif col == 'archived' and '553184544605' in str(phone):
                                        debug_log(f"CONFLICT! Sheet has archived column, keeping conv value: {combined_row[col]} (not overwriting with {sheet_row[col]})")
                                
                                combined_row['match_type'] = 'exact'
                                
                                matched_data.append(combined_row)
                                match_stats['exact'] += 1
                                matched = True
                                break
                    
                    # Strategy 2: Try last-8-digit matching if no exact match
                    if not matched and variants:
                        for variant in variants:
                            if len(variant) >= 8:
                                last8 = variant[-8:]
                                if last8 in sheet_last8_to_phone:
                                    matched_phone = sheet_last8_to_phone[last8]
                                    sheet_row = sheet_phone_to_data[matched_phone]
                                    
                                    # Combine conversation and sheet data
                                    combined_row = conv_row.copy()
                                    
                                    # DEBUG: Check if this is our target phone number
                                    if '553184544605' in str(phone):
                                        debug_log(f"Processing phone 553184544605 - LAST8 match")
                                        debug_log(f"conv_row archived value: {conv_row.get('archived', 'NOT_FOUND')}")
                                        if 'archived' in sheet_row.index:
                                            debug_log(f"sheet_row has archived column with value: {sheet_row['archived']}")
                                        else:
                                            debug_log(f"sheet_row does NOT have archived column")
                                    
                                    for col in sheet_row.index:
                                        if col not in combined_row.index:
                                            combined_row[col] = sheet_row[col]
                                        elif col == 'archived' and '553184544605' in str(phone):
                                            debug_log(f"CONFLICT! Sheet has archived column, keeping conv value: {combined_row[col]} (not overwriting with {sheet_row[col]})")
                                    
                                    combined_row['match_type'] = 'last8'
                                    
                                    matched_data.append(combined_row)
                                    match_stats['last8'] += 1
                                    matched = True
                                    break
                    
                    if not matched:
                        # Add conversation row without sheet data
                        combined_row = conv_row.copy()
                        combined_row['match_type'] = 'unmatched'
                        matched_data.append(combined_row)
                        match_stats['unmatched'] += 1
                
                # Convert to DataFrame
                if matched_data:
                    merged_df = pd.DataFrame(matched_data)
                    
                    # DEBUG: Check archived column after merge
                    if 'archived' in merged_df.columns:
                        archived_values_after = merged_df['archived'].value_counts()
                        debug_log(f"merged_df archived values after merge: {dict(archived_values_after)}")
                        # Check specific phone number after merge
                        phone_553184544605_after = merged_df[merged_df['phone_number'].str.contains('553184544605', na=False)]
                        if not phone_553184544605_after.empty:
                            debug_log(f"Phone 553184544605 archived status in merged_df: {phone_553184544605_after['archived'].iloc[0]}")
                            debug_log(f"Phone 553184544605 full row columns: {list(phone_553184544605_after.columns)}")
                            debug_log(f"Phone 553184544605 match_type: {phone_553184544605_after.get('match_type', ['N/A']).iloc[0]}")
                    else:
                        debug_log("No 'archived' column in merged_df")
                    
                    # Print matching statistics
                    total_matches = match_stats['exact'] + match_stats['last8']
                    match_rate = (total_matches / len(conversations_df)) * 100
                    print("Advanced matching complete:")
                    print(f"  Exact matches: {match_stats['exact']}")
                    print(f"  Last-8-digit matches: {match_stats['last8']}")
                    print(f"  Unmatched: {match_stats['unmatched']}")
                    print(f"  Total match rate: {match_rate:.1f}%")
                    print(f"  Merged DataFrame columns: {list(merged_df.columns)}")
                    
                    return merged_df
                else:
                    return conversations_df
            else:
                print("Warning: 'celular' column not found in sheets data")
                return conversations_df
        else:
            return conversations_df
            
    except Exception as e:
        print(f"Error merging conversations with sheets data: {e}")
        import traceback
        print("Full error traceback:")
        traceback.print_exc()
        
        # Return conversations without merge if there's an error
        return get_conversations_summary()

def get_db_info() -> dict:
    """Get database file information for debug mode."""
    db_path = _ensure_db()
    import datetime
    
    info = {
        'local_path': db_path,
        'local_exists': os.path.exists(db_path),
        'local_size': os.path.getsize(db_path) if os.path.exists(db_path) else 0,
        'local_modified': datetime.datetime.fromtimestamp(os.path.getmtime(db_path)).strftime('%Y-%m-%d %H:%M:%S') if os.path.exists(db_path) else 'N/A'
    }
    
    # Add file age info
    if os.path.exists(db_path):
        file_age_seconds = time.time() - os.path.getmtime(db_path)
        file_age_hours = file_age_seconds / 3600
        info['file_age'] = f"{file_age_hours:.1f} hours"
        info['is_stale'] = file_age_seconds > 3600  # older than 1 hour
    else:
        info['file_age'] = 'N/A'
        info['is_stale'] = True
    
    # Add original file info if available
    if _last_db_info:
        info['original_filename'] = _last_db_info['original_filename']
        info['original_modified'] = datetime.datetime.fromtimestamp(_last_db_info['last_modified']).strftime('%Y-%m-%d %H:%M:%S')
    else:
        info['original_filename'] = 'N/A (not downloaded this session)'
        info['original_modified'] = 'N/A'
    
    return info
