# db_loader.py  ─ one-path loader
import os, sqlite3, pandas as pd, time

GDRIVE_FILE_ID   = "1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw"   # snapshot folder/file ID
LOCAL_DB_PATH    = "whatsapp_conversations.db"           # always the same name
TABLE            = "deepseek_results"                    # what the UI expects

# Global variable to store database info for debug mode
_last_db_info = None

def _download_from_drive(dest: str):
    import gdown, pathlib, shutil, os, tempfile

    tmp_dir = tempfile.mkdtemp()
    gdown.download_folder(
        id="1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw",        # folder id
        output=tmp_dir,
        quiet=False,
        use_cookies=False
    )
    # pick newest *.db
    newest = max(pathlib.Path(tmp_dir).glob("*.db"), key=os.path.getmtime)
    
    # Store original filename and modification time for debug info
    global _last_db_info
    _last_db_info = {
        'original_filename': newest.name,
        'last_modified': os.path.getmtime(newest)
    }
    
    shutil.move(newest, dest)
    shutil.rmtree(tmp_dir)


def _ensure_db() -> str:
    """
    Return a local path to an up-to-date DB file.
    • Always check for fresh file on startup
    • If file is older than 1 hour ⇒ fetch from Drive
    """
    # On Streamlit Cloud write to /tmp; locally write beside the script
    path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
    
    # Check if file exists and its age
    if os.path.isfile(path):
        # Check if file is older than 1 hour (3600 seconds) - much more aggressive
        file_age = time.time() - os.path.getmtime(path)
        if file_age > 3600:  # 1 hour in seconds
            # File is too old, download fresh copy
            _download_from_drive(path)
    else:
        # File doesn't exist, download it
        _download_from_drive(path)
    
    return path

def get_dataframe() -> pd.DataFrame:
    """Load <TABLE> into a DataFrame (guarantees column names untouched)."""
    db_path = _ensure_db()
    
    # Get the file modification time to use as cache key
    file_mtime = os.path.getmtime(db_path)
    
    # Use the file modification time as part of the cache key
    return _load_dataframe_with_cache(db_path, file_mtime)

def _load_dataframe_with_cache(db_path: str, file_mtime: float) -> pd.DataFrame:
    """Load dataframe with cache that's aware of file changes."""
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE}", conn)

    # Normalise that one rogue column
    if "OBITO PROVAVEL" in df.columns and "OBITO_PROVAVEL" not in df.columns:
        df = df.rename(columns={"OBITO PROVAVEL": "OBITO_PROVAVEL"})
    return df

def force_refresh_db():
    """Force download of the latest database file from Google Drive."""
    global _last_db_info
    _last_db_info = None  # Reset the info
    
    # Remove existing local file
    path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
    if os.path.exists(path):
        os.remove(path)
    
    # Download fresh copy
    _download_from_drive(path)
    return path

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
