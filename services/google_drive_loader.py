"""
Simple Google Drive API loader for downloading files without UI elements.
This avoids CachedWidgetWarning when imported by other modules.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

class GoogleDriveLoader:
    """Simple class to handle Google Drive API interactions without UI elements"""
    
    def __init__(self):
        pass
    
    def list_files(self, folder_id):
        """List files in a Google Drive folder"""
        try:
            import json
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build
            
            # Load credentials from Streamlit secrets or local file
            credentials_info = None
            
            try:
                # Try to load from Streamlit secrets first (for production)
                if hasattr(st, 'secrets') and 'google_sheets' in st.secrets:
                    credentials_info = dict(st.secrets['google_sheets'])
                else:
                    raise Exception("Streamlit secrets not available")
            except:
                # Fallback to local credentials.json file (for development)
                import os
                CREDENTIALS_FILE = "credentials.json"
                if not os.path.exists(CREDENTIALS_FILE):
                    return []
                
                with open(CREDENTIALS_FILE, 'r') as f:
                    credentials_info = json.load(f)
            
            SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
            credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
            drive_service = build('drive', 'v3', credentials=credentials)
            
            # List files in the folder
            query = f"'{folder_id}' in parents and trashed=false"
            results = drive_service.files().list(
                q=query,
                orderBy='createdTime desc',
                fields='files(id, name, createdTime, mimeType)',
                pageSize=100
            ).execute()
            
            return results.get('files', [])
            
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
    
    def download_file(self, file_id, local_path):
        """Download a file from Google Drive"""
        try:
            import json
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaIoBaseDownload
            
            # Load credentials from Streamlit secrets or local file
            credentials_info = None
            
            try:
                # Try to load from Streamlit secrets first (for production)
                if hasattr(st, 'secrets') and 'google_sheets' in st.secrets:
                    credentials_info = dict(st.secrets['google_sheets'])
                else:
                    raise Exception("Streamlit secrets not available")
            except:
                # Fallback to local credentials.json file (for development)
                import os
                CREDENTIALS_FILE = "credentials.json"
                if not os.path.exists(CREDENTIALS_FILE):
                    return False
                
                with open(CREDENTIALS_FILE, 'r') as f:
                    credentials_info = json.load(f)
            
            SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
            credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
            drive_service = build('drive', 'v3', credentials=credentials)
            
            # Download the file
            request = drive_service.files().get_media(fileId=file_id)
            
            with open(local_path, 'wb') as local_file:
                downloader = MediaIoBaseDownload(local_file, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
            
            return True
            
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False
    
    def get_file_modified_time(self, folder_id, file_name):
        """Get the modification time of a specific file in a Google Drive folder"""
        try:
            import json
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build
            from datetime import datetime
            
            # Load credentials from Streamlit secrets or local file
            credentials_info = None
            
            try:
                # Try to load from Streamlit secrets first (for production)
                if hasattr(st, 'secrets') and 'google_sheets' in st.secrets:
                    credentials_info = dict(st.secrets['google_sheets'])
                else:
                    raise Exception("Streamlit secrets not available")
            except:
                # Fallback to local credentials.json file (for development)
                import os
                CREDENTIALS_FILE = "credentials.json"
                if not os.path.exists(CREDENTIALS_FILE):
                    return None
                
                with open(CREDENTIALS_FILE, 'r') as f:
                    credentials_info = json.load(f)
            
            SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
            credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
            drive_service = build('drive', 'v3', credentials=credentials)
            
            # Search for the specific file
            query = f"'{folder_id}' in parents and name='{file_name}' and trashed=false"
            results = drive_service.files().list(
                q=query,
                fields='files(id, name, modifiedTime)',
                pageSize=10
            ).execute()
            
            files = results.get('files', [])
            if files:
                # Return the modification time of the first matching file
                modified_time = files[0].get('modifiedTime')
                if modified_time:
                    # Parse ISO format and return datetime object
                    return datetime.fromisoformat(modified_time.replace('Z', '+00:00'))
            
            return None
            
        except Exception as e:
            print(f"Error getting file modified time: {e}")
            return None
    
    def has_file_been_updated(self, folder_id, file_name, last_known_time):
        """Check if a file has been updated since the last known time"""
        current_time = self.get_file_modified_time(folder_id, file_name)
        
        if current_time is None or last_known_time is None:
            return False
            
        return current_time > last_known_time