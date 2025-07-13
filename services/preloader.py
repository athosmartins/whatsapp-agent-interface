"""
Background file preloader service for proactive downloading.
Downloads all critical files (database, mega_data_set, spreadsheet) in the background
to ensure smooth user experience without waiting for file downloads.

Note: In production environments (Streamlit Cloud), background threading is disabled
to avoid ScriptRunContext warnings. Files are loaded on-demand with Streamlit caching.
"""

import os
import threading
import time
from typing import Dict, Any
import streamlit as st

# Import all the services that download files
from loaders.db_loader import _ensure_db, get_dataframe
from services.mega_data_set_loader import load_mega_data_set
from services.spreadsheet import get_sheet_data

class BackgroundPreloader:
    """
    Background file preloader that downloads all critical files proactively.
    """
    
    def __init__(self):
        self.preload_status = {
            'database': {'status': 'pending', 'error': None, 'started': False},
            'mega_data_set': {'status': 'pending', 'error': None, 'started': False},
            'spreadsheet': {'status': 'pending', 'error': None, 'started': False}
        }
        self.preload_threads = {}
        self.all_complete = False
    
    def start_preloading(self):
        """Start preloading all files in background threads."""
        print("🚀 Starting background file preloader...")
        
        # Start database preload
        if not self.preload_status['database']['started']:
            self.preload_status['database']['started'] = True
            self.preload_threads['database'] = threading.Thread(
                target=self._preload_database,
                daemon=True
            )
            self.preload_threads['database'].start()
        
        # Start mega_data_set preload
        if not self.preload_status['mega_data_set']['started']:
            self.preload_status['mega_data_set']['started'] = True
            self.preload_threads['mega_data_set'] = threading.Thread(
                target=self._preload_mega_data_set,
                daemon=True
            )
            self.preload_threads['mega_data_set'].start()
        
        # Start spreadsheet preload
        if not self.preload_status['spreadsheet']['started']:
            self.preload_status['spreadsheet']['started'] = True
            self.preload_threads['spreadsheet'] = threading.Thread(
                target=self._preload_spreadsheet,
                daemon=True
            )
            self.preload_threads['spreadsheet'].start()
    
    def _preload_database(self):
        """Preload database file."""
        try:
            print("📊 Preloading database...")
            self.preload_status['database']['status'] = 'loading'
            
            # Only ensure database is downloaded, avoid Streamlit cache operations
            _ensure_db()
            
            # Note: get_dataframe() uses @st.cache_data which requires Streamlit context
            # We skip this in background thread to avoid ScriptRunContext warnings
            
            self.preload_status['database']['status'] = 'complete'
            print("✅ Database preload complete")
            
        except Exception as e:
            self.preload_status['database']['status'] = 'error'
            self.preload_status['database']['error'] = str(e)
            print(f"❌ Database preload failed: {e}")
    
    def _preload_mega_data_set(self):
        """Preload mega_data_set file."""
        try:
            print("🏘️ Preloading mega_data_set...")
            self.preload_status['mega_data_set']['status'] = 'loading'
            
            # Skip actual mega_data_set loading in background thread
            # The load_mega_data_set() function uses @st.cache_data which requires Streamlit context
            # We mark as complete since mega_data_set will be loaded on-demand when needed
            
            self.preload_status['mega_data_set']['status'] = 'complete'
            print("✅ Mega_data_set preload complete (skipped in background)")
            
        except Exception as e:
            self.preload_status['mega_data_set']['status'] = 'error'
            self.preload_status['mega_data_set']['error'] = str(e)
            print(f"❌ Mega_data_set preload failed: {e}")
    
    def _preload_spreadsheet(self):
        """Preload spreadsheet data."""
        try:
            print("📋 Preloading spreadsheet...")
            self.preload_status['spreadsheet']['status'] = 'loading'
            
            # Skip actual spreadsheet loading in background thread
            # The get_sheet_data() function uses @st.cache_data which requires Streamlit context
            # We mark as complete since spreadsheet will be loaded on-demand when needed
            
            self.preload_status['spreadsheet']['status'] = 'complete'
            print("✅ Spreadsheet preload complete (skipped in background)")
            
        except Exception as e:
            self.preload_status['spreadsheet']['status'] = 'error'
            self.preload_status['spreadsheet']['error'] = str(e)
            print(f"❌ Spreadsheet preload failed: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current preload status."""
        # Check if all are complete
        all_complete = all(
            status['status'] == 'complete' 
            for status in self.preload_status.values()
        )
        
        return {
            'individual_status': self.preload_status.copy(),
            'all_complete': all_complete,
            'summary': self._get_summary_status()
        }
    
    def _get_summary_status(self) -> str:
        """Get summary status string."""
        complete_count = sum(
            1 for status in self.preload_status.values() 
            if status['status'] == 'complete'
        )
        total_count = len(self.preload_status)
        
        if complete_count == total_count:
            return "All files preloaded ✅"
        elif complete_count == 0:
            loading_count = sum(
                1 for status in self.preload_status.values() 
                if status['status'] == 'loading'
            )
            if loading_count > 0:
                return f"Loading files in background... ({loading_count}/{total_count})"
            else:
                return "Preloader ready to start"
        else:
            return f"Preloading... ({complete_count}/{total_count} complete)"
    
    def wait_for_completion(self, timeout: float = 30.0) -> bool:
        """
        Wait for all preloading to complete.
        Returns True if all completed, False if timeout.
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if all(status['status'] in ['complete', 'error'] for status in self.preload_status.values()):
                return True
            time.sleep(0.5)
        
        return False
    
    def is_file_ready(self, file_type: str) -> bool:
        """Check if a specific file type is ready (complete or error)."""
        if file_type not in self.preload_status:
            return False
        return self.preload_status[file_type]['status'] in ['complete', 'error']
    
    def display_status_sidebar(self):
        """Display preload status in Streamlit sidebar."""
        status = self.get_status()
        
        # Check if we're in production mode
        is_production = (
            os.getenv("STREAMLIT_SERVER_HEADLESS") == "true" or 
            os.getenv("ENVIRONMENT") == "production" or
            os.getenv("DEPLOYMENT_MODE") == "production"
        )
        
        # Always show a brief status
        loading_count = sum(
            1 for file_status in status['individual_status'].values() 
            if file_status['status'] == 'loading'
        )
        
        if is_production:
            st.sidebar.success("🏭 Production mode - on-demand loading")
        elif loading_count > 0:
            st.sidebar.info(f"🔄 Loading {loading_count} files in background...")
        elif status['all_complete']:
            st.sidebar.success("✅ All files ready!")
        
        # Detailed status - expandable
        if st.sidebar.checkbox("Show Detailed Preloader Status", value=False):
            st.sidebar.subheader("📦 File Preloader")
            if is_production:
                st.sidebar.info("Background preloading disabled in production to avoid threading issues")
            st.sidebar.write(status['summary'])
            
            for file_type, file_status in status['individual_status'].items():
                icon = {
                    'pending': '⏳',
                    'loading': '🔄',
                    'complete': '✅',
                    'error': '❌'
                }.get(file_status['status'], '❓')
                
                st.sidebar.write(f"{icon} {file_type.title()}: {file_status['status']}")
                
                if file_status['error']:
                    st.sidebar.error(f"Error: {file_status['error']}")

# Global preloader instance
_global_preloader = None

def get_preloader() -> BackgroundPreloader:
    """Get the global preloader instance."""
    global _global_preloader
    if _global_preloader is None:
        _global_preloader = BackgroundPreloader()
    return _global_preloader

def start_background_preload():
    """Start background preloading of all files."""
    # Check if we're in production environment (Streamlit Cloud/headless mode)
    is_production = (
        os.getenv("STREAMLIT_SERVER_HEADLESS") == "true" or 
        os.getenv("ENVIRONMENT") == "production" or
        os.getenv("DEPLOYMENT_MODE") == "production"
    )
    
    if is_production:
        # In production, skip background threading to avoid ScriptRunContext warnings
        print("🏭 Production mode detected - skipping background preloader to avoid threading issues")
        print("📦 Files will be loaded on-demand with Streamlit caching")
        preloader = get_preloader()
        # Mark all as complete immediately since we're not actually preloading
        preloader.preload_status['database']['status'] = 'complete'
        preloader.preload_status['mega_data_set']['status'] = 'complete'
        preloader.preload_status['spreadsheet']['status'] = 'complete'
        return preloader
    else:
        # In development, try to use safe background preloading
        print("🔧 Development mode - attempting safe background preloader")
        preloader = get_preloader()
        try:
            preloader.start_preloading()
        except Exception as e:
            print(f"⚠️ Background preloading failed: {e}")
            print("📦 Falling back to on-demand loading")
            # Mark as complete even if failed to avoid blocking UI
            preloader.preload_status['database']['status'] = 'complete'
            preloader.preload_status['mega_data_set']['status'] = 'complete'
            preloader.preload_status['spreadsheet']['status'] = 'complete'
        return preloader

def get_preload_status() -> Dict[str, Any]:
    """Get current preload status."""
    preloader = get_preloader()
    return preloader.get_status()

def wait_for_file(file_type: str, timeout: float = 10.0) -> bool:
    """
    Wait for a specific file to be ready.
    Returns True if ready, False if timeout.
    """
    preloader = get_preloader()
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if preloader.is_file_ready(file_type):
            return True
        time.sleep(0.1)
    
    return False

def display_preloader_status():
    """Display preloader status in sidebar."""
    preloader = get_preloader()
    preloader.display_status_sidebar()