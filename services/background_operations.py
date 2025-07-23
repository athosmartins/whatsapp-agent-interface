"""
Background Operations Service

Handles background processing for sync sheet and archive conversation operations
to improve user experience by not blocking the UI during these operations.

Key Features:
- Thread-safe operation queue
- Background worker thread
- Real-time status tracking
- Session state integration
- Comprehensive error handling
"""

import threading
import time
import queue
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
import streamlit as st
import logging
from collections import defaultdict

# Configure logging
logger = logging.getLogger(__name__)

# Thread-safe global storage for operations (accessible from background threads)
class ThreadSafeOperationStorage:
    """Thread-safe storage for background operations that doesn't rely on session state."""
    
    def __init__(self):
        self.lock = threading.RLock()
        self.operations = {}  # operation_id -> operation_dict
        self.completed_operations = []
        self.stats = {
            'total_queued': 0,
            'total_completed': 0,
            'total_failed': 0
        }
    
    def set_operation(self, operation_id: str, operation_dict: Dict[str, Any]):
        """Set operation data (thread-safe)."""
        with self.lock:
            self.operations[operation_id] = operation_dict.copy()
    
    def get_operation(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get operation data (thread-safe)."""
        with self.lock:
            return self.operations.get(operation_id, {}).copy()
    
    def get_all_operations(self) -> Dict[str, Dict[str, Any]]:
        """Get all operations (thread-safe)."""
        with self.lock:
            return {k: v.copy() for k, v in self.operations.items()}
    
    def add_completed_operation(self, operation_dict: Dict[str, Any]):
        """Add to completed operations list (thread-safe)."""
        with self.lock:
            self.completed_operations.append(operation_dict.copy())
    
    def get_completed_operations(self) -> List[Dict[str, Any]]:
        """Get completed operations (thread-safe)."""
        with self.lock:
            return [op.copy() for op in self.completed_operations]
    
    def clear_completed_operations(self):
        """Clear completed operations (thread-safe)."""
        with self.lock:
            self.completed_operations.clear()
    
    def increment_stat(self, stat_name: str):
        """Increment a stat counter (thread-safe)."""
        with self.lock:
            if stat_name in self.stats:
                self.stats[stat_name] += 1
    
    def get_stats(self) -> Dict[str, int]:
        """Get stats (thread-safe)."""
        with self.lock:
            return self.stats.copy()
    
    def sync_to_session_state(self):
        """Sync the global storage to session state (call from main thread only)."""
        with self.lock:
            if 'background_operations' not in st.session_state:
                st.session_state.background_operations = {}
            if 'completed_operations' not in st.session_state:
                st.session_state.completed_operations = []
            if 'operation_stats' not in st.session_state:
                st.session_state.operation_stats = {
                    'total_queued': 0,
                    'total_completed': 0,
                    'total_failed': 0
                }
            
            # Update session state with global storage data
            st.session_state.background_operations = self.get_all_operations()
            st.session_state.completed_operations = self.get_completed_operations()
            st.session_state.operation_stats = self.get_stats()
    
    def sync_from_session_state(self):
        """Sync from session state to global storage (call from main thread only)."""
        with self.lock:
            if hasattr(st, 'session_state'):
                if hasattr(st.session_state, 'background_operations'):
                    self.operations = st.session_state.background_operations.copy()
                if hasattr(st.session_state, 'completed_operations'):
                    self.completed_operations = st.session_state.completed_operations.copy()
                if hasattr(st.session_state, 'operation_stats'):
                    self.stats = st.session_state.operation_stats.copy()

# Global thread-safe storage instance
global_storage = ThreadSafeOperationStorage()

class BackgroundOperation:
    """Represents a single background operation."""
    
    def __init__(self, operation_type: str, operation_id: str, data: Dict[str, Any]):
        self.operation_type = operation_type  # "sync_sheet" or "archive_conversation"
        self.operation_id = operation_id
        self.data = data
        self.status = "queued"  # queued, running, completed, failed
        self.created_at = datetime.now()
        self.started_at = None
        self.completed_at = None
        self.result = None
        self.error = None
        self.progress = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert operation to dictionary for session state storage."""
        return {
            "operation_type": self.operation_type,
            "operation_id": self.operation_id,
            "data": self.data,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "error": self.error,
            "progress": self.progress
        }


class BackgroundOperationsManager:
    """Manages background operations queue and worker thread."""
    
    def __init__(self):
        self.operation_queue = queue.Queue()
        self.worker_thread = None
        self.worker_running = False
        self.lock = threading.Lock()
        self._init_session_state()
    
    def _init_session_state(self):
        """Initialize session state for operation tracking."""
        if 'background_operations' not in st.session_state:
            st.session_state.background_operations = {}
        
        if 'completed_operations' not in st.session_state:
            st.session_state.completed_operations = []
        
        if 'operation_stats' not in st.session_state:
            st.session_state.operation_stats = {
                'total_queued': 0,
                'total_completed': 0,
                'total_failed': 0
            }
        
        # Sync session state to global storage
        global_storage.sync_from_session_state()
    
    def start_worker(self):
        """Start the background worker thread if not already running."""
        with self.lock:
            if not self.worker_running:
                self.worker_running = True
                self.worker_thread = threading.Thread(
                    target=self._worker_loop, 
                    daemon=True,
                    name="BackgroundOperationsWorker"
                )
                self.worker_thread.start()
                logger.info("Background operations worker started")
    
    def stop_worker(self):
        """Stop the background worker thread."""
        with self.lock:
            self.worker_running = False
            if self.worker_thread:
                self.worker_thread.join(timeout=5)
                logger.info("Background operations worker stopped")
    
    def queue_operation(self, operation_type: str, data: Dict[str, Any]) -> str:
        """
        Queue a background operation.
        
        Args:
            operation_type: "sync_sheet" or "archive_conversation"
            data: Operation-specific data
            
        Returns:
            operation_id: Unique ID for tracking the operation
        """
        operation_id = str(uuid.uuid4())
        operation = BackgroundOperation(operation_type, operation_id, data)
        
        # Add to global storage (thread-safe)
        global_storage.set_operation(operation_id, operation.to_dict())
        global_storage.increment_stat('total_queued')
        
        # Sync to session state for UI display
        global_storage.sync_to_session_state()
        
        # Add to queue for processing
        self.operation_queue.put(operation)
        
        # Start worker if not running
        self.start_worker()
        
        logger.info(f"Queued {operation_type} operation {operation_id}")
        return operation_id
    
    def get_operation_status(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get the current status of an operation."""
        # First try global storage, then session state as fallback
        result = global_storage.get_operation(operation_id)
        if not result and hasattr(st, 'session_state') and hasattr(st.session_state, 'background_operations'):
            result = st.session_state.background_operations.get(operation_id)
        return result
    
    def get_all_operations(self) -> Dict[str, Dict[str, Any]]:
        """Get all operations and their statuses."""
        # Sync from global storage to session state for UI consistency
        global_storage.sync_to_session_state()
        return global_storage.get_all_operations()
    
    def get_running_operations(self) -> List[Dict[str, Any]]:
        """Get currently running operations."""
        all_ops = global_storage.get_all_operations()
        return [
            op for op in all_ops.values()
            if op['status'] in ['queued', 'running']
        ]
    
    def get_completed_operations(self) -> List[Dict[str, Any]]:
        """Get completed operations."""
        return global_storage.get_completed_operations()
    
    def clear_completed_operations(self):
        """Clear the completed operations list."""
        global_storage.clear_completed_operations()
        # Sync to session state for UI consistency
        global_storage.sync_to_session_state()
    
    def _worker_loop(self):
        """Main worker thread loop."""
        logger.info("Background operations worker loop started")
        
        while self.worker_running:
            try:
                # Get next operation with timeout
                operation = self.operation_queue.get(timeout=1.0)
                self._process_operation(operation)
                self.operation_queue.task_done()
                
            except queue.Empty:
                # No operations to process, continue loop
                continue
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                continue
        
        logger.info("Background operations worker loop stopped")
    
    def _process_operation(self, operation: BackgroundOperation):
        """Process a single operation."""
        try:
            # Update status to running
            operation.status = "running"
            operation.started_at = datetime.now()
            operation.progress = 10
            self._update_operation_in_session(operation)
            
            logger.info(f"Processing {operation.operation_type} operation {operation.operation_id}")
            
            # Execute the operation based on type
            if operation.operation_type == "sync_sheet":
                result = self._execute_sync_sheet(operation)
            elif operation.operation_type == "archive_conversation":
                result = self._execute_archive_conversation(operation)
            else:
                raise ValueError(f"Unknown operation type: {operation.operation_type}")
            
            # Mark as completed
            operation.status = "completed"
            operation.completed_at = datetime.now()
            operation.result = result
            operation.progress = 100
            
            # Update stats (thread-safe)
            global_storage.increment_stat('total_completed')
            
            # Move to completed operations (thread-safe)
            global_storage.add_completed_operation(operation.to_dict())
            
            logger.info(f"Completed {operation.operation_type} operation {operation.operation_id}")
            
        except Exception as e:
            # Mark as failed
            operation.status = "failed"
            operation.completed_at = datetime.now()
            operation.error = str(e)
            operation.progress = 0
            
            # Update stats (thread-safe)
            global_storage.increment_stat('total_failed')
            
            # Move to completed operations (so user can see the error) (thread-safe)
            global_storage.add_completed_operation(operation.to_dict())
            
            logger.error(f"Failed {operation.operation_type} operation {operation.operation_id}: {e}")
        
        finally:
            # Update session state
            self._update_operation_in_session(operation)
    
    def _update_operation_in_session(self, operation: BackgroundOperation):
        """Update operation status in global storage (thread-safe)."""
        global_storage.set_operation(operation.operation_id, operation.to_dict())
    
    def _execute_sync_sheet(self, operation: BackgroundOperation) -> Dict[str, Any]:
        """Execute a sync sheet operation."""
        from services.spreadsheet import sync_record_to_sheet
        
        data = operation.data
        
        # Extract required data
        sync_data = data.get('sync_data')
        whatsapp_number = data.get('whatsapp_number')
        sheet_name = data.get('sheet_name', 'report')
        essential_fields = data.get('essential_fields', {})
        
        if not sync_data or not whatsapp_number:
            raise ValueError("Missing required data for sync operation")
        
        # Update progress
        operation.progress = 30
        self._update_operation_in_session(operation)
        
        # Execute the sync with partial update support
        result = sync_record_to_sheet(sync_data, whatsapp_number, sheet_name, essential_fields, partial_update=True)
        
        # Update progress
        operation.progress = 80
        self._update_operation_in_session(operation)
        
        if not result.get('success'):
            raise Exception(f"Sync failed: {result.get('error', 'Unknown error')}")
        
        return result
    
    def _execute_archive_conversation(self, operation: BackgroundOperation) -> Dict[str, Any]:
        """Execute an archive conversation operation."""
        import requests
        import os
        
        data = operation.data
        
        # Extract required data
        phone_number = data.get('phone_number')
        conversation_id = data.get('conversation_id')
        
        if not phone_number:
            raise ValueError("Missing phone number for archive operation")
        
        # Update progress
        operation.progress = 20
        self._update_operation_in_session(operation)
        
        # Clean phone number (remove formatting)
        clean_phone = phone_number.replace("+", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
        
        # Get chatId from secrets (with fallback for background thread)
        try:
            chat_id = st.secrets.get("VOXUY_CHAT_ID", "07d49a4a-1b9c-4a02-847d-bad0fb3870eb")
        except Exception:
            # Fallback for background thread (secrets might not be accessible)
            import os
            chat_id = os.environ.get("VOXUY_CHAT_ID", "07d49a4a-1b9c-4a02-847d-bad0fb3870eb")
        
        # Prepare payload
        payload = {
            "phone": clean_phone,
            "chatId": chat_id
        }
        
        # Update progress
        operation.progress = 40
        self._update_operation_in_session(operation)
        
        # Make HTTP request to archive webhook
        webhook_url = "https://voxuy-archive-conversation.athosmartins.workers.dev/"
        
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=30,  # 30 second timeout
        )
        
        # Update progress
        operation.progress = 80
        self._update_operation_in_session(operation)
        
        # Parse response
        response_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text
        
        if response.status_code == 200:
            return {
                "success": True,
                "message": f"Conversa arquivada com sucesso! (Phone: {clean_phone})",
                "details": response_data
            }
        else:
            raise Exception(f"Archive failed (HTTP {response.status_code}): {response_data}")


# Global instance
background_manager = BackgroundOperationsManager()


def queue_sync_operation(sync_data: Dict[str, Any], whatsapp_number: str, sheet_name: str = "report", essential_fields: Dict[str, Any] = None) -> str:
    """
    Queue a sync sheet operation.
    
    Args:
        sync_data: Data to sync to the sheet (only changed fields)
        whatsapp_number: WhatsApp number for the conversation
        sheet_name: Name of the sheet to sync to
        essential_fields: Required fields for new row creation (cpf, Nome, etc.)
    
    Returns:
        operation_id: Unique ID for tracking the operation
    """
    operation_data = {
        'sync_data': sync_data,
        'whatsapp_number': whatsapp_number,
        'sheet_name': sheet_name,
        'essential_fields': essential_fields or {}
    }
    
    return background_manager.queue_operation("sync_sheet", operation_data)


def queue_archive_operation(phone_number: str, conversation_id: str) -> str:
    """
    Queue an archive conversation operation.
    
    Args:
        phone_number: Phone number of the conversation
        conversation_id: ID of the conversation
    
    Returns:
        operation_id: Unique ID for tracking the operation
    """
    operation_data = {
        'phone_number': phone_number,
        'conversation_id': conversation_id
    }
    
    return background_manager.queue_operation("archive_conversation", operation_data)


def get_operation_status(operation_id: str) -> Optional[Dict[str, Any]]:
    """Get the status of a specific operation."""
    return background_manager.get_operation_status(operation_id)


def get_running_operations() -> List[Dict[str, Any]]:
    """Get all currently running operations."""
    return background_manager.get_running_operations()


def get_completed_operations() -> List[Dict[str, Any]]:
    """Get all completed operations."""
    return background_manager.get_completed_operations()


def clear_completed_operations():
    """Clear the completed operations list."""
    background_manager.clear_completed_operations()


def render_operations_sidebar():
    """Render the operations status in the sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔄 Background Operations")
    
    # Sync global storage to session state for UI display
    global_storage.sync_to_session_state()
    
    # Get current operations
    running_ops = get_running_operations()
    completed_ops = get_completed_operations()
    
    # Show running operations
    if running_ops:
        st.sidebar.markdown("**Currently Running:**")
        for op in running_ops:
            status_icon = "🔄" if op['status'] == 'running' else "⏳"
            op_type = op['operation_type'].replace('_', ' ').title()
            progress = op.get('progress', 0)
            
            st.sidebar.write(f"{status_icon} {op_type}")
            if progress > 0:
                st.sidebar.progress(progress / 100.0)
    
    # Show completed operations summary
    if completed_ops:
        successful = len([op for op in completed_ops if op['status'] == 'completed'])
        failed = len([op for op in completed_ops if op['status'] == 'failed'])
        
        st.sidebar.markdown("**Completed:**")
        if successful > 0:
            st.sidebar.write(f"✅ {successful} successful")
        if failed > 0:
            st.sidebar.write(f"❌ {failed} failed")
        
        # Show recent completed operations
        if len(completed_ops) > 0:
            with st.sidebar.expander("Recent Operations", expanded=False):
                for op in completed_ops[-5:]:  # Show last 5
                    status_icon = "✅" if op['status'] == 'completed' else "❌"
                    op_type = op['operation_type'].replace('_', ' ').title()
                    
                    st.write(f"{status_icon} {op_type}")
                    if op.get('error'):
                        st.error(f"Error: {op['error']}")
        
        # Clear completed operations button
        if st.sidebar.button("🗑️ Clear Completed", key="clear_completed_ops"):
            clear_completed_operations()
            st.rerun()
    
    # Show stats
    stats = global_storage.get_stats()
    if stats['total_queued'] > 0:
        st.sidebar.markdown(f"**Total Operations:** {stats['total_queued']}")
        if stats['total_completed'] > 0:
            st.sidebar.markdown(f"**Success Rate:** {stats['total_completed']}/{stats['total_queued']} ({100*stats['total_completed']/stats['total_queued']:.0f}%)")


def stop_background_operations():
    """Stop all background operations (for cleanup)."""
    background_manager.stop_worker()