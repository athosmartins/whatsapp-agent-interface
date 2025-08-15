"""
Event-Driven Operations Service

Handles operations using the 10/10 architecture with Cloudflare Workers.
This replaces the problematic background_operations.py for critical actions.

Key Features:
- Fire-and-forget HTTP requests to Cloudflare Workers
- Real-time status polling
- No race conditions with navigation
- Cross-session persistence
- Enterprise-grade reliability
"""

import requests
import time
import streamlit as st
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import os

logger = logging.getLogger(__name__)

# Create a debug log file that Kiro can read
DEBUG_LOG_FILE = "event_driven_operations_debug.log"

def debug_log(message: str):
    """Write debug message to both console and file for Kiro to read."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    log_message = f"[{timestamp}] {message}"
    
    # Print to console (for your terminal)
    print(log_message)
    
    # Write to file (for Kiro to read)
    try:
        with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_message + "\n")
            f.flush()  # Ensure immediate write
    except Exception as e:
        print(f"❌ Failed to write to debug log: {e}")

def clear_debug_log():
    """Clear the debug log file."""
    try:
        with open(DEBUG_LOG_FILE, "w", encoding="utf-8") as f:
            f.write("")
        debug_log("🧹 DEBUG LOG CLEARED")
    except Exception as e:
        print(f"❌ Failed to clear debug log: {e}")

# Configuration
ARCHIVE_WORKER_URL = "https://voxuy-archive-conversation-v2.athosmartins.workers.dev"
REQUEST_TIMEOUT = 10  # seconds - short timeout for fire-and-forget

class EventDrivenOperationsManager:
    """Manages event-driven operations with Cloudflare Workers."""
    
    def __init__(self):
        self._init_session_state()
    
    def _init_session_state(self):
        """Initialize session state for operation tracking."""
        if 'pending_operations' not in st.session_state:
            st.session_state.pending_operations = []
        
        if 'completed_operations' not in st.session_state:
            st.session_state.completed_operations = []
        
        if 'operation_stats' not in st.session_state:
            st.session_state.operation_stats = {
                'total_queued': 0,
                'total_completed': 0,
                'total_failed': 0
            }
        
        # DEBUG: Log session state initialization
        debug_log(f"🔧 SESSION STATE INIT: pending={len(st.session_state.pending_operations)}, completed={len(st.session_state.completed_operations)}")
    
    def queue_archive_operation(self, phone_number: str, chat_id: str) -> Dict[str, Any]:
        """
        Queue an archive operation with Cloudflare Worker.
        
        Args:
            phone_number: Phone number to archive
            chat_id: Chat ID for the operation
            
        Returns:
            Dict with operation_id and status, or error info
        """
        try:
            # Clean phone number
            clean_phone = phone_number.replace("+", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            
            payload = {
                "phone": clean_phone,
                "chatId": chat_id
            }
            
            logger.info(f"🚀 Queueing archive operation: {payload}")
            debug_log(f"🚀 QUEUEING ARCHIVE: {payload}")
            
            # Fire-and-forget request to Cloudflare Worker
            response = requests.post(
                ARCHIVE_WORKER_URL,
                json=payload,
                timeout=REQUEST_TIMEOUT,
                headers={"Content-Type": "application/json"}
            )
            
            debug_log(f"🚀 QUEUE RESPONSE: {response.status_code} - {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                operation_id = result.get("operation_id")
                
                if operation_id:
                    debug_log(f"🚀 OPERATION QUEUED: {operation_id}")
                    
                    # Store operation for tracking
                    operation_info = {
                        "operation_id": operation_id,
                        "type": "archive_conversation",
                        "phone_number": phone_number,
                        "chat_id": chat_id,
                        "status": "queued",
                        "created_at": datetime.now().isoformat(),
                        "status_url": f"{ARCHIVE_WORKER_URL}/status/{operation_id}"
                    }
                    
                    st.session_state.pending_operations.append(operation_info)
                    st.session_state.operation_stats['total_queued'] += 1
                    
                    logger.info(f"✅ Archive operation queued successfully: {operation_id}")
                    debug_log(f"✅ STORED IN SESSION STATE: {operation_id}")
                    
                    return {
                        "success": True,
                        "operation_id": operation_id,
                        "message": f"Archive operation queued! (ID: {operation_id[:8]}...)",
                        "status_url": f"/status/{operation_id}"
                    }
                else:
                    logger.error(f"❌ No operation_id in response: {result}")
                    return {
                        "success": False,
                        "error": "No operation ID returned from worker",
                        "details": result
                    }
            else:
                error_text = response.text
                logger.error(f"❌ Worker returned {response.status_code}: {error_text}")
                return {
                    "success": False,
                    "error": f"Worker error (HTTP {response.status_code})",
                    "details": error_text
                }
                
        except requests.exceptions.Timeout:
            logger.error("❌ Timeout queuing archive operation")
            return {
                "success": False,
                "error": "Timeout connecting to archive service",
                "details": "The request timed out, but the operation may still be processing"
            }
        except Exception as e:
            logger.error(f"❌ Error queuing archive operation: {e}")
            return {
                "success": False,
                "error": "Failed to queue archive operation",
                "details": str(e)
            }
    
    def get_operation_status(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status of an operation from Cloudflare Worker.
        
        Args:
            operation_id: The operation ID to check
            
        Returns:
            Operation status dict or None if not found
        """
        try:
            status_url = f"{ARCHIVE_WORKER_URL}/status/{operation_id}"
            
            # DEBUG: Log the request
            debug_log(f"🌐 STATUS REQUEST: {status_url}")
            
            response = requests.get(
                status_url,
                timeout=5,  # Quick timeout for status checks
                headers={"Content-Type": "application/json"}
            )
            
            # DEBUG: Log the response
            debug_log(f"🌐 STATUS RESPONSE: {response.status_code}")
            debug_log(f"🌐 RESPONSE BODY: {response.text}")
            debug_log(f"🌐 RESPONSE HEADERS: {dict(response.headers)}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"⚠️ Operation {operation_id} not found")
                debug_log(f"⚠️ OPERATION NOT FOUND: {operation_id}")
                return None
            else:
                logger.error(f"❌ Error getting status for {operation_id}: {response.status_code}")
                debug_log(f"❌ STATUS ERROR: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error checking operation status: {e}")
            debug_log(f"❌ STATUS EXCEPTION: {e}")
            return None
    
    def update_pending_operations(self):
        """
        Update the status of all pending operations.
        This should be called on each page load to refresh statuses.
        """
        # Ensure session state is initialized
        self._init_session_state()
        
        if not st.session_state.pending_operations:
            return
        
        updated_pending = []
        
        for operation in st.session_state.pending_operations:
            operation_id = operation["operation_id"]
            
            # DEBUG: Always log what we're checking
            debug_log(f"🔍 CHECKING STATUS: Operation {operation_id}")
            
            current_status = self.get_operation_status(operation_id)
            
            # DEBUG: Log the response
            debug_log(f"🔍 STATUS RESPONSE: {current_status}")
            
            if current_status:
                status = current_status.get("status", "unknown")
                debug_log(f"🔍 PARSED STATUS: {status}")
                debug_log(f"🔍 FULL STATUS OBJECT: {current_status}")
                
                if status in ["completed", "failed"]:
                    debug_log(f"🎯 OPERATION FINISHED: {operation_id} - {status}")
                    # Move to completed operations
                    completed_operation = {
                        **operation,
                        "status": status,
                        "completed_at": datetime.now().isoformat(),
                        "result": current_status.get("result", {}),
                        "error": current_status.get("error")
                    }
                    
                    st.session_state.completed_operations.append(completed_operation)
                    
                    # Update stats
                    if status == "completed":
                        st.session_state.operation_stats['total_completed'] += 1
                    else:
                        st.session_state.operation_stats['total_failed'] += 1
                    
                    logger.info(f"📊 Operation {operation_id} {status}")
                    debug_log(f"✅ MOVED TO COMPLETED: {operation_id} - {status}")
                else:
                    # Still pending, update status
                    operation["status"] = status
                    operation["last_checked"] = datetime.now().isoformat()
                    updated_pending.append(operation)
                    debug_log(f"⏳ STILL PENDING: {operation_id} - {status}")
            else:
                # Keep in pending if we can't get status (might be temporary network issue)
                debug_log(f"❌ NO STATUS RESPONSE: {operation_id} - keeping in pending")
                updated_pending.append(operation)
        
        st.session_state.pending_operations = updated_pending
        
        # Keep only last 20 completed operations to prevent memory bloat
        if len(st.session_state.completed_operations) > 20:
            st.session_state.completed_operations = st.session_state.completed_operations[-20:]
    
    def get_pending_operations(self) -> List[Dict[str, Any]]:
        """Get all pending operations."""
        # Ensure session state is initialized
        self._init_session_state()
        return st.session_state.pending_operations.copy()
    
    def get_completed_operations(self) -> List[Dict[str, Any]]:
        """Get all completed operations."""
        # Ensure session state is initialized
        self._init_session_state()
        return st.session_state.completed_operations.copy()
    
    def clear_completed_operations(self):
        """Clear completed operations list."""
        # Ensure session state is initialized
        self._init_session_state()
        st.session_state.completed_operations = []
    
    def render_operations_sidebar(self):
        """Render operations status in the sidebar."""
        # Ensure session state is initialized
        self._init_session_state()
        
        # Update statuses first
        self.update_pending_operations()
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🚀 Event-Driven Operations")
        
        pending_ops = self.get_pending_operations()
        completed_ops = self.get_completed_operations()
        
        # Show pending operations
        if pending_ops:
            st.sidebar.markdown("**Currently Processing:**")
            
            for op in pending_ops:
                status_icon = "🔄" if op['status'] == 'processing' else "⏳"
                op_type = op['type'].replace('_', ' ').title()
                op_id_short = op['operation_id'][:8]
                
                st.sidebar.write(f"{status_icon} {op_type} ({op_id_short}...)")
                
                # Show phone number for context
                if 'phone_number' in op:
                    st.sidebar.caption(f"📱 {op['phone_number']}")
                
                # Show last checked time
                if 'last_checked' in op:
                    from datetime import datetime
                    try:
                        last_checked = datetime.fromisoformat(op['last_checked'])
                        st.sidebar.caption(f"🕐 Last checked: {last_checked.strftime('%H:%M:%S')}")
                    except:
                        pass
                
                # DEBUG: Show full operation ID and last checked time
                if st.sidebar.button(f"🔍 Check {op_id_short}", key=f"check_{op['operation_id']}"):
                    st.sidebar.write(f"**Full ID:** {op['operation_id']}")
                    status = self.get_operation_status(op['operation_id'])
                    st.sidebar.write(f"**Status:** {status}")
            
            # Enhanced manual refresh with status info
            col1, col2 = st.sidebar.columns(2)
            with col1:
                if st.button("🔄 Refresh", key="manual_refresh_operations"):
                    debug_log("🔄 MANUAL REFRESH: Regular refresh button clicked")
                    self.update_pending_operations()
                    st.rerun()
            with col2:
                if st.button("🧹 Clear Log", key="clear_debug_log"):
                    clear_debug_log()
                    st.success("Log cleared!")
            
            # Add a simple meta refresh for browsers that support it
            if pending_ops:
                st.sidebar.markdown("""
                <meta http-equiv="refresh" content="5">
                <p style="color: #666; font-size: 0.8em;">⏰ Page will auto-refresh in 5 seconds</p>
                """, unsafe_allow_html=True)
        
        # Show completed operations summary
        if completed_ops:
            successful = len([op for op in completed_ops if op['status'] == 'completed'])
            failed = len([op for op in completed_ops if op['status'] == 'failed'])
            
            st.sidebar.markdown("**Recent Results:**")
            if successful > 0:
                st.sidebar.write(f"✅ {successful} successful")
            if failed > 0:
                st.sidebar.write(f"❌ {failed} failed")
            
            # Show recent operations details
            if len(completed_ops) > 0:
                with st.sidebar.expander("Recent Operations", expanded=False):
                    for op in completed_ops[-5:]:  # Show last 5
                        status_icon = "✅" if op['status'] == 'completed' else "❌"
                        op_type = op['type'].replace('_', ' ').title()
                        op_id_short = op['operation_id'][:8]
                        
                        st.write(f"{status_icon} {op_type} ({op_id_short}...)")
                        
                        if op.get('error'):
                            st.error(f"Error: {op['error']}")
                        elif op.get('result', {}).get('success'):
                            st.success("Completed successfully")
            
            # Clear completed operations button
            if st.sidebar.button("🗑️ Clear Completed", key="clear_completed_event_ops"):
                self.clear_completed_operations()
                st.rerun()
        
        # Show stats
        stats = st.session_state.operation_stats
        if stats['total_queued'] > 0:
            st.sidebar.markdown(f"**Total Operations:** {stats['total_queued']}")
            if stats['total_completed'] > 0:
                success_rate = 100 * stats['total_completed'] / stats['total_queued']
                st.sidebar.markdown(f"**Success Rate:** {stats['total_completed']}/{stats['total_queued']} ({success_rate:.0f}%)")
        
        # Simple auto-refresh with manual button emphasis
        if pending_ops:
            st.sidebar.markdown("### 🔄 Auto-Refresh Status")
            st.sidebar.info("💡 **Manual refresh recommended** - Click 'Refresh' button below to check operation status")
            
            # Show a prominent refresh button
            if st.sidebar.button("🔄 **CHECK STATUS NOW**", key="prominent_refresh", type="primary"):
                debug_log("🔄 MANUAL REFRESH: User clicked prominent refresh button")
                st.rerun()
            
            debug_log(f"🔄 PENDING OPS: {len(pending_ops)} operations waiting for status check")
        else:
            debug_log("✅ NO PENDING OPS - All operations completed")


# Global instance
event_operations_manager = EventDrivenOperationsManager()

# Public API functions
def queue_archive_operation(phone_number: str, chat_id: str) -> Dict[str, Any]:
    """Queue an archive operation using event-driven architecture."""
    return event_operations_manager.queue_archive_operation(phone_number, chat_id)

def get_operation_status(operation_id: str) -> Optional[Dict[str, Any]]:
    """Get the status of a specific operation."""
    return event_operations_manager.get_operation_status(operation_id)

def update_pending_operations():
    """Update the status of all pending operations."""
    # Ensure manager is initialized
    event_operations_manager._init_session_state()
    event_operations_manager.update_pending_operations()

def render_operations_sidebar():
    """Render operations status in the sidebar."""
    # Ensure manager is initialized
    event_operations_manager._init_session_state()
    event_operations_manager.render_operations_sidebar()

def get_pending_operations() -> List[Dict[str, Any]]:
    """Get all pending operations."""
    # Ensure manager is initialized
    event_operations_manager._init_session_state()
    return event_operations_manager.get_pending_operations()

def get_completed_operations() -> List[Dict[str, Any]]:
    """Get all completed operations."""
    # Ensure manager is initialized
    event_operations_manager._init_session_state()
    return event_operations_manager.get_completed_operations()