"""
Sync UI Components - User interface elements for conversation synchronization.
Provides controls and status indicators for real-time message syncing.
"""

import streamlit as st
import datetime
from typing import Optional, Dict, Any

from services.conversation_sync import (
    start_auto_sync,
    stop_auto_sync,
    get_sync_status,
    check_for_updates,
    manual_sync
)

def initialize_sync_state():
    """Initialize sync-related session state variables."""
    if 'auto_sync_enabled' not in st.session_state:
        st.session_state.auto_sync_enabled = True
    if 'sync_notifications' not in st.session_state:
        st.session_state.sync_notifications = True
    if 'current_sync_conversation' not in st.session_state:
        st.session_state.current_sync_conversation = None
    if 'last_sync_check' not in st.session_state:
        st.session_state.last_sync_check = None

def setup_conversation_sync(conversation_id: str):
    """Setup auto-sync for a specific conversation."""
    initialize_sync_state()
    
    # Debug logging (use global DEBUG variable if available)
    debug_mode = globals().get('DEBUG', False)
    if debug_mode:
        st.write(f"🔍 **Setup Sync Debug:**")
        st.write(f"- conversation_id: {conversation_id}")
        st.write(f"- auto_sync_enabled: {st.session_state.auto_sync_enabled}")
        st.write(f"- current_sync_conversation: {st.session_state.current_sync_conversation}")
    
    # Check sync start conditions
    if debug_mode:
        st.write(f"🔍 **Sync Start Conditions:**")
        st.write(f"- auto_sync_enabled: {st.session_state.auto_sync_enabled}")
        st.write(f"- current_sync_conversation: {st.session_state.current_sync_conversation}")
        st.write(f"- conversation_id: {conversation_id}")
    
    # Start sync if enabled
    if st.session_state.auto_sync_enabled:
        
        # Check if sync is already running for this conversation
        sync_status = get_sync_status(conversation_id)
        is_already_active = sync_status.get("active", False)
        
        if debug_mode:
            st.write(f"🔍 **Auto-sync already active:** {is_already_active}")
        
        if not is_already_active:
            if debug_mode:
                st.write("🔄 Starting auto-sync...")
            
            # Stop previous sync if different conversation
            if (st.session_state.current_sync_conversation and 
                st.session_state.current_sync_conversation != conversation_id):
                stop_auto_sync(st.session_state.current_sync_conversation)
            
            # Start new sync
            if start_auto_sync(conversation_id):
                st.session_state.current_sync_conversation = conversation_id
                # Check for debug mode - try multiple methods
                debug_mode = (
                    globals().get('DEBUG', False) or 
                    getattr(st.session_state, 'debug_mode', False)
                )
                if st.session_state.sync_notifications and debug_mode:
                    st.success("🔄 Auto-sync started for this conversation")
                if debug_mode:
                    st.write(f"✅ Auto-sync started successfully for {conversation_id}")
            else:
                if debug_mode:
                    st.write(f"❌ Failed to start auto-sync for {conversation_id}")
        else:
            if debug_mode:
                st.write("✅ Auto-sync already active for this conversation")
    else:
        if debug_mode:
            st.write("⏸️ Auto-sync disabled - sync not started")

def render_sync_sidebar(conversation_id: str):
    """Render sync controls in the sidebar."""
    with st.sidebar:
        st.subheader("🔄 Auto-Sync")
        
        # Main sync toggle
        col1, col2 = st.columns([3, 1])
        with col1:
            new_auto_sync = st.toggle(
                "Auto-sync every 60s", 
                value=st.session_state.auto_sync_enabled,
                help="Automatically fetch new messages every 60 seconds"
            )
            
            # Handle toggle change
            if new_auto_sync != st.session_state.auto_sync_enabled:
                st.session_state.auto_sync_enabled = new_auto_sync
                if new_auto_sync:
                    start_auto_sync(conversation_id)
                    st.session_state.current_sync_conversation = conversation_id
                else:
                    stop_auto_sync(conversation_id)
                    st.session_state.current_sync_conversation = None
                st.rerun()
        
        with col2:
            # Manual sync button
            if st.button("🔄", help="Manual sync", use_container_width=True, key="manual_sync_btn"):
                # Debug output
                debug_mode = globals().get('DEBUG', False)
                if debug_mode:
                    st.write(f"🔍 **Manual Sync Button Clicked!** conversation_id: {conversation_id}")
                perform_manual_sync(conversation_id)
        
        # Notifications toggle
        st.session_state.sync_notifications = st.toggle(
            "Show sync notifications",
            value=st.session_state.sync_notifications,
            help="Show notifications when sync completes"
        )
        
        # Sync status display
        render_sync_status(conversation_id)

def render_sync_status(conversation_id: str):
    """Render sync status information."""
    status = get_sync_status(conversation_id)
    
    if not status.get("active", False):
        st.caption("🔴 Sync inactive")
        return
    
    # Status indicator
    status_text = status.get("status", "Unknown")
    if status_text.startswith("✅"):
        st.success(status_text)
    elif status_text.startswith("❌"):
        st.error(status_text)
    else:
        st.info(status_text)
    
    # Last sync time
    last_sync = status.get("last_sync")
    if last_sync:
        time_str = last_sync.strftime("%H:%M:%S")
        st.caption(f"Last sync: {time_str}")
    
    # Next sync countdown
    next_sync_in = status.get("next_sync_in", 0)
    if next_sync_in > 0:
        st.caption(f"Next sync in: {int(next_sync_in)}s")
    
    # Statistics
    total_syncs = status.get("total_syncs", 0)
    messages_added = status.get("messages_added", 0)
    if total_syncs > 0:
        st.caption(f"Syncs: {total_syncs} | Messages: {messages_added}")

def render_sync_header(conversation_id: str):
    """Render sync status in the main header."""
    col1, col2, col3 = st.columns([6, 2, 2])

    
    with col2:
        # Live countdown - only show in debug mode
        debug_mode = (
            globals().get('DEBUG', False) or 
            getattr(st.session_state, 'debug_mode', False)
        )
        if debug_mode:
            if st.session_state.auto_sync_enabled:
                status = get_sync_status(conversation_id)
                next_sync_in = status.get("next_sync_in", 0)
                if next_sync_in > 0:
                    st.metric("Next sync", f"{int(next_sync_in)}s")
                else:
                    st.metric("Auto-sync", "Active")
            else:
                st.metric("Auto-sync", "Off")
    
    with col3:
        # Quick status badge - only show in debug mode
        debug_mode = (
            globals().get('DEBUG', False) or 
            getattr(st.session_state, 'debug_mode', False)
        )
        if debug_mode:
            status = get_sync_status(conversation_id)
            status_text = status.get("status", "Inactive")
            
            if status_text.startswith("✅"):
                st.success("Synced")
            elif status_text.startswith("❌"):
                st.error("Error")
            elif status.get("active", False):
                st.info("Active")
            else:
                st.warning("Inactive")

def check_for_sync_updates(conversation_id: str) -> bool:
    """Check for sync updates and handle them. Returns True if UI should refresh."""
    if not conversation_id:
        return False
    
    # Check for new sync results
    result = check_for_updates(conversation_id)
    if not result:
        return False
    
    # Handle successful sync (always refresh to ensure UI is up to date)
    if result.get("success", False):
        messages_added = result.get("messages_added", 0)
        
        # Force refresh of conversation data - clear all conversation-related cache
        cache_keys_to_clear = []
        for key in st.session_state.keys():
            if any(cache_key in key.lower() for cache_key in [
                'conversation_data', 'messages_', 'chat_', 'processor_conversation',
                'current_conversation', 'conversation_history'
            ]):
                cache_keys_to_clear.append(key)
        
        for key in cache_keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        # Also clear any Streamlit data cache that might be holding conversation data
        if hasattr(st, 'cache_data') and hasattr(st.cache_data, 'clear'):
            try:
                st.cache_data.clear()
            except:
                pass  # Ignore errors if cache clearing fails
        
        # Show notification for new messages
        if messages_added > 0 and st.session_state.sync_notifications:
            st.toast(f"🔄 {messages_added} new messages synced!")
        
        return True  # UI should refresh
    
    # Handle sync errors
    elif not result.get("success", False):
        error_msg = result.get("error", "Unknown error")
        if st.session_state.sync_notifications:
            st.toast(f"❌ Sync failed: {error_msg}")
    
    return False

def perform_manual_sync(conversation_id: str):
    """Perform immediate manual sync with user feedback."""
    # Debug logging (use global DEBUG variable if available)
    debug_mode = globals().get('DEBUG', False)
    if debug_mode:
        st.write(f"🔍 **Manual Sync Debug:** Starting sync for {conversation_id}")
    
    # Console logging for production debugging
    st.markdown(f"""
    <script>
    console.log('🔍 MANUAL SYNC - Starting manual sync for conversation:', '{conversation_id}');
    console.log('🔍 MANUAL SYNC - Triggered at:', new Date().toISOString());
    </script>
    """, unsafe_allow_html=True)
    
    with st.spinner("🔄 Syncing conversation..."):
        result = manual_sync(conversation_id)
    
    # Console log the result
    result_safe = str(result).replace('"', '\\"').replace('\n', '\\n')[:500]
    st.markdown(f"""
    <script>
    console.log('🔍 MANUAL SYNC - Result received:', '{result_safe}');
    console.log('🔍 MANUAL SYNC - Success:', {result.get("success", False)});
    console.log('🔍 MANUAL SYNC - Messages added:', {result.get("messages_added", 0)});
    </script>
    """, unsafe_allow_html=True)
    
    if debug_mode:
        st.write(f"🔍 **Manual Sync Result:** {result}")
        
        # Show detailed debug info if available
        debug_info = result.get("debug_info")
        if debug_info:
            st.write("🔍 **API Debug Details:**")
            with st.expander("API Request/Response Details", expanded=True):
                st.json(debug_info)
    
    if result.get("success", False):
        messages_added = result.get("messages_added", 0)
        
        # Clear conversation cache but preserve navigation state
        cache_keys_to_clear = []
        for key in st.session_state.keys():
            if any(cache_key in key.lower() for cache_key in [
                'conversation_data', 'messages_', 'chat_', 'processor_conversation',
                'conversation_history'
            ]):
                # Don't clear navigation-critical state like 'current_conversation_id', 'idx', etc.
                if not any(nav_key in key.lower() for nav_key in [
                    'current_conversation_id', 'idx', 'selected_', 'show_'
                ]):
                    cache_keys_to_clear.append(key)
        
        for key in cache_keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        if messages_added > 0:
            st.success(f"✅ Synced! {messages_added} new messages added.")
        else:
            st.info("✅ Conversation synced and refreshed.")
        
        # Preserve conversation_id in query params before rerun
        if conversation_id:
            st.query_params["conversation_id"] = conversation_id
        
        # Set flag to prevent database redownload after manual sync
        st.session_state.skip_db_download = True
        
        # Force UI refresh
        st.rerun()
    else:
        error_msg = result.get("error", "Unknown error")
        st.error(f"❌ Sync failed: {error_msg}")
        
        # Even if sync fails, set skip flag to prevent unnecessary database redownload
        # This prevents the page from redownloading the entire database when sync fails
        st.session_state.skip_db_download = True
        
        # Preserve conversation_id in query params before rerun
        if conversation_id:
            st.query_params["conversation_id"] = conversation_id
        
        # Still refresh UI to show error state
        st.rerun()

def cleanup_sync_on_exit(conversation_id: str):
    """Clean up sync resources when leaving a conversation."""
    if st.session_state.current_sync_conversation == conversation_id:
        stop_auto_sync(conversation_id)
        st.session_state.current_sync_conversation = None

def render_sync_metrics_card():
    """Render a compact metrics card for sync information."""
    if not st.session_state.current_sync_conversation:
        return
    
    conversation_id = st.session_state.current_sync_conversation
    status = get_sync_status(conversation_id)
    
    if not status.get("active", False):
        return
    
    # Create metrics container
    with st.container():
        st.markdown("**Sync Status**")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Check for debug mode - try multiple methods
            debug_mode = (
                globals().get('DEBUG', False) or 
                getattr(st.session_state, 'debug_mode', False)
            )
            if debug_mode:
                st.metric(
                    "Status",
                    "Active" if status.get("active") else "Inactive",
                    delta="🔄" if status.get("active") else "⏸️"
                )
        
        with col2:
            total_syncs = status.get("total_syncs", 0)
            st.metric("Syncs", total_syncs)
        
        with col3:
            messages_added = status.get("messages_added", 0)
            st.metric("New Messages", messages_added)

# Auto-refresh mechanism for real-time updates
def setup_auto_refresh():
    """Setup automatic page refresh for sync updates."""
    if st.session_state.auto_sync_enabled and st.session_state.current_sync_conversation:
        # Add JavaScript for auto-refresh every 30 seconds
        st.markdown("""
        <script>
        // Auto-refresh for sync updates
        if (!window.syncRefreshInterval) {
            window.syncRefreshInterval = setInterval(function() {
                // Only refresh if auto-sync is enabled
                if (window.parent.streamlit && window.parent.streamlit.runtime) {
                    console.log('Auto-refresh for sync updates');
                    window.parent.location.reload();
                }
            }, 30000); // 30 seconds
        }
        </script>
        """, unsafe_allow_html=True)
    else:
        # Clear refresh interval if sync is disabled
        st.markdown("""
        <script>
        if (window.syncRefreshInterval) {
            clearInterval(window.syncRefreshInterval);
            window.syncRefreshInterval = null;
        }
        </script>
        """, unsafe_allow_html=True)