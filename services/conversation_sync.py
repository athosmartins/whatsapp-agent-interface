"""
Conversation Sync Service - Real-time message synchronization for WhatsApp conversations.
Enhanced version with background threading for seamless user experience.
"""

import sqlite3
import datetime
import threading
import queue
import time
from typing import Dict, Optional
import pytz
import streamlit as st


# Configuration
LOCAL_DB_PATH = "whatsapp_conversations.db"
SYNC_INTERVAL = 60  # seconds
PAGE_SIZE = 50  # Number of recent messages to fetch


class ConversationSyncManager:
    """Manages background synchronization of WhatsApp conversations."""

    def __init__(self):
        self.sync_threads: Dict[str, threading.Thread] = {}
        self.result_queues: Dict[str, queue.Queue] = {}
        self.active_syncs: Dict[str, bool] = {}
        self.last_sync_times: Dict[str, datetime.datetime] = {}
        self.sync_stats: Dict[str, Dict] = {}

    def start_sync_for_conversation(self, conversation_id: str) -> bool:
        """Start background sync for a specific conversation."""
        if conversation_id in self.active_syncs and self.active_syncs[conversation_id]:
            return False  # Already running

        try:
            # Initialize sync components
            self.result_queues[conversation_id] = queue.Queue()
            self.active_syncs[conversation_id] = True
            self.sync_stats[conversation_id] = {
                "total_syncs": 0,
                "messages_added": 0,
                "last_error": None,
                "status": "Starting...",
            }

            # Create and start background thread
            sync_thread = threading.Thread(
                target=self._background_sync_worker,
                args=(conversation_id,),
                daemon=True,  # Dies when main thread dies
            )
            sync_thread.start()
            self.sync_threads[conversation_id] = sync_thread

            return True
        except Exception as e:
            self.active_syncs[conversation_id] = False
            self.sync_stats[conversation_id] = {"status": f"Failed to start: {str(e)}"}
            return False

    def stop_sync_for_conversation(self, conversation_id: str) -> bool:
        """Stop background sync for a specific conversation."""
        if conversation_id in self.active_syncs:
            self.active_syncs[conversation_id] = False

            # Wait a moment for thread to finish
            if conversation_id in self.sync_threads:
                self.sync_threads[conversation_id].join(timeout=2.0)
                del self.sync_threads[conversation_id]

            # Clean up
            if conversation_id in self.result_queues:
                del self.result_queues[conversation_id]

            return True
        return False

    def get_sync_results(self, conversation_id: str) -> Optional[Dict]:
        """Get latest sync results without blocking."""
        if conversation_id not in self.result_queues:
            return None

        try:
            # Non-blocking check for results
            result = self.result_queues[conversation_id].get_nowait()
            self.sync_stats[conversation_id].update(result)
            return result
        except queue.Empty:
            return None

    def get_sync_status(self, conversation_id: str) -> Dict:
        """Get current sync status and statistics."""
        if conversation_id not in self.sync_stats:
            return {"status": "Not initialized", "active": False}

        status = self.sync_stats[conversation_id].copy()
        status["active"] = self.active_syncs.get(conversation_id, False)
        status["last_sync"] = self.last_sync_times.get(conversation_id)

        # Calculate time until next sync
        if status["last_sync"] and status["active"]:
            elapsed = (datetime.datetime.now() - status["last_sync"]).total_seconds()
            status["next_sync_in"] = max(0, SYNC_INTERVAL - elapsed)
        else:
            status["next_sync_in"] = 0

        return status

    def _background_sync_worker(self, conversation_id: str):
        """Background worker thread for syncing messages."""
        while self.active_syncs.get(conversation_id, False):
            try:
                # Perform sync
                result = self._sync_single_conversation(conversation_id)

                # Update stats
                self.sync_stats[conversation_id]["total_syncs"] += 1
                self.last_sync_times[conversation_id] = datetime.datetime.now()

                # Send result to main thread
                if conversation_id in self.result_queues:
                    self.result_queues[conversation_id].put(result)

                # Wait for next sync interval
                time.sleep(SYNC_INTERVAL)

            except Exception as e:
                error_result = {
                    "success": False,
                    "error": str(e),
                    "messages_added": 0,
                    "total_fetched": 0,
                    "timestamp": datetime.datetime.now(),
                    "status": f"Error: {str(e)}",
                }

                if conversation_id in self.result_queues:
                    self.result_queues[conversation_id].put(error_result)

                # Continue syncing despite errors
                time.sleep(SYNC_INTERVAL)

    def _sync_single_conversation(self, conversation_id: str) -> Dict:
        """Sync messages for one conversation using Cloudflare Worker."""
        try:
            # Import here to avoid circular imports
            import requests

            # Get chat ID from environment or config
            chat_id = st.secrets.get(
                "VOXUY_CHAT_ID", "07d49a4a-1b9c-4a02-847d-bad0fb3870eb"
            )
            if not chat_id:
                return {
                    "success": False,
                    "error": "VOXUY_CHAT_ID not configured",
                    "messages_added": 0,
                    "total_fetched": 0,
                    "status": "Missing chat ID",
                }

            # Cloudflare Worker endpoint
            worker_url = "https://voxuy-sync-conversation.athosmartins.workers.dev/sync"
            
            payload = {
                "conversation_id": conversation_id,
                "chat_id": chat_id,
                "page_size": PAGE_SIZE
            }

            debug_info = {
                "worker_url": worker_url,
                "payload": payload,
                "conversation_id": conversation_id,
                "chat_id": chat_id,
                "auth_method": "Cloudflare Worker",
                "page_size": PAGE_SIZE,
            }

            print("🔍 **Worker Request Debug:**")
            print(f"Worker URL: {worker_url}")
            print(f"Payload: {payload}")

            # Add JavaScript console logging for production debugging
            import streamlit as st
            st.markdown(f"""
            <script>
            console.log('🔍 SYNC API CALL - Worker URL: {worker_url}');
            console.log('🔍 SYNC API CALL - Request Payload:', {payload});
            console.log('🔍 SYNC API CALL - Starting request at:', new Date().toISOString());
            </script>
            """, unsafe_allow_html=True)

            try:
                # Call Cloudflare Worker
                response = requests.post(
                    worker_url,
                    json=payload,
                    timeout=30,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "UrbanLink-Streamlit/1.0"
                    }
                )

                # Debug: Log the response
                print(f"🔍 **Worker Response Debug:**")
                print(f"Status Code: {response.status_code}")
                print(f"Response Headers: {dict(response.headers)}")
                print(f"Response Text (first 200 chars): {response.text[:200]}")
                
                # Add JavaScript console logging for response
                response_text_safe = response.text.replace('"', '\\"').replace('\n', '\\n')[:200]
                st.markdown(f"""
                <script>
                console.log('🔍 SYNC API RESPONSE - Status Code: {response.status_code}');
                console.log('🔍 SYNC API RESPONSE - Headers:', {dict(response.headers)});
                console.log('🔍 SYNC API RESPONSE - Body (first 200 chars): "{response_text_safe}");
                console.log('🔍 SYNC API RESPONSE - Response received at:', new Date().toISOString());
                </script>
                """, unsafe_allow_html=True)

                if response.status_code != 200:
                    error_text = response.text[:500] if response.text else "No response body"
                    return {
                        "success": False,
                        "error": f"Worker error: {response.status_code}",
                        "messages_added": 0,
                        "total_fetched": 0,
                        "status": f"❌ Worker error {response.status_code}",
                        "debug_info": {
                            **debug_info,
                            "response_status": response.status_code,
                            "response_text": error_text,
                            "response_headers": dict(response.headers),
                        },
                    }

            except requests.exceptions.RequestException as req_error:
                print(f"🔍 **Worker Request Error:** {req_error}")
                return {
                    "success": False,
                    "error": f"Worker request error: {req_error}",
                    "messages_added": 0,
                    "total_fetched": 0,
                    "status": f"❌ Worker request failed",
                    "debug_info": {**debug_info, "request_error": str(req_error)},
                }

            # Parse worker response
            try:
                worker_data = response.json()
                print(f"🔍 **Worker Response Parsed:** {worker_data}")
                
                # Check if worker request was successful
                if not worker_data.get("success", False):
                    worker_error = worker_data.get("error", "Unknown worker error")
                    voxuy_status = worker_data.get("voxuyStatus", "unknown")
                    
                    # Handle authentication errors gracefully (like the old code)
                    if "401" in str(voxuy_status) or "authentication" in worker_error.lower():
                        print("⚠️ **Worker Auth Error**: Voxuy authentication failed in worker")
                        print("   Returning success to not block UI - worker will handle cookie refresh")
                        return {
                            "success": True,  # Return success to not block UI
                            "error": f"Worker auth error: {worker_error}",
                            "messages_added": 0,
                            "total_fetched": 0,
                            "status": "⚠️ Auth error - sync skipped",
                            "debug_info": {
                                **debug_info,
                                "worker_error": worker_error,
                                "voxuy_status": voxuy_status,
                                "auth_note": "Worker will handle cookie refresh automatically",
                            },
                        }
                    
                    # Other errors
                    return {
                        "success": False,
                        "error": f"Worker sync failed: {worker_error}",
                        "messages_added": 0,
                        "total_fetched": 0,
                        "status": f"❌ Worker error",
                        "debug_info": {
                            **debug_info,
                            "worker_error": worker_error,
                            "voxuy_status": voxuy_status,
                        },
                    }

                # Extract messages from worker response
                messages = worker_data.get("messages", [])
                total_fetched = worker_data.get("total_fetched", len(messages))
                
                print(f"🔍 **Worker Messages:** Found {len(messages)} messages")
                
                # Console log the successful parsing
                st.markdown(f"""
                <script>
                console.log('🔍 SYNC SUCCESS - Messages extracted:', {len(messages)});
                console.log('🔍 SYNC SUCCESS - Total fetched:', {total_fetched});
                console.log('🔍 SYNC SUCCESS - Worker data keys:', {list(worker_data.keys())});
                </script>
                """, unsafe_allow_html=True)
                
            except Exception as parse_error:
                print(f"🔍 **Worker JSON Parse Error:** {parse_error}")
                return {
                    "success": False,
                    "error": f"Worker JSON parse error: {parse_error}",
                    "messages_added": 0,
                    "total_fetched": 0,
                    "status": f"❌ Parse error",
                    "debug_info": {**debug_info, "parse_error": str(parse_error)},
                }

            # Add messages to database
            added = self._add_messages_to_database(messages, conversation_id)
            print(f"🔍 **Database Update:** {added} new messages added to DB")
            
            # Console log database operations
            st.markdown(f"""
            <script>
            console.log('🔍 DATABASE UPDATE - Messages added to DB:', {added});
            console.log('🔍 DATABASE UPDATE - Conversation ID:', '{conversation_id}');
            </script>
            """, unsafe_allow_html=True)

            # Update conversation metadata
            self._update_conversation_metadata(conversation_id)

            return {
                "success": True,
                "messages_added": added,
                "total_fetched": total_fetched,
                "timestamp": datetime.datetime.now(),
                "status": (
                    f"✅ Synced - {added} new messages"
                    if added > 0
                    else "✅ Up to date"
                ),
                "debug_info": {
                    **debug_info,
                    "response_status": response.status_code,
                    "messages_fetched": len(messages),
                    "messages_added": added,
                    "worker_success": True,
                },
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "messages_added": 0,
                "total_fetched": 0,
                "timestamp": datetime.datetime.now(),
                "status": f"❌ Error: {str(e)}",
            }

    def _get_existing_message_count(self, conversation_id: str) -> int:
        """Get count of existing messages in database for this conversation."""
        try:
            with sqlite3.connect(LOCAL_DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id = ?",
                    [conversation_id],
                )
                count = cur.fetchone()[0]
                return count
        except Exception:
            return 0

    def _add_messages_to_database(self, messages: list, conversation_id: str) -> int:
        """Insert messages, ignore duplicates, return number of new rows."""
        if not messages:
            return 0

        conn = sqlite3.connect(LOCAL_DB_PATH)
        cur = conn.cursor()
        added = 0

        for msg in messages:
            try:
                msg_id = msg.get("key", {}).get("id", "")
                ts = msg.get("messageTimestamp", 0)
                sender = msg.get("pushName", "")
                from_me = msg.get("key", {}).get("fromMe", False)

                # Timestamp → BRT naive datetime
                brt_dt = None
                if ts:
                    brt_dt = datetime.datetime.fromtimestamp(
                        ts, tz=datetime.timezone.utc
                    )
                    brt_dt = brt_dt.astimezone(
                        pytz.timezone("America/Sao_Paulo")
                    ).replace(tzinfo=None)

                # Content extraction
                m = msg.get("message", {})
                text = ""
                m_type = "text"
                if "conversation" in m:
                    text = m["conversation"]
                elif "extendedTextMessage" in m:
                    text = m["extendedTextMessage"].get("text", "")
                    m_type = "extended_text"
                elif "imageMessage" in m:
                    text, m_type = "[IMAGE]", "image"
                elif "audioMessage" in m:
                    text, m_type = "[AUDIO]", "audio"
                elif "videoMessage" in m:
                    text, m_type = "[VIDEO]", "video"
                elif "documentMessage" in m:
                    text, m_type = "[DOCUMENT]", "document"
                else:
                    text, m_type = "[OTHER]", "other"

                if not text.strip() and m_type == "text":
                    continue  # skip empties

                is_biz = bool(msg.get("verifiedBizName", ""))

                cur.execute(
                    """
                    INSERT OR IGNORE INTO messages
                    (message_id, conversation_id, timestamp, datetime_brt, sender,
                     message_text, from_me, message_type, is_business_message)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        msg_id,
                        conversation_id,
                        ts,
                        brt_dt,
                        sender,
                        text,
                        from_me,
                        m_type,
                        is_biz,
                    ],
                )
                if cur.rowcount:
                    added += 1
            except Exception:
                continue

        conn.commit()
        conn.close()
        return added

    def _update_conversation_metadata(self, conversation_id: str) -> None:
        """Update conversation metadata after sync."""
        with sqlite3.connect(LOCAL_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE conversations SET
                  total_messages         = (SELECT COUNT(*) FROM messages WHERE conversation_id=?),
                  last_message_timestamp = (SELECT MAX(timestamp) FROM messages WHERE conversation_id=?),
                  first_message_timestamp= (SELECT MIN(timestamp) FROM messages WHERE conversation_id=?),
                  last_extraction_timestamp=?,
                  needs_analysis         = TRUE,
                  last_updated           = CURRENT_TIMESTAMP
                WHERE conversation_id=?
                """,
                [
                    conversation_id,
                    conversation_id,
                    conversation_id,
                    int(datetime.datetime.now().timestamp()),
                    conversation_id,
                ],
            )


# Global sync manager instance
_sync_manager = None


def get_sync_manager() -> ConversationSyncManager:
    """Get or create the global sync manager instance."""
    global _sync_manager
    if _sync_manager is None:
        _sync_manager = ConversationSyncManager()
    return _sync_manager


# Convenience functions for Streamlit integration
def start_auto_sync(conversation_id: str) -> bool:
    """Start auto-sync for a conversation."""
    return get_sync_manager().start_sync_for_conversation(conversation_id)


def stop_auto_sync(conversation_id: str) -> bool:
    """Stop auto-sync for a conversation."""
    return get_sync_manager().stop_sync_for_conversation(conversation_id)


def get_sync_status(conversation_id: str) -> Dict:
    """Get sync status for a conversation."""
    return get_sync_manager().get_sync_status(conversation_id)


def check_for_updates(conversation_id: str) -> Optional[Dict]:
    """Check for new sync results."""
    return get_sync_manager().get_sync_results(conversation_id)


def manual_sync(conversation_id: str) -> Dict:
    """Perform immediate manual sync."""
    manager = get_sync_manager()
    return manager._sync_single_conversation(conversation_id)
