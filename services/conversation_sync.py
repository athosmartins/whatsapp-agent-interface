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

            # Test worker health first
            health_url = "https://voxuy-sync-conversation.athosmartins.workers.dev/health"
            try:
                health_resp = requests.get(health_url, timeout=10)
                print(f"🏥 **Health Check:** {health_url} returned {health_resp.status_code}")
                if health_resp.status_code == 200:
                    print(f"Health response: {health_resp.text[:100]}")
                else:
                    print(f"Health error: {health_resp.text[:100]}")
            except Exception as e:
                print(f"🏥 **Health Check Failed:** {e}")

            # Cloudflare Worker endpoints to try - focus on /sync since that's what the worker expects
            worker_endpoints = [
                "https://voxuy-sync-conversation.athosmartins.workers.dev/sync"
            ]
            
            payload = {
                "conversation_id": conversation_id,
                "chat_id": chat_id,
                "page_size": PAGE_SIZE
            }

            # Validate payload before sending
            try:
                import json
                # Test JSON serialization to catch issues early
                payload_json = json.dumps(payload)
                if not payload_json or len(payload_json) < 10:
                    raise ValueError("Payload serialization failed or too short")
                print(f"🔍 **Payload Validation:** JSON serialized successfully ({len(payload_json)} chars)")
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Payload validation failed: {str(e)}",
                    "messages_added": 0,
                    "total_fetched": 0,
                    "status": "❌ Invalid payload",
                }

            # Try each endpoint until one works
            response = None
            last_error = None
            successful_url = None
            
            for worker_url in worker_endpoints:
                debug_info = {
                    "worker_url": worker_url,
                    "payload": payload,
                    "conversation_id": conversation_id,
                    "chat_id": chat_id,
                    "auth_method": "Cloudflare Worker",
                    "page_size": PAGE_SIZE,
                }

                print(f"🔍 **Worker Request Debug:** Trying {worker_url}")
                print(f"Payload: {payload}")

                # Skip JavaScript logging in background thread (no ScriptRunContext)
                # st.markdown would cause "missing ScriptRunContext" error

                # Retry logic for each endpoint
                max_retries = 3
                retry_delay = 1  # seconds
                
                for attempt in range(max_retries):
                    try:
                        # Debug: Log exact request details
                        print(f"🔍 **Request Details (Attempt {attempt + 1}/{max_retries}):**")
                        print(f"URL: {worker_url}")
                        print(f"Method: POST")
                        print(f"Headers: Content-Type: application/json")
                        print(f"JSON Payload: {payload}")
                        
                        # Ensure payload is properly serialized before each request
                        try:
                            validated_payload = json.loads(json.dumps(payload))
                        except Exception as e:
                            print(f"❌ **Payload Re-serialization Failed:** {e}")
                            continue
                        
                        # Call Cloudflare Worker with POST - try both json= and data= methods
                        
                        # First try with json parameter (should work)
                        response = requests.post(
                            worker_url,
                            json=validated_payload,
                            timeout=30,
                            headers={
                                "Content-Type": "application/json",
                                "User-Agent": "UrbanLink-Streamlit/1.0"
                            }
                        )
                        
                        # If that fails with JSON error, try sending as string
                        if response.status_code == 400 and "json" in response.text.lower():
                            print(f"🔄 **Retrying with string payload (Attempt {attempt + 1}):** {worker_url}")
                            response = requests.post(
                                worker_url,
                                data=json.dumps(validated_payload),
                                timeout=30,
                                headers={
                                    "Content-Type": "application/json",
                                    "User-Agent": "UrbanLink-Streamlit/1.0"
                                }
                            )
                        
                        # If POST fails with 405, try GET with query params
                        if response.status_code == 405:
                            print(f"🔄 **Trying GET (Attempt {attempt + 1}):** {worker_url}")
                            params = {
                                "conversation_id": conversation_id,
                                "chat_id": chat_id,
                                "page_size": PAGE_SIZE
                            }
                            response = requests.get(
                                worker_url,
                                params=params,
                                timeout=30,
                                headers={"User-Agent": "UrbanLink-Streamlit/1.0"}
                            )
                        
                        # If we get a 200 response, use this endpoint
                        if response.status_code == 200:
                            successful_url = worker_url
                            print(f"✅ **Worker Success (Attempt {attempt + 1}):** {worker_url} returned 200")
                            break
                        # If we get 500 (JSON parsing error), retry after delay
                        elif response.status_code == 500 and "json" in response.text.lower():
                            error_text = response.text[:200] if response.text else "No response body"
                            print(f"🔄 **Worker 500 - JSON Error (Attempt {attempt + 1}):** {worker_url}")
                            print(f"Response body: {error_text}")
                            if attempt < max_retries - 1:
                                print(f"⏱️ **Retrying in {retry_delay}s...**")
                                time.sleep(retry_delay)
                                retry_delay *= 2  # Exponential backoff
                                continue
                            else:
                                last_error = f"500 JSON error after {max_retries} attempts: {error_text}"
                                break
                        # If we get 405, try next endpoint (no retry needed)
                        elif response.status_code == 405:
                            print(f"❌ **Worker 405:** {worker_url} - Method Not Allowed, trying next endpoint...")
                            last_error = f"405 Method Not Allowed at {worker_url}"
                            break
                        else:
                            # Other errors, retry or continue to next endpoint
                            error_text = response.text[:200] if response.text else "No response body"
                            print(f"❌ **Worker Error (Attempt {attempt + 1}):** {worker_url} returned {response.status_code}")
                            print(f"Response body: {error_text}")
                            if attempt < max_retries - 1:
                                print(f"⏱️ **Retrying in {retry_delay}s...**")
                                time.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                            else:
                                last_error = f"{response.status_code} error after {max_retries} attempts: {error_text}"
                                break
                                
                    except requests.exceptions.RequestException as req_error:
                        print(f"❌ **Worker Request Error (Attempt {attempt + 1}):** {worker_url} - {req_error}")
                        if attempt < max_retries - 1:
                            print(f"⏱️ **Retrying in {retry_delay}s...**")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        else:
                            last_error = f"Request error after {max_retries} attempts: {req_error}"
                            break
                
                # If we got a successful response, break out of endpoint loop
                if successful_url and response and response.status_code == 200:
                    break
            
            # If no endpoint worked, return error
            if not successful_url or not response or response.status_code != 200:
                return {
                    "success": False,
                    "error": f"All worker endpoints failed. Last error: {last_error}",
                    "messages_added": 0,
                    "total_fetched": 0,
                    "status": f"❌ All worker endpoints failed",
                    "debug_info": {
                        "endpoints_tried": worker_endpoints,
                        "last_error": last_error,
                        "auth_method": "Cloudflare Worker",
                    },
                }
            
            # Success! Log the response
            print(f"🔍 **Worker Response Debug:**")
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Text (first 200 chars): {response.text[:200]}")
            
            # Skip JavaScript logging in background thread (no ScriptRunContext)

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
                
                # Skip JavaScript logging in background thread (no ScriptRunContext)
                
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
            
            # Skip JavaScript logging in background thread (no ScriptRunContext)

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
            # Get the current database path (don't redownload)
            import os
            db_path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREAMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
            with sqlite3.connect(db_path) as conn:
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

        # Get the current database path (don't redownload)
        import os
        db_path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREAMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
        conn = sqlite3.connect(db_path)
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
                    # Force UTC interpretation - timestamps from WhatsApp are always UTC
                    utc_dt = datetime.datetime.utcfromtimestamp(ts).replace(tzinfo=datetime.timezone.utc)
                    brt_dt = utc_dt.astimezone(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
                    
                    # Debug logging for production troubleshooting
                    if len(st.session_state.get('sync_debug_log', [])) < 3:  # Only log first few messages
                        debug_info = {
                            'timestamp': ts,
                            'utc_time': utc_dt.strftime('%H:%M:%S'),
                            'brt_time': brt_dt.strftime('%H:%M:%S'),
                            'message_id': msg_id[:10] + '...' if len(msg_id) > 10 else msg_id
                        }
                        if 'sync_debug_log' not in st.session_state:
                            st.session_state.sync_debug_log = []
                        st.session_state.sync_debug_log.append(debug_info)
                        print(f"🕐 Timezone Debug: {debug_info}")

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
        # Get the current database path (don't redownload)
        import os
        db_path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREAMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
        with sqlite3.connect(db_path) as conn:
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
