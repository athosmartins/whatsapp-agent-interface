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
        """Sync messages for one conversation."""
        try:
            # Import here to avoid circular imports
            import requests

            # Use cookies-based authentication (like your working code)
            cookies = {
                "intercom-device-id-fyij4g09": "0aef106d-fc5f-468b-b1fc-5895f7629867",
                "_gcl_au": "1.1.1587009254.1751331469",
                "_ga": "GA1.1.779566562.1751331470",
                "_fbp": "fb.1.1751331981705.841783711308725611",
                "_delighted_web": "{%229nbsfH9ASzqOdnYE%22:{%22_delighted_fst%22:{%22t%22:%221751377860896%22}}}",
                "ARRAffinity": "4918ce12697d563429e9a8fa569f93d60282c0546949764027b7b20f28437854",
                "ARRAffinitySameSite": "4918ce12697d563429e9a8fa569f93d60282c0546949764027b7b20f28437854",
                "_clck": "1xzl9am%7C2%7Cfx9%7C0%7C2008",
                "_ga_MQ8KT0SKK8": "GS2.1.s1751416954$o2$g0$t1751416954$j60$l0$h0",
                "_uetsid": "9a0ecd80561711f0ba22652a1eb6ad63",
                "_uetvid": "3033a1d0363311f0bd47475bac58aa83",
                ".AspNetCore.Identity.Application": "CfDJ8ERNfom8FeFMkiblNdy-dvjB-9N-QOa2BfSZ-L8LDDLZNv3295-3IIwjyGILffnpAa4vAAjA8DjaZj5ypHaK4ruAapTKktRhE_z_HcqAw_IkTMMOQtvV8_Hh3306AYAciX6W-wHMp5wsYFriVndTXJo7DN41zznYM4WaREbHnwUbR5oWkgDFNUm9ppb760IzRD8ee4zycZZB7Z5L6VA2FwqgjB2IWGiMDJAEfIXNebzPkW5jCp5csTbKfH47wQlk66_Op_JF42ZnVOim-Ne6joQcop3PeXLTIcpEF3D3uN4I1x1T7V816Zhm08P3RD0-9h4t1m9BIqIg7oYWs6NeUQdIWMip-jnujyI-yHztMk05wQ0IaabllowMOq3KrDbJ9d7mDO_TX9DmfEQmanaNH1r4u099Vv7NL9EST4FR5MKpgXtiU6-mVZ1Es8NTsNFVVg1GDJTuqB-ew8uTmJEhobfaqr_BywWSGej95hFNewuNkH130-T4_miqlQlGAmJqRPQJk9zWA5VwlxKOqqQ9l3FXWfs8_NO1QnpRhf6fDtm4VVcvZvaDqZzZGe6SpI5H1Oom044Xdyln1wcnI6JhioYNnHYw9VGBmsvp_KiXeA-Kkth36-iLVShBeGy5mxTwozWtubOjJe78nanPHDJDazoOYlNk0IcmqKEU7i0ShTrZWhAg_89TT8EW1zKBggXhZgjKq-9QET_gBOQ9hh5xEbiagfZ-EVKIvurqtyS3Xs0Q-luBam7dQoTDXkS0mo_YU_xiMfBkNVew57nmnSI9EkbBmpxURW4AyzqS4GwUkk3Y1rpWoi86ryQxeiPFxfLHu3TLjTXsGR1KYTn4eI3YZWW5kD8igXP6EeZYbaJaAbV2bOMv3ahmsL9EAu08DZei2935cBoyktHfpKyB1iOmdDEpkEEXiLcW6_xKIXcB2saUyZwWKS8ZBWsrTw540DHmobFiTceXrj7dvjsVqk-yIi_ZBqHzvIOKXnK8oz1sMt9hHeKEN---hgmp7O7vC4L3IQ",
                "_ga_HKBN00L8C8": "GS2.1.s1751416194$o7$g1$t1751419910$j60$l0$h0",
                "_clsk": "1fkepxh%7C1751419911674%7C13%7C1%7Ca.clarity.ms%2Fcollect",
                "intercom-session-fyij4g09": "c2VIKytmWXQzRXdMeHljbGhDZXRiWVVNTUNkMDRmT1FJTm45d04yV2Y3R2RXdTQ0TFNOak9PS2w0UWtjN3BxRy9ndmpaaDFGWjNzNHpwUVdoN1dJcU0wTDJtYkxrNm40Q1hyMzZaQ1RTQjQ9LS1SNFJQYkdLMDhaR3lrSkN2SG1zTThBPT0=--fee3e9c003d2993cbc4e9481924913a16679626f",
            }

            print("🔍 **Auth Debug:** Using session cookies (like your working code)")
            print(
                "🔍 **Main Cookie:** .AspNetCore.Identity.Application present: {bool(cookies.get('.AspNetCore.Identity.Application'))}"
            )

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

            # Get chat ID from environment or config
            chat_id = st.secrets.get(
                "VOXUY_CHAT_ID", "07d49a4a-1b9c-4a02-847d-bad0fb3870eb"
            )

            # Fetch recent messages using cookies (exactly like your working code)
            url = f"https://web.voxuy.com/api/chats/{chat_id}/conversations/{conversation_id}/messages"
            params = {"PageSize": PAGE_SIZE}

            # Create session with cookies (like S.cookies.update(COOKIES) in your code)
            session = requests.Session()
            session.cookies.update(cookies)

            debug_info = {
                "url": url,
                "params": params,
                "conversation_id": conversation_id,
                "chat_id": chat_id,
                "auth_method": "Session Cookies",
                "cookies_count": len(cookies),
            }

            print("🔍 **API Request Debug:**")
            print(f"URL: {url}")
            print(f"Params: {params}")
            print(f"Cookies: {len(cookies)} cookies set")
            print(
                f"Key cookie present: {bool(cookies.get('.AspNetCore.Identity.Application'))}"
            )

            try:
                response = session.get(url, params=params, timeout=30)

                # Debug: Log the response
                print(f"🔍 **API Response Debug:**")
                print(f"Status Code: {response.status_code}")
                print(f"Response Headers: {dict(response.headers)}")
                print(f"Response Text (first 200 chars): {response.text[:200]}")

                if response.status_code == 401:
                    # TEMPORARY FIX: Authentication cookies expired
                    print("⚠️ **TEMPORARY FIX**: Voxuy API authentication cookies expired")
                    print("   Skipping API sync to prevent blocking conversation loading")
                    return {
                        "success": True,  # Return success to not block UI
                        "error": "Authentication cookies expired - skipping sync",
                        "messages_added": 0,
                        "total_fetched": 0,
                        "status": "⚠️ Auth expired - sync skipped",
                        "debug_info": {
                            **debug_info,
                            "response_status": response.status_code,
                            "auth_note": "Cookies expired - requires manual renewal",
                        },
                    }

                if response.status_code != 200:
                    return {
                        "success": False,
                        "error": f"API error: {response.status_code}",
                        "messages_added": 0,
                        "total_fetched": 0,
                        "status": f"❌ API error {response.status_code}",
                        "debug_info": {
                            **debug_info,
                            "response_status": response.status_code,
                            "response_text": response.text[:200],
                            "response_headers": dict(response.headers),
                        },
                    }

            except requests.exceptions.RequestException as req_error:
                print(f"🔍 **Request Error:** {req_error}")
                return {
                    "success": False,
                    "error": f"Request error: {req_error}",
                    "messages_added": 0,
                    "total_fetched": 0,
                    "status": f"❌ Request failed",
                    "debug_info": {**debug_info, "request_error": str(req_error)},
                }

            # Parse successful response
            try:
                response_data = response.json()
                messages = response_data.get("Data", [])
                print(f"🔍 **API Response Parsed:** Found {len(messages)} messages")
            except Exception as parse_error:
                print(f"🔍 **JSON Parse Error:** {parse_error}")
                return {
                    "success": False,
                    "error": f"JSON parse error: {parse_error}",
                    "messages_added": 0,
                    "total_fetched": 0,
                    "status": f"❌ Parse error",
                    "debug_info": {**debug_info, "parse_error": str(parse_error)},
                }

            # Add messages to database
            added = self._add_messages_to_database(messages, conversation_id)
            print(f"🔍 **Database Update:** {added} new messages added to DB")

            # Update conversation metadata
            self._update_conversation_metadata(conversation_id)

            return {
                "success": True,
                "messages_added": added,
                "total_fetched": len(messages),
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
