"""
Comprehensive Debug Logger Service

Captures all print statements and debug output to files that Kiro can read.
This provides full visibility into application behavior for better debugging.
"""

import sys
import os
from datetime import datetime
from typing import TextIO

# Simple file-based logging that always works
DEBUG_FILE = "kiro_debug.log"

def debug_log(message: str):
    """Log a debug message with timestamp to file that Kiro can read."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_line = f"[{timestamp}] 🔍 {message}\n"
    
    try:
        with open(DEBUG_FILE, "a", encoding="utf-8") as f:
            f.write(log_line)
            f.flush()
    except Exception as e:
        print(f"❌ Failed to write debug log: {e}")
    
    # Also print to console
    print(f"🔍 {message}")

def clear_debug_log():
    """Clear the debug log file."""
    try:
        with open(DEBUG_FILE, "w", encoding="utf-8") as f:
            f.write("")
        debug_log("DEBUG LOG CLEARED")
    except Exception as e:
        print(f"❌ Failed to clear debug log: {e}")

class StreamCapture:
    """Captures stdout/stderr and writes to both console and file."""
    
    def __init__(self, original_stream, stream_name):
        self.original_stream = original_stream
        self.stream_name = stream_name
    
    def write(self, text):
        # Write to original stream (console)
        self.original_stream.write(text)
        self.original_stream.flush()
        
        # Write to debug file if it's meaningful content
        if text.strip():
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            try:
                with open(DEBUG_FILE, "a", encoding="utf-8") as f:
                    f.write(f"[{timestamp}] [{self.stream_name}] {text}")
                    f.flush()
            except:
                pass  # Don't break if file write fails
    
    def flush(self):
        self.original_stream.flush()
    
    def __getattr__(self, name):
        return getattr(self.original_stream, name)

def start_debug_logging():
    """Start comprehensive debug logging."""
    try:
        # Clear previous log
        clear_debug_log()
        
        # Capture stdout and stderr
        sys.stdout = StreamCapture(sys.stdout, "STDOUT")
        sys.stderr = StreamCapture(sys.stderr, "STDERR")
        
        debug_log("🚀 COMPREHENSIVE DEBUG LOGGING STARTED")
        debug_log(f"Debug file: {DEBUG_FILE}")
        
    except Exception as e:
        print(f"❌ Failed to start debug logging: {e}")

def stop_debug_logging():
    """Stop comprehensive debug logging."""
    debug_log("🛑 DEBUG LOGGING STOPPED")