"""
Performance Monitoring Service for Backend Operations.
Tracks function calls, timing, memory usage, and data loading operations.
"""

import time
import functools
import traceback
import psutil
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import streamlit as st


class PerformanceMonitor:
    """Monitor backend performance and operations."""
    
    def __init__(self):
        self.session_key = "performance_monitor_data"
        self._init_session_state()
    
    def _init_session_state(self):
        """Initialize session state for performance monitoring."""
        if self.session_key not in st.session_state:
            st.session_state[self.session_key] = {
                "function_calls": [],
                "data_operations": [],
                "memory_snapshots": [],
                "cache_operations": [],
                "start_time": time.time()
            }
    
    def log_function_call(self, func_name: str, args_info: str, execution_time: float, 
                         memory_before: float, memory_after: float, result_info: str = "",
                         cache_hit: bool = False):
        """Log a function call with performance metrics."""
        self._init_session_state()
        
        call_data = {
            "timestamp": datetime.now().isoformat(),
            "function": func_name,
            "args": args_info,
            "execution_time_ms": round(execution_time * 1000, 2),
            "memory_before_mb": round(memory_before, 2),
            "memory_after_mb": round(memory_after, 2),
            "memory_delta_mb": round(memory_after - memory_before, 2),
            "result_info": result_info,
            "cache_hit": cache_hit
        }
        
        st.session_state[self.session_key]["function_calls"].append(call_data)
        
        # Keep only last 50 calls to avoid memory bloat
        if len(st.session_state[self.session_key]["function_calls"]) > 50:
            st.session_state[self.session_key]["function_calls"] = \
                st.session_state[self.session_key]["function_calls"][-50:]
        
        # Print to console for immediate feedback
        memory_delta_mb = memory_after - memory_before
        print(f"📊 {func_name}({args_info}) -> {execution_time*1000:.1f}ms | "
              f"RAM: {memory_before:.1f}→{memory_after:.1f}MB ({memory_delta_mb:+.1f}) | "
              f"{'💾 CACHE HIT' if cache_hit else '🔄 FRESH CALL'} | {result_info}")
    
    def log_data_operation(self, operation: str, details: Dict[str, Any]):
        """Log data loading/processing operations."""
        self._init_session_state()
        
        operation_data = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "details": details
        }
        
        st.session_state[self.session_key]["data_operations"].append(operation_data)
        
        # Keep only last 20 operations
        if len(st.session_state[self.session_key]["data_operations"]) > 20:
            st.session_state[self.session_key]["data_operations"] = \
                st.session_state[self.session_key]["data_operations"][-20:]
        
        print(f"💽 DATA: {operation} | {details}")
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for display."""
        self._init_session_state()
        data = st.session_state[self.session_key]
        
        total_calls = len(data["function_calls"])
        cache_hits = sum(1 for call in data["function_calls"] if call["cache_hit"])
        
        if total_calls > 0:
            avg_execution_time = sum(call["execution_time_ms"] for call in data["function_calls"]) / total_calls
            total_memory_delta = sum(call["memory_delta_mb"] for call in data["function_calls"])
        else:
            avg_execution_time = 0
            total_memory_delta = 0
        
        return {
            "total_function_calls": total_calls,
            "cache_hit_rate": f"{(cache_hits/total_calls*100):.1f}%" if total_calls > 0 else "0%",
            "avg_execution_time_ms": round(avg_execution_time, 2),
            "total_memory_delta_mb": round(total_memory_delta, 2),
            "current_memory_mb": round(self.get_memory_usage(), 2),
            "data_operations": len(data["data_operations"]),
            "session_duration_min": round((time.time() - data["start_time"]) / 60, 1)
        }
    
    def get_recent_calls(self, limit: int = 10) -> List[Dict]:
        """Get recent function calls."""
        self._init_session_state()
        calls = st.session_state[self.session_key]["function_calls"]
        return calls[-limit:] if calls else []
    
    def get_expensive_calls(self, min_time_ms: float = 100) -> List[Dict]:
        """Get calls that took longer than specified time."""
        self._init_session_state()
        calls = st.session_state[self.session_key]["function_calls"]
        return [call for call in calls if call["execution_time_ms"] >= min_time_ms]
    
    def clear_data(self):
        """Clear all monitoring data."""
        if self.session_key in st.session_state:
            del st.session_state[self.session_key]
        self._init_session_state()
        print("🧹 Performance monitoring data cleared")


# Global monitor instance
monitor = PerformanceMonitor()


def performance_monitor(func_name: Optional[str] = None, log_args: bool = True, log_result: bool = True):
    """
    Decorator to monitor function performance.
    
    Args:
        func_name: Custom function name for logging
        log_args: Whether to log function arguments
        log_result: Whether to log result information
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get function name
            name = func_name or f"{func.__module__}.{func.__name__}"
            
            # Get memory before
            memory_before = monitor.get_memory_usage()
            
            # Prepare args info
            args_info = ""
            if log_args:
                try:
                    args_str = ", ".join([str(arg)[:50] for arg in args[:3]])  # First 3 args, truncated
                    kwargs_str = ", ".join([f"{k}={str(v)[:30]}" for k, v in list(kwargs.items())[:2]])
                    args_info = f"{args_str}" + (f", {kwargs_str}" if kwargs_str else "")
                    if len(args_info) > 100:
                        args_info = args_info[:100] + "..."
                except:
                    args_info = "args parsing failed"
            
            # Check if this might be a cache hit (for Streamlit cached functions)
            cache_hit = False
            if hasattr(func, '__wrapped__') and 'cache' in str(type(func)):
                cache_hit = True
            
            # Execute function
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                # Get memory after
                memory_after = monitor.get_memory_usage()
                
                # Prepare result info
                result_info = ""
                if log_result:
                    try:
                        if hasattr(result, '__len__'):
                            result_info = f"len={len(result)}"
                        elif isinstance(result, (int, float)):
                            result_info = f"={result}"
                        elif result is not None:
                            result_info = f"type={type(result).__name__}"
                        else:
                            result_info = "None"
                    except:
                        result_info = "result info failed"
                
                # Log the call
                monitor.log_function_call(
                    func_name=name,
                    args_info=args_info,
                    execution_time=execution_time,
                    memory_before=memory_before,
                    memory_after=memory_after,
                    result_info=result_info,
                    cache_hit=cache_hit
                )
                
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                memory_after = monitor.get_memory_usage()
                
                # Log the failed call
                monitor.log_function_call(
                    func_name=name,
                    args_info=args_info,
                    execution_time=execution_time,
                    memory_before=memory_before,
                    memory_after=memory_after,
                    result_info=f"ERROR: {str(e)[:50]}",
                    cache_hit=cache_hit
                )
                
                raise
        
        return wrapper
    return decorator


def log_data_operation(operation: str, **details):
    """Log a data operation."""
    monitor.log_data_operation(operation, details)


def get_performance_summary() -> Dict[str, Any]:
    """Get performance summary."""
    return monitor.get_performance_summary()


def get_recent_calls(limit: int = 10) -> List[Dict]:
    """Get recent function calls."""
    return monitor.get_recent_calls(limit)


def get_expensive_calls(min_time_ms: float = 100) -> List[Dict]:
    """Get expensive function calls."""
    return monitor.get_expensive_calls(min_time_ms)


def clear_performance_data():
    """Clear performance data."""
    monitor.clear_data()


def render_performance_sidebar():
    """Render performance monitoring in sidebar."""
    if st.sidebar.button("🔄 Refresh Performance Stats"):
        st.sidebar.rerun()
    
    summary = get_performance_summary()
    
    st.sidebar.markdown("### 📊 Performance Monitor")
    st.sidebar.metric("Function Calls", summary["total_function_calls"])
    st.sidebar.metric("Cache Hit Rate", summary["cache_hit_rate"])
    st.sidebar.metric("Avg Execution (ms)", summary["avg_execution_time_ms"])
    st.sidebar.metric("Memory Delta (MB)", summary["total_memory_delta_mb"])
    st.sidebar.metric("Current RAM (MB)", summary["current_memory_mb"])
    
    # Show expensive calls
    expensive = get_expensive_calls(50)  # Calls > 50ms
    if expensive:
        st.sidebar.markdown("### ⚠️ Slow Calls (>50ms)")
        for call in expensive[-3:]:  # Last 3 slow calls
            st.sidebar.write(f"• **{call['function']}**: {call['execution_time_ms']:.1f}ms")
    
    # Show recent data operations
    recent_calls = get_recent_calls(5)
    if recent_calls:
        st.sidebar.markdown("### 🔄 Recent Calls")
        for call in recent_calls:
            cache_icon = "💾" if call["cache_hit"] else "🔄"
            st.sidebar.write(f"{cache_icon} {call['function']}: {call['execution_time_ms']:.1f}ms")
    
    if st.sidebar.button("🧹 Clear Performance Data"):
        clear_performance_data()
        st.sidebar.success("Performance data cleared!")
        st.sidebar.rerun()