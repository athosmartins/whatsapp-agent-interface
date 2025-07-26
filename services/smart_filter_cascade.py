"""
Smart Filter Cascading Service.
Implements intelligent filter reloading logic to avoid unnecessary data fetching.
"""

import streamlit as st
from typing import List, Dict, Set, Optional
from services.lazy_column_loader import get_column_values, get_column_metadata
import time


class SmartFilterCascade:
    """
    Manages intelligent filter cascading with minimal reloading.
    
    Cascade Rules:
    1. Filter changes only affect filters BELOW them
    2. Filter changes affect filters ABOVE them that have NO selected value
    3. Never reload: bairros filter, filters with selected values
    """
    
    def __init__(self):
        self.session_key = "smart_filter_cascade_state"
        self._init_session_state()
    
    def _init_session_state(self):
        """Initialize session state for filter cascade management."""
        if self.session_key not in st.session_state:
            st.session_state[self.session_key] = {
                "filter_states": {},  # Track state of each filter
                "value_cache": {},    # Cache loaded values
                "last_update_times": {},  # Track when filters were last updated
                "cascade_blocked": set(),  # Temporarily block certain filters from cascading
            }
    
    def should_reload_filter(self, filter_index: int, changed_filter_index: int, 
                           filter_configs: List[Dict]) -> bool:
        """
        Determine if a filter should be reloaded based on cascade rules.
        
        Args:
            filter_index: Index of filter to check for reload
            changed_filter_index: Index of filter that changed
            filter_configs: List of all filter configurations
            
        Returns:
            True if filter should be reloaded
        """
        # Never reload the changed filter itself
        if filter_index == changed_filter_index:
            return False
        
        # Never reload bairros filter (index -1 by convention)
        if filter_index == -1:
            return False
        
        # Get current filter state
        filter_config = filter_configs[filter_index] if filter_index < len(filter_configs) else {}
        
        # Check if filter has a selected value
        has_value = self._filter_has_value(filter_config)
        
        # CRITICAL FIX: NEVER reload filters that already have values
        # This prevents the VALPARAISO disappearing issue
        if has_value:
            debug_msg = f"Filter {filter_index} has value {filter_config.get('value', 'UNKNOWN')} - PRESERVING (no reload)"
            print(debug_msg)
            try:
                with open("/tmp/mega_data_debug.log", "a") as f:
                    import time
                    f.write(f"[{time.strftime('%H:%M:%S')}] CASCADE_PRESERVE: {debug_msg}\n")
            except:
                pass
            return False
        
        # Only reload filters that have NO value
        # Rule 1: Reload filters BELOW the changed filter if they have no value
        if filter_index > changed_filter_index:
            debug_msg = f"Filter {filter_index} (below {changed_filter_index}) has no value - reloading"
            print(debug_msg)
            try:
                with open("/tmp/mega_data_debug.log", "a") as f:
                    import time
                    f.write(f"[{time.strftime('%H:%M:%S')}] CASCADE_RELOAD_BELOW: {debug_msg}\n")
            except:
                pass
            return True
        
        # Rule 2: Reload filters ABOVE the changed filter if they have no value
        if filter_index < changed_filter_index:
            debug_msg = f"Filter {filter_index} (above {changed_filter_index}) has no value - reloading"
            print(debug_msg)
            try:
                with open("/tmp/mega_data_debug.log", "a") as f:
                    import time
                    f.write(f"[{time.strftime('%H:%M:%S')}] CASCADE_RELOAD_ABOVE: {debug_msg}\n")
            except:
                pass
            return True
        
        return False
    
    def _filter_has_value(self, filter_config: Dict) -> bool:
        """Check if a filter has a selected value."""
        value = filter_config.get('value')
        operator = filter_config.get('operator')
        
        if value is None:
            return False
        
        if operator == "between":
            if isinstance(value, (list, tuple)) and len(value) == 2:
                return (value[0] is not None and value[1] is not None and 
                       str(value[0]).strip() != "" and str(value[1]).strip() != "")
        elif operator in ["is_one_of", "is_not_one_of"]:
            if isinstance(value, (list, tuple)):
                return len(value) > 0 and any(str(v).strip() != "" for v in value)
        else:
            return str(value).strip() != ""
        
        return False
    
    def get_filters_to_reload(self, changed_filter_index: int, 
                            filter_configs: List[Dict], 
                            bairros_changed: bool = False) -> Set[int]:
        """
        Get the set of filter indices that need to be reloaded.
        
        Args:
            changed_filter_index: Index of the filter that changed
            filter_configs: List of all filter configurations
            bairros_changed: Whether the bairros filter changed
            
        Returns:
            Set of filter indices to reload
        """
        filters_to_reload = set()
        
        # If bairros changed, reload all filters that don't have values
        if bairros_changed:
            for i, filter_config in enumerate(filter_configs):
                if not self._filter_has_value(filter_config):
                    filters_to_reload.add(i)
            return filters_to_reload
        
        # Apply cascade rules for regular filter changes
        for i, filter_config in enumerate(filter_configs):
            if self.should_reload_filter(i, changed_filter_index, filter_configs):
                filters_to_reload.add(i)
        
        return filters_to_reload
    
    def get_cascaded_filter_data(self, filter_index: int, filter_configs: List[Dict], 
                               selected_bairros: List[str]) -> Dict:
        """
        Get the filtered data context for loading column values.
        This includes all active filters EXCEPT the current filter.
        
        Args:
            filter_index: Index of filter to get values for
            filter_configs: List of all filter configurations
            selected_bairros: List of selected bairros
            
        Returns:
            Dictionary with filtering context
        """
        # Include bairros filter
        filtered_data = {
            'bairros': selected_bairros,
            'filters': []
        }
        
        # Include all other active filters except the current one
        for i, filter_config in enumerate(filter_configs):
            if i != filter_index and self._filter_has_value(filter_config):
                filtered_data['filters'].append(filter_config)
        
        return filtered_data
    
    def load_filter_values_smart(self, filter_index: int, column_name: str,
                               filter_configs: List[Dict], selected_bairros: List[str],
                               force_reload: bool = False) -> List[str]:
        """
        Load filter values with smart caching and cascading.
        
        Args:
            filter_index: Index of the filter
            column_name: Name of the column
            filter_configs: List of all filter configurations
            selected_bairros: List of selected bairros
            force_reload: Force reload ignoring cache
            
        Returns:
            List of unique values for the column
        """
        self._init_session_state()  # Ensure session state is initialized
        
        # Create cache key
        other_filters = [f for i, f in enumerate(filter_configs) 
                        if i != filter_index and self._filter_has_value(f)]
        cache_key = self._create_cache_key(column_name, selected_bairros, other_filters)
        
        # Check cache first
        state = st.session_state[self.session_key]
        
        if not force_reload and cache_key in state['value_cache']:
            cached_data = state['value_cache'][cache_key]
            # Check if cache is still valid (1 hour TTL)
            if time.time() - cached_data['timestamp'] < 3600:
                return cached_data['values']
        
        # Load values with cascaded filtering
        filtered_data = self.get_cascaded_filter_data(filter_index, filter_configs, selected_bairros)
        values = get_column_values(column_name, filtered_data)
        
        # Cache the result
        state['value_cache'][cache_key] = {
            'values': values,
            'timestamp': time.time()
        }
        
        print(f"✅ Loaded {len(values)} values for {column_name} (filter {filter_index})")
        return values
    
    def _create_cache_key(self, column_name: str, bairros: List[str], 
                         other_filters: List[Dict]) -> str:
        """Create a cache key for the given parameters."""
        bairros_key = "|".join(sorted(bairros))
        
        filters_key = ""
        if other_filters:
            filter_parts = []
            for f in other_filters:
                col = f.get('column', '')
                op = f.get('operator', '')
                val = str(f.get('value', ''))
                filter_parts.append(f"{col}:{op}:{val}")
            filters_key = "|".join(sorted(filter_parts))
        
        return f"{column_name}::{bairros_key}::{filters_key}"
    
    def mark_filter_changed(self, filter_index: int, filter_configs: List[Dict]):
        """Mark that a filter has changed and update timestamps."""
        state = st.session_state[self.session_key]
        
        # Update timestamp for the changed filter
        state['last_update_times'][filter_index] = time.time()
        
        # Mark dependent filters for cache invalidation
        filters_to_reload = self.get_filters_to_reload(filter_index, filter_configs)
        
        # Clear cache for affected filters
        for affected_filter in filters_to_reload:
            # Remove cache entries that might be affected
            keys_to_remove = []
            for cache_key in state['value_cache']:
                if str(affected_filter) in cache_key or filter_index in cache_key:
                    keys_to_remove.append(cache_key)
            
            for key in keys_to_remove:
                del state['value_cache'][key]
        
        print(f"✅ Marked filter {filter_index} as changed, cleared cache for {len(filters_to_reload)} dependent filters")
    
    def mark_bairros_changed(self, filter_configs: List[Dict]):
        """Mark that bairros filter has changed."""
        self._init_session_state()  # Ensure session state is initialized
        state = st.session_state[self.session_key]
        
        # Clear all cached values since bairros affect everything
        state['value_cache'].clear()
        state['last_update_times']['bairros'] = time.time()
        
        print("✅ Bairros changed - cleared all filter value cache")
    
    def get_cache_stats(self) -> Dict:
        """Get statistics about the current cache state."""
        state = st.session_state[self.session_key]
        
        return {
            'cached_entries': len(state['value_cache']),
            'last_updates': state['last_update_times'],
            'cache_size_mb': len(str(state['value_cache'])) / (1024 * 1024)
        }
    
    def clear_cache(self):
        """Clear all cached data."""
        state = st.session_state[self.session_key]
        state['value_cache'].clear()
        state['last_update_times'].clear()
        print("✅ Cleared smart filter cascade cache")


# Global instance
smart_cascade = SmartFilterCascade()


def should_reload_filter(filter_index: int, changed_filter_index: int, 
                        filter_configs: List[Dict]) -> bool:
    """Check if a filter should be reloaded."""
    return smart_cascade.should_reload_filter(filter_index, changed_filter_index, filter_configs)


def get_filters_to_reload(changed_filter_index: int, filter_configs: List[Dict], 
                         bairros_changed: bool = False) -> Set[int]:
    """Get filters that need to be reloaded."""
    return smart_cascade.get_filters_to_reload(changed_filter_index, filter_configs, bairros_changed)


def load_filter_values_smart(filter_index: int, column_name: str, filter_configs: List[Dict], 
                           selected_bairros: List[str], force_reload: bool = False) -> List[str]:
    """Load filter values with smart caching."""
    return smart_cascade.load_filter_values_smart(
        filter_index, column_name, filter_configs, selected_bairros, force_reload
    )


def mark_filter_changed(filter_index: int, filter_configs: List[Dict]):
    """Mark that a filter has changed."""
    smart_cascade.mark_filter_changed(filter_index, filter_configs)


def mark_bairros_changed(filter_configs: List[Dict]):
    """Mark that bairros filter has changed."""
    smart_cascade.mark_bairros_changed(filter_configs)


def get_cascade_cache_stats() -> Dict:
    """Get cache statistics."""
    return smart_cascade.get_cache_stats()


def clear_cascade_cache():
    """Clear cascade cache."""
    smart_cascade.clear_cache()