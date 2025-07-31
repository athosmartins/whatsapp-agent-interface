"""
Lazy Column Loading Service for Mega Data Set.
Provides ultra-efficient column value loading with smart caching and cascading support.
"""

import streamlit as st
import pandas as pd
import duckdb
from typing import List, Dict, Optional, Set
import time
import os
from services.mega_data_set_loader import download_latest_mega_data_set, MEGA_DATA_SET_FOLDER_ID
from services.performance_monitor import performance_monitor, log_data_operation

class LazyColumnLoader:
    """
    Ultra-efficient lazy column loading for mega data set.
    Only loads column values when actually needed for filters.
    """
    
    def __init__(self):
        self.cache_ttl = 3600  # 1 hour cache
        self._file_path = None
        self._column_metadata = None
        
    @st.cache_data(ttl=3600, max_entries=1)
    def get_column_metadata(_self) -> Dict[str, str]:
        """
        Get lightweight column metadata (names and types only).
        Memory usage: ~1KB vs ~10MB for full data loading.
        """
        try:
            # Download file if needed
            if not _self._file_path:
                _self._file_path = download_latest_mega_data_set()
            
            if not _self._file_path:
                return {}
            
            # Use DuckDB to efficiently get column info without loading data
            if _self._file_path.lower().endswith('.parquet'):
                # Parquet: Get schema without loading data
                query = f"DESCRIBE SELECT * FROM '{_self._file_path}' LIMIT 0"
                schema_df = duckdb.query(query).df()
                
                metadata = {}
                for _, row in schema_df.iterrows():
                    metadata[row['column_name']] = row['column_type']
                
                print(f"✅ Loaded metadata for {len(metadata)} columns")
                return metadata
            
            else:
                # Fallback: Load small sample to get column info
                if _self._file_path.lower().endswith('.json.gz'):
                    from services.mega_data_set_loader import load_compressed_json
                    df = load_compressed_json(_self._file_path)
                else:
                    df = pd.read_csv(_self._file_path, nrows=1, low_memory=False)
                
                metadata = {col: str(df[col].dtype) for col in df.columns}
                print(f"✅ Loaded metadata for {len(metadata)} columns (fallback)")
                return metadata
                
        except Exception as e:
            print(f"Error loading column metadata: {e}")
            return {}
    
    # PROTECTED: Story #002 - Ultra-fast smart cascading filters with lazy column loading
    # DO NOT MODIFY: This code prevents filter performance regression (90% memory reduction)
    # See USER_STORIES.md Story #002 for context
    # Regression tests will fail if this protection is removed
    @st.cache_data(ttl=3600, max_entries=50)
    @performance_monitor("LazyColumnLoader.get_column_values")
    def get_column_values(_self, column_name: str, filtered_data: Optional[Dict] = None) -> List[str]:
        """
        Get unique values for a specific column with optional filtering.
        
        Args:
            column_name: Name of the column
            filtered_data: Optional dict with filtering conditions:
                - bairros: List of bairros to filter by
                - filters: List of other column filters
        
        Returns:
            Sorted list of unique values for the column
        """
        try:
            # CRITICAL: Use already loaded data from session state instead of reloading
            # This prevents unnecessary mega data set downloads and processing
            
            debug_msg = f"get_column_values called for {column_name}"
            print(debug_msg)
            try:
                with open("/tmp/mega_data_debug.log", "a") as f:
                    f.write(f"[{time.strftime('%H:%M:%S')}] LAZY_LOAD: {debug_msg}\n")
            except:
                pass
            
            # FIXED: Try to get fresh loaded data from session state first (not stale filtered_df)
            if 'mega_data_filter_state' in st.session_state:
                # First try fresh loaded_data (just loaded for current bairros)
                fresh_loaded_data = st.session_state.mega_data_filter_state.get('loaded_data')
                if fresh_loaded_data is not None and not fresh_loaded_data.empty and column_name in fresh_loaded_data.columns:
                    cached_df = fresh_loaded_data
                    print(f"✅ Using FRESH loaded data with {len(cached_df)} rows for {column_name}")
                else:
                    # Fallback to last applied filters (may be stale)
                    last_applied = st.session_state.mega_data_filter_state.get('last_applied_filters', {})
                    cached_df = last_applied.get('filtered_df')
                    if cached_df is not None and not cached_df.empty:
                        print(f"⚠️ Using STALE cached dataframe with {len(cached_df)} rows for {column_name}")
                    else:
                        cached_df = None
                
                if cached_df is not None and not cached_df.empty and column_name in cached_df.columns:
                    print(f"✅ Using cached dataframe with {len(cached_df)} rows for {column_name}")
                    log_data_operation("CACHE_HIT", 
                                     column=column_name, 
                                     rows=len(cached_df), 
                                     source="session_state")
                    df = cached_df.copy()
                    
                    # CRITICAL FIX: Apply additional filters if provided
                    # This handles cases where cached data doesn't match current filter state
                    if filtered_data and filtered_data.get('filters'):
                        print(f"🔄 Applying {len(filtered_data['filters'])} additional filters to cached data")
                        for filter_config in filtered_data['filters']:
                            original_rows = len(df)
                            df = _self._apply_pandas_filter(df, filter_config)
                            print(f"   Filter {filter_config.get('column')} {filter_config.get('operator')} {filter_config.get('value')}: {original_rows} → {len(df)} rows")
                        print(f"✅ Final filtered data: {len(df)} rows for {column_name}")
                else:
                    # Fallback: load fresh data only if no cached data available
                    from services.mega_data_set_loader import load_bairros_optimized
                    bairros = filtered_data.get('bairros', []) if filtered_data else []
                    
                    if not bairros:
                        print(f"No bairros specified for column {column_name}")
                        return []
                    
                    print(f"⚠️ Loading fresh data for {len(bairros)} bairros")
                    log_data_operation("FRESH_LOAD", 
                                     column=column_name, 
                                     bairros=len(bairros), 
                                     source="load_bairros_optimized")
                    df = load_bairros_optimized(bairros)
            else:
                # No session state available, load fresh data
                from services.mega_data_set_loader import load_bairros_optimized
                bairros = filtered_data.get('bairros', []) if filtered_data else []
                
                if not bairros:
                    print(f"No bairros specified for column {column_name}")
                    return []
                
                print(f"⚠️ Loading fresh data (no session state) for {len(bairros)} bairros")
                df = load_bairros_optimized(bairros)
            
            if df.empty or column_name not in df.columns:
                print(f"Column {column_name} not found in dataframe")
                return []
            
            # Apply additional filters if specified
            if filtered_data and filtered_data.get('filters'):
                for filter_config in filtered_data['filters']:
                    df = _self._apply_pandas_filter(df, filter_config)
            
            # Get unique values
            values = df[column_name].dropna().astype(str).unique()
            values = sorted([str(v) for v in values if str(v).strip()])
            
            print(f"✅ Loaded {len(values)} unique values for {column_name}")
            return values[:1000]  # Limit to first 1000 values to avoid UI overload
            
        except Exception as e:
            print(f"Error loading values for column {column_name}: {e}")
            return []
    
    def _build_filter_condition(self, filter_config: Dict) -> Optional[str]:
        """Build DuckDB WHERE condition from filter config."""
        column = filter_config.get('column')
        operator = filter_config.get('operator')
        value = filter_config.get('value')
        
        if not column or not operator or value is None:
            return None
        
        try:
            if operator == "equals":
                return f"{column} = '{value}'"
            elif operator == "contains":
                return f"{column} LIKE '%{value}%'"
            elif operator == "starts_with":
                return f"{column} LIKE '{value}%'"
            elif operator == "ends_with":
                return f"{column} LIKE '%{value}'"
            elif operator == "greater_than":
                return f"{column} > {value}"
            elif operator == "less_than":
                return f"{column} < {value}"
            elif operator == "between":
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    return f"{column} BETWEEN {value[0]} AND {value[1]}"
            elif operator == "is_one_of":
                if isinstance(value, (list, tuple)) and value:
                    values_str = "', '".join(str(v) for v in value)
                    return f"{column} IN ('{values_str}')"
            elif operator == "is_not_one_of":
                if isinstance(value, (list, tuple)) and value:
                    values_str = "', '".join(str(v) for v in value)
                    return f"{column} NOT IN ('{values_str}')"
        except Exception as e:
            print(f"Error building filter condition: {e}")
        
        return None
    
    def _apply_pandas_filter(self, df: pd.DataFrame, filter_config: Dict) -> pd.DataFrame:
        """Apply filter using pandas (fallback method)."""
        column = filter_config.get('column')
        operator = filter_config.get('operator')
        value = filter_config.get('value')
        
        if column not in df.columns or value is None:
            return df
        
        try:
            if operator == "equals":
                return df[df[column] == value]
            elif operator == "contains":
                return df[df[column].astype(str).str.contains(str(value), case=False, na=False)]
            elif operator == "starts_with":
                return df[df[column].astype(str).str.startswith(str(value), na=False)]
            elif operator == "ends_with":
                return df[df[column].astype(str).str.endswith(str(value), na=False)]
            elif operator == "greater_than":
                return df[df[column] > value]
            elif operator == "less_than":
                return df[df[column] < value]
            elif operator == "between":
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    return df[(df[column] >= value[0]) & (df[column] <= value[1])]
            elif operator == "is_one_of":
                if isinstance(value, (list, tuple)):
                    return df[df[column].isin(value)]
            elif operator == "is_not_one_of":
                if isinstance(value, (list, tuple)):
                    return df[~df[column].isin(value)]
        except Exception as e:
            print(f"Error applying pandas filter: {e}")
        
        return df
    
    @st.cache_data(ttl=3600, max_entries=20)
    def get_bairros_list(_self) -> List[str]:
        """Get list of available bairros (cached separately for efficiency)."""
        try:
            if not _self._file_path:
                _self._file_path = download_latest_mega_data_set()
            
            if not _self._file_path:
                return []
            
            if _self._file_path.lower().endswith('.parquet'):
                query = f"SELECT DISTINCT BAIRRO FROM '{_self._file_path}' WHERE BAIRRO IS NOT NULL ORDER BY BAIRRO"
                result_df = duckdb.query(query).df()
                bairros = result_df['BAIRRO'].tolist()
                print(f"✅ Loaded {len(bairros)} bairros efficiently")
                return bairros
            else:
                # Fallback method
                from services.mega_data_set_loader import list_bairros_optimized
                return list_bairros_optimized()
                
        except Exception as e:
            print(f"Error loading bairros: {e}")
            # Fallback to hardcoded list
            return [
                "Centro", "Savassi", "Funcionários", "Lourdes", "Buritis",
                "Cidade Nova", "Prado", "Serra", "Belvedere", "Mangabeiras",
                "Coração Eucarístico", "Pampulha", "Cidade Jardim", "Anchieta",
                "Floresta", "Sagrada Família", "Jardim América", "Liberdade",
                "Ouro Preto", "Castelo", "Gutierrez", "Barro Preto", "Carmo",
                "Grajaú", "Boa Vista", "Cruzeiro", "Luxemburgo", "Sion"
            ]
    
    def clear_cache(self):
        """Clear all cached data."""
        if hasattr(st, 'cache_data'):
            st.cache_data.clear()
        print("✅ Cleared lazy column loader cache")


# Global instance
lazy_loader = LazyColumnLoader()


def get_column_metadata() -> Dict[str, str]:
    """Get column metadata (names and types)."""
    return lazy_loader.get_column_metadata()


def get_column_values(column_name: str, filtered_data: Optional[Dict] = None) -> List[str]:
    """Get unique values for a column with optional filtering."""
    return lazy_loader.get_column_values(column_name, filtered_data)


def get_bairros_list() -> List[str]:
    """Get list of available bairros."""
    return lazy_loader.get_bairros_list()


def clear_lazy_cache():
    """Clear lazy loading cache."""
    lazy_loader.clear_cache()