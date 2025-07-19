"""
Unified Property Loader Service

This service provides memory-aware property loading strategies to prevent
memory overflow crashes on Streamlit Community Cloud (2.7GB RAM limit).

Key features:
- Memory-safe loading with automatic fallbacks
- Session-wide data sharing across pages
- Progressive loading for large datasets
- Circuit breakers for memory protection
"""

import os
import pandas as pd
import streamlit as st
import psutil
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime

# Import existing optimized functions
from services.mega_data_set_loader import (
    load_bairros_optimized,
    list_bairros_optimized,
    get_available_bairros,
    load_mega_data_set,
    get_properties_for_phone,
    find_properties_by_documento
)

# Memory thresholds (in MB)
MEMORY_WARNING_THRESHOLD = 2000  # 2GB
MEMORY_CRITICAL_THRESHOLD = 2500  # 2.5GB
MEMORY_LIMIT = 2700  # Streamlit Community Cloud limit

class MemoryMonitor:
    """Monitor memory usage and provide warnings/limits."""
    
    @staticmethod
    def get_current_memory_mb() -> float:
        """Get current memory usage in MB."""
        try:
            current_process = psutil.Process(os.getpid())
            return current_process.memory_info().rss / 1e6
        except Exception:
            return 0.0
    
    @staticmethod
    def check_memory_safety(operation_name: str = "operation") -> bool:
        """Check if it's safe to perform a memory-intensive operation."""
        current_memory = MemoryMonitor.get_current_memory_mb()
        
        if current_memory > MEMORY_CRITICAL_THRESHOLD:
            st.error(f"⚠️ Memory usage too high ({current_memory:.0f} MB). Cannot perform {operation_name}.")
            return False
        elif current_memory > MEMORY_WARNING_THRESHOLD:
            st.warning(f"⚠️ High memory usage ({current_memory:.0f} MB). Operation may be slower.")
        
        return True
    
    @staticmethod
    def display_memory_widget():
        """Display memory usage widget in sidebar."""
        current_memory = MemoryMonitor.get_current_memory_mb()
        
        if current_memory > MEMORY_CRITICAL_THRESHOLD:
            st.sidebar.error(f"🚨 Critical memory: {current_memory:.0f} MB")
        elif current_memory > MEMORY_WARNING_THRESHOLD:
            st.sidebar.warning(f"⚠️ High memory: {current_memory:.0f} MB")
        else:
            st.sidebar.success(f"✅ Memory: {current_memory:.0f} MB")


class UnifiedPropertyLoader:
    """Unified property loading service with memory management."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._init_session_state()
    
    def _init_session_state(self):
        """Initialize session state for cross-page data sharing."""
        if 'property_cache' not in st.session_state:
            st.session_state.property_cache = {}
        
        if 'loaded_bairros' not in st.session_state:
            st.session_state.loaded_bairros = set()
        
        if 'available_bairros' not in st.session_state:
            st.session_state.available_bairros = None
    
    def get_available_bairros(self) -> List[str]:
        """Get available bairros with session caching."""
        if st.session_state.available_bairros is None:
            if not MemoryMonitor.check_memory_safety("loading bairros list"):
                return []
            
            try:
                st.session_state.available_bairros = list_bairros_optimized()
            except Exception as e:
                self.logger.error(f"Error loading bairros: {e}")
                st.session_state.available_bairros = []
        
        return st.session_state.available_bairros or []
    
    def load_properties_for_assignment(self, 
                                     preselected_bairros: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Load properties for assignment dialog with memory safety.
        
        This replaces the problematic load_mega_data_set() call in property assignment.
        """
        if not MemoryMonitor.check_memory_safety("property assignment"):
            return pd.DataFrame()
        
        # Start with a reasonable default of popular bairros if none specified
        if not preselected_bairros:
            available_bairros = self.get_available_bairros()
            # Select first 5 bairros as default to limit memory usage
            preselected_bairros = available_bairros[:5] if available_bairros else []
        
        # Load data for selected bairros
        return self.load_bairros_safe(preselected_bairros)
    
    def load_bairros_safe(self, bairros: List[str]) -> pd.DataFrame:
        """
        Safely load data for selected bairros with memory monitoring.
        """
        if not bairros:
            return pd.DataFrame()
        
        if not MemoryMonitor.check_memory_safety(f"loading {len(bairros)} bairros"):
            return pd.DataFrame()
        
        # Check cache first
        cache_key = "_".join(sorted(bairros))
        if cache_key in st.session_state.property_cache:
            return st.session_state.property_cache[cache_key]
        
        try:
            # Use the existing optimized function
            df = load_bairros_optimized(bairros)
            
            # Cache the result (but limit cache size to prevent memory issues)
            if len(st.session_state.property_cache) > 5:
                # Remove oldest cache entry
                oldest_key = next(iter(st.session_state.property_cache))
                del st.session_state.property_cache[oldest_key]
            
            st.session_state.property_cache[cache_key] = df
            st.session_state.loaded_bairros.update(bairros)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading bairros {bairros}: {e}")
            st.error(f"Error loading property data: {e}")
            return pd.DataFrame()
    
    def expand_bairros_selection(self, current_bairros: List[str], 
                               additional_bairros: List[str]) -> pd.DataFrame:
        """
        Progressively expand the loaded bairros set.
        """
        all_bairros = list(set(current_bairros + additional_bairros))
        return self.load_bairros_safe(all_bairros)
    
    def get_properties_for_phone_safe(self, phone: str) -> List[Dict]:
        """
        Safely get properties for a phone number with memory checks.
        """
        if not MemoryMonitor.check_memory_safety("phone property lookup"):
            return []
        
        try:
            return get_properties_for_phone(phone)
        except Exception as e:
            self.logger.error(f"Error getting properties for phone {phone}: {e}")
            return []
    
    def find_properties_by_documento_safe(self, documento: str, 
                                        mega_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Safely find properties by document with memory checks.
        """
        if not MemoryMonitor.check_memory_safety("document property lookup"):
            return pd.DataFrame()
        
        try:
            return find_properties_by_documento(documento, mega_df)
        except Exception as e:
            self.logger.error(f"Error finding properties for document {documento}: {e}")
            return pd.DataFrame()
    
    def clear_cache(self):
        """Clear the property cache to free memory."""
        st.session_state.property_cache.clear()
        st.session_state.loaded_bairros.clear()
        st.session_state.available_bairros = None
        
        # Also clear Streamlit cache
        if hasattr(st, 'cache_data'):
            st.cache_data.clear()
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics for debugging."""
        return {
            'cached_datasets': len(st.session_state.property_cache),
            'loaded_bairros': len(st.session_state.loaded_bairros),
            'available_bairros': len(st.session_state.available_bairros or []),
            'memory_mb': MemoryMonitor.get_current_memory_mb()
        }


# Global instance for easy access
property_loader = UnifiedPropertyLoader()


def render_property_assignment_dialog(conversation_id: str) -> Optional[str]:
    """
    Render the property assignment dialog with memory-safe loading.
    
    This replaces the problematic full dataset loading in Processor.py.
    
    Returns:
        str: Selected property ID if any, None otherwise
    """
    if not MemoryMonitor.check_memory_safety("property assignment dialog"):
        st.error("Cannot open property assignment - memory usage too high")
        return None
    
    st.markdown("### 🏢 Atribuir Propriedade")
    st.markdown("---")
    
    # Display memory usage
    MemoryMonitor.display_memory_widget()
    
    # Step 1: Select bairros to load data for
    available_bairros = property_loader.get_available_bairros()
    
    if not available_bairros:
        st.error("Erro ao carregar lista de bairros.")
        return None
    
    # Default to first 5 bairros for initial load
    default_bairros = available_bairros[:5]
    
    selected_bairros = st.multiselect(
        "Selecione os bairros para buscar propriedades:",
        options=available_bairros,
        default=default_bairros,
        help="Selecione poucos bairros inicialmente para otimizar a performance. Você pode adicionar mais conforme necessário."
    )
    
    if not selected_bairros:
        st.info("Selecione pelo menos um bairro para continuar.")
        return None
    
    # Step 2: Load data for selected bairros
    with st.spinner(f"Carregando propriedades para {len(selected_bairros)} bairro(s)..."):
        properties_df = property_loader.load_bairros_safe(selected_bairros)
    
    if properties_df.empty:
        st.warning("Nenhuma propriedade encontrada para os bairros selecionados.")
        return None
    
    st.success(f"Carregadas {len(properties_df):,} propriedades de {len(selected_bairros)} bairro(s)")
    
    # Step 3: Property selection interface
    if len(properties_df) > 1000:
        st.warning(f"Muitas propriedades ({len(properties_df):,}). Use filtros adicionais:")
        
        # Add filters to narrow down selection
        if 'TIPO' in properties_df.columns:
            property_types = st.multiselect(
                "Filtrar por tipo:",
                options=properties_df['TIPO'].dropna().unique()
            )
            if property_types:
                properties_df = properties_df[properties_df['TIPO'].isin(property_types)]
        
        if 'AREA_TERRENO' in properties_df.columns:
            min_area, max_area = st.select_slider(
                "Área do terreno (m²):",
                options=sorted(properties_df['AREA_TERRENO'].dropna().unique()),
                value=(
                    properties_df['AREA_TERRENO'].min(),
                    properties_df['AREA_TERRENO'].max()
                )
            )
            properties_df = properties_df[
                (properties_df['AREA_TERRENO'] >= min_area) & 
                (properties_df['AREA_TERRENO'] <= max_area)
            ]
    
    # Display properties for selection
    if len(properties_df) > 100:
        st.info(f"Mostrando primeiras 100 de {len(properties_df)} propriedades. Use filtros para refinar.")
        display_df = properties_df.head(100)
    else:
        display_df = properties_df
    
    # Create selection interface
    if not display_df.empty:
        # Display key columns for property selection
        display_cols = ['INSCRICAO_IMOBILIARIA', 'BAIRRO', 'LOGRADOURO']
        if 'TIPO' in display_df.columns:
            display_cols.append('TIPO')
        if 'AREA_TERRENO' in display_df.columns:
            display_cols.append('AREA_TERRENO')
        
        available_cols = [col for col in display_cols if col in display_df.columns]
        
        # Show data table
        st.dataframe(display_df[available_cols])
        
        # Property selection
        property_ids = display_df.get('INSCRICAO_IMOBILIARIA', display_df.index).tolist()
        selected_property = st.selectbox(
            "Selecione uma propriedade:",
            options=[""] + property_ids,
            format_func=lambda x: f"Propriedade {x}" if x else "Selecione..."
        )
        
        if selected_property:
            # Show property details
            property_row = display_df[display_df.get('INSCRICAO_IMOBILIARIA', display_df.index) == selected_property].iloc[0]
            
            st.markdown("**Detalhes da propriedade selecionada:**")
            for col in available_cols:
                if col in property_row:
                    st.write(f"- **{col}**: {property_row[col]}")
            
            # Confirm selection
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Confirmar Atribuição", type="primary"):
                    return selected_property
            with col2:
                if st.button("❌ Cancelar"):
                    return None
    
    return None