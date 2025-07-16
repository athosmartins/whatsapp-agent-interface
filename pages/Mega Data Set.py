"""Mega Data Set page with bairro filter and map visualization."""

import streamlit as st
import pandas as pd
from datetime import datetime
import time
import os
import gc  # Garbage collection for memory management
import traceback
import psutil  # For memory monitoring
import logging
from pathlib import Path

try:
    from services.mega_data_set_loader import (
        load_mega_data_set,
        get_property_summary_stats,
        get_available_bairros,
        get_data_by_bairros,
        list_bairros_optimized,
        load_bairros_optimized,
    )
    from utils.property_map import render_property_map_streamlit
    from services.hex_api import render_hex_dropdown_interface
except ImportError as e:
    st.error(f"❌ Error importing modules: {e}")
    st.info(
        "Please ensure all dependencies are installed: pip install -r requirements.txt"
    )
    st.stop()

# Page config
st.set_page_config(page_title="Mega Data Set", layout="wide")
st.title("🏢 Mega Data Set")

# Debug mode check
DEBUG = st.sidebar.checkbox("Debug Mode", value=False)

# Production environment detection
IS_PRODUCTION = os.getenv("STREAMLIT_SERVER_HEADLESS") == "true"

# Add live RAM counter for debugging
import os
current_process = psutil.Process(os.getpid())
ram_mb = current_process.memory_info().rss / 1e6
st.sidebar.write(f"🖥️ RAM: {ram_mb:.0f} MB")
if ram_mb > 2000:
    st.sidebar.error(f"⚠️ High RAM usage: {ram_mb:.0f} MB")

# Enhanced debugging functions
def log_system_info():
    """Log comprehensive system information for debugging."""
    try:
        # Memory info
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        info = {
            "timestamp": datetime.now().isoformat(),
            "environment": "PRODUCTION" if IS_PRODUCTION else "DEVELOPMENT",
            "memory_total_mb": round(memory.total / (1024**2), 1),
            "memory_available_mb": round(memory.available / (1024**2), 1),
            "memory_percent_used": memory.percent,
            "disk_total_gb": round(disk.total / (1024**3), 1),
            "disk_free_gb": round(disk.free / (1024**3), 1),
            "disk_percent_used": round((disk.used / disk.total) * 100, 1),
            "python_version": f"{os.sys.version_info.major}.{os.sys.version_info.minor}.{os.sys.version_info.micro}",
        }
        
        if DEBUG or IS_PRODUCTION:
            st.sidebar.write("🖥️ **System Info:**")
            for key, value in info.items():
                if "memory" in key or "disk" in key:
                    st.sidebar.write(f"  {key}: {value}")
        
        return info
    except Exception as e:
        error_msg = f"Error logging system info: {e}"
        if DEBUG:
            st.sidebar.error(error_msg)
        print(error_msg)
        return {"error": str(e)}

def log_error_with_context(error, context="", show_user=True):
    """Log errors with full context for debugging."""
    error_info = {
        "timestamp": datetime.now().isoformat(),
        "environment": "PRODUCTION" if IS_PRODUCTION else "DEVELOPMENT",
        "context": context,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": traceback.format_exc(),
    }
    
    # Always log to console
    print(f"\n=== ERROR LOG ===")
    print(f"Time: {error_info['timestamp']}")
    print(f"Environment: {error_info['environment']}")
    print(f"Context: {error_info['context']}")
    print(f"Error: {error_info['error_type']}: {error_info['error_message']}")
    if DEBUG:
        print(f"Traceback:\n{error_info['traceback']}")
    print(f"=== END ERROR LOG ===\n")
    
    # Show to user if requested
    if show_user:
        st.error(f"❌ {context}: {error_info['error_type']}: {error_info['error_message']}")
        if DEBUG:
            st.code(error_info['traceback'])
    
    return error_info

def monitor_memory_usage(operation_name):
    """Monitor memory usage during operations."""
    try:
        memory = psutil.virtual_memory()
        usage_mb = round((memory.total - memory.available) / (1024**2), 1)
        percent = memory.percent
        
        log_msg = f"Memory during {operation_name}: {usage_mb}MB ({percent}%)"
        print(log_msg)
        
        if DEBUG:
            st.sidebar.write(f"📊 {log_msg}")
        
        # Warning if memory usage is high
        if percent > 80:
            warning_msg = f"⚠️ High memory usage during {operation_name}: {percent}%"
            print(warning_msg)
            if DEBUG:
                st.warning(warning_msg)
        
        return {"usage_mb": usage_mb, "percent": percent}
    except Exception as e:
        print(f"Error monitoring memory for {operation_name}: {e}")
        return {"error": str(e)}

# Log system info at startup
log_system_info()
monitor_memory_usage("page_startup")

# Initialize session state for filters
if "mega_data_filter_state" not in st.session_state:
    st.session_state.mega_data_filter_state = {
        "bairro_filter": [],
        "map_loaded": False,
        "dynamic_filters": [],  # List of dynamic filter configurations
        "filters_need_update": False,  # Track if filters changed and need map update
        "filter_groups": [  # Groups of filters with AND/OR logic
            {
                "id": 0,
                "name": "Grupo 1",
                "logic": "AND",  # AND or OR
                "filters": [],  # List of filter indices
            }
        ],
        "global_group_logic": "AND",  # Logic between groups
        "rerun_count": 0,  # Track reruns to prevent infinite loops
    }

# Add rerun protection
if "rerun_count" not in st.session_state.mega_data_filter_state:
    st.session_state.mega_data_filter_state["rerun_count"] = 0

# Reset rerun count every 10 runs to prevent accumulation
if st.session_state.mega_data_filter_state["rerun_count"] > 10:
    st.session_state.mega_data_filter_state["rerun_count"] = 0


def get_column_dtype_info(df, column):
    """Get information about column data type and suggest appropriate operators."""
    col_dtype = df[column].dtype
    unique_count = df[column].nunique()
    total_count = len(df)

    # Determine appropriate operators based on data type
    if pd.api.types.is_numeric_dtype(col_dtype):
        operators = [
            "equals",
            "greater_than",
            "less_than",
            "between",
            "is_one_of",
            "is_not_one_of",
        ]
    elif pd.api.types.is_datetime64_any_dtype(col_dtype):
        operators = [
            "equals",
            "after",
            "before",
            "between",
            "is_one_of",
            "is_not_one_of",
        ]
    else:  # String/object type
        if (
            unique_count < 50
        ):  # Few unique values - suggest categorical operators (is_one_of as default)
            operators = [
                "is_one_of",
                "is_not_one_of",
                "equals",
                "contains",
                "starts_with",
                "ends_with",
            ]
        else:  # Many unique values - suggest text operators
            operators = [
                "contains",
                "starts_with",
                "ends_with",
                "equals",
                "is_one_of",
                "is_not_one_of",
            ]

    return {
        "dtype": col_dtype,
        "unique_count": unique_count,
        "total_count": total_count,
        "suggested_operators": operators,
        "is_numeric": pd.api.types.is_numeric_dtype(col_dtype),
        "is_datetime": pd.api.types.is_datetime64_any_dtype(col_dtype),
        "is_categorical": unique_count < 50,
    }


def is_filter_active(filter_config):
    """Check if a filter configuration is active (has non-empty value)."""
    value = filter_config.get("value")
    operator = filter_config.get("operator")

    if value is None:
        return False

    if operator == "between":
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return (
                value[0] is not None
                and value[1] is not None
                and str(value[0]).strip() != ""
                and str(value[1]).strip() != ""
            )
        return False
    elif operator in ["is_one_of", "is_not_one_of"]:
        if isinstance(value, (list, tuple)):
            return len(value) > 0 and any(str(v).strip() != "" for v in value)
        return False
    else:
        # Single value operators
        return str(value).strip() != ""


def apply_dynamic_filter(df, filter_config):
    """Apply a single dynamic filter to the dataframe."""
    column = filter_config["column"]
    operator = filter_config["operator"]
    value = filter_config["value"]

    if column not in df.columns:
        return df

    # Check if filter is active
    if not is_filter_active(filter_config):
        return df

    try:
        if operator == "equals":
            return df[df[column] == value]
        elif operator == "contains":
            return df[
                df[column].astype(str).str.contains(str(value), case=False, na=False)
            ]
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
                # Filter out empty values
                valid_values = [v for v in value if str(v).strip() != ""]
                if valid_values:
                    return df[df[column].isin(valid_values)]
        elif operator == "is_not_one_of":
            if isinstance(value, (list, tuple)):
                # Filter out empty values
                valid_values = [v for v in value if str(v).strip() != ""]
                if valid_values:
                    return df[~df[column].isin(valid_values)]
        elif operator == "after":
            return df[df[column] > value]
        elif operator == "before":
            return df[df[column] < value]
    except Exception as e:
        st.error(f"Error applying filter on {column}: {e}")
        return df

    return df


def apply_dynamic_filters_with_logic(df, dynamic_filters, logic="AND"):
    """Apply multiple dynamic filters with AND/OR logic."""
    if not dynamic_filters:
        return df

    # Get all active filters
    active_filters = [
        filter_config
        for filter_config in dynamic_filters
        if (
            filter_config.get("column")
            and filter_config.get("operator")
            and is_filter_active(filter_config)
        )
    ]

    if not active_filters:
        return df

    if logic == "AND":
        # Apply filters sequentially (AND logic)
        result_df = df.copy()
        for filter_config in active_filters:
            try:
                result_df = apply_dynamic_filter(result_df, filter_config)
            except Exception:
                pass  # Skip problematic filters
        return result_df

    else:  # OR logic
        # Apply each filter to original df and combine results
        filtered_dfs = []
        for filter_config in active_filters:
            try:
                filtered_df = apply_dynamic_filter(df.copy(), filter_config)
                filtered_dfs.append(filtered_df)
            except Exception:
                pass  # Skip problematic filters

        if filtered_dfs:
            # Combine all filtered dataframes (union)
            combined_df = pd.concat(filtered_dfs, ignore_index=True)
            # Remove duplicates
            combined_df = combined_df.drop_duplicates()
            return combined_df
        else:
            return df


# SAVED COMPLEX GROUPING LOGIC - NOT IN PRODUCTION
# def apply_grouped_filters(df, filter_groups, global_logic="AND"):
#     """SAVED: Apply grouped filters with nested AND/OR logic."""
#     # [Complex grouping application logic saved but commented out]
#     pass


def get_cascaded_dataframe(bairro_filter, dynamic_filters, base_df, bairro_col):
    """Get dataframe with all currently active filters applied (for cascading)."""
    cascaded_df = base_df.copy()

    # Apply bairro filter
    if bairro_filter:
        cascaded_df = cascaded_df[cascaded_df[bairro_col].isin(bairro_filter)]

    # Apply active dynamic filters
    for filter_config in dynamic_filters:
        if (
            filter_config.get("column")
            and filter_config.get("operator")
            and is_filter_active(filter_config)
        ):
            try:
                cascaded_df = apply_dynamic_filter(cascaded_df, filter_config)
            except Exception:
                pass  # Skip problematic filters

    return cascaded_df


# SAVED COMPLEX GROUPING LOGIC - NOT IN PRODUCTION
# def render_dynamic_filters_with_groups(df):
#     """SAVED: Complex grouping interface with nested AND/OR logic."""
#     # [Complex grouping code saved but commented out]
#     pass


def render_dynamic_filters(df):
    """Render the simple dynamic filter interface with cascading options."""
    # Set default filter logic to AND (no UI selector)
    st.session_state.mega_data_filter_state["filter_logic"] = "AND"

    # Get current bairro filter state
    bairro_filter = st.session_state.mega_data_filter_state.get("bairro_filter", [])

    # Get available columns (exclude geometry and other technical columns) - sorted alphabetically
    excluded_columns = {"GEOMETRY", "geometry", "id", "ID", "_id", "index"}
    available_columns = sorted(
        [col for col in df.columns if col not in excluded_columns]
    )

    # Render existing filters
    filters_to_remove = []
    for i, filter_config in enumerate(
        st.session_state.mega_data_filter_state["dynamic_filters"]
    ):
        with st.expander(f"Filtro {i+1}", expanded=True):
            col1, col2, col3, col4 = st.columns([2.5, 2, 4, 0.5])

            with col1:
                # Column selection
                selected_column = st.selectbox(
                    "Coluna:",
                    options=[""] + available_columns,
                    index=(
                        0
                        if filter_config["column"] is None
                        else available_columns.index(filter_config["column"]) + 1
                    ),
                    key=f"filter_column_{i}",
                )
                filter_config["column"] = selected_column if selected_column else None

            with col2:
                # Operator selection based on column type
                if selected_column:
                    col_info = get_column_dtype_info(df, selected_column)
                    operators = col_info["suggested_operators"]
                    operator_labels = {
                        "equals": "Igual a",
                        "contains": "Contém",
                        "starts_with": "Começa com",
                        "ends_with": "Termina com",
                        "greater_than": "Maior que",
                        "less_than": "Menor que",
                        "between": "Entre",
                        "is_one_of": "É um de",
                        "is_not_one_of": "Não é um de",
                        "after": "Depois de",
                        "before": "Antes de",
                    }

                    operator_options = [operator_labels.get(op, op) for op in operators]

                    # Set default index - prefer "is_one_of" for categorical columns or specific columns
                    default_idx = 0

                    # Check if should default to "is_one_of"
                    should_default_to_is_one_of = (
                        col_info["is_categorical"]  # Categories with < 50 unique values
                        or "NOME"
                        in selected_column.upper()  # Name fields like "nome Logradouro"
                        or "TIPO" in selected_column.upper()  # Type fields
                        or "ZONA" in selected_column.upper()  # Zone fields
                    )

                    if should_default_to_is_one_of and "is_one_of" in operators:
                        default_idx = operators.index("is_one_of")

                    # If filter already has an operator, use that
                    if (
                        filter_config.get("operator")
                        and filter_config["operator"] in operators
                    ):
                        default_idx = operators.index(filter_config["operator"])

                    selected_operator_idx = st.selectbox(
                        "Operador:",
                        options=range(len(operators)),
                        format_func=lambda x: operator_options[x],
                        index=default_idx,
                        key=f"filter_operator_{i}",
                    )
                    filter_config["operator"] = operators[selected_operator_idx]
                else:
                    st.write("Selecione uma coluna")
                    filter_config["operator"] = None

            with col3:
                # Value input based on operator and column type
                if selected_column and filter_config["operator"]:
                    # Get cascaded dataframe EXCLUDING current filter for options
                    other_filters = [
                        f
                        for j, f in enumerate(
                            st.session_state.mega_data_filter_state["dynamic_filters"]
                        )
                        if j != i
                    ]
                    cascaded_df = get_cascaded_dataframe(
                        bairro_filter,
                        other_filters,
                        df,
                        next(col for col in df.columns if "BAIRRO" in col.upper()),
                    )

                    col_info = get_column_dtype_info(cascaded_df, selected_column)
                    operator = filter_config["operator"]

                    if operator == "between":
                        # Two inputs for range
                        col3a, col3b = st.columns(2)
                        current_range = filter_config.get("value", [None, None])
                        if (
                            not isinstance(current_range, list)
                            or len(current_range) != 2
                        ):
                            current_range = [None, None]

                        with col3a:
                            if col_info["is_numeric"]:
                                min_val = st.number_input(
                                    "Min:",
                                    value=(
                                        float(current_range[0])
                                        if current_range[0] is not None
                                        else 0.0
                                    ),
                                    key=f"filter_min_{i}_{selected_column}_{operator}",
                                )
                            else:
                                min_val = st.text_input(
                                    "De:",
                                    value=(
                                        str(current_range[0])
                                        if current_range[0] is not None
                                        else ""
                                    ),
                                    key=f"filter_min_{i}_{selected_column}_{operator}",
                                )
                        with col3b:
                            if col_info["is_numeric"]:
                                max_val = st.number_input(
                                    "Max:",
                                    value=(
                                        float(current_range[1])
                                        if current_range[1] is not None
                                        else 0.0
                                    ),
                                    key=f"filter_max_{i}_{selected_column}_{operator}",
                                )
                            else:
                                max_val = st.text_input(
                                    "Para:",
                                    value=(
                                        str(current_range[1])
                                        if current_range[1] is not None
                                        else ""
                                    ),
                                    key=f"filter_max_{i}_{selected_column}_{operator}",
                                )
                        filter_config["value"] = [min_val, max_val]

                    elif operator in ["is_one_of", "is_not_one_of"]:
                        # Multi-select for "is one of" and "is not one of" - use cascaded options
                        if col_info["is_categorical"]:
                            # Get unique values from cascaded dataframe
                            unique_values = (
                                cascaded_df[selected_column].dropna().unique()
                            )
                            unique_values = sorted([str(v) for v in unique_values])

                            # Get current selections, filtering out invalid ones
                            current_selections = filter_config.get("value", [])
                            if not isinstance(current_selections, list):
                                current_selections = []
                            # Convert to strings for comparison
                            current_selections = [str(v) for v in current_selections]
                            valid_selections = [
                                v for v in current_selections if v in unique_values
                            ]

                            # Use a stable key that doesn't change based on operator
                            stable_key = f"filter_multiselect_{i}_{selected_column}"
                            selected_values = st.multiselect(
                                "Valores:",
                                options=unique_values,
                                default=valid_selections,
                                key=stable_key,
                            )
                            filter_config["value"] = selected_values
                        else:
                            # Text input for manual entry
                            current_value = filter_config.get("value", [])
                            if isinstance(current_value, list):
                                current_text = ", ".join(str(v) for v in current_value)
                            else:
                                current_text = ""

                            # Use a stable key that doesn't change based on operator
                            stable_key = f"filter_text_multi_{i}_{selected_column}"
                            values_text = st.text_input(
                                "Valores (separados por vírgula):",
                                value=current_text,
                                key=stable_key,
                            )
                            filter_config["value"] = [
                                v.strip() for v in values_text.split(",") if v.strip()
                            ]

                    else:
                        # Single value input
                        current_value = filter_config.get("value", "")
                        if col_info["is_numeric"]:
                            filter_config["value"] = st.number_input(
                                "Valor:",
                                value=(
                                    float(current_value)
                                    if current_value
                                    and str(current_value)
                                    .replace(".", "")
                                    .replace("-", "")
                                    .isdigit()
                                    else 0.0
                                ),
                                key=f"filter_number_{i}_{selected_column}_{operator}",
                            )
                        elif col_info["is_datetime"]:
                            filter_config["value"] = st.date_input(
                                "Data:",
                                key=f"filter_date_{i}_{selected_column}_{operator}",
                            )
                        else:
                            filter_config["value"] = st.text_input(
                                "Valor:",
                                value=str(current_value) if current_value else "",
                                key=f"filter_text_{i}_{selected_column}_{operator}",
                            )
                else:
                    st.write("Configure coluna e operador")

            with col4:
                # Remove filter button
                if st.button("❌", key=f"remove_filter_{i}", help="Remover filtro"):
                    filters_to_remove.append(i)

    # Remove filters marked for removal
    for i in sorted(filters_to_remove, reverse=True):
        st.session_state.mega_data_filter_state["dynamic_filters"].pop(i)

    if filters_to_remove:
        st.rerun()

    # Add buttons row: "Adicionar Filtro" and "Atualizar e Carregar Mapa"
    col1, col2 = st.columns([1, 1], gap="small")

    with col1:
        if st.button("➕ Adicionar Filtro"):
            # Add empty filter configuration
            new_filter = {
                "id": len(st.session_state.mega_data_filter_state["dynamic_filters"]),
                "column": None,
                "operator": None,
                "value": None,
            }
            st.session_state.mega_data_filter_state["dynamic_filters"].append(
                new_filter
            )
            st.rerun()

    with col2:
        # Style the "Atualizar e Carregar Mapa" button with light green
        update_button_style = """
        <style>
        .update-button > button {
            width: 100% !important;
            background-color: #F8D7DA !important;
            color: #721C24 !important;
            border: 1px solid #F5C6CB !important;
            border-radius: 6px !important;
            padding: 0.5rem 1rem !important;
            font-size: 14px !important;
            font-weight: 500 !important;
            text-align: center !important;
        }
        .update-button > button:hover {
            background-color: #F5C6CB !important;
            border-color: #F1B0B7 !important;
        }
        </style>
        """
        st.markdown(update_button_style, unsafe_allow_html=True)
        st.markdown('<div class="update-button">', unsafe_allow_html=True)

        if st.button("🔄 Atualizar e Carregar Mapa"):
            st.session_state.mega_data_filter_state["filters_need_update"] = True
            st.session_state.mega_data_filter_state["map_loaded"] = True
            st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)

    return st.session_state.mega_data_filter_state["dynamic_filters"]


# load_mega_data() function removed - now using optimized bairro-based loading


# get_summary_stats() function removed - now using direct stats from loaded bairro data to avoid full dataset loading


# MEMORY OPTIMIZATION: Bairro-based lazy loading (95% memory reduction!)
mega_df = None

# Step 1: Load available bairros first (lightweight operation)

try:
    monitor_memory_usage("before_bairros_loading")
    with st.spinner("Carregando lista de bairros..."):
        available_bairros = list_bairros_optimized()
    monitor_memory_usage("after_bairros_loading")
    
    if available_bairros:
        is_fallback = len(available_bairros) <= 28  # Likely using hardcoded fallback
        if is_fallback:
            st.warning(f"⚠️ Usando {len(available_bairros)} bairros de exemplo (dados reais indisponíveis)")
            st.info("💡 Os bairros abaixo são comuns em Belo Horizonte. Selecione alguns para continuar.")
        else:
            st.success(f"✅ Encontrados {len(available_bairros)} bairros disponíveis")
        
        # Bairro selection
        col1, col2 = st.columns([1, 2])
        with col1:
            selected_bairros = st.multiselect(
                "📍 Selecione os bairros:",
                options=available_bairros,
                help="Selecione apenas os bairros que você precisa para reduzir drasticamente o uso de memória"
            )
        
        with col2:
            if selected_bairros:
                # Store selected bairros in session state
                st.session_state.mega_data_filter_state["bairro_filter"] = selected_bairros
                
                estimated_reduction = (len(selected_bairros) / len(available_bairros)) * 100
                st.metric(
                    "📊 Dados a carregar",
                    f"{estimated_reduction:.1f}% do total",
                    f"{100 - estimated_reduction:.1f}% de redução"
                )
                
                # Auto-load data when bairros are selected (no manual button needed)
                load_data_btn = True
                st.info("🚀 Carregando dados automaticamente...")
            else:
                st.info("👆 Selecione pelo menos um bairro para continuar")
                load_data_btn = False
    else:
        st.error("❌ Não foi possível carregar a lista de bairros")
        st.info("🔧 Tente recarregar a página ou entre em contato com o suporte")
        load_data_btn = False
        
except Exception as e:
    log_error_with_context(e, "Error loading bairros list")
    st.error("❌ Erro ao carregar lista de bairros")
    load_data_btn = False
    selected_bairros = []

# Step 2: Load data only for selected bairros
if load_data_btn and selected_bairros:
    # Load data with bairro-based optimization
    try:
        monitor_memory_usage("before_bairro_data_loading")
        print(f"Starting bairro-filtered data load at {datetime.now().isoformat()}")
        print(f"Loading data for bairros: {', '.join(selected_bairros)}")
        
        with st.spinner(f"Carregando dados para {len(selected_bairros)} bairro(s)..."):
            mega_df = load_bairros_optimized(selected_bairros)
            monitor_memory_usage("after_bairro_data_load")

        if mega_df.empty:
            st.error("❌ Nenhum dado encontrado para os bairros selecionados.")
            st.info("Tente selecionar outros bairros ou verifique se os dados estão disponíveis.")
            st.stop()

        # Calculate actual memory savings
        total_data_estimate = len(mega_df) * (len(available_bairros) / len(selected_bairros))
        memory_savings = (1 - len(selected_bairros) / len(available_bairros)) * 100
        
        st.success(f"🎯 Carregados {len(mega_df):,} registros para {len(selected_bairros)} bairro(s)")
        
        # Add row cap warning for large datasets
        if len(mega_df) > 100000:
            st.warning(f"⚠️ Dataset muito grande ({len(mega_df):,} registros). Considere refinar os filtros antes de continuar para evitar lentidão.")
            st.info("💡 Dica: Selecione menos bairros ou use filtros dinâmicos para reduzir ainda mais o dataset.")
        
    except Exception as e:
        log_error_with_context(e, "Critical error during bairro data loading")
        st.error(f"❌ Erro crítico: {e}")
        st.stop()

# Only proceed if data is loaded
if mega_df is None:
    st.stop()

# Show summary statistics for the loaded bairro data
if DEBUG:
    st.sidebar.write("**Estatísticas dos Bairros Carregados:**")
    st.sidebar.write(f"**Total de registros**: {len(mega_df):,}")
    st.sidebar.write(f"**Colunas**: {len(mega_df.columns)}")
    st.sidebar.write(f"**Bairros únicos**: {mega_df['BAIRRO'].nunique() if 'BAIRRO' in mega_df.columns else 'N/A'}")
    # Don't call get_summary_stats() as it loads the full dataset

# Get unique bairros for filter (moved outside DEBUG block)
bairro_col = None
for col in mega_df.columns:
    if "BAIRRO" in col.upper():
        bairro_col = col
        break

if bairro_col is None:
    st.error("❌ Coluna 'BAIRRO' não encontrada no Mega Data Set.")
    st.stop()

# Get unique bairros, filtering out empty values
unique_bairros = mega_df[bairro_col].dropna().astype(str)
unique_bairros = unique_bairros[unique_bairros.str.strip() != ""]
unique_bairros = sorted(unique_bairros.unique())

if DEBUG:
    st.sidebar.write(f"**Bairros únicos**: {len(unique_bairros)}")
    st.sidebar.write(f"**Coluna bairro**: {bairro_col}")

# Filter section - moved outside DEBUG block

# Bairro filter with cascading (integrated into main filter area)
col1, col2 = st.columns([2, 1])

with col1:
    # Get current selections first
    current_bairros = st.session_state.mega_data_filter_state.get(
        "bairro_filter", []
    )

    # Use selected bairros from earlier step (no duplicate selector)
    selected_bairros = st.session_state.mega_data_filter_state.get("bairro_filter", [])
    
    # Ensure default "Nome Logradouro" filter is added automatically
    if selected_bairros and len(st.session_state.mega_data_filter_state.get("dynamic_filters", [])) == 0:
        # Add default Nome Logradouro filter
        nome_logradouro_filter = {
            "column": "NOME LOGRADOURO",
            "operator": "is_one_of",
            "value": [],
            "enabled": True
        }
        st.session_state.mega_data_filter_state["dynamic_filters"] = [nome_logradouro_filter]
        st.rerun()

    # Dynamic filters with reorganized buttons
    dynamic_filters = render_dynamic_filters(mega_df)

    # Apply filters - Only when map update is requested or on initial load
    # Always calculate current filters for cascading (but don't apply to final results unless button pressed)
    current_filtered_df = mega_df.copy()

    # Apply bairro filter to current state for cascading
    if selected_bairros:
        current_filtered_df = current_filtered_df[
            current_filtered_df[bairro_col].isin(selected_bairros)
        ]

    # Apply dynamic filters with simple AND/OR logic
    dynamic_filters = st.session_state.mega_data_filter_state.get("dynamic_filters", [])
    filter_logic = st.session_state.mega_data_filter_state.get("filter_logic", "AND")

    # Get active filters for counting
    active_dynamic_filters = [
        filter_config
        for filter_config in dynamic_filters
        if (
            filter_config.get("column")
            and filter_config.get("operator")
            and is_filter_active(filter_config)
        )
    ]

    # Apply filters with logic and monitor performance
    monitor_memory_usage("before_filter_application")
    print(f"Applying {len(active_dynamic_filters)} dynamic filters to {len(current_filtered_df)} records")
    
    try:
        current_filtered_df = apply_dynamic_filters_with_logic(
            current_filtered_df, dynamic_filters, filter_logic
        )
        monitor_memory_usage("after_filter_application")
        print(f"Filter application completed, {len(current_filtered_df)} records remaining")
    except Exception as e:
        log_error_with_context(e, f"Dynamic filter application failed with {len(active_dynamic_filters)} filters")
        st.error(f"Erro ao aplicar filtros dinâmicos: {e}")

    # Store the last applied state in session state if this is the first load or if update was requested
    if (
        "last_applied_filters" not in st.session_state.mega_data_filter_state
        or st.session_state.mega_data_filter_state.get("filters_need_update", False)
    ):

        st.session_state.mega_data_filter_state["last_applied_filters"] = {
            "bairro_filter": selected_bairros.copy(),
            "dynamic_filters": [f.copy() for f in active_dynamic_filters],
            "filtered_df": current_filtered_df.copy(),
        }
        st.session_state.mega_data_filter_state["filters_need_update"] = False
        filtered_df = current_filtered_df.copy()
    else:
        # Use the last applied state for map and results
        last_applied = st.session_state.mega_data_filter_state.get(
            "last_applied_filters", {}
        )
        filtered_df = last_applied.get("filtered_df", mega_df.copy())
        # Update active_dynamic_filters to show count correctly
        active_dynamic_filters = last_applied.get("dynamic_filters", [])

    # Show filtered results count and update status
    last_applied = st.session_state.mega_data_filter_state.get(
        "last_applied_filters", {}
    )
    applied_bairros = last_applied.get("bairro_filter", [])
    applied_dynamic_filters = last_applied.get("dynamic_filters", [])

    total_applied_filters = len([f for f in [applied_bairros] if f]) + len(
        applied_dynamic_filters
    )
    current_total_filters = len([f for f in [selected_bairros] if f]) + len(
        active_dynamic_filters
    )

    # Show real-time property count for current filter state (live updates)
    current_count = len(current_filtered_df)
    
    # Always show the live count prominently
    if current_total_filters > 0:
        current_filter_info = []
        if selected_bairros:
            current_filter_info.append(f"{len(selected_bairros)} bairro(s)")
        if active_dynamic_filters:
            current_filter_info.append(
                f"{len(active_dynamic_filters)} filtro(s) dinâmico(s)"
            )
        
        # Show live count in a prominent way
        st.success(f"🏠 **{current_count:,} propriedades filtradas**")
    else:
        st.info(f"🏠 **{current_count:,} propriedades disponíveis**")

    # Show update status if there are pending changes
    if (
        current_total_filters != total_applied_filters
        or selected_bairros != applied_bairros
        or len(active_dynamic_filters) != len(applied_dynamic_filters)
    ):
        st.write(
            "⚠️ Há mudanças nos filtros. Clique em 'Atualizar e Carregar Mapa' para aplicar as alterações."
        )

    # Hex API Integration Section
    if total_applied_filters > 0 and len(filtered_df) > 0:

        # Dropdown interface
        render_hex_dropdown_interface(filtered_df, funnel="mega_data_set")

    # Map section
    st.markdown("---")

    # Show map if loaded
    if (
        st.session_state.mega_data_filter_state.get("map_loaded", False)
        and total_applied_filters > 0
        and len(filtered_df) > 0
    ):

        # Prepare data for map (no limit)
        display_df = filtered_df

        # Convert DataFrame to list of dictionaries for map
        properties_for_map = []
        
        # PRODUCTION: Much stricter limits for Streamlit Cloud
        MAX_PROPERTIES = 10000 if os.getenv("STREAMLIT_SERVER_HEADLESS") == "true" else 50000
        if len(display_df) > MAX_PROPERTIES:
            st.warning(f"⚠️ Limitando a {MAX_PROPERTIES:,} propriedades no mapa para evitar problemas de memória")
            display_df = display_df.head(MAX_PROPERTIES)

        for _, row in display_df.iterrows():
            try:
                property_dict = row.to_dict()
            except Exception as e:
                if DEBUG:
                    st.error(f"Error converting row to dict: {e}")
                continue

            # Ensure GEOMETRY column exists
            if "GEOMETRY" not in property_dict:
                # Try to find a geometry column
                geom_col = None
                for col in property_dict.keys():
                    if "GEOMETRY" in col.upper() or "GEOM" in col.upper():
                        geom_col = col
                        break

                if geom_col:
                    property_dict["GEOMETRY"] = property_dict[geom_col]
                else:
                    # Skip properties without geometry
                    continue

            properties_for_map.append(property_dict)

        if not properties_for_map:
            st.error("❌ Nenhuma propriedade com dados geográficos encontrada")
        else:
            # Simple progress bar for entire process
            total_props = len(properties_for_map)

            # Create progress containers
            progress_container = st.container()
            with progress_container:
                progress_bar = st.progress(0)
                progress_text = st.empty()
                time_text = st.empty()

            start_time = time.time()

            # Step 1: Filter properties with valid geometry
            progress_text.text("Filtrando propriedades com geometria válida...")
            progress_bar.progress(0.1)

            valid_properties = []
            for prop in properties_for_map:
                geometry = prop.get("GEOMETRY")
                if geometry and pd.notna(geometry) and str(geometry).strip():
                    valid_properties.append(prop)

            elapsed_time = time.time() - start_time
            time_text.text(f"⏱️ Tempo decorrido: {elapsed_time:.1f}s")

            if valid_properties:
                # Step 2: Preparing map rendering
                progress_text.text(
                    f"Preparando renderização de {len(valid_properties):,} propriedades..."
                )
                progress_bar.progress(0.2)

                # Set debug mode for map
                st.session_state.debug_mode = DEBUG

                # Step 3: Start map rendering
                progress_text.text("Renderizando mapa...")
                progress_bar.progress(0.5)

                elapsed_time = time.time() - start_time
                time_text.text(f"⏱️ Tempo decorrido: {elapsed_time:.1f}s")

                # Render the map (this is where the actual time is spent)
                monitor_memory_usage("before_map_rendering")
                print(f"Starting map rendering with {len(valid_properties)} properties")
                
                try:
                    render_property_map_streamlit(
                        valid_properties,
                        map_style="Light",
                        enable_extra_options=True,
                        enable_style_selector=False,
                    )
                    monitor_memory_usage("after_successful_map_rendering")
                    print("Map rendering completed successfully")
                    
                except Exception as map_error:
                    monitor_memory_usage("after_failed_map_rendering")
                    log_error_with_context(map_error, f"Map rendering failed with {len(valid_properties)} properties")
                    progress_bar.progress(1.0)
                    progress_text.text("❌ Erro ao renderizar mapa")
                    st.error(f"Erro ao renderizar mapa: {str(map_error)}")
                    st.stop()

                # Complete progress after map is rendered
                progress_bar.progress(1.0)
                progress_text.text("✅ Mapa carregado com sucesso!")

                final_time = time.time() - start_time
                time_text.text(f"⏱️ Tempo total: {final_time:.1f}s")

                # Clear progress after a short delay
                time.sleep(1)
                progress_container.empty()
                
                # PRODUCTION: Force garbage collection to free memory
                if os.getenv("STREAMLIT_SERVER_HEADLESS") == "true":
                    gc.collect()
            else:
                st.error(
                    "❌ Nenhuma propriedade com dados geográficos válidos encontrada"
                )

                if DEBUG:
                    st.write("**Debug - Primeiras 3 propriedades:**")
                    for i, prop in enumerate(properties_for_map[:3]):
                        geom = prop.get("GEOMETRY", "N/A")
                        st.write(
                            f"  {i+1}. GEOMETRY: {type(geom)} - {str(geom)[:100]}..."
                        )

    # Show property details table if requested
    if (
        st.session_state.mega_data_filter_state.get("map_loaded", False)
        and total_applied_filters > 0
    ):
        st.markdown("---")
        st.subheader("📋 Detalhes das Propriedades")

        # Show ALL columns from the mega_data_set
        excluded_columns = {"GEOMETRY", "geometry", "id", "ID", "_id", "index"}
        display_columns = [
            col for col in filtered_df.columns if col not in excluded_columns
        ]

        if display_columns:
            display_df = filtered_df[display_columns].copy()

            # Format specific numeric columns with error handling
            try:
                for col in display_df.columns:
                    if "VALOR" in col.upper():
                        display_df[col] = display_df[col].apply(
                            lambda x: (
                                f"R$ {x:,.2f}".replace(",", "X")
                                .replace(".", ",")
                                .replace("X", ".")
                                if isinstance(x, (int, float)) and pd.notna(x)
                                else "N/A"
                            )
                        )
                    elif "AREA" in col.upper():
                        display_df[col] = display_df[col].apply(
                            lambda x: (
                                f"{x} m²"
                                if isinstance(x, (int, float)) and pd.notna(x)
                                else "N/A"
                            )
                        )
            except Exception as format_error:
                if DEBUG:
                    st.error(f"Error formatting columns: {format_error}")
                # Continue without formatting

            # Show table with pagination - smaller in production
            MAX_ROWS = 50 if os.getenv("STREAMLIT_SERVER_HEADLESS") == "true" else 100
            st.dataframe(
                display_df.head(MAX_ROWS),  # Show fewer rows in production
                hide_index=True,
                use_container_width=True,
            )

            if len(display_df) > 100:
                st.info(
                    f"📊 Mostrando primeiros 100 registros de {len(display_df):,} propriedades"
                )

            # Show column count
            st.info(f"📋 Total de {len(display_columns)} colunas disponíveis")
        else:
            st.warning("⚠️ Nenhuma coluna encontrada para exibição")
    st.stop()
