"""Mega Data Set page with bairro filter and map visualization."""

import streamlit as st
import pandas as pd
from datetime import datetime
import time

try:
    from services.mega_data_set_loader import (
        load_mega_data_set,
        get_property_summary_stats,
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
    }


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
            col1, col2, col3, col4 = st.columns([3, 2, 3, 1])

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


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_mega_data():
    """Load mega data set with caching."""
    return load_mega_data_set()


@st.cache_data(ttl=3600)
def get_summary_stats():
    """Get summary statistics with caching."""
    return get_property_summary_stats()


# Load data
try:
    with st.spinner("Carregando Mega Data Set..."):
        mega_df = load_mega_data()

    if mega_df.empty:
        st.error("❌ Nenhum dado encontrado no Mega Data Set.")
        st.info(
            "Verifique se o arquivo está disponível no Google Drive ou configurado localmente."
        )
        st.stop()

    st.write(f"✅ Loaded {len(mega_df):,} registros do Mega Data Set")

    # Show summary statistics
    if DEBUG:
        st.sidebar.write("**Estatísticas do Mega Data Set:**")
        stats = get_summary_stats()
        for key, value in stats.items():
            if isinstance(value, dict):
                st.sidebar.write(f"**{key}**: {len(value)} únicos")
            else:
                st.sidebar.write(f"**{key}**: {value:,}")

    # Get unique bairros for filter
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

    # Filter section

    # Bairro filter with cascading (integrated into main filter area)
    col1, col2 = st.columns([1, 2])

    with col1:
        # Get current selections first
        current_bairros = st.session_state.mega_data_filter_state.get(
            "bairro_filter", []
        )

        # Get available bairros from full dataset (not cascaded) to preserve existing selections
        available_bairros = mega_df[bairro_col].dropna().astype(str)
        available_bairros = available_bairros[available_bairros.str.strip() != ""]
        available_bairros = sorted(available_bairros.unique())

        # All current selections are valid since we're using the full dataset
        valid_bairros = [b for b in current_bairros if b in available_bairros]

        selected_bairros = st.multiselect(
            "🗺️ Selecione os Bairros:",
            options=available_bairros,
            default=valid_bairros,
            help="Selecione um ou mais bairros para filtrar as propriedades",
        )
        # Update session state
        st.session_state.mega_data_filter_state["bairro_filter"] = selected_bairros

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

    # Apply filters with logic
    try:
        current_filtered_df = apply_dynamic_filters_with_logic(
            current_filtered_df, dynamic_filters, filter_logic
        )
    except Exception as e:
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

    # Show real-time property count for current filter state (without triggering map reload)
    current_count = len(current_filtered_df)
    if current_count != len(filtered_df):
        if current_total_filters > 0:
            current_filter_info = []
            if selected_bairros:
                current_filter_info.append(f"{len(selected_bairros)} bairro(s)")
            if active_dynamic_filters:
                current_filter_info.append(
                    f"{len(active_dynamic_filters)} filtro(s) dinâmico(s)"
                )

            st.write(f"🔍 Filtros atuais mostrarão {current_count:,} imóveis")
        else:
            st.write(f"🔍 Filtros atuais mostrarão {current_count:,} imóveis")

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

        # Description row
        st.write(
            f"{len(filtered_df):,} propriedades filtradas"
        )

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

        for _, row in display_df.iterrows():
            property_dict = row.to_dict()

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
                render_property_map_streamlit(
                    valid_properties,
                    map_style="Light",
                    enable_extra_options=True,
                    enable_style_selector=False,
                )

                # Complete progress after map is rendered
                progress_bar.progress(1.0)
                progress_text.text("✅ Mapa carregado com sucesso!")

                final_time = time.time() - start_time
                time_text.text(f"⏱️ Tempo total: {final_time:.1f}s")

                # Clear progress after a short delay
                time.sleep(1)
                progress_container.empty()
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

            # Format specific numeric columns
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

            # Show table with pagination
            st.dataframe(
                display_df.head(100),  # Show first 100 rows
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

except Exception as e:
    st.error(f"❌ Erro ao carregar dados: {str(e)}")
    if DEBUG:
        st.exception(e)
    st.info("Verifique se o Mega Data Set está configurado corretamente.")
