"""Processor.py - Streamlit interface for WhatsApp Agent with authentication."""

import os
import time
from datetime import datetime

import pandas as pd
import streamlit as st
import requests

# Import centralized phone utilities
from services.phone_utils import format_phone_for_display as format_phone_display

# Conditional import to prevent crashes when auth not needed
from config import (
    ACOES_OPTS,
    CLASSIFICACAO_OPTS,
    INTENCAO_OPTS,
    PAGAMENTO_OPTS,
    PERCEPCAO_OPTS,
    PRESET_RESPONSES,
    STANDBY_REASONS,
    STATUS_URBLINK_OPTS,
)
from loaders.db_loader import get_dataframe, get_db_info, get_conversation_messages
from services.spreadsheet import sync_record_to_sheet, format_phone_for_storage, format_address_field
from services.voxuy_api import send_whatsapp_message
from services.background_operations import (
    queue_sync_operation,
    queue_archive_operation,
    render_operations_sidebar,
    get_running_operations,
    background_manager
)
from services.mega_data_set_loader import (
    get_properties_for_phone,
    format_property_for_display,
)
from services.preloader import start_background_preload, display_preloader_status
from utils.styles import STYLES
from utils.ui_helpers import (
    bold_asterisks,
    build_highlights,
    fmt_num,
    highlight,
    parse_chat,
    parse_familiares_grouped,
    parse_imoveis,
)
from utils.sync_ui import (
    setup_conversation_sync,
    render_sync_sidebar,
    render_sync_header,
    check_for_sync_updates,
    cleanup_sync_on_exit,
    setup_auto_refresh,
)
from services.conversation_sync import get_sync_status


# ──────────────────────────────────────────────────────────────────────────────
# PHONE NUMBER FORMATTING FUNCTION
# ──────────────────────────────────────────────────────────────────────────────

def format_phone_for_display(phone_number: str) -> str:
    """Format phone number to (XX) XXXXX-XXXX format for display."""
    # Use centralized phone utility for consistent behavior
    return format_phone_display(phone_number)

# ──────────────────────────────────────────────────────────────────────────────
# CONVERSATION ARCHIVE FUNCTION
# ──────────────────────────────────────────────────────────────────────────────

def archive_conversation(phone_number: str, conversation_id: str) -> dict:
    """Archive a conversation using the Voxuy webhook."""
    try:
        # Get chatId from secrets (same as conversation sync service)
        chat_id = st.secrets.get("VOXUY_CHAT_ID", "07d49a4a-1b9c-4a02-847d-bad0fb3870eb")
        
        # Format phone number correctly
        # Remove @s.whatsapp.net suffix if present
        clean_phone = phone_number.split('@')[0] if '@' in phone_number else phone_number
        
        # Ensure phone has proper format (+55...)
        if not clean_phone.startswith('+'):
            # Add country code if not present
            if clean_phone.startswith('55') and len(clean_phone) > 10:
                clean_phone = '+' + clean_phone
            else:
                # Add Brazil country code
                clean_phone = '+55' + clean_phone
        
        # Prepare webhook payload
        payload = {
            "phone": clean_phone,
            "chatId": chat_id
        }
        
        # Make HTTP request to archive webhook
        webhook_url = "https://voxuy-archive-conversation.athosmartins.workers.dev/"
        
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=30,  # 30 second timeout
            headers={"Content-Type": "application/json"}
        )
        
        # Parse response
        response_data = response.json() if response.content else {}
        
        if response.status_code == 200:
            return {
                "success": True,
                "message": "Conversa arquivada com sucesso!",
                "details": response_data
            }
        else:
            return {
                "success": False,
                "message": f"Erro ao arquivar conversa (HTTP {response.status_code})",
                "details": response_data,
                "error": f"Status: {response.status_code}"
            }
            
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "message": "Timeout ao tentar arquivar conversa",
            "error": "Request timeout after 30 seconds"
        }
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "message": "Erro de conexão ao tentar arquivar conversa",
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "message": "Erro inesperado ao arquivar conversa",
            "error": str(e)
        }

# ──────────────────────────────────────────────────────────────────────────────
# PROPERTY ASSIGNMENT FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def show_property_assignment_popup():
    """Show the property assignment popup with memory-safe loading."""
    # Safety check - only show if we have a valid conversation loaded
    if not hasattr(st.session_state, 'current_conversation_id') or not st.session_state.current_conversation_id:
        st.error("Por favor, carregue uma conversa primeiro.")
        st.session_state.show_property_assignment = False
        st.rerun()
        return
    
    from services.unified_property_loader import property_loader, MemoryMonitor
    
    # Create a modal-like container
    with st.container():
        st.markdown("### 🏢 Atribuir Propriedade")
        st.markdown("---")
        
        # Display memory usage for monitoring
        MemoryMonitor.display_memory_widget()
        
        # Memory safety check before proceeding
        if not MemoryMonitor.check_memory_safety("property assignment"):
            st.error("Cannot open property assignment - memory usage too high")
            st.session_state.show_property_assignment = False
            return
        
        # Get available bairros using memory-safe approach
        try:
            bairros = property_loader.get_available_bairros()
            if not bairros:
                st.warning("Nenhum bairro encontrado no mega data set. Usando fallback.")
                bairros = ["Centro", "Savassi", "Funcionários", "Lourdes", "Buritis", "Pampulha", "Belvedere"]
        except Exception as e:
            st.error(f"Erro ao carregar bairros: {e}")
            return
        
        # Bairro filter
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_bairros = st.multiselect(
                "Selecionar Bairros:",
                options=bairros,
                default=st.session_state.property_assignment_state.get("bairro_filter", []),
                key="property_bairro_filter"
            )
            st.session_state.property_assignment_state["bairro_filter"] = selected_bairros
        
        with col2:
            if st.button("🗑️ Limpar Filtros", help="Limpar todos os filtros aplicados", use_container_width=True):
                # Clear all filter-related session state
                st.session_state.property_assignment_state = {
                    "bairro_filter": [],
                    "dynamic_filters": []
                }
                # Force rerun to update the interface
                st.rerun()
        
        # Load data for selected bairros only (memory-safe approach)
        if not selected_bairros:
            filtered_df = pd.DataFrame()  # Empty dataframe when no bairros selected
        else:
            with st.spinner(f"Carregando propriedades para {len(selected_bairros)} bairro(s)..."):
                filtered_df = property_loader.load_bairros_safe(selected_bairros)
            
            if filtered_df.empty:
                st.warning(f"Nenhuma propriedade encontrada para os bairros selecionados: {', '.join(selected_bairros)}")
            else:
                st.success(f"✅ Carregadas {len(filtered_df):,} propriedades de {len(selected_bairros)} bairro(s)")
                
                # Auto-add Nome Logradouro filter when bairro is selected
                if len(st.session_state.property_assignment_state.get("dynamic_filters", [])) == 0:
                    nome_logradouro_filter = {
                        "column": "NOME LOGRADOURO",
                        "operator": "is_one_of",
                        "value": [],
                        "enabled": True
                    }
                    st.session_state.property_assignment_state["dynamic_filters"] = [nome_logradouro_filter]
        
        # Dynamic filters
        render_property_dynamic_filters(filtered_df)
        
        # Apply dynamic filters
        final_df = apply_property_filters(filtered_df, st.session_state.property_assignment_state)
        
        # Display filtered results
        if not final_df.empty:
            st.markdown(f"**Resultados: {len(final_df)} propriedades encontradas**")
            
            # Select columns to display
            display_columns = ["BAIRRO", "NOME LOGRADOURO", "ENDERECO", "COMPLEMENTO ENDERECO", "TIPO CONSTRUTIVO", "AREA TERRENO", "AREA CONSTRUCAO", "ANO CONSTRUCAO", "NOME PROPRIETARIO PBH", "DOCUMENTO PROPRIETARIO", "IDADE", "OBITO PROVAVEL", "INDICE CADASTRAL"]
            display_columns = [col for col in display_columns if col in final_df.columns]
            
            # Add selection column - reset index to ensure proper mapping
            display_df = final_df.copy().reset_index(drop=True)
            display_df.insert(0, "Selecionar", False)
            
            # Show dataframe with selection
            selection = st.data_editor(
                display_df[["Selecionar"] + display_columns],
                column_config={
                    "Selecionar": st.column_config.CheckboxColumn(
                        "Selecionar",
                        help="Selecione uma propriedade",
                        default=False,
                    )
                },
                disabled=display_columns,
                hide_index=True,
                use_container_width=True,
                key="property_selection"
            )
            
            # Handle selection
            if st.button("Confirmar Seleção", key="confirm_property_selection"):
                try:
                    # Get selected rows
                    selected_mask = selection["Selecionar"] == True
                    selected_indices = selection[selected_mask].index.tolist()
                    
                    if len(selected_indices) == 1:
                        # Get the selected property using the display_df index
                        selected_idx = selected_indices[0]
                        if selected_idx < len(display_df):
                            # Get the property from display_df (which is a copy of final_df with reset index)
                            selected_property = display_df.iloc[selected_idx]
                            
                            # Remove the "Selecionar" column from the property dict
                            property_dict = selected_property.to_dict()
                            property_dict.pop("Selecionar", None)
                            
                            st.session_state.property_assignment_state["selected_property"] = property_dict
                            
                            # Add to conversation property data
                            conversation_id = st.session_state.get("current_conversation_id", "")
                            if conversation_id:
                                if "assigned_properties" not in st.session_state:
                                    st.session_state.assigned_properties = {}
                                st.session_state.assigned_properties[conversation_id] = property_dict
                            
                            st.success("Propriedade atribuída com sucesso!")
                            st.session_state.show_property_assignment = False
                            st.rerun()
                        else:
                            st.error("Índice de seleção inválido. Tente novamente.")
                        
                    elif len(selected_indices) > 1:
                        st.error("Selecione apenas uma propriedade.")
                    else:
                        st.error("Selecione uma propriedade.")
                        
                except Exception as e:
                    st.error(f"Erro na funcionalidade de atribuição de propriedade: {e}")
                    import traceback
                    traceback.print_exc()
        
        # Close button
        if st.button("Fechar", key="close_property_assignment"):
            st.session_state.show_property_assignment = False
            st.rerun()


def render_property_dynamic_filters(df):
    """Render dynamic filters for property assignment."""
    # Get available columns
    excluded_columns = {"GEOMETRY", "geometry", "id", "ID", "_id", "index"}
    available_columns = sorted([col for col in df.columns if col not in excluded_columns])
    
    # Render existing filters
    for i, filter_config in enumerate(st.session_state.property_assignment_state["dynamic_filters"]):
        with st.expander(f"Filtro {i+1}", expanded=True):
            col1, col2, col3, col4 = st.columns([2.5, 2, 4, 0.5])
            
            with col1:
                # Column selection
                selected_column = st.selectbox(
                    "Coluna:",
                    options=[""] + available_columns,
                    index=0 if filter_config["column"] is None else available_columns.index(filter_config["column"]) + 1,
                    key=f"property_filter_column_{i}",
                )
                filter_config["column"] = selected_column if selected_column else None
            
            with col2:
                # Operator selection
                if selected_column:
                    operators = ["is_one_of", "contains", "equals", "starts_with", "ends_with"]
                    operator_labels = {
                        "equals": "Igual a",
                        "contains": "Contém", 
                        "starts_with": "Começa com",
                        "ends_with": "Termina com",
                        "is_one_of": "É um de",
                    }
                    
                    operator_options = [operator_labels.get(op, op) for op in operators]
                    default_idx = 0 if filter_config["operator"] == "is_one_of" else 0
                    
                    selected_operator_label = st.selectbox(
                        "Operador:",
                        options=operator_options,
                        index=default_idx,
                        key=f"property_filter_operator_{i}",
                    )
                    
                    # Map back to operator key
                    reverse_labels = {v: k for k, v in operator_labels.items()}
                    filter_config["operator"] = reverse_labels[selected_operator_label]
            
            with col3:
                # Value input
                if selected_column and filter_config["operator"]:
                    if filter_config["operator"] == "is_one_of":
                        # Multi-select for categorical values
                        unique_values = sorted(df[selected_column].dropna().unique())
                        selected_values = st.multiselect(
                            "Valores:",
                            options=unique_values,
                            default=filter_config.get("value", []),
                            key=f"property_filter_value_{i}",
                        )
                        filter_config["value"] = selected_values
                    else:
                        # Text input for other operators
                        value = st.text_input(
                            "Valor:",
                            value=filter_config.get("value", ""),
                            key=f"property_filter_value_{i}",
                        )
                        filter_config["value"] = value
            
            with col4:
                # Remove filter button
                if st.button("🗑️", key=f"remove_property_filter_{i}"):
                    st.session_state.property_assignment_state["dynamic_filters"].pop(i)
                    st.rerun()
    
    # Add new filter button
    if st.button("➕ Adicionar Filtro", key="add_property_filter"):
        new_filter = {
            "column": None,
            "operator": "is_one_of",
            "value": [],
            "enabled": True
        }
        st.session_state.property_assignment_state["dynamic_filters"].append(new_filter)
        st.rerun()


def apply_property_filters(df, filter_state):
    """Apply filters to the property dataframe."""
    filtered_df = df.copy()
    
    # Apply dynamic filters
    for filter_config in filter_state.get("dynamic_filters", []):
        if not filter_config.get("enabled", True) or not filter_config.get("column"):
            continue
            
        column = filter_config["column"]
        operator = filter_config["operator"]
        value = filter_config["value"]
        
        if not value:
            continue
            
        try:
            if operator == "is_one_of":
                filtered_df = filtered_df[filtered_df[column].isin(value)]
            elif operator == "contains":
                filtered_df = filtered_df[filtered_df[column].str.contains(value, case=False, na=False)]
            elif operator == "equals":
                filtered_df = filtered_df[filtered_df[column] == value]
            elif operator == "starts_with":
                filtered_df = filtered_df[filtered_df[column].str.startswith(value, na=False)]
            elif operator == "ends_with":
                filtered_df = filtered_df[filtered_df[column].str.endswith(value, na=False)]
        except Exception as e:
            st.error(f"Erro no filtro {column}: {e}")
            continue
    
    return filtered_df


# ──────────────────────────────────────────────────────────────────────────────
# MAIN FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────


def format_last_message_date(timestamp):
    """Format timestamp to format: 01/Jul/25 (10 dias)"""
    if pd.isna(timestamp) or timestamp == 0 or not timestamp or timestamp == "":
        return None

    try:
        # Try different timestamp formats
        dt = None

        # If it's already a datetime object
        if isinstance(timestamp, pd.Timestamp):
            dt = timestamp
        # If it's a string, try to parse it
        elif isinstance(timestamp, str):
            dt = pd.to_datetime(timestamp)
        # If it's a number, try as unix timestamp
        elif isinstance(timestamp, (int, float)):
            dt = pd.to_datetime(timestamp, unit="s")
        else:
            dt = pd.to_datetime(timestamp)

        # Calculate days ago
        now = pd.Timestamp.now()
        days_ago = (now - dt).days

        # Portuguese month abbreviations
        months_pt_abbr = {
            1: "Jan",
            2: "Fev",
            3: "Mar",
            4: "Abr",
            5: "Mai",
            6: "Jun",
            7: "Jul",
            8: "Ago",
            9: "Set",
            10: "Out",
            11: "Nov",
            12: "Dez",
        }

        # Format as DD/MMM/YY
        day = f"{dt.day:02d}"
        month = months_pt_abbr[dt.month]
        year = f"{dt.year % 100:02d}"

        # Format the relative time
        if days_ago == 0:
            relative_time = "hoje"
        elif days_ago == 1:
            relative_time = "1 dia"
        else:
            relative_time = f"{days_ago} dias"

        return f"{day}/{month}/{year} ({relative_time})"
    except Exception as e:
        if DEBUG:
            print(f"DEBUG: Error formatting timestamp '{timestamp}': {e}")
        return None


def find_conversations_with_same_property(
    current_property_address,
    current_property_neighborhood,
    current_conversation_id=None,
):
    """
    Find all conversations that have the same property (address + neighborhood).
    Returns a DataFrame with columns: classificacao, display_name, expected_name, phone, status
    """
    try:
        # Get all conversations data
        all_conversations_df = get_dataframe()

        if all_conversations_df.empty:
            return pd.DataFrame(
                columns=[
                    "classificacao",
                    "display_name",
                    "expected_name",
                    "phone",
                    "status",
                ]
            )

        # Find conversations with matching properties
        matching_conversations = []

        for idx, row in all_conversations_df.iterrows():
            # Skip current conversation - use the idx parameter passed from button click
            if (
                hasattr(st.session_state, "property_modal_data")
                and st.session_state.property_modal_data.get("current_idx") == idx
            ):
                if DEBUG:
                    print(f"DEBUG: Skipping current conversation at index {idx}")
                continue

            # ONLY check original IMOVEIS field - no mega_data_set lookup for performance
            original_imoveis = parse_imoveis(row.get("IMOVEIS"))
            if isinstance(original_imoveis, dict):
                original_imoveis = [original_imoveis]
            elif not isinstance(original_imoveis, list):
                original_imoveis = []

            # Check if any property matches
            found_match = False
            for property_item in original_imoveis:
                if not isinstance(property_item, dict):
                    continue

                prop_address = property_item.get("ENDERECO", "").strip()
                prop_neighborhood = property_item.get("BAIRRO", "").strip()

                # Match on both address and neighborhood
                if (
                    prop_address
                    and prop_neighborhood
                    and prop_address.lower() == current_property_address.lower()
                    and prop_neighborhood.lower()
                    == current_property_neighborhood.lower()
                ):

                    # Add to matching conversations
                    matching_conversations.append(
                        {
                            "expected_name": row.get("nome", "")
                            or row.get("name", "")
                            or row.get("nome_proprietario", ""),
                            "classificacao": row.get("classificacao", "")
                            or row.get("Classificação do dono do número", ""),
                            "display_name": row.get("display_name", ""),
                            "intencao": row.get("intencao", "")
                            or row.get("Intenção", ""),
                            "last_message_date": row.get("last_message_timestamp", "")
                            or row.get("last_message_time", "")
                            or row.get("timestamp", ""),
                            "conversation_id": row.get(
                                "conversation_id", ""
                            ),  # Add this for navigation
                            "row_index": idx,  # Add row index for navigation
                        }
                    )
                    found_match = True
                    break  # Found match, no need to check other properties for this conversation

            if found_match:
                continue  # Move to next conversation

        # Create DataFrame
        result_df = pd.DataFrame(matching_conversations)

        if DEBUG:
            print(f"DEBUG: Found {len(result_df)} conversations with same property")
            print(
                f"DEBUG: Property search: {current_property_address}, {current_property_neighborhood}"
            )

        return result_df

    except Exception as e:
        if DEBUG:
            print(f"DEBUG: Error in find_conversations_with_same_property: {e}")
        return pd.DataFrame(
            columns=[
                "classificacao",
                "display_name",
                "expected_name",
                "phone",
                "status",
            ]
        )


# ─── PAGE CONFIG (MUST BE FIRST) ────────────────────────────────────────
st.set_page_config(page_title="Processador de Conversas", page_icon="📱", layout="wide")

# ─── START BACKGROUND PRELOADER ─────────────────────────────────────────────
# Start background downloading of all critical files for smooth UX
if "preloader_started" not in st.session_state:
    st.session_state.preloader_started = True
    start_background_preload()

# ─── PROPERTY ASSIGNMENT SESSION STATE INITIALIZATION ──────────────────────
# Initialize property assignment session state early to prevent errors
if "show_property_assignment" not in st.session_state:
    st.session_state.show_property_assignment = False
if "property_assignment_state" not in st.session_state:
    st.session_state.property_assignment_state = {
        "bairro_filter": [],
        "dynamic_filters": [],
        "selected_property": None,
        "filter_logic": "AND"
    }

# ─── FLAGS ──────────────────────────────────────────────────────────────────
DEV = True  # Set based on your environment

# Initialize DEBUG mode
DEBUG = False
if DEV:
    DEBUG = st.sidebar.checkbox("🐛 Debug Mode", value=False)
    # Also store in session state for cross-module access
    st.session_state.debug_mode = DEBUG

# Display preloader status in sidebar
display_preloader_status()

# LOGIN_ENABLED flag - HARDCODED TO FALSE to prevent WhatsApp login issues
LOGIN_ENABLED = False  # ALWAYS disabled - no more login prompts

HIGHLIGHT_ENABLE = False

# LOGIN DISABLED: Commenting out all login logic to prevent any prompts
# if "LOGIN_ENABLED" in os.environ:
#     env_value = os.environ["LOGIN_ENABLED"].lower()
#     LOGIN_ENABLED = env_value in ("true", "1", "yes", "on")
# elif hasattr(st, "secrets") and "LOGIN_ENABLED" in st.secrets:
#     LOGIN_ENABLED = st.secrets["LOGIN_ENABLED"]
# elif DEV:
#     LOGIN_ENABLED = False

# ─── AUTHENTICATION ─────────────────────────────────────────────────────
# Check authentication only if LOGIN_ENABLED is True
if LOGIN_ENABLED:
    try:
        from auth.login_manager import simple_auth
        if not simple_auth():
            st.stop()
    except Exception as auth_error:
        st.error(f"❌ Authentication error: {auth_error}")
        st.info("Login disabled due to configuration error")
        LOGIN_ENABLED = False
else:
    # When login is disabled, show a warning in DEBUG mode
    if DEBUG:
        st.warning("🔓 Login is disabled (DEV mode)")

# ─── AUTHENTICATED APP STARTS HERE ──────────────────────────────────────
# Apply styles
st.markdown(STYLES, unsafe_allow_html=True)

# ─── URL PARAMETER HANDLING ─────────────────────────────────────────────────
# Check for conversation_id in URL parameters or session storage
auto_load_conversation = None

# Method 1: Check URL parameters
try:
    query_params = st.query_params
    if "conversation_id" in query_params:
        auto_load_conversation = query_params["conversation_id"]
        if DEBUG:
            st.info(f"🔗 Auto-loading conversation: {auto_load_conversation}")
    elif "_preserved_conversation_id" in st.session_state:
        # Restore conversation context after widget-triggered rerun
        auto_load_conversation = st.session_state["_preserved_conversation_id"]
        st.query_params["conversation_id"] = auto_load_conversation
        if DEBUG:
            st.info(f"🔄 Restored conversation context: {auto_load_conversation}")
        # Clear the preserved context after restoration
        del st.session_state["_preserved_conversation_id"]
except:
    pass

# Debug information display
if DEBUG:
    # Show timezone debug information
    if 'sync_debug_log' in st.session_state and st.session_state.sync_debug_log:
        with st.expander("🕐 Timezone Debug Log"):
            for debug_info in st.session_state.sync_debug_log:
                st.json(debug_info)
    
    # Show image debug information  
    if 'image_debug_log' in st.session_state and st.session_state.image_debug_log:
        with st.expander("🖼️ Image Debug Log"):
            for debug_info in st.session_state.image_debug_log:
                st.json(debug_info)

# Method 2: Check session storage (set by map navigation)
if not auto_load_conversation:
    # JavaScript to check session storage and listen for map navigation
    st.markdown(
        """
    <script>
    if (sessionStorage.getItem('auto_load_conversation')) {
        const convId = sessionStorage.getItem('auto_load_conversation');
        sessionStorage.removeItem('auto_load_conversation'); // Clear after use
        window.parent.postMessage({type: 'auto_load_conversation', conversation_id: convId}, '*');
    }
    
    // Listen for messages from the map (for navigation from map popups)
    window.addEventListener('message', function(event) {
        if (event.data.type === 'navigate_to_processor') {
            const conversationId = event.data.conversation_id;
            
            // Navigate to processor page with conversation_id parameter in same tab
            const processorUrl = window.location.origin + '/Processor?conversation_id=' + encodeURIComponent(conversationId);
            console.log('Map navigation to:', processorUrl);
            window.location.href = processorUrl;
        }
    });
    </script>
    """,
        unsafe_allow_html=True,
    )

# Method 3: Check if stored in session state from map navigation
if "auto_load_conversation_id" in st.session_state:
    auto_load_conversation = st.session_state.auto_load_conversation_id
    del st.session_state.auto_load_conversation_id  # Clear after use
    st.info(f"🗺️ Loading conversation from map: {auto_load_conversation}")

# ─── FLAGS ──────────────────────────────────────────────────────────────────
DEV = True  # Set based on your environment


# ─── DATA LOADER ────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(force_load_spreadsheet: bool = False):
    """Load the WhatsApp conversations DataFrame with Google Sheets data - same as Conversations page."""
    from loaders.db_loader import get_conversations_with_sheets_data
    return get_conversations_with_sheets_data(force_load_spreadsheet=force_load_spreadsheet)


# ─── CONVERSATION DISPLAY HELPER FUNCTIONS ─────────────────────────────────
def format_time_only(timestamp):
    """Format timestamp to show only HH:MM in BRT."""
    if pd.isna(timestamp) or timestamp == 0:
        return ""
    try:
        return datetime.fromtimestamp(timestamp).strftime("%H:%M")
    except:
        return ""


def format_date_header(timestamp):
    """Format timestamp to show date header in Portuguese with weekday."""
    if pd.isna(timestamp) or timestamp == 0:
        return ""
    try:
        from datetime import timedelta

        dt = datetime.fromtimestamp(timestamp)
        today = datetime.now().date()
        msg_date = dt.date()

        if msg_date == today:
            return "Hoje"
        elif msg_date == today - timedelta(days=1):
            return "Ontem"
        else:
            # Portuguese month names
            months_pt = {
                1: "Janeiro",
                2: "Fevereiro",
                3: "Março",
                4: "Abril",
                5: "Maio",
                6: "Junho",
                7: "Julho",
                8: "Agosto",
                9: "Setembro",
                10: "Outubro",
                11: "Novembro",
                12: "Dezembro",
            }

            # Portuguese weekday names
            weekdays_pt = {
                0: "Segunda-feira",
                1: "Terça-feira",
                2: "Quarta-feira",
                3: "Quinta-feira",
                4: "Sexta-feira",
                5: "Sábado",
                6: "Domingo",
            }

            day = dt.day
            month = months_pt[dt.month]
            year = dt.year
            weekday = weekdays_pt[dt.weekday()]

            return f"{day} {month}, {year} - {weekday}"
    except:
        return ""


# ─── DEBUG PANEL (dev‐only) ─────────────────────────────────────────────────
debug_panel = None
logged_messages = set()

if DEV and DEBUG:
    debug_panel = st.sidebar.expander("🔍 Debug Log", expanded=False)

    # Add database info to debug panel
    db_info_panel = st.sidebar.expander("📊 Database Info", expanded=True)
    if db_info_panel:
        db_info = get_db_info()
        db_info_panel.write("**Database File Information:**")
        db_info_panel.write(
            f"📁 **Original filename:** {db_info['original_filename']}"
        )
        db_info_panel.write(
            f"🕒 **Original modified:** {db_info['original_modified']}"
        )
        db_info_panel.write(f"💾 **Local path:** {db_info['local_path']}")
        db_info_panel.write(f"📅 **Local modified:** {db_info['local_modified']}")
        db_info_panel.write(f"⏰ **File age:** {db_info['file_age']}")
        db_info_panel.write(f"📏 **File size:** {db_info['local_size']:,} bytes")

        # Show freshness status
        if db_info.get("is_stale", False):
            db_info_panel.warning(
                "⚠️ Database file is older than 1 hour. Will auto-refresh on next load."
            )
        else:
            db_info_panel.success("✅ Database file is fresh (< 1 hour old)")

        db_info_panel.info(
            "🔄 Database automatically refreshes when older than 1 hour"
        )

        # Add DataFrame info
        if "master_df" in st.session_state:
            db_info_panel.write("**DataFrame Info:**")
            db_info_panel.write(
                f"📊 **Total records:** {len(st.session_state.master_df)}"
            )
            db_info_panel.write(
                f"📝 **Available columns:** {list(st.session_state.master_df.columns)}"
            )
            if "original_values" in st.session_state:
                db_info_panel.write(
                    f"✏️ **Modified records:** {len(st.session_state.original_values)}"
                )


def dbg(message: str):
    """Write a debug message once to the sidebar panel."""
    if DEBUG and debug_panel and message not in logged_messages:
        logged_messages.add(message)
        debug_panel.write(message)


# ─── STATE MANAGEMENT FUNCTIONS ─────────────────────────────────────────
def initialize_session_state():
    """Initialize the session state with proper data structure."""
    # Check if we're coming from the Conversations page with specific conversation data
    if (
        "processor_conversation_data" in st.session_state
        and st.session_state.processor_conversation_data
    ):
        # We're coming from Conversations page - create a single-row dataframe with the conversation data
        conversation_data = st.session_state.processor_conversation_data.copy()

        # Helper function to parse list fields from spreadsheet
        def parse_spreadsheet_list(value):
            if not value or pd.isna(value):
                return []
            if isinstance(value, list):
                return value
            # Try to parse as comma-separated or JSON
            import json

            try:
                return json.loads(value) if value else []
            except:
                return (
                    [v.strip() for v in str(value).split(",") if v.strip()]
                    if value
                    else []
                )

        # Helper function to parse boolean fields from spreadsheet
        def parse_spreadsheet_bool(value):
            if isinstance(value, bool):
                return value
            if pd.isna(value) or value is None:
                return False
            return str(value).lower() in ("true", "1", "yes", "sim", "verdadeiro")

        # Map the conversation data to the expected deepseek_results format
        mapped_data = {
            "display_name": conversation_data.get("display_name", ""),
            "phone_number": conversation_data.get("phone_number", ""),
            "whatsapp_number": conversation_data.get("conversation_id", ""),
            "conversation_id": conversation_data.get("conversation_id", ""),
            "total_messages": conversation_data.get("total_messages", 0),
            "last_message_timestamp": conversation_data.get(
                "last_message_timestamp", 0
            ),
            # Add all the spreadsheet data that was merged
            "endereco": conversation_data.get("endereco", ""),
            "endereco_bairro": conversation_data.get("endereco_bairro", ""),
            "endereco_complemento": conversation_data.get("endereco_complemento", ""),
            "Nome": conversation_data.get("Nome", ""),
            "Classificação do dono do número": conversation_data.get(
                "Classificação do dono do número", ""
            ),
            "status": conversation_data.get("status", ""),
            "status_manual": conversation_data.get("status_manual", ""),
            # Initialize fields for classification - pre-load from spreadsheet where available
            "conversation_history": "",  # Will be loaded from messages
            "classificacao": conversation_data.get(
                "Classificação do dono do número", ""
            ),
            "intencao": conversation_data.get(
                "status_manual", ""
            ),  # Maps to intencao field
            "pagamento": conversation_data.get("pagamento", ""),
            "resposta": "",  # Resposta field doesn't exist in spreadsheet, keep empty
            "Razao": conversation_data.get("standby_reason", ""),
            "acoes_urblink": parse_spreadsheet_list(conversation_data.get("Ações", "")),
            "status_urblink": conversation_data.get("status_manual_urb.link", ""),
            "obs": conversation_data.get("OBS", ""),  # Maps OBS column to obs field - FIXED!
            "stakeholder": parse_spreadsheet_bool(
                conversation_data.get("stakeholder", False)
            ),
            "intermediador": parse_spreadsheet_bool(
                conversation_data.get("intermediador", False)
            ),
            "inventario_flag": parse_spreadsheet_bool(
                conversation_data.get("imovel_em_inventario", False)
            ),
            "standby": bool(
                conversation_data.get("standby_reason", "")
            ),  # True if standby_reason exists
            "razao_standby": parse_spreadsheet_list(
                conversation_data.get("standby_reason", "")
            ),
            "followup_date": conversation_data.get("fup_date", ""),
            "familiares": "",
            "IMOVEIS": "",
            "IDADE": "",
            "OBITO_PROVAVEL": "",
            "expected_name": conversation_data.get("Nome", ""),
            "percepcao_valor_esperado": conversation_data.get(
                "percepcao_valor_esperado", ""
            ),
            "imovel_em_inventario": conversation_data.get("imovel_em_inventario", ""),
            "PictureUrl": conversation_data.get("PictureUrl", ""),
        }

        # Create a dataframe with this single conversation
        st.session_state.master_df = pd.DataFrame([mapped_data])

        # Initialize display format for follow-up date if it exists
        if mapped_data.get("followup_date"):
            try:
                from datetime import datetime

                date_obj = datetime.strptime(
                    mapped_data["followup_date"], "%Y-%m-%d"
                ).date()
                days_pt = [
                    "Segunda",
                    "Terça",
                    "Quarta",
                    "Quinta",
                    "Sexta",
                    "Sábado",
                    "Domingo",
                ]
                day_name = days_pt[date_obj.weekday()]
                display_format = f"{date_obj.strftime('%d/%m/%Y')} ({day_name})"
                st.session_state["followup_date_display_0"] = display_format
            except:
                pass  # If date format is invalid, just skip

        # Also set as original data
        st.session_state.original_db_data = st.session_state.master_df.copy()

        # Clear the conversation data from session state so it doesn't persist
        del st.session_state.processor_conversation_data

        st.info("📝 Processing conversation from Conversations page")

    else:
        # Normal initialization - load from deepseek_results with error handling
        try:
            if "master_df" not in st.session_state:
                st.session_state.master_df = load_data()

            # Initialize original_db_data (store the original database values)
            if "original_db_data" not in st.session_state:
                st.session_state.original_db_data = load_data()
        except Exception as e:
            st.error(f"🚨 **PRODUCTION ERROR - Data Loading Failed**")
            st.error(f"**Error:** {str(e)}")
            
            with st.expander("🔍 Debug Information", expanded=False):
                st.write(f"**Error type:** {type(e).__name__}")
                st.write(f"**Error message:** {str(e)}")
                st.write(f"**Session state keys:** {list(st.session_state.keys())}")
                
                if DEBUG:
                    st.exception(e)
            
            st.write("**Possible Causes:**")
            st.write("• Database connection issues")
            st.write("• Missing Google Drive credentials")
            st.write("• Network connectivity problems")
            st.write("• Database file corruption")
            
            st.stop()

    # Initialize original_values storage
    if "original_values" not in st.session_state:
        st.session_state.original_values = {}

    # Ensure all required columns exist in master_df
    required_columns = {
        "acoes_urblink": [],
        "status_urblink": "",
        "razao_standby": [],
        "obs": "",
        "stakeholder": False,
        "intermediador": False,
        "inventario_flag": False,
        "standby": False,
    }

    for col, default_value in required_columns.items():
        if col not in st.session_state.master_df.columns:
            if isinstance(default_value, list):
                st.session_state.master_df[col] = [[]] * len(st.session_state.master_df)
            else:
                st.session_state.master_df[col] = [default_value] * len(
                    st.session_state.master_df
                )

    # Also ensure original_db_data has the same columns
    for col, default_value in required_columns.items():
        if col not in st.session_state.original_db_data.columns:
            if isinstance(default_value, list):
                st.session_state.original_db_data[col] = [[]] * len(
                    st.session_state.original_db_data
                )
            else:
                st.session_state.original_db_data[col] = [default_value] * len(
                    st.session_state.original_db_data
                )

    # Initialize index
    if "selected_idx" in st.session_state:
        st.session_state.idx = st.session_state.selected_idx
        del st.session_state.selected_idx
    elif "idx" not in st.session_state:
        st.session_state.idx = 0


def store_original_values(idx, row):
    """Store original values for a record if not already stored."""
    if idx not in st.session_state.original_values:
        # Store original values from the database, not current state
        original_row = st.session_state.original_db_data.iloc[idx]

        # Handle list fields properly - they might come as strings from database
        def parse_list_field(value):
            if isinstance(value, str):
                # Try to parse as JSON or comma-separated values
                import json

                try:
                    return json.loads(value) if value else []
                except:
                    return (
                        [v.strip() for v in value.split(",") if v.strip()]
                        if value
                        else []
                    )
            elif isinstance(value, list):
                return value
            elif pd.isna(value) or value is None:
                return []
            else:
                return []

        # Handle boolean fields properly
        def parse_bool_field(value):
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ["true", "1", "yes", "on"]
            elif pd.isna(value) or value is None:
                return False
            else:
                return bool(value)

        st.session_state.original_values[idx] = {
            "classificacao": original_row.get("classificacao", "") or "",
            "intencao": original_row.get("intencao", "") or "",
            "acoes_urblink": parse_list_field(original_row.get("acoes_urblink")),
            "status_urblink": original_row.get("status_urblink", "") or "",
            "pagamento": original_row.get("pagamento", "") or "",
            "percepcao_valor_esperado": original_row.get("percepcao_valor_esperado", "")
            or "",
            "razao_standby": parse_list_field(original_row.get("razao_standby")),
            "resposta": original_row.get("resposta", "") or "",
            "obs": original_row.get("obs", "") or "",
            "stakeholder": parse_bool_field(original_row.get("stakeholder")),
            "intermediador": parse_bool_field(original_row.get("intermediador")),
            "inventario_flag": parse_bool_field(original_row.get("inventario_flag")),
            "standby": parse_bool_field(original_row.get("standby")),
            "followup_date": original_row.get("followup_date", "") or "",
        }


def reset_to_original(idx):
    """Reset all fields to original AI values."""
    if idx in st.session_state.original_values:
        original = st.session_state.original_values[idx]
        for field, value in original.items():
            if field in st.session_state.master_df.columns:
                st.session_state.master_df.at[idx, field] = value
        # Also clear any widget state for this record
        widget_keys = [
            f"classificacao_select_{idx}",
            f"intencao_select_{idx}",
            f"acoes_select_{idx}",
            f"status_select_{idx}",
            f"pagamento_select_{idx}",
            f"percepcao_select_{idx}",
            f"razao_select_{idx}",
            f"resposta_input_{idx}",
            f"obs_input_{idx}",
            f"stakeholder_input_{idx}",
            f"intermediador_input_{idx}",
            f"inventario_input_{idx}",
            f"standby_input_{idx}",
        ]
        for key in widget_keys:
            if key in st.session_state:
                del st.session_state[key]


def update_field(idx, field, value):
    """Update a field value directly in master_df."""
    
    # Prevent updates during widget initialization to avoid false change detection
    initialization_key = f"initializing_widgets_{idx}"
    if st.session_state.get(initialization_key, False):
        # During initialization, only skip if the value hasn't actually changed
        # This allows real user changes to be processed even during initialization phase
        if idx in st.session_state.original_values:
            original = st.session_state.original_values[idx]
            if field in original and compare_values(original[field], value):
                return  # Skip update - value is same as original (initialization)
        # If we reach here during initialization, it means the value actually changed
        # So we allow the update to proceed
    # Ensure the column exists in master_df
    if field not in st.session_state.master_df.columns:
        # Add missing column with default values
        if field in ["acoes_urblink", "razao_standby"]:
            st.session_state.master_df[field] = [[]] * len(st.session_state.master_df)
        elif field in ["stakeholder", "intermediador", "inventario_flag", "standby"]:
            st.session_state.master_df[field] = [False] * len(
                st.session_state.master_df
            )
        else:
            st.session_state.master_df[field] = [""] * len(st.session_state.master_df)

    # Convert column to object dtype if necessary to avoid dtype warnings
    if st.session_state.master_df[field].dtype != "object":
        st.session_state.master_df[field] = st.session_state.master_df[field].astype(
            "object"
        )

    st.session_state.master_df.at[idx, field] = value


def compare_values(original, current):
    """Compare two values, handling lists and different types properly."""
    # Handle None values first
    if original is None:
        original = (
            []
            if isinstance(current, list)
            else (False if isinstance(current, bool) else "")
        )
    if current is None:
        current = (
            []
            if isinstance(original, list)
            else (False if isinstance(original, bool) else "")
        )

    # Handle NaN values for non-list types only
    try:
        if not isinstance(original, list) and pd.isna(original):
            original = (
                []
                if isinstance(current, list)
                else (False if isinstance(current, bool) else "")
            )
    except (TypeError, ValueError):
        pass

    try:
        if not isinstance(current, list) and pd.isna(current):
            current = (
                []
                if isinstance(original, list)
                else (False if isinstance(original, bool) else "")
            )
    except (TypeError, ValueError):
        pass

    # Handle list comparison
    if isinstance(original, list) and isinstance(current, list):
        return sorted([str(x) for x in original]) == sorted([str(x) for x in current])
    elif isinstance(original, list) and not isinstance(current, list):
        return False
    elif not isinstance(original, list) and isinstance(current, list):
        return False
    # Handle boolean comparison properly
    elif isinstance(original, bool) and isinstance(current, bool):
        return original == current
    elif isinstance(original, bool) or isinstance(current, bool):
        # Convert both to boolean for comparison
        def to_bool(value):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.upper() in ["TRUE", "1", "YES", "ON"]
            return bool(value)
        return to_bool(original) == to_bool(current)
    else:
        return str(original) == str(current)


# ─── STATE INIT ─────────────────────────────────────────────────────────────
# Initialize session state
initialize_session_state()

# Work with master_df
df = st.session_state.master_df

# ─── AUTO-LOADING CONVERSATION WITH ERROR HANDLING ───────────────────────
# Always check current URL for conversation_id (handles navigation)
current_url_conversation_id = st.query_params.get("conversation_id", None)
auto_load_target = auto_load_conversation or current_url_conversation_id

# If we have navigation context, we can't rely on dataframe index since it only has 1 row
# Instead, we'll create a synthetic row for the current conversation
if ("processor_navigation_context" in st.session_state and 
    st.session_state.processor_navigation_context.get("from_conversations_page", False) and
    current_url_conversation_id):
    
    # Load the full data to find the specific conversation
    full_df = load_data()  # This loads all conversations
    
    # Check which column exists and search appropriately, using phone matching for better results
    current_conversation_match = pd.DataFrame()
    
    # Try exact conversation_id match first
    if "conversation_id" in full_df.columns:
        current_conversation_match = full_df[
            full_df["conversation_id"] == current_url_conversation_id
        ]
    
    # If no exact match found and it looks like a phone number, try phone matching
    if current_conversation_match.empty and ("@s.whatsapp.net" in current_url_conversation_id or current_url_conversation_id.isdigit()):
        # Extract phone number and try phone matching
        target_phone = current_url_conversation_id.split('@')[0] if '@' in current_url_conversation_id else current_url_conversation_id
        
        # DEBUG: Show phone matching process
        print(f"🔍 PHONE MATCHING DEBUG:")
        print(f"   - Target phone: {target_phone}")
        
        # Try matching against whatsapp_number column with variants
        if "whatsapp_number" in full_df.columns:
            from services.phone_utils import generate_phone_variants, clean_phone_for_matching
            variants = generate_phone_variants(target_phone)
            print(f"   - Generated variants: {variants}")
            
            for variant in variants:
                print(f"   - Trying variant: {variant}")
                matches = full_df[full_df["whatsapp_number"].apply(
                    lambda x: clean_phone_for_matching(str(x)) == clean_phone_for_matching(variant)
                )]
                if not matches.empty:
                    print(f"   - ✅ MATCH FOUND with variant: {variant}")
                    current_conversation_match = matches
                    break
                else:
                    print(f"   - ❌ No match for variant: {variant}")
            
            if current_conversation_match.empty:
                print(f"   - ❌ NO MATCHES FOUND for any variant")
                # Show what phone numbers are actually available for comparison
                available_phones = full_df["whatsapp_number"].head(10).tolist()
                print(f"   - Available phones (first 10): {available_phones}")
                # Show cleaned versions for comparison
                cleaned_available = [clean_phone_for_matching(str(p)) for p in available_phones]
                print(f"   - Cleaned available: {cleaned_available}")
                cleaned_variants = [clean_phone_for_matching(v) for v in variants]
                print(f"   - Cleaned variants: {cleaned_variants}")
    
    # Fallback: try exact whatsapp_number match if exists
    if current_conversation_match.empty and "whatsapp_number" in full_df.columns:
        current_conversation_match = full_df[
            full_df["whatsapp_number"] == current_url_conversation_id
        ]
    
    if not current_conversation_match.empty:
        # Replace the master_df with just this conversation for display
        st.session_state.master_df = current_conversation_match.copy()
        df = st.session_state.master_df  # Update our local reference
        st.session_state.idx = 0  # Always index 0 since we have just one row
        
        # ALWAYS show debug info to track what's happening
        print(f"✅ NAVIGATION SUCCESS: Found and loaded conversation {current_url_conversation_id}")
        print(f"   - Conversation data shape: {current_conversation_match.shape}")
        print(f"   - Contact name: {current_conversation_match.iloc[0].get('display_name', 'N/A')}")
        print(f"   - Phone: {current_conversation_match.iloc[0].get('whatsapp_number', 'N/A')}")
        
        if DEBUG:
            st.success(f"✅ Navigation: Loaded conversation {current_url_conversation_id}")
    else:
        # ALWAYS show debug info when conversation not found
        print(f"❌ NAVIGATION FAILED: Conversation {current_url_conversation_id} NOT FOUND")
        print(f"   - Full dataframe shape: {full_df.shape}")
        print(f"   - Available columns: {list(full_df.columns)}")
        if "whatsapp_number" in full_df.columns:
            print(f"   - Sample whatsapp_numbers: {full_df['whatsapp_number'].head().tolist()}")
        
        if DEBUG:
            st.warning(f"⚠️ Navigation: Conversation {current_url_conversation_id} not found")

# Fallback: Standard auto-loading for cases without navigation context
elif auto_load_target:
    try:
        # Search for the conversation by conversation_id or whatsapp_number
        if "conversation_id" in df.columns:
            matching_conversations = df[df["conversation_id"] == auto_load_target]
        else:
            matching_conversations = df[df["whatsapp_number"] == auto_load_target]

        if not matching_conversations.empty:
            # Found the conversation, set idx to its position
            conversation_idx = matching_conversations.index[0]
            st.session_state.idx = conversation_idx
            if DEBUG:
                st.success(f"✅ Successfully loaded conversation: {auto_load_target}")
        else:
            if DEBUG:
                st.warning(
                    f"⚠️ Conversation {auto_load_target} not found in current dataset"
                )
                
                # Show debug info for missing conversation
                with st.expander("🔍 Debug: Why conversation not found?", expanded=False):
                    st.write(f"**Looking for:** {auto_load_target}")
                    st.write(f"**DataFrame shape:** {df.shape}")
                    st.write(f"**Available columns:** {list(df.columns)}")
                    
                    if 'whatsapp_number' in df.columns:
                        unique_numbers = df['whatsapp_number'].unique()
                        st.write(f"**Total unique phone numbers:** {len(unique_numbers)}")
                        st.write(f"**Sample phone numbers:** {unique_numbers[:10].tolist()}")
                        
                        # Check if it's a partial match issue
                        partial_matches = df[df['whatsapp_number'].str.contains(auto_load_conversation[-8:], na=False)]
                        if not partial_matches.empty:
                            st.write(f"**Partial matches found:** {len(partial_matches)}")
                            st.write(f"**Partial matches:** {partial_matches['whatsapp_number'].tolist()}")
                        
    except Exception as e:
        st.error(f"🚨 **Error loading conversation {auto_load_conversation}**")
        st.error(f"**Error:** {str(e)}")
        
        with st.expander("🔍 Conversation Loading Error Details", expanded=True):
            st.write(f"**Target conversation:** {auto_load_conversation}")
            st.write(f"**Error type:** {type(e).__name__}")
            
            if 'df' in locals():
                st.write(f"**DataFrame loaded:** Yes, shape: {df.shape}")
                st.write(f"**DataFrame columns:** {list(df.columns)}")
            else:
                st.write("**DataFrame loaded:** No")
                
            if DEBUG:
                st.exception(e)
                
        # Don't stop the app, just use default index
        if "idx" not in st.session_state:
            st.session_state.idx = 0

# Ensure idx is within bounds
st.session_state.idx = min(st.session_state.idx, len(df) - 1)
idx = st.session_state.idx

# Get current row
row = df.iloc[idx]

# Handle pending conversation_id from navigation
if "pending_conversation_id" in st.session_state:
    pending_id = st.session_state.pending_conversation_id
    st.query_params["conversation_id"] = pending_id
    del st.session_state.pending_conversation_id

# Update URL with current conversation_id if not already set
elif not auto_load_conversation:
    # Use whatsapp_number as conversation_id if conversation_id column doesn't exist
    conversation_id = row.get("conversation_id", row.get("whatsapp_number", ""))
    if conversation_id:
        st.query_params["conversation_id"] = conversation_id

# Store original values for this record
store_original_values(idx, row)

# Normalize odd column name
if "OBITO PROVAVEL" in df.columns and "OBITO_PROVAVEL" not in df.columns:
    df = df.rename(columns={"OBITO PROVAVEL": "OBITO_PROVAVEL"})
    st.session_state.master_df = df

# ─── HEADER & PROGRESS ──────────────────────────────────────────────────────
_, progress_col, _ = st.columns([1, 2, 1])
with progress_col:
    # Check if we have navigation context from Conversations page
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        # Get current conversation ID from URL (most up-to-date after navigation)
        current_conversation_id = st.query_params.get("conversation_id", row.get("conversation_id", row.get("whatsapp_number", "")))
        
        # Find current position in filtered results
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id) + 1
            total_filtered = len(conversation_ids)
            st.progress(current_position / total_filtered)
            st.caption(f"{current_position}/{total_filtered} mensagens processadas (filtradas)")
        else:
            # Fallback to original behavior
            st.progress((idx + 1) / len(df))
            st.caption(f"{idx + 1}/{len(df)} mensagens processadas")
    else:
        # Original behavior when not coming from Conversations page
        st.progress((idx + 1) / len(df))
        st.caption(f"{idx + 1}/{len(df)} mensagens processadas")
st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)

# Dashboard navigation moved to bottom

st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)


# ─── NAVIGATION TOP ─────────────────────────────────────────────────────────
def goto_prev():
    """Go to the previous conversation."""
    # Check if we have navigation context from Conversations page
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        # Use navigation context instead of limited dataframe
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        current_conversation_id = st.query_params.get("conversation_id", "")
        
        print(f"🔍 GOTO_PREV - Using navigation context with {len(conversation_ids)} conversations")
        
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            print(f"🔍 GOTO_PREV - Current position: {current_position + 1}/{len(conversation_ids)}")
            
            if current_position > 0:
                # Go to previous conversation in filtered list
                prev_conversation_id = conversation_ids[current_position - 1]
                print(f"🔍 GOTO_PREV - Navigating to: {prev_conversation_id}")
                st.query_params["conversation_id"] = prev_conversation_id
                return
            else:
                print("🔍 GOTO_PREV - Already at first conversation in filtered list")
                return
        else:
            print(f"🔍 GOTO_PREV - Current conversation not found in navigation context")
    
    # Access dataframe from session state for fallback
    df = st.session_state.master_df
    
    # Debug logging
    print(f"🔍 GOTO_PREV - Fallback: Current idx: {st.session_state.idx}, Total conversations: {len(df)}")
    
    # Cleanup sync for current conversation
    current_row = df.iloc[st.session_state.idx]
    current_conversation_id = current_row.get("conversation_id", current_row.get("whatsapp_number", ""))
    if current_conversation_id:
        cleanup_sync_on_exit(current_conversation_id)
    
    # Clear conversation cache when navigating
    cache_keys_to_clear = []
    for key in st.session_state.keys():
        if any(cache_key in key.lower() for cache_key in [
            'conversation_data', 'messages_', 'chat_', 'processor_conversation',
            'current_conversation', 'conversation_history'
        ]):
            cache_keys_to_clear.append(key)
    
    for key in cache_keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    
    # Check if we have navigation context from Conversations page
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        
        # Find current position in filtered results
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            if current_position > 0:
                # Go to previous conversation in filtered list
                prev_conversation_id = conversation_ids[current_position - 1]
                
                # Find this conversation in the main dataframe
                matching_rows = df[
                    (df["conversation_id"] == prev_conversation_id) |
                    (df["whatsapp_number"] == prev_conversation_id)
                ]
                if not matching_rows.empty:
                    st.session_state.idx = matching_rows.index[0]
                    st.query_params["conversation_id"] = prev_conversation_id
                    return
    
    # Fallback to original behavior
    old_idx = st.session_state.idx
    st.session_state.idx = max(st.session_state.idx - 1, 0)
    print(f"🔍 GOTO_PREV - Changed idx from {old_idx} to {st.session_state.idx}")
    
    # Update URL with conversation_id
    new_idx = st.session_state.idx
    if new_idx < len(df):
        conversation_id = df.iloc[new_idx].get(
            "conversation_id", df.iloc[new_idx].get("whatsapp_number", "")
        )
        if conversation_id:
            st.query_params["conversation_id"] = conversation_id
            print(f"🔍 GOTO_PREV - Updated URL to conversation_id: {conversation_id}")


def goto_next():
    """Go to the next conversation."""
    # Check if we have navigation context from Conversations page
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        # Use navigation context instead of limited dataframe
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        current_conversation_id = st.query_params.get("conversation_id", "")
        
        print(f"🔍 GOTO_NEXT - Using navigation context with {len(conversation_ids)} conversations")
        
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            print(f"🔍 GOTO_NEXT - Current position: {current_position + 1}/{len(conversation_ids)}")
            
            if current_position < len(conversation_ids) - 1:
                # Go to next conversation in filtered list
                next_conversation_id = conversation_ids[current_position + 1]
                print(f"🔍 GOTO_NEXT - Navigating to: {next_conversation_id}")
                st.query_params["conversation_id"] = next_conversation_id
                return
            else:
                print("🔍 GOTO_NEXT - Already at last conversation in filtered list")
                return
        else:
            print(f"🔍 GOTO_NEXT - Current conversation not found in navigation context")
    
    # Access dataframe from session state for fallback
    df = st.session_state.master_df
    
    # Debug logging
    print(f"🔍 GOTO_NEXT - Fallback: Current idx: {st.session_state.idx}, Total conversations: {len(df)}")
    
    # Cleanup sync for current conversation
    current_row = df.iloc[st.session_state.idx]
    current_conversation_id = current_row.get("conversation_id", current_row.get("whatsapp_number", ""))
    if current_conversation_id:
        cleanup_sync_on_exit(current_conversation_id)
    
    # Clear conversation cache when navigating
    cache_keys_to_clear = []
    for key in st.session_state.keys():
        if any(cache_key in key.lower() for cache_key in [
            'conversation_data', 'messages_', 'chat_', 'processor_conversation',
            'current_conversation', 'conversation_history'
        ]):
            cache_keys_to_clear.append(key)
    
    for key in cache_keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    
    # Check if we have navigation context from Conversations page
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        
        # Find current position in filtered results
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            if current_position < len(conversation_ids) - 1:
                # Go to next conversation in filtered list
                next_conversation_id = conversation_ids[current_position + 1]
                
                # Find this conversation in the main dataframe
                matching_rows = df[
                    (df["conversation_id"] == next_conversation_id) |
                    (df["whatsapp_number"] == next_conversation_id)
                ]
                if not matching_rows.empty:
                    st.session_state.idx = matching_rows.index[0]
                    st.query_params["conversation_id"] = next_conversation_id
                    return
    
    # Fallback to original behavior
    old_idx = st.session_state.idx
    st.session_state.idx = min(st.session_state.idx + 1, len(df) - 1)
    print(f"🔍 GOTO_NEXT - Changed idx from {old_idx} to {st.session_state.idx}")
    
    # Update URL with conversation_id
    new_idx = st.session_state.idx
    if new_idx < len(df):
        conversation_id = df.iloc[new_idx].get(
            "conversation_id", df.iloc[new_idx].get("whatsapp_number", "")
        )
        if conversation_id:
            st.query_params["conversation_id"] = conversation_id
            print(f"🔍 GOTO_NEXT - Updated URL to conversation_id: {conversation_id}")


nav_prev_col, nav_property_col, nav_archive_col, nav_next_col = st.columns([1, 1, 1, 1])
with nav_prev_col:
    # Calculate prev button disabled state based on navigation context
    prev_disabled = False
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        current_conversation_id = row.get("conversation_id", row.get("whatsapp_number", ""))
        
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            prev_disabled = (current_position == 0)
        else:
            prev_disabled = True  # Not in filtered list
    else:
        prev_disabled = bool(idx == 0)  # Original behavior
    
    st.button(
        "⬅️ Anterior",
        key="top_prev",
        disabled=prev_disabled,
        on_click=goto_prev,
        use_container_width=True,
    )
with nav_property_col:
    if st.button(
        "🏢 Atribuir a Imóvel",
        key="assign_property",
        use_container_width=True,
    ):
        st.session_state.show_property_assignment = True
        st.rerun()
with nav_archive_col:
    if st.button(
        "📁 Arquivar Conversa",
        key="archive_conversation",
        use_container_width=True,
    ):
        # Get phone number from current row
        phone_number = row.get("phone_number") or row.get("whatsapp_number", "")
        conversation_id = row.get("conversation_id", row.get("whatsapp_number", ""))
        
        if phone_number:
            # Queue archive operation in background
            try:
                operation_id = queue_archive_operation(phone_number, conversation_id)
                
                # Show immediate feedback
                st.success(f"✅ Archive queued! (Operation ID: {operation_id[:8]}...)")
                st.info("📁 The conversation will be archived in the background. Check the sidebar for progress.")
                
            except Exception as e:
                st.error(f"❌ Error queueing archive operation: {e}")
                if DEBUG:
                    import traceback
                    st.write("**Full error traceback:**")
                    st.code(traceback.format_exc())
        else:
            st.error("Número de telefone não encontrado para esta conversa.")
with nav_next_col:
    # Calculate next button disabled state based on navigation context
    next_disabled = False
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        current_conversation_id = row.get("conversation_id", row.get("whatsapp_number", ""))
        
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            next_disabled = (current_position >= len(conversation_ids) - 1)
        else:
            next_disabled = True  # Not in filtered list
    else:
        next_disabled = bool(idx >= len(df) - 1)  # Original behavior
    
    st.button(
        "Próximo ➡️",
        key="top_next",
        disabled=next_disabled,
        on_click=goto_next,
        use_container_width=True,
    )

# ─── SYNC INITIALIZATION ────────────────────────────────────────────────────
# Clear old sync operations on fresh session starts
if "session_initialized" not in st.session_state:
    st.session_state.recent_sync_operations = []
    st.session_state.session_initialized = True

# Initialize auto-sync for the current conversation with fallback to URL
conversation_id = row.get("conversation_id", row.get("whatsapp_number", "")) or st.query_params.get("conversation_id", "")

# Store current conversation ID in session state
st.session_state.current_conversation_id = conversation_id

# Debug conversation ID
if DEBUG:
    st.write(f"🔍 **Sync Debug:** conversation_id = {conversation_id}")
    st.write(f"🔍 **Row keys:** {list(row.keys()) if hasattr(row, 'keys') else 'No keys'}")

if conversation_id:
    # Setup auto-sync
    setup_conversation_sync(conversation_id)
    
    # Check for sync updates and refresh if needed (only when auto-sync is enabled)
    if st.session_state.get('auto_sync_enabled', False) and check_for_sync_updates(conversation_id):
        st.rerun()
    
    
    # Setup regular auto-refresh mechanism
    setup_auto_refresh()
    
    # Render sync header
    render_sync_header(conversation_id)
    
    # Render sync sidebar controls
    render_sync_sidebar(conversation_id)
else:
    if DEBUG:
        st.error("🚨 **Sync Error:** No conversation_id found!")
        st.write(f"Row content: {dict(row) if hasattr(row, 'keys') else str(row)}")

# ─── PRIORITY LOADING LAYOUT ────────────────────────────────────────────────
# Create empty containers for priority loading
left_col, right_col = st.columns([1, 1])

# Priority containers (empty initially, filled in order of importance)
with left_col:
    contact_container = st.empty()  # Load last: Contact info (slower due to image loading)
with right_col:
    chat_container = st.empty()     # Load first: Chat history (most important for user)

# Classification section - load second
classification_container = st.empty()

# ─── PRIORITY 1: CHAT HISTORY (Most important for user) ────────────────────────
with chat_container.container():
    # ─── CHAT HISTORY ───────────────────────────────────────────────────────────
    
    # Simple sync status indicator + Console logging for production debugging
    if conversation_id and st.session_state.get('auto_sync_enabled', True):
        sync_status = get_sync_status(conversation_id)
        
        # Add JavaScript console logging for production debugging
        st.markdown(f"""
        <script>
        // Production sync debugging - visible in Chrome DevTools Console
        console.log('🔍 SYNC DEBUG - Conversation ID: {conversation_id}');
        console.log('🔍 SYNC DEBUG - Sync Status:', {dict(sync_status)});
        console.log('🔍 SYNC DEBUG - Auto-sync enabled:', {st.session_state.get('auto_sync_enabled', True)});
        console.log('🔍 SYNC DEBUG - Current URL:', window.location.href);
        console.log('🔍 SYNC DEBUG - Timestamp:', new Date().toISOString());
        </script>
        """, unsafe_allow_html=True)
        
        if sync_status.get("active", False):
            next_sync = sync_status.get("next_sync_in", 0)
            if next_sync > 0:
                st.info(f"🔄 Auto-sync active • Next check in {int(next_sync)} seconds")
            else:
                st.success("🔄 Auto-sync active • Checking for updates...")
        else:
            st.warning("⏸️ Auto-sync inactive")

    # Parse the conversation history and display in WhatsApp style
    try:
        # First try to load messages from database if we have a conversation_id
        messages = []
        conversation_id = row.get("conversation_id")
        
        # Conversation header with sync status
        col1, col2 = st.columns([3, 1])

        with col2:
            # Show sync status if available
            if conversation_id:
                from services.conversation_sync import get_sync_status
                sync_status = get_sync_status(conversation_id)
                if sync_status.get("active", False):
                    next_sync = sync_status.get("next_sync_in", 0)
                    if next_sync > 0:
                        st.caption(f"🔄 Sync in {int(next_sync)}s")
                    else:
                        st.caption("🔄 Syncing...")
                else:
                    st.caption("⏸️ Sync off")
        
        # Create cache key for this conversation
        cache_key = f"messages_{conversation_id}_{idx}"
        
        # Check if conversation data was cleared by sync (force reload)
        force_reload = 'conversation_data' not in st.session_state
        
        # Debug information for conversation loading
        if DEBUG:
            st.write(f"🔍 **Debug - Conversation Loading:**")
            st.write(f"- **Phone:** {row.get('whatsapp_number', 'N/A')}")
            st.write(f"- **Conversation ID:** {conversation_id}")
            st.write(f"- **Row index:** {idx}")
            st.write(f"- **DataFrame shape:** {df.shape}")
        
        if "conversation_id" in row.index and pd.notna(conversation_id):
            try:
                if DEBUG:
                    st.write(f"⏳ Attempting to load messages for conversation: {conversation_id}")
                
                messages_df = get_conversation_messages(conversation_id)
                
                if DEBUG:
                    st.write(f"✅ Messages loaded successfully. Shape: {messages_df.shape if not messages_df.empty else 'Empty DataFrame'}")
                
                if not messages_df.empty:
                    # Convert database messages to the expected format
                    for msg_idx, msg_row in messages_df.iterrows():
                        try:
                            sender = (
                                "Urb.Link"
                                if msg_row.get("from_me", False)
                                else row.get("display_name", "Contact")
                            )
                            messages.append(
                                {
                                    "sender": sender,
                                    "msg": msg_row.get("message_text", ""),
                                    "ts": datetime.fromtimestamp(
                                        msg_row.get("timestamp", 0)
                                    ).strftime("%d/%m/%Y %H:%M"),
                                }
                            )
                        except Exception as msg_error:
                            st.error(f"❌ **Error processing message {msg_idx}:** {str(msg_error)}")
                            st.write(f"**Message data:** {dict(msg_row)}")
                            if DEBUG:
                                st.exception(msg_error)
                            
            except Exception as e:
                st.error(f"🚨 **CRITICAL ERROR loading conversation messages**")
                st.error(f"**Conversation ID:** {conversation_id}")
                st.error(f"**Error type:** {type(e).__name__}")
                st.error(f"**Error message:** {str(e)}")
                
                with st.expander("🔍 Detailed Error Information", expanded=True):
                    st.write(f"**Phone number:** {row.get('whatsapp_number', 'N/A')}")
                    st.write(f"**Display name:** {row.get('display_name', 'N/A')}")
                    st.write(f"**Row data keys:** {list(row.keys())}")
                    
                    # Show the actual conversation_id value and type
                    st.write(f"**Conversation ID type:** {type(conversation_id)}")
                    st.write(f"**Conversation ID repr:** {repr(conversation_id)}")
                    
                    if DEBUG:
                        st.exception(e)
                
                # Don't crash the app, just continue without messages
                messages = []

        # If no messages from database, fall back to parsed conversation history
        if (
            not messages
            and "conversation_history" in row.index
            and pd.notna(row.get("conversation_history"))
            and row.get("conversation_history")
        ):
            messages = parse_chat(row.get("conversation_history", ""))
        elif not messages:
            # No conversation history available
            messages = []

        if messages:
            # Build complete HTML like in the old Processor page, but with WhatsApp styling
            chat_html = "<div style='height: 840px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 5px; background-color: #f9f9f9;'>"

            # Display messages in WhatsApp style with date headers
            last_date = None

            for msg in messages:
                # Parse the timestamp to get date
                dt = None
                try:
                    # Try to parse different timestamp formats
                    timestamp_str = msg["ts"].strip()

                    # Debug: let's see what format we're dealing with
                    if DEBUG:
                        print(f"DEBUG: Parsing timestamp: '{timestamp_str}'")

                    # Try various common formats
                    formats_to_try = [
                        "%d/%m/%Y %H:%M",  # 25/06/2025 15:30
                        "%Y-%m-%d %H:%M",  # 2025-06-25 15:30
                        "%d/%m/%Y %H:%M:%S",  # 25/06/2025 15:30:45
                        "%Y-%m-%d %H:%M:%S",  # 2025-06-25 15:30:45
                        "%H:%M",  # 15:30 (time only)
                        "%d/%m %H:%M",  # 25/06 15:30 (no year)
                    ]

                    for fmt in formats_to_try:
                        try:
                            dt = datetime.strptime(timestamp_str, fmt)
                            if fmt == "%H:%M":
                                # If only time, assume today
                                dt = dt.replace(
                                    year=datetime.now().year,
                                    month=datetime.now().month,
                                    day=datetime.now().day,
                                )
                            elif fmt == "%d/%m %H:%M":
                                # If no year, assume current year
                                dt = dt.replace(year=datetime.now().year)
                            break
                        except ValueError:
                            continue

                    if dt:
                        current_date = dt.date()

                        # Check if we need a date header
                        if last_date != current_date:
                            # Create date header in format "25 de Junho, 2025 (Terça-Feira)"
                            today = datetime.now().date()
                            from datetime import timedelta

                            if current_date == today:
                                date_header = "Hoje"
                            elif current_date == today - timedelta(days=1):
                                date_header = "Ontem"
                            else:
                                # Portuguese month names
                                months_pt = {
                                    1: "Janeiro",
                                    2: "Fevereiro",
                                    3: "Março",
                                    4: "Abril",
                                    5: "Maio",
                                    6: "Junho",
                                    7: "Julho",
                                    8: "Agosto",
                                    9: "Setembro",
                                    10: "Outubro",
                                    11: "Novembro",
                                    12: "Dezembro",
                                }

                                # Portuguese weekday names
                                weekdays_pt = {
                                    0: "Segunda-feira",
                                    1: "Terça-feira",
                                    2: "Quarta-feira",
                                    3: "Quinta-feira",
                                    4: "Sexta-feira",
                                    5: "Sábado",
                                    6: "Domingo",
                                }

                                day = dt.day
                                month = months_pt[dt.month]
                                year = dt.year
                                weekday = weekdays_pt[dt.weekday()]

                                # Format: "25 de Junho, 2025 (Terça-Feira)"
                                date_header = f"{day} de {month}, {year} ({weekday})"

                            # Add date header to HTML
                            chat_html += f'<div style="text-align: center; margin: 20px 0 10px 0;"><span style="background-color: #e0e0e0; padding: 5px 15px; border-radius: 15px; font-size: 12px; color: #666;">{date_header}</span></div>'
                            last_date = current_date

                        # Format message time (only HH:MM in BRT)
                        msg_time = dt.strftime("%H:%M")
                    else:
                        # If all parsing fails, extract time manually
                        if ":" in timestamp_str:
                            time_part = (
                                timestamp_str.split()[-1]
                                if " " in timestamp_str
                                else timestamp_str
                            )
                            if ":" in time_part:
                                msg_time = time_part[:5]  # Get only HH:MM
                            else:
                                msg_time = timestamp_str
                        else:
                            msg_time = timestamp_str

                except Exception as e:
                    # If timestamp parsing fails completely, use original
                    if DEBUG:
                        print(f"DEBUG: Timestamp parsing failed: {e}")
                    msg_time = msg["ts"]

                # Determine if message is from business or contact
                is_from_me = msg["sender"] in ("Urb.Link", "Athos")

                # Process the message text but DON'T escape HTML tags (we want <strong> to work)
                clean_msg = bold_asterisks(msg["msg"])
                clean_time = msg_time

                # DEBUG: Let's see what we're actually working with
                if DEBUG:
                    print(f"DEBUG: Message content: '{msg['msg']}'")
                    print(f"DEBUG: Message length: {len(msg['msg'])}")
                    print(f"DEBUG: Clean message: '{clean_msg}'")
                    print(f"DEBUG: Clean message length: {len(clean_msg)}")

                # Create message container (WhatsApp style) - using the original approach
                if is_from_me:
                    # Message from the business/user (right side, green-ish)
                    chat_html += f"""<div style="display: flex; justify-content: flex-end; margin: 2px 0; width: 100%;">
                        <div style="background-color: #dcf8c6; padding: 8px 12px; border-radius: 18px; max-width: 400px; min-width: 120px; display: inline-block;">
                            <div style="display: inline-block; max-width: 100%;">{clean_msg}</div>
                            <div style="font-size: 11px; color: #666; text-align: right; margin-top: 2px;">{clean_time}</div>
                        </div>
                    </div>"""
                else:
                    # Message from contact (left side, white/light gray)
                    chat_html += f"""<div style="display: flex; justify-content: flex-start; margin: 2px 0; width: 100%;">
                        <div style="background-color: #ffffff; padding: 8px 12px; border-radius: 18px; max-width: 400px; min-width: 120px; border: 1px solid #e0e0e0; display: inline-block;">
                            <div style="display: inline-block; max-width: 100%;">{clean_msg}</div>
                            <div style="font-size: 11px; color: #666; text-align: right; margin-top: 2px;">{clean_time}</div>
                        </div>
                    </div>"""

            # Close the scrollable container
            chat_html += "</div>"

            # Display the complete chat HTML (same approach as original Processor)
            st.markdown(chat_html, unsafe_allow_html=True)
        else:
            st.info("No conversation history available.")

    except Exception as e:
        st.error(f"Error displaying conversation history: {e}")
        st.info("Could not parse conversation history.")

# ─── PRIORITY 2: CLASSIFICATION (User needs this for processing) ────────────────
with classification_container.container():
    st.markdown("---")
    
    # ─── CLASSIFICAÇÃO & RESPOSTA ───────────────────────────────────────────────
    st.subheader("📝 Classificação e Resposta")
    
    # Set initialization flag to prevent false change detection during widget creation
    initialization_key = f"initializing_widgets_{idx}"
    st.session_state[initialization_key] = True
    
    # Create two columns for presets and racional
    preset_col, racional_col = st.columns([1, 1])
    
    with preset_col:
        # Presets dropdown (smaller section)
        preset_selected = st.selectbox(
            "Respostas Prontas",
            options=list(PRESET_RESPONSES.keys()),
            format_func=lambda tag: tag or "-- selecione uma resposta pronta --",
            key=f"preset_key_{idx}",  # Unique key per record
        )
    
    with racional_col:
        # Racional in a compact yellow box
        # Get the AI reasoning from the correct column - check multiple possible column names
        ai_reasoning = row.get('Razao', row.get('standby_reason', row.get('razao_standby', '')))
        
        st.markdown(
            f"""
        <div style="margin-top: 25px;">
            <strong>📋 Racional usado pela AI classificadora:</strong><br>
            <div class='reason-box' style="margin-top: 5px; font-size: 0.85rem; max-height: 100px; overflow-y: auto;">
                {ai_reasoning}
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )
    
    # Apply preset if selected
    if preset_selected and preset_selected in PRESET_RESPONSES:
        preset_data = PRESET_RESPONSES[preset_selected]
        # Apply preset values directly to master_df
        for field, value in preset_data.items():
            update_field(idx, field, value)
        # Only rerun if actually applying a preset (not on initial load)
        if f"preset_applied_{idx}" not in st.session_state:
            st.session_state[f"preset_applied_{idx}"] = True
            st.rerun()
    
    left_col, right_col = st.columns(2)
    
    with left_col:
        # Classificação - Fix field mapping to use correct spreadsheet column
        current_classificacao = row.get("Classificação do dono do número", "") or row.get("classificacao", "")
        print(f"🔍 TERMINAL DEBUG: Widget loading - classificacao from row: {repr(current_classificacao)}")  # Terminal debug
        classificacao_index = (
            CLASSIFICACAO_OPTS.index(current_classificacao)
            if current_classificacao in CLASSIFICACAO_OPTS
            else 0
        )
        classificacao_sel = st.selectbox(
            "🏷️ Classificação",
            CLASSIFICACAO_OPTS,
            index=classificacao_index,
            key=f"classificacao_select_{idx}",
            on_change=lambda: update_field(
                idx, "classificacao", st.session_state[f"classificacao_select_{idx}"]
            ),
        )
    
        # Intenção - Fix field mapping to use correct spreadsheet column
        current_intencao = row.get("status_manual", "") or row.get("intencao", "")
        intencao_index = (
            INTENCAO_OPTS.index(current_intencao)
            if current_intencao in INTENCAO_OPTS
            else 0
        )
        intencao_sel = st.selectbox(
            "🔍 Intenção",
            INTENCAO_OPTS,
            index=intencao_index,
            key=f"intencao_select_{idx}",
            on_change=lambda: update_field(
                idx, "intencao", st.session_state[f"intencao_select_{idx}"]
            ),
        )
    
        # Ações Urb.Link
        current_acoes = row.get("acoes_urblink", [])
        if isinstance(current_acoes, str):
            import json
    
            try:
                current_acoes = json.loads(current_acoes) if current_acoes else []
            except:
                current_acoes = (
                    [v.strip() for v in current_acoes.split(",") if v.strip()]
                    if current_acoes
                    else []
                )
        elif not isinstance(current_acoes, list):
            current_acoes = []
    
        def on_acoes_change():
            new_value = st.session_state[f"acoes_select_{idx}"]
            update_field(idx, "acoes_urblink", new_value)
            dbg(f"Acoes updated to: {new_value}")
    
        acoes_sel = st.multiselect(
            "📞 Ações Urb.Link",
            ACOES_OPTS,
            default=current_acoes,
            key=f"acoes_select_{idx}",
            on_change=on_acoes_change,
        )
    
        # Status Urb.Link
        status_opts = [""] + STATUS_URBLINK_OPTS
        current_status = row.get("status_urblink", "")
        status_index = (
            status_opts.index(current_status) if current_status in status_opts else 0
        )
    
        def on_status_change():
            new_value = st.session_state[f"status_select_{idx}"]
            update_field(idx, "status_urblink", new_value)
            dbg(f"Status updated to: {new_value}")
    
        status_sel = st.selectbox(
            "🚦 Status Urb.Link",
            status_opts,
            index=status_index,
            key=f"status_select_{idx}",
            on_change=on_status_change,
        )
    
        # Helper function to safely split comma-separated values
        def safe_split_csv(value):
            """Safely split a value into a list, handling various data types."""
            if pd.isna(value) or value is None:
                return []
            value_str = str(value).strip()
            return (
                [v.strip() for v in value_str.split(",") if v.strip()] if value_str else []
            )
    
        # Forma de Pagamento
        current_pagamento = row.get("pagamento", "")
        pag_default = safe_split_csv(current_pagamento)
        pagamento_sel = st.multiselect(
            "💳 Forma de Pagamento",
            PAGAMENTO_OPTS,
            default=pag_default,
            key=f"pagamento_select_{idx}",
            on_change=lambda: update_field(
                idx, "pagamento", ", ".join(st.session_state[f"pagamento_select_{idx}"])
            ),
        )
    
        # Percepção de Valor
        percepcao_opts = [""] + PERCEPCAO_OPTS
        current_percepcao = row.get("percepcao_valor_esperado", "")
        percepcao_index = (
            percepcao_opts.index(current_percepcao)
            if current_percepcao in percepcao_opts
            else 0
        )
        percepcao_sel = st.selectbox(
            "💎 Percepção de Valor",
            percepcao_opts,
            index=percepcao_index,
            key=f"percepcao_select_{idx}",
            on_change=lambda: update_field(
                idx, "percepcao_valor_esperado", st.session_state[f"percepcao_select_{idx}"]
            ),
        )
    
        # Razão Stand-by
        current_razao = row.get("razao_standby", [])
        if isinstance(current_razao, str):
            import json
    
            try:
                current_razao = json.loads(current_razao) if current_razao else []
            except:
                current_razao = (
                    [v.strip() for v in current_razao.split(",") if v.strip()]
                    if current_razao
                    else []
                )
        elif not isinstance(current_razao, list):
            current_razao = []
    
        def on_razao_change():
            new_value = st.session_state[f"razao_select_{idx}"]
            update_field(idx, "razao_standby", new_value)
            dbg(f"Razao updated to: {new_value}")
    
        razao_sel = st.multiselect(
            "🤔 Razão Stand-by",
            STANDBY_REASONS,
            default=current_razao,
            key=f"razao_select_{idx}",
            on_change=on_razao_change,
        )
    
    with right_col:
        # Resposta - check both possible column names 
        current_resposta = row.get("resposta", "")
        resposta_input = st.text_area(
            "✏️ Resposta",
            value=current_resposta,
            height=180,
            key=f"resposta_input_{idx}",
            on_change=lambda: update_field(
                idx, "resposta", st.session_state[f"resposta_input_{idx}"]
            ),
        )
    
        # Send button for the message
        if st.button("📤 Enviar Mensagem", key=f"send_btn_{idx}"):
            if resposta_input.strip():
                # Get phone number from the conversation data
                phone_number = row.get("phone_number", "")
                client_name = row.get("name", "")
    
                # Show loading spinner
                with st.spinner("Enviando mensagem..."):
                    result = send_whatsapp_message(
                        phone_number=phone_number,
                        message_content=resposta_input,
                        client_name=client_name,
                    )
    
                # Show result
                if result["success"]:
                    st.success("✅ Mensagem enviada com sucesso!")
                else:
                    # Show detailed error information
                    st.error("❌ Erro ao enviar mensagem:")
    
                    # Create expandable section with full API response details
                    with st.expander("🔍 Detalhes do Erro (clique para expandir)"):
                        st.write("**Status Code:**", result.get("status_code", "N/A"))
                        st.write("**API Success:**", result.get("api_success", "N/A"))
                        st.write("**API Message:**", result.get("api_message", "N/A"))
                        st.write("**API Errors:**", result.get("api_errors", "N/A"))
                        st.write("**Raw API Response:**")
                        st.code(result.get("api_response", "N/A"))
                        if result.get("error"):
                            st.write("**Python Error:**", result.get("error"))
    
                        # Show what was sent to the API
                        st.write("**Dados enviados:**")
                        st.json(
                            {
                                "phone_number": phone_number,
                                "message_content": (
                                    resposta_input[:100] + "..."
                                    if len(resposta_input) > 100
                                    else resposta_input
                                ),
                                "client_name": client_name,
                            }
                        )
            else:
                st.warning("⚠️ Por favor, digite uma mensagem antes de enviar.")
    
        # OBS - check both possible column names (obs from database, OBS from spreadsheet)
        current_obs = row.get("obs", row.get("OBS", ""))
        
        # DEBUG: Show comprehensive field values comparison - original vs current
        if DEV and DEBUG:
            with st.expander("🔍 Debug - All Field Values (Original vs Current)", expanded=True):
                current_row = st.session_state.master_df.iloc[idx]
                
                # All fields we want to show in debug
                all_fields = {
                    "classificacao": "Classificação",
                    "intencao": "Intenção", 
                    "acoes_urblink": "Ações",
                    "status_urblink": "Status Urblink",
                    "pagamento": "Pagamento",
                    "percepcao_valor_esperado": "Percepção Valor",
                    "razao_standby": "Razão Standby",
                    "resposta": "Resposta",
                    "obs": "Observações",
                    "stakeholder": "Stakeholder",
                    "intermediador": "Intermediador", 
                    "inventario_flag": "Inventário",
                    "standby": "Standby",
                    "followup_date": "Follow-up",
                }
                
                if idx in st.session_state.original_values:
                    original = st.session_state.original_values[idx]
                    
                    st.write("**Field-by-field comparison:**")
                    for field, display_name in all_fields.items():
                        orig_val = original.get(field, "NOT_STORED")
                        curr_val = current_row.get(field, "NOT_FOUND")
                        
                        # Check if values match
                        if field in original:
                            matches = compare_values(orig_val, curr_val)
                            status = "✅" if matches else "❌"
                        else:
                            status = "❓"
                        
                        st.write(f"**{display_name} ({field}):** {status}")
                        st.write(f"  📋 Original: `{repr(orig_val)}` ({type(orig_val).__name__})")
                        st.write(f"  📝 Current:  `{repr(curr_val)}` ({type(curr_val).__name__})")
                        st.write("")  # Empty line for spacing
                else:
                    st.warning("⚠️ No original values stored for this conversation")
                    st.write("**Current values only:**")
                    for field, display_name in all_fields.items():
                        curr_val = current_row.get(field, "NOT_FOUND")
                        st.write(f"**{display_name} ({field}):**")
                        st.write(f"  📝 Current: `{repr(curr_val)}` ({type(curr_val).__name__})")
    
        def on_obs_change():
            new_value = st.session_state[f"obs_input_{idx}"]
            update_field(idx, "obs", new_value)
            dbg(f"OBS updated to: {new_value}")
    
        obs_input = st.text_area(
            "📋 OBS",
            value=current_obs,
            height=120,
            key=f"obs_input_{idx}",
            on_change=on_obs_change,
        )
    
        # Checkboxes
        def parse_bool_value(value):
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ["true", "1", "yes", "on"]
            elif pd.isna(value) or value is None:
                return False
            else:
                return bool(value)
    
        def on_stakeholder_change():
            new_value = st.session_state[f"stakeholder_input_{idx}"]
            update_field(idx, "stakeholder", new_value)
            dbg(f"Stakeholder updated to: {new_value}")
    
        def on_intermediador_change():
            new_value = st.session_state[f"intermediador_input_{idx}"]
            update_field(idx, "intermediador", new_value)
            dbg(f"Intermediador updated to: {new_value}")
    
        def on_inventario_change():
            new_value = st.session_state[f"inventario_input_{idx}"]
            update_field(idx, "inventario_flag", new_value)
            dbg(f"Inventario updated to: {new_value}")
    
        def on_standby_change():
            new_value = st.session_state[f"standby_input_{idx}"]
            update_field(idx, "standby", new_value)
            dbg(f"Standby updated to: {new_value}")
    
        # Create layout with checkboxes and calendar icon
        flags_col, calendar_col = st.columns([5, 1])
    
        with flags_col:
            current_stakeholder = parse_bool_value(row.get("stakeholder", False))
            stakeholder_input = st.checkbox(
                "Stakeholder",
                value=current_stakeholder,
                key=f"stakeholder_input_{idx}",
                on_change=on_stakeholder_change,
            )
    
            current_intermediador = parse_bool_value(row.get("intermediador", False))
            intermediador_input = st.checkbox(
                "Intermediador",
                value=current_intermediador,
                key=f"intermediador_input_{idx}",
                on_change=on_intermediador_change,
            )
    
            current_inventario = parse_bool_value(row.get("inventario_flag", False))
            inventario_input = st.checkbox(
                "Inventário",
                value=current_inventario,
                key=f"inventario_input_{idx}",
                on_change=on_inventario_change,
            )
    
            current_standby = parse_bool_value(row.get("standby", False))
            standby_input = st.checkbox(
                "Stand-by",
                value=current_standby,
                key=f"standby_input_{idx}",
                on_change=on_standby_change,
            )
    
        with calendar_col:
            # Calendar icon button for follow-up date
            current_followup = row.get("followup_date", "")
            current_followup_display = st.session_state.get(
                f"followup_date_display_{idx}", ""
            )
    
            if current_followup:
                button_text = "📅✅"
                # Show user-friendly format in tooltip if available, otherwise convert ISO to display format
                if current_followup_display:
                    button_help = f"Follow-up: {current_followup_display}"
                else:
                    # Convert ISO format to display format for existing data
                    try:
                        from datetime import datetime
    
                        date_obj = datetime.strptime(current_followup, "%Y-%m-%d").date()
                        days_pt = [
                            "Segunda",
                            "Terça",
                            "Quarta",
                            "Quinta",
                            "Sexta",
                            "Sábado",
                            "Domingo",
                        ]
                        day_name = days_pt[date_obj.weekday()]
                        display_format = f"{date_obj.strftime('%d/%m/%Y')} ({day_name})"
                        button_help = f"Follow-up: {display_format}"
                    except:
                        button_help = f"Follow-up: {current_followup}"
            else:
                button_text = "📅"
                button_help = "Definir data de follow-up"
    
            if st.button(button_text, key=f"calendar_btn_{idx}", help=button_help):
                st.session_state[f"show_followup_modal_{idx}"] = True
    
        # Follow-up date modal
        if st.session_state.get(f"show_followup_modal_{idx}", False):
            with st.container():
                st.markdown("---")
                st.subheader("📅 Definir Follow-up")
    
                # Follow-up input fields
                followup_col1, followup_col2 = st.columns([1, 1])
    
                with followup_col1:
                    def on_followup_amount_change():
                        # Preserve conversation context during number input changes
                        if "conversation_id" in st.query_params:
                            st.session_state["_preserved_conversation_id"] = st.query_params["conversation_id"]
                    
                    followup_amount = st.number_input(
                        "Quantidade",
                        min_value=1,
                        max_value=365,
                        value=st.session_state.get(f"followup_amount_{idx}", 1),
                        key=f"followup_amount_{idx}",
                        on_change=on_followup_amount_change,
                    )
    
                with followup_col2:
                    def on_followup_unit_change():
                        # Preserve conversation context during selectbox changes
                        if "conversation_id" in st.query_params:
                            st.session_state["_preserved_conversation_id"] = st.query_params["conversation_id"]
                    
                    followup_unit = st.selectbox(
                        "Período",
                        options=["dias", "semanas", "meses"],
                        index=["dias", "semanas", "meses"].index(
                            st.session_state.get(f"followup_unit_{idx}", "dias")
                        ),
                        key=f"followup_unit_{idx}",
                        on_change=on_followup_unit_change,
                    )
    
                # Calculate automatically when inputs change
                from datetime import datetime, timedelta
    
                try:
                    from dateutil.relativedelta import relativedelta
    
                    has_relativedelta = True
                except ImportError:
                    has_relativedelta = False
    
                # Calculate follow-up date
                today = datetime.now().date()
    
                if followup_unit == "dias":
                    target_date = today + timedelta(days=followup_amount)
                elif followup_unit == "semanas":
                    target_date = today + timedelta(weeks=followup_amount)
                elif followup_unit == "meses":
                    # Use relativedelta for accurate month calculation
                    if has_relativedelta:
                        target_date = today + relativedelta(months=followup_amount)
                    else:
                        # Fallback to approximate calculation if relativedelta is not available
                        target_date = today + timedelta(days=followup_amount * 30)
    
                # Check if it's a business day (Monday=0, Sunday=6)
                while target_date.weekday() >= 5:  # Saturday=5, Sunday=6
                    target_date += timedelta(days=1)
    
                # Create two formats: one for display and one for spreadsheet
                iso_date = target_date.strftime("%Y-%m-%d")  # For spreadsheet (2025-12-28)
                days_pt = [
                    "Segunda",
                    "Terça",
                    "Quarta",
                    "Quinta",
                    "Sexta",
                    "Sábado",
                    "Domingo",
                ]
                day_name = days_pt[target_date.weekday()]
                display_date = f"{target_date.strftime('%d/%m/%Y')} ({day_name})"  # For display (28/12/2025 (Segunda))
    
                # Update the follow-up date automatically
                current_followup_display = st.session_state.get(
                    f"followup_date_display_{idx}", ""
                )
                current_followup_iso = st.session_state.get(f"followup_date_{idx}", "")
    
                if (
                    current_followup_display != display_date
                    or current_followup_iso != iso_date
                ):
                    st.session_state[f"followup_date_display_{idx}"] = display_date
                    st.session_state[f"followup_date_{idx}"] = iso_date
                    update_field(
                        idx, "followup_date", iso_date
                    )  # Store ISO format in dataframe
    
                # Display calculated date
                if st.session_state.get(f"followup_date_display_{idx}"):
                    st.success(
                        f"📅 Follow-up agendado para: **{st.session_state[f'followup_date_display_{idx}']}**"
                    )
    
                # Action buttons
                button_col1, button_col2 = st.columns([1, 1])
    
                with button_col1:
                    if st.button("Limpar", key=f"clear_followup_{idx}"):
                        st.session_state[f"followup_date_{idx}"] = ""
                        st.session_state[f"followup_date_display_{idx}"] = ""
                        update_field(idx, "followup_date", "")
                        st.rerun()
    
                with button_col2:
                    if st.button("Fechar", key=f"close_followup_{idx}"):
                        st.session_state[f"show_followup_modal_{idx}"] = False
                        st.rerun()
    
        # Clear initialization flag - widgets are now created and initialized
        if initialization_key in st.session_state:
            del st.session_state[initialization_key]
    
        # Show modifications status - compare against original values stored at session start
        changes = {}
        
        if idx in st.session_state.original_values:
            original = st.session_state.original_values[idx]
            current_row = st.session_state.master_df.iloc[idx]
            
            # Only check fields that are synced to spreadsheet (exclude resposta and other non-synced fields)
            synced_fields = {
                "classificacao", "intencao", "acoes_urblink", "status_urblink", 
                "pagamento", "percepcao_valor_esperado", "razao_standby", "obs", 
                "stakeholder", "intermediador", "inventario_flag", "standby", "followup_date"
            }
            
            for field in synced_fields:
                if field in original:
                    curr_val = current_row.get(field, "")
                    orig_val = original[field]
                    
                    # Use the same comparison logic as the compare_values function for consistency
                    if not compare_values(orig_val, curr_val):
                        changes[field] = {
                            'original': orig_val,
                            'current': curr_val
                        }
        
        # Show pending modifications 
        if changes:
            # Create field names mapping for display - ONLY for fields that sync to spreadsheet
            field_display_names = {
                "classificacao": "Classificação",
                "intencao": "Intenção", 
                "acoes_urblink": "Ações",
                "status_urblink": "Status Urblink",
                "pagamento": "Pagamento",
                "percepcao_valor_esperado": "Percepção Valor",
                "razao_standby": "Razão Standby",
                "obs": "Observações",
                "stakeholder": "Stakeholder",
                "intermediador": "Intermediador", 
                "inventario_flag": "Inventário",
                "standby": "Standby",
                "followup_date": "Follow-up",
            }
            
            # Get display names for changed fields
            changed_field_names = []
            for field in changes.keys():
                display_name = field_display_names.get(field, field.title())
                changed_field_names.append(display_name)
            
            # Show the list of changed fields
            fields_list = ", ".join(changed_field_names)
            st.info(f"🔄 **Modificações pendentes:** {fields_list}")
            
            # Debug information showing field changes
            if DEV and DEBUG:
                with st.expander("🔍 Debug - Field Changes", expanded=True):
                    for field, values in changes.items():
                        display_name = field_display_names.get(field, field.title())
                        st.write(f"**{display_name} ({field}):**")
                        st.write(f"  - Original: `{repr(values['original'])}` ({type(values['original']).__name__})")
                        st.write(f"  - Current: `{repr(values['current'])}` ({type(values['current']).__name__})")
        else:
            st.success("✅ Sem modificações pendentes")

# ─── PRIORITY 3: CONTACT INFO (Load last, slower due to images) ─────────────────
with contact_container.container():
    # ─── CONTACT SECTION ────────────────────────────────────────────────────────
    hl_words = build_highlights(row.get("display_name", ""), row.get("expected_name", ""))

    # Create contact info HTML with fixed height
    picture = row.get("PictureUrl")
    # Clean and validate picture URL with enhanced debugging
    picture_debug = {
        'raw_value': repr(picture),
        'is_none': picture is None,
        'is_na': pd.isna(picture) if picture is not None else False,
        'stripped': str(picture).strip() if picture is not None else '',
        'conversation_id': conversation_id
    }
    
    if (
        picture
        and not pd.isna(picture)
        and str(picture).strip()
        and str(picture).strip().lower() not in ["none", "null", ""]
    ):
        picture = str(picture).strip()
        picture_debug['final_url'] = picture
        picture_debug['status'] = 'valid'
        if DEBUG:
            dbg(f"Picture URL found: {picture[:50]}...")
    else:
        picture = None
        picture_debug['final_url'] = None
        picture_debug['status'] = 'invalid'
        if DEBUG:
            dbg(f"No valid picture URL (raw value: {repr(row.get('PictureUrl'))})")
    
    # Store debug info in session state for production troubleshooting (only when DEBUG is ON)
    if DEBUG:
        if 'image_debug_log' not in st.session_state:
            st.session_state.image_debug_log = []
        if len(st.session_state.image_debug_log) < 5:  # Only keep last 5 entries
            st.session_state.image_debug_log.append(picture_debug)
        
        # Print debug info for console logging (only in DEBUG mode)
        print(f"🖼️ Image Debug: {picture_debug}")

    display_name = (
        highlight(row.get("display_name", ""), hl_words)
        if HIGHLIGHT_ENABLE
        else row.get("display_name", "")
    )
    expected_name = highlight(row.get("expected_name", ""), hl_words)
    familiares_list = parse_familiares_grouped(row.get("familiares", ""))
    age = row.get("IDADE")
    age_text = ""
    if pd.notna(age) and str(age).strip() and str(age).strip() != "":
        try:
            age_int = int(float(str(age).strip()))
            age_text = f"**{age_int} anos**"
        except (ValueError, TypeError):
            age_text = ""
    alive_status = (
        "✝︎ Provável Óbito" if row.get("OBITO_PROVAVEL", False) else "🌟 Provável vivo"
    )
    
    # Format phone number for display
    raw_phone = row.get("phone_number") or row.get("whatsapp_number", "")
    formatted_phone = format_phone_for_display(raw_phone)

    # Build familiares HTML
    familiares_html = ""
    for card in familiares_list:
        familiares_html += f"<li>{card}</li>"

    # Build picture HTML with simple error handling
    if picture:
        # Try using a CORS proxy for WhatsApp CDN images
        original_picture = picture
        if "pps.whatsapp.net" in picture:
            # Use a public CORS proxy to bypass WhatsApp CDN restrictions
            picture_proxied = f"https://corsproxy.io/?{picture}"
            picture_debug['proxy_url'] = picture_proxied
        else:
            picture_proxied = picture
            
        # Create unique IDs for this image (with null check)
        safe_conversation_id = conversation_id if conversation_id else "unknown"
        img_id = f"profile_img_{safe_conversation_id.replace('@', '').replace('+', '').replace('-', '')}"
        fallback_id = f"fallback_{safe_conversation_id.replace('@', '').replace('+', '').replace('-', '')}"
        
        # Simple image with fallback - minimal JavaScript to avoid f-string issues
        picture_html = f'''<img id="{img_id}" src="{original_picture}" 
            style="width: 80px; height: 80px; border-radius: 50%; object-fit: cover; border: 2px solid #ddd;" 
            onerror="this.style.display='none'; document.getElementById('{fallback_id}').style.display='flex';"
            crossorigin="anonymous" />
        <div id="{fallback_id}" style="width: 80px; height: 80px; border-radius: 50%; background-color: #f0f0f0; display: none; align-items: center; justify-content: center; font-size: 24px; border: 2px solid #ddd; cursor: help;" 
             title="Profile picture failed to load">
            📷
        </div>'''
    else:
        picture_html = '<div style="width: 80px; height: 80px; border-radius: 50%; background-color: #f0f0f0; display: flex; align-items: center; justify-content: center; font-size: 32px; border: 2px solid #ddd;" title="No profile picture available">👤</div>'

    # Build the complete contact HTML without nesting picture_html variable
    contact_html_start = f"""
    <div style="height: 400px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 5px; background-color: #f9f9f9; margin-bottom: 10px;">
        <h3>👤 Informações Pessoais</h3>
        <div style="display: flex; align-items: flex-start; margin-bottom: 10px;">
            <div style="margin-right: 15px;">"""
    
    contact_html_end = f"""
            </div>
            <div style="flex: 1;">
                <div style="margin-bottom: 10px;">
                    <strong>Nome no WhatsApp:</strong> {display_name}<br>
                    <strong>Nome Esperado:</strong> {expected_name}<br>
                    <strong>Celular:</strong> {formatted_phone}
                </div>
                <div style="margin-bottom: 10px;">
                    {age_text}<br>
                    {alive_status}
                </div>
                <div>
                    <strong>Familiares:</strong><br>
                    <ul style="margin: 5px 0; padding-left: 20px;">{familiares_html}</ul>
                </div>
            </div>
        </div>
    </div>
    """

    # Combine the HTML parts without f-string nesting
    complete_contact_html = contact_html_start + picture_html + contact_html_end
    st.markdown(complete_contact_html, unsafe_allow_html=True)

    # ─── IMÓVEIS ────────────────────────────────────────────────────────────────
    # Enhanced debugging function
    def debug_property_mapping(phone_number, row_data):
        """Comprehensive debugging for property mapping process"""
        debug_info = {
            "phone_number": phone_number,
            "spreadsheet_mapping": None,
            "cpf_found": None,
            "mega_data_properties": [],
            "errors": [],
        }

        try:
            # Step 1: Debug phone cleaning
            from services.phone_utils import clean_phone_for_matching

            clean_phone = clean_phone_for_matching(phone_number)
            debug_info["clean_phone"] = clean_phone

            # Step 2: Debug spreadsheet mapping (controlled loading)
            from services.spreadsheet import get_sheet_data

            sheet_data = get_sheet_data()  # Uses session-controlled loading

            if sheet_data:
                headers = sheet_data[0] if sheet_data else []
                debug_info["spreadsheet_headers"] = headers

                # Find column indices
                cpf_col_index = None
                phone_col_index = None

                for i, header in enumerate(headers):
                    header_lower = str(header).lower()
                    if any(
                        term in header_lower for term in ["cpf", "documento", "doc"]
                    ):
                        cpf_col_index = i
                    if any(
                        term in header_lower
                        for term in ["celular", "phone", "telefone", "contato"]
                    ):
                        phone_col_index = i

                debug_info["cpf_column_index"] = cpf_col_index
                debug_info["phone_column_index"] = phone_col_index

                # Search for matching row
                debug_info["spreadsheet_matches"] = []
                for row_idx, data_row in enumerate(sheet_data[1:], 1):  # Skip header
                    if phone_col_index is not None and phone_col_index < len(data_row):
                        sheet_phone = data_row[phone_col_index]
                        sheet_phone_clean = clean_phone_for_matching(sheet_phone)

                        if sheet_phone_clean == clean_phone:
                            cpf = (
                                data_row[cpf_col_index]
                                if cpf_col_index is not None
                                and cpf_col_index < len(data_row)
                                else None
                            )
                            debug_info["spreadsheet_matches"].append(
                                {
                                    "row_number": row_idx,
                                    "original_phone": sheet_phone,
                                    "cleaned_phone": sheet_phone_clean,
                                    "cpf": cpf,
                                    "full_row": data_row,
                                }
                            )
                            debug_info["cpf_found"] = cpf
                            break

                # Step 3: Debug mega_data_set lookup (memory-safe)
                if debug_info["cpf_found"]:
                    from services.mega_data_set_loader import (
                        find_properties_by_documento,
                    )
                    from services.unified_property_loader import property_loader, MemoryMonitor

                    # Check memory before proceeding with debug
                    if MemoryMonitor.check_memory_safety("debug property lookup"):
                        # Use a sample of bairros for debug instead of full dataset
                        sample_bairros = property_loader.get_available_bairros()[:3]  # Just first 3 bairros
                        if sample_bairros:
                            debug_df = property_loader.load_bairros_safe(sample_bairros)
                            debug_info["mega_data_sample_rows"] = len(debug_df)
                            debug_info["mega_data_columns"] = list(debug_df.columns) if not debug_df.empty else []
                            debug_info["debug_note"] = f"Sample from {len(sample_bairros)} bairros for memory safety"
                        else:
                            debug_info["mega_data_sample_rows"] = 0
                            debug_info["mega_data_columns"] = []
                            debug_info["debug_note"] = "No bairros available for debug"
                    else:
                        debug_info["mega_data_sample_rows"] = 0
                        debug_info["mega_data_columns"] = []
                        debug_info["debug_note"] = "Skipped debug due to memory constraints"

                    # Find document column using sample data
                    doc_col = None
                    if 'debug_df' in locals() and not debug_df.empty:
                        for col in debug_df.columns:
                            if col == "DOCUMENTO PROPRIETARIO":
                                doc_col = col
                                break

                    debug_info["mega_data_document_column"] = doc_col

                    if doc_col and 'debug_df' in locals() and not debug_df.empty:
                        # Show CPF cleaning
                        from services.mega_data_set_loader import clean_document_number

                        clean_cpf = clean_document_number(debug_info["cpf_found"])
                        debug_info["clean_cpf"] = clean_cpf

                        # Check for matches in sample data only (memory-safe)
                        debug_info["mega_data_matches"] = []
                        checked_count = 0
                        # Limit to first 10 rows of sample for debug
                        sample_rows = debug_df.head(10)
                        for idx, mega_row in sample_rows.iterrows():
                            row_cpf = clean_document_number(str(mega_row[doc_col]))
                            checked_count += 1

                            # Show first few comparisons
                            if checked_count <= 5:
                                debug_info["errors"].append(
                                    f"Sample Row {idx}: '{mega_row[doc_col]}' -> '{row_cpf}' vs '{clean_cpf}'"
                                )

                            if row_cpf == clean_cpf:
                                debug_info["mega_data_matches"].append(
                                    {
                                        "row_index": idx,
                                        "original_cpf": mega_row[doc_col],
                                        "cleaned_cpf": row_cpf,
                                        "property_data": mega_row.to_dict(),
                                    }
                                )
                                debug_info["errors"].append(
                                    f"MATCH FOUND at row {idx}!"
                                )
                                break  # Found one match, that's enough

                        debug_info["errors"].append(
                            f"Checked {checked_count} rows in mega_data_set"
                        )

                    # Get properties using the service
                    print(
                        f"\nDEBUG: About to call find_properties_by_documento with CPF: '{debug_info['cpf_found']}'"
                    )
                    properties = find_properties_by_documento(debug_info["cpf_found"])
                    debug_info["mega_data_properties"] = properties
                    print(
                        f"DEBUG: find_properties_by_documento returned {len(properties)} properties"
                    )

            else:
                debug_info["errors"].append("No spreadsheet data available")

        except Exception as e:
            debug_info["errors"].append(f"Debug error: {str(e)}")
            import traceback

            debug_info["traceback"] = traceback.format_exc()

        return debug_info

    # Get properties from mega_data_set using phone number
    phone_number = row.get("phone_number") or row.get("whatsapp_number", "")
    debug_info = None

    if phone_number:
        try:
            # Debug information for property loading
            if DEBUG:
                st.write(f"🔍 **Debug - Property Loading:**")
                st.write(f"- **Phone number:** {phone_number}")
                st.write(f"- **Phone type:** {type(phone_number)}")
                st.write(f"- **Loading properties from mega_data_set...**")
            
            # Run comprehensive debugging
            if DEBUG:
                debug_info = debug_property_mapping(phone_number, row)

            properties_from_mega = get_properties_for_phone(phone_number)
            
            if DEBUG:
                st.write(f"✅ **Properties loaded:** {len(properties_from_mega)} found")
            
            # Format properties for display
            imoveis = [
                format_property_for_display(prop) for prop in properties_from_mega
            ]
            if DEBUG:
                print(
                    f"DEBUG: Found {len(imoveis)} properties from mega_data_set for phone {phone_number}"
                )
        except Exception as e:
            st.error(f"🚨 **CRITICAL ERROR loading properties**")
            st.error(f"**Phone number:** {phone_number}")
            st.error(f"**Error type:** {type(e).__name__}")
            st.error(f"**Error message:** {str(e)}")
            
            with st.expander("🔍 Property Loading Error Details", expanded=True):
                st.write(f"**Phone number type:** {type(phone_number)}")
                st.write(f"**Phone number repr:** {repr(phone_number)}")
                
                if DEBUG:
                    st.exception(e)
            
            if DEBUG:
                print(f"DEBUG: Error getting properties from mega_data_set: {e}")
            imoveis = []
    else:
        imoveis = []

    # Fallback to original method if no properties found
    if not imoveis:
        imoveis = parse_imoveis(row.get("IMOVEIS"))
        if isinstance(imoveis, dict):
            imoveis = [imoveis]
        elif not isinstance(imoveis, list):
            imoveis = []
        if DEBUG:
            print(f"DEBUG: Using fallback method, found {len(imoveis)} properties")

    # Display imoveis with clickable buttons
    imoveis_container = st.container()

    with imoveis_container:
        st.markdown("### 🏢 Imóveis")

        # Create a bordered container using native streamlit
        with st.container():
            # Add the custom CSS class
            st.markdown('<div class="imoveis-container">', unsafe_allow_html=True)

            # Check for assigned properties
            assigned_property = None
            if "assigned_properties" in st.session_state and conversation_id in st.session_state.assigned_properties:
                assigned_property = st.session_state.assigned_properties[conversation_id]

            # Show assigned property first if exists
            if assigned_property:
                st.markdown("#### 🎯 Propriedade Atribuída")
                with st.expander("Propriedade Selecionada", expanded=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Bairro:** {assigned_property.get('BAIRRO', 'N/A')}")
                        st.write(f"**Logradouro:** {assigned_property.get('NOME LOGRADOURO', 'N/A')}")
                        st.write(f"**Endereço:** {assigned_property.get('ENDERECO', 'N/A')}")
                    with col2:
                        st.write(f"**Tipo:** {assigned_property.get('TIPO CONSTRUTIVO', 'N/A')}")
                        st.write(f"**Área Terreno:** {assigned_property.get('AREA TERRENO', 'N/A')}")
                        st.write(f"**Área Construção:** {assigned_property.get('AREA CONSTRUCAO', 'N/A')}")
                    st.write(f"**Índice Cadastral:** {assigned_property.get('INDICE CADASTRAL', 'N/A')}")
                    
                    # Remove assignment button
                    if st.button("🗑️ Remover Atribuição", key="remove_assigned_property"):
                        del st.session_state.assigned_properties[conversation_id]
                        st.rerun()
                
                st.markdown("#### 📋 Propriedades Associadas (CPF)")

            # Show original imoveis
            if imoveis:
                for i, item in enumerate(imoveis):
                    if not isinstance(item, dict):
                        continue

                    # Handle both old format (AREA TERRENO) and new format (area_terreno)
                    area_terreno = item.get("area_terreno") or item.get(
                        "AREA TERRENO", "?"
                    )
                    area_construcao = item.get("area_construcao") or item.get(
                        "AREA CONSTRUCAO", "?"
                    )
                    fraction = item.get("fracao_ideal") or item.get("FRACAO IDEAL", "")
                    build_type = (
                        item.get("tipo_construtivo")
                        or item.get("TIPO CONSTRUTIVO", "").strip()
                    )
                    address = item.get("endereco") or item.get("ENDERECO", "?")
                    neighborhood = item.get("bairro") or item.get("BAIRRO", "?")
                    indice_cadastral = item.get("indice_cadastral") or item.get(
                        "INDICE CADASTRAL", ""
                    )

                    # Format areas
                    area_terreno_text = fmt_num(area_terreno) if area_terreno else "?"
                    area_construcao_text = (
                        fmt_num(area_construcao) if area_construcao else "?"
                    )

                    # Format fraction
                    try:
                        fraction_percent = f"{int(round(float(fraction) * 100 if float(fraction) <= 1 else float(fraction)))}%"
                    except (ValueError, TypeError):
                        fraction_percent = str(fraction) if fraction else "N/A"

                    # Create columns for property info and button
                    prop_col1, prop_col2 = st.columns([4, 1])

                    with prop_col1:
                        # Enhanced property display with more information
                        # Build optional parts separately to avoid nested f-string issues
                        build_type_part = f" | <em>{build_type}</em>" if build_type else ""
                        fraction_part = f" | Fração: {fraction_percent}" if fraction_percent != "N/A" else ""
                        cadastral_part = f"<br><small style='color: #666;'>Cadastro: {indice_cadastral}</small>" if indice_cadastral else ""
                        
                        property_info = f"""
                        <div style="margin-bottom: 10px; padding: 8px; border-left: 3px solid #007bff; background-color: #f8f9fa; border-radius: 4px;">
                            <strong>{address}, {neighborhood}</strong><br>
                            <small>Terreno: {area_terreno_text} m² | Construção: {area_construcao_text} m²{build_type_part}{fraction_part}</small>
                            {cadastral_part}
                        </div>
                        """
                        st.markdown(property_info, unsafe_allow_html=True)

                    with prop_col2:
                        # Check if there are related conversations for this property
                        try:
                            related_conversations_df = (
                                find_conversations_with_same_property(
                                    address, neighborhood, row.get("conversation_id")
                                )
                            )
                            has_related_conversations = (
                                not related_conversations_df.empty
                            )

                            if DEBUG:
                                if has_related_conversations:
                                    print(
                                        f"DEBUG: Found {len(related_conversations_df)} related conversations for {address}, {neighborhood}"
                                    )
                                else:
                                    print(
                                        f"DEBUG: No related conversations for {address}, {neighborhood}"
                                    )
                        except Exception as e:
                            if DEBUG:
                                print(
                                    f"DEBUG: Error checking related conversations: {e}"
                                )
                            has_related_conversations = False

                        if has_related_conversations:
                            # Show active button when there are related conversations
                            if st.button("🔍", key=f"property_btn_{idx}_{i}"):
                                # Store property info in session state for modal
                                st.session_state.property_modal_data = {
                                    "address": address,
                                    "neighborhood": neighborhood,
                                    "current_conversation_id": row.get(
                                        "conversation_id"
                                    ),
                                    "current_idx": idx,  # Add current row index
                                    "show_modal": True,
                                }
                                st.rerun()
                        else:
                            # Show greyed out button when no related conversations
                            st.button(
                                "🔍",
                                key=f"property_btn_{idx}_{i}_disabled",
                                disabled=True,
                                help="Nenhuma conversa relacionada encontrada para esta propriedade",
                            )
            else:
                st.markdown(
                    '<div style="color: #888; font-style: italic;">Nenhum imóvel encontrado</div>',
                    unsafe_allow_html=True,
                )

            st.markdown("</div>", unsafe_allow_html=True)

    # ─── COMPREHENSIVE DEBUG INFORMATION ─────────────────────────────────────────
    if DEBUG and debug_info:
        with st.expander("🔍 **Property Mapping Debug Information**", expanded=True):
            st.markdown("### 📞 **Step 1: Phone Number Processing**")
            st.write(f"**Original phone:** `{debug_info['phone_number']}`")
            st.write(f"**Cleaned phone:** `{debug_info.get('clean_phone', 'N/A')}`")

            st.markdown("### 📊 **Step 2: Spreadsheet Lookup**")
            if debug_info.get("spreadsheet_headers"):
                st.write(
                    f"**Spreadsheet headers:** {debug_info['spreadsheet_headers']}"
                )
                st.write(f"**CPF column index:** {debug_info.get('cpf_column_index')}")
                st.write(
                    f"**Phone column index:** {debug_info.get('phone_column_index')}"
                )

                if debug_info.get("spreadsheet_matches"):
                    st.success(
                        f"✅ **Found {len(debug_info['spreadsheet_matches'])} spreadsheet match(es)**"
                    )
                    for match in debug_info["spreadsheet_matches"]:
                        st.write(
                            f"**→ Row {match['row_number']}:** `{match['original_phone']}` → `{match['cleaned_phone']}` → CPF: `{match['cpf']}`"
                        )
                else:
                    st.error("❌ **No spreadsheet matches found**")
                    st.write("**Phone number not found in spreadsheet data**")
            else:
                st.error("❌ **No spreadsheet data available**")

            # Show general errors
            if debug_info.get("errors"):
                st.write("**Errors/Debug info:**")
                for error in debug_info["errors"]:
                    st.write(f"- {error}")

            st.markdown("### 🏢 **Step 3: Mega Data Set Lookup**")
            if debug_info.get("cpf_found"):
                st.write(f"**CPF to search:** `{debug_info['cpf_found']}`")
                st.write(f"**Cleaned CPF:** `{debug_info.get('clean_cpf', 'N/A')}`")

                # Show data source status
                total_rows = debug_info.get("mega_data_total_rows", "N/A")
                if total_rows != "N/A" and int(total_rows) < 10000:
                    st.error(
                        f"⚠️ **SAMPLE DATA DETECTED:** {total_rows} rows (should be 350k+)"
                    )
                    st.error(
                        "**This is NOT production data! See ENABLE_GOOGLE_DRIVE_API.md**"
                    )
                else:
                    st.success(f"✅ **Real mega data:** {total_rows} rows")

                st.write(
                    f"**Mega data columns:** {debug_info.get('mega_data_columns', [])}"
                )
                st.write(
                    f"**Document column:** `{debug_info.get('mega_data_document_column', 'N/A')}`"
                )

                if debug_info.get("mega_data_matches"):
                    st.success(
                        f"✅ **Found {len(debug_info['mega_data_matches'])} property match(es)**"
                    )
                    for match in debug_info["mega_data_matches"]:
                        property_data = match["property_data"]
                        st.write(
                            f"**→ Row {match['row_index']}:** `{match['original_cpf']}` → `{match['cleaned_cpf']}`"
                        )
                        st.write(
                            f"   **Address:** {property_data.get('ENDERECO', 'N/A')}"
                        )
                        st.write(
                            f"   **Neighborhood:** {property_data.get('BAIRRO', 'N/A')}"
                        )
                        st.write(
                            f"   **Cadastral Index:** {property_data.get('INDICE CADASTRAL', 'N/A')}"
                        )
                else:
                    st.error("❌ **No property matches found in mega data set**")
                    st.error("CPF not found in mega data set")

                    # Show debugging info
                    if debug_info.get("errors"):
                        st.write("**Debug info:**")
                        for error in debug_info["errors"]:
                            st.write(f"- {error}")
                    st.error("❌ **No property matches found in mega data set**")
                    st.write("**CPF not found in mega data set**")
            else:
                st.warning("⚠️ **No CPF found to search properties**")

            st.markdown("### 📋 **Step 4: Final Results**")
            st.write(
                f"**Properties returned:** {len(debug_info.get('mega_data_properties', []))}"
            )
            if debug_info.get("mega_data_properties"):
                for i, prop in enumerate(debug_info["mega_data_properties"]):
                    st.write(
                        f"**Property {i+1}:** {prop.get('ENDERECO', 'N/A')} - {prop.get('BAIRRO', 'N/A')}"
                    )

            if debug_info.get("errors"):
                st.markdown("### ⚠️ **Errors**")
                for error in debug_info["errors"]:
                    st.error(error)

            if debug_info.get("traceback"):
                st.markdown("### 🐛 **Traceback**")
                st.code(debug_info["traceback"])

with right_col:
    # ─── CHAT HISTORY ───────────────────────────────────────────────────────────
    
    # Simple sync status indicator + Console logging for production debugging
    if conversation_id and st.session_state.get('auto_sync_enabled', True):
        sync_status = get_sync_status(conversation_id)
        
        # Add JavaScript console logging for production debugging
        st.markdown(f"""
        <script>
        // Production sync debugging - visible in Chrome DevTools Console
        console.log('🔍 SYNC DEBUG - Conversation ID: {conversation_id}');
        console.log('🔍 SYNC DEBUG - Sync Status:', {dict(sync_status)});
        console.log('🔍 SYNC DEBUG - Auto-sync enabled:', {st.session_state.get('auto_sync_enabled', True)});
        console.log('🔍 SYNC DEBUG - Current URL:', window.location.href);
        console.log('🔍 SYNC DEBUG - Timestamp:', new Date().toISOString());
        </script>
        """, unsafe_allow_html=True)
        
        if sync_status.get("active", False):
            next_sync = sync_status.get("next_sync_in", 0)
            if next_sync > 0:
                st.info(f"🔄 Auto-sync active • Next check in {int(next_sync)} seconds")
            else:
                st.success("🔄 Auto-sync active • Checking for updates...")
        else:
            st.warning("⏸️ Auto-sync inactive")

# ─── NAVIGATION BOTTOM ──────────────────────────────────────────────────────
st.markdown("---")
bot_prev_col, dashboard_col, reset_col, sheet_reset_col, load_sheet_col, sync_col, bot_next_col = st.columns(
    [1, 1, 1, 1, 1, 1, 1]
)
with bot_prev_col:
    # Use same navigation context logic as top buttons
    prev_disabled = False
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        current_conversation_id = row.get("conversation_id", row.get("whatsapp_number", ""))
        
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            prev_disabled = (current_position == 0)
        else:
            prev_disabled = True  # Not in filtered list
    else:
        prev_disabled = bool(idx == 0)  # Original behavior
    
    st.button(
        "⬅️ Anterior",
        key="bottom_prev",
        disabled=prev_disabled,
        on_click=goto_prev,
        use_container_width=True,
    )
with dashboard_col:
    if st.button("🏠 Dashboard", key="bottom_dashboard", use_container_width=True):
        st.switch_page("app.py")

with reset_col:
    if st.button("🤖 AI Reset", key="bottom_reset", use_container_width=True):
        reset_to_original(idx)
        st.success("✅ Valores originais da AI carregados!")
        st.rerun()

with sheet_reset_col:
    if st.button("📄 Sheet Reset", key="bottom_sheet_reset", use_container_width=True):
        print("🔍 TERMINAL DEBUG: Sheet Reset button clicked!")  # Terminal debug
        st.write("🔍 DEBUG: Sheet Reset button clicked!")  # Debug line
        
        # Get current conversation phone number for spreadsheet lookup
        current_row = st.session_state.master_df.iloc[idx]
        print(f"🔍 TERMINAL DEBUG: Current row keys: {list(current_row.keys()) if hasattr(current_row, 'keys') else 'No keys'}")  # Terminal debug
        
        # Debug the entire row to see what phone number fields exist
        if DEV and DEBUG:
            st.write("🔍 Debug Sheet Reset - Current row data:")
            phone_related_cols = [col for col in current_row.index if any(keyword in col.lower() for keyword in ['phone', 'whats', 'celular', 'numero'])]
            st.write(f"**Phone-related columns:** {phone_related_cols}")
            for col in phone_related_cols:
                st.write(f"  - {col}: {repr(current_row.get(col))}")
            
            # Also check conversation_id which might have the phone
            conversation_id = current_row.get("conversation_id", "")
            st.write(f"**conversation_id:** {repr(conversation_id)}")
        
        # Try multiple possible phone number columns (based on actual database structure)
        raw_whatsapp_number = ""
        phone_sources = [
            current_row.get("whatsapp_number", ""),  # This is the main column in database
            current_row.get("Phone number", ""),     # This might exist from spreadsheet sync
            current_row.get("phone", ""),
            current_row.get("phone_number", ""),     # Add this as it's in the merged data
            current_row.get("celular", ""),
            current_row.get("conversation_id", ""),  # Less likely to exist in dataframe
        ]
        
        print(f"🔍 TERMINAL DEBUG: Phone sources: {phone_sources}")  # Terminal debug
        st.write(f"🔍 DEBUG: Phone sources: {phone_sources}")  # Debug line
        
        # Use the first non-empty phone source
        for source in phone_sources:
            if source and str(source).strip():
                raw_whatsapp_number = str(source).strip()
                break
        
        print(f"🔍 TERMINAL DEBUG: Selected phone: {repr(raw_whatsapp_number)}")  # Terminal debug
        st.write(f"🔍 DEBUG: Selected phone: {repr(raw_whatsapp_number)}")  # Debug line
        
        # Debug phone number extraction
        if DEV and DEBUG:
            st.write(f"🔍 Debug Sheet Reset - Selected phone: {repr(raw_whatsapp_number)}")
        
        # Clean phone number: remove @s.whatsapp.net and format properly
        if raw_whatsapp_number:
            whatsapp_number = raw_whatsapp_number.split('@')[0] if '@' in raw_whatsapp_number else raw_whatsapp_number
            
            # Remove any non-digit characters and ensure we have a phone number
            clean_number = ''.join(filter(str.isdigit, whatsapp_number))
            
            if DEV and DEBUG:
                st.write(f"🔍 Debug Sheet Reset - Clean number: {repr(clean_number)}")
            
            if not clean_number:
                print(f"🔍 TERMINAL DEBUG: No valid phone number found")  # Terminal debug
                st.error("❌ No valid phone number found for this conversation")
            else:
                print(f"🔍 TERMINAL DEBUG: Clean number: {repr(clean_number)}")  # Terminal debug
                try:
                    # Import db loader function that handles spreadsheet data
                    from loaders.db_loader import get_conversations_with_sheets_data
                    
                    # Fetch fresh data from spreadsheet with force load for Sheet Reset
                    with st.spinner("🔄 Loading spreadsheet data..."):
                        print(f"🔍 TERMINAL DEBUG: Loading spreadsheet data (Sheet Reset - force load)...")  # Terminal debug
                        fresh_df = get_conversations_with_sheets_data(force_load_spreadsheet=True)
                    
                    print(f"🔍 TERMINAL DEBUG: Spreadsheet loaded: {len(fresh_df)} rows")  # Terminal debug
                    if DEV and DEBUG:
                        st.write(f"🔍 Debug Sheet Reset - Spreadsheet loaded: {len(fresh_df)} rows")
                    
                    # Find the phone column (look for 'celular', 'phone', etc.)
                    phone_column = None
                    for col in fresh_df.columns:
                        if col is not None:
                            col_lower = str(col).lower()
                            if any(term in col_lower for term in ['celular', 'phone', 'telefone', 'whatsapp', 'contato']):
                                phone_column = col
                                break
                    
                    if phone_column is None:
                        st.error("❌ No phone column found in spreadsheet")
                        if DEV and DEBUG:
                            st.write(f"Available columns: {list(fresh_df.columns)}")
                            st.write("Looking for columns containing: 'celular', 'phone', 'telefone', 'whatsapp', 'contato'")
                    else:
                        if DEV and DEBUG:
                            st.write(f"🔍 Debug Sheet Reset - Using phone column: {repr(phone_column)}")
                        # Create multiple phone formats to try matching (based on actual spreadsheet format)
                        phone_formats = [
                            clean_number,        # 553199821610 (matches spreadsheet format!)
                            f"+{clean_number}",  # +553199821610
                            clean_number[2:] if clean_number.startswith('55') and len(clean_number) > 2 else clean_number,  # 3199821610
                            f"+55{clean_number}" if not clean_number.startswith('55') else f"+{clean_number}",
                        ]
                        
                        # Remove duplicates while preserving order
                        phone_formats = list(dict.fromkeys(phone_formats))
                        
                        if DEV and DEBUG:
                            st.write(f"🔍 Debug Sheet Reset - Phone formats to try: {phone_formats}")
                        
                        # Look for any matching phone format - try exact matching first, then flexible matching
                        matching_rows = fresh_df[fresh_df[phone_column].astype(str).isin([str(f) for f in phone_formats])]
                        
                        # If no exact match, try flexible matching using centralized phone utilities
                        if matching_rows.empty:
                            import pandas as pd
                            from services.phone_utils import clean_phone_for_matching
                            
                            # Use sophisticated phone matching that handles format differences
                            target_normalized = clean_phone_for_matching(raw_whatsapp_number)
                            
                            # Find rows where normalized phones match
                            matching_rows = fresh_df[fresh_df[phone_column].apply(
                                lambda x: clean_phone_for_matching(x) == target_normalized
                            )]
                            
                            if DEV and DEBUG:
                                st.write(f"🔍 Debug Sheet Reset - Flexible matching attempted")
                                st.write(f"Target normalized: {repr(target_normalized)}")
                                # Show a few examples of normalized spreadsheet phones
                                sample_cleaned = []
                                for phone in fresh_df[phone_column].head(10):
                                    cleaned = clean_phone_for_matching(phone)
                                    sample_cleaned.append(f"{repr(phone)} -> {repr(cleaned)}")
                                st.write("Sample normalized phones:")
                                for example in sample_cleaned[:5]:
                                    st.write(f"  {example}")
                                st.write(f"Flexible match result: {len(matching_rows)} rows found")
                        
                        if matching_rows.empty:
                            st.error(f"❌ Conversation not found in spreadsheet: {raw_whatsapp_number}")
                            with st.expander("🔍 Debug Info", expanded=True):
                                st.write(f"**Raw phone:** {raw_whatsapp_number}")
                                st.write(f"**Clean number:** {clean_number}")
                                st.write(f"**Phone formats tried:** {phone_formats}")
                                sample_phones = fresh_df[phone_column].head(10).tolist()
                                st.write(f"**Sample spreadsheet phones:** {sample_phones}")
                                
                                # COMPREHENSIVE SEARCH: Check if the phone exists anywhere in the spreadsheet
                                st.write("**🔍 COMPREHENSIVE SEARCH:**")
                                target_digits = clean_number  # 5531999821610
                                found_matches = []
                                
                                for idx_search, phone_val in enumerate(fresh_df[phone_column]):
                                    if phone_val is not None:
                                        # Clean this phone for comparison
                                        phone_str = str(phone_val)
                                        phone_digits = ''.join(filter(str.isdigit, phone_str))
                                        
                                        # Check various matching strategies
                                        if phone_digits == target_digits:
                                            found_matches.append(f"Row {idx_search}: EXACT DIGIT MATCH - '{phone_val}' -> '{phone_digits}'")
                                        elif phone_str == raw_whatsapp_number:
                                            found_matches.append(f"Row {idx_search}: EXACT STRING MATCH - '{phone_val}'")
                                        elif phone_str in phone_formats:
                                            found_matches.append(f"Row {idx_search}: FORMAT MATCH - '{phone_val}' (matches our format)")
                                        elif target_digits in phone_digits:
                                            found_matches.append(f"Row {idx_search}: PARTIAL MATCH - '{phone_val}' contains '{target_digits}'")
                                
                                if found_matches:
                                    st.write("**MATCHES FOUND:**")
                                    for match in found_matches[:5]:  # Show first 5 matches
                                        st.write(f"  {match}")
                                        
                                    # If we found matches, show the row data for the first match
                                    if found_matches:
                                        st.write("**📋 DATA FROM FIRST MATCH:**")
                                        first_match_idx = int(found_matches[0].split(':')[0].replace('Row ', ''))
                                        match_row = fresh_df.iloc[first_match_idx]
                                        for col in fresh_df.columns:
                                            st.write(f"  {col}: {repr(match_row[col])}")
                                else:
                                    st.write("**NO MATCHES FOUND** - Phone number genuinely not in spreadsheet")
                                
                                # Show total rows searched
                                st.write(f"**Searched {len(fresh_df)} total rows in spreadsheet**")
                        else:
                            # Get the fresh spreadsheet values
                            spreadsheet_row = matching_rows.iloc[0]
                            
                            if DEV and DEBUG:
                                st.write(f"🔍 Debug Sheet Reset - Found matching row!")
                            
                            print(f"🔍 TERMINAL DEBUG: Found match! Updating form values...")  # Terminal debug
                            st.write(f"🔍 DEBUG: Found match! Updating form values...")  # Debug line
                            
                            # Temporarily disable auto-sync during reset to prevent conflicts
                            original_auto_sync = st.session_state.get('auto_sync_enabled', True)
                            st.session_state['auto_sync_enabled'] = False
                            
                            # Create reverse mapping from spreadsheet columns to database fields
                            spreadsheet_to_db_mapping = {
                                "Classificação do dono do número": "classificacao",
                                "status_manual": "intencao", 
                                "Ações": "acoes_urblink",
                                "status_manual_urb.link": "status_urblink",
                                "pagamento": "pagamento",
                                "percepcao_valor_esperado": "percepcao_valor_esperado",
                                "standby_reason": "razao_standby",
                                "OBS": "obs",
                                "stakeholder": "stakeholder",
                                "intermediador": "intermediador", 
                                "imovel_em_inventario": "inventario_flag",
                                "standby": "standby",
                                "fup_date": "followup_date",
                            }
                            
                            # Update master_df with both spreadsheet columns AND mapped database fields
                            for column in fresh_df.columns:
                                if column in st.session_state.master_df.columns:
                                    st.session_state.master_df.at[idx, column] = spreadsheet_row[column]
                                    print(f"🔍 TERMINAL DEBUG: Updated {column} = {spreadsheet_row[column]}")  # Terminal debug
                                
                                # CRITICAL: Also update the database field name if mapping exists
                                if column in spreadsheet_to_db_mapping:
                                    db_field = spreadsheet_to_db_mapping[column]
                                    if db_field in st.session_state.master_df.columns:
                                        st.session_state.master_df.at[idx, db_field] = spreadsheet_row[column]
                                        print(f"🔍 TERMINAL DEBUG: Mapped {column} -> {db_field} = {spreadsheet_row[column]}")  # Terminal debug
                            
                            # Reset original values to match spreadsheet (clear pending changes)
                            store_original_values(idx, st.session_state.master_df.iloc[idx])
                            
                            # CRITICAL: Clear widget state so form shows spreadsheet values
                            widget_keys = [
                                f"classificacao_select_{idx}",
                                f"intencao_select_{idx}",
                                f"acoes_select_{idx}",
                                f"status_select_{idx}",
                                f"pagamento_select_{idx}",
                                f"percepcao_select_{idx}",
                                f"razao_select_{idx}",
                                f"resposta_input_{idx}",
                                f"obs_input_{idx}",
                                f"stakeholder_input_{idx}",
                                f"intermediador_input_{idx}",
                                f"inventario_input_{idx}",
                                f"standby_input_{idx}",
                                f"preset_key_{idx}",
                                f"followup_date_display_{idx}",
                            ]
                            
                            for key in widget_keys:
                                if key in st.session_state:
                                    print(f"🔍 TERMINAL DEBUG: Clearing widget state: {key}")  # Terminal debug
                                    del st.session_state[key]
                            
                            # Mark as already synced since we're loading from spreadsheet
                            st.session_state.master_df.at[idx, "sheet_synced"] = True
                            
                            # Re-enable auto-sync
                            st.session_state['auto_sync_enabled'] = original_auto_sync
                            
                            print(f"🔍 TERMINAL DEBUG: Sheet Reset completed successfully!")  # Terminal debug
                            st.success("✅ Conversation reset to spreadsheet values!")
                            st.rerun()
                        
                except Exception as e:
                    st.error(f"❌ Error resetting from spreadsheet: {e}")
                    if DEV and DEBUG:
                        import traceback
                        st.write("**Full error traceback:**")
                        st.code(traceback.format_exc())
        else:
            st.error("❌ No phone number found for this conversation")

with load_sheet_col:
    if st.button("📥 Load Spreadsheet", key="bottom_load_spreadsheet", use_container_width=True):
        print("🔍 TERMINAL DEBUG: Load Spreadsheet button clicked!")  # Terminal debug
        st.write("🔍 DEBUG: Load Spreadsheet button clicked!")  # Debug line
        
        try:
            # Disable auto-sync temporarily to prevent interference
            original_auto_sync = st.session_state.get('auto_sync_enabled', True)
            st.session_state['auto_sync_enabled'] = False
            
            with st.spinner("🔄 Loading fresh spreadsheet data for all conversations..."):
                print(f"🔍 TERMINAL DEBUG: Force loading spreadsheet data for all conversations...")  # Terminal debug
                
                # Force reload spreadsheet data and update all conversations
                fresh_df = load_data(force_load_spreadsheet=True)
                
                # Update master_df with fresh data
                st.session_state.master_df = fresh_df
                
                print(f"🔍 TERMINAL DEBUG: Spreadsheet loaded with {len(fresh_df)} conversations")  # Terminal debug
                if DEV and DEBUG:
                    st.write(f"🔍 Debug Load Spreadsheet - Updated {len(fresh_df)} conversations")
                
                # Clear all widget states to prevent stale data
                keys_to_clear = [key for key in st.session_state.keys() if any(
                    pattern in key for pattern in [
                        "classificacao_input",
                        "intencao_input", 
                        "resposta_input",
                        "standby_input",
                        "preset_key",
                        "followup_date_display"
                    ]
                )]
                
                for key in keys_to_clear:
                    print(f"🔍 TERMINAL DEBUG: Clearing widget state: {key}")  # Terminal debug
                    del st.session_state[key]
                
                # Re-enable auto-sync
                st.session_state['auto_sync_enabled'] = original_auto_sync
                
                print(f"🔍 TERMINAL DEBUG: Load Spreadsheet completed successfully!")  # Terminal debug
                st.success("✅ All conversations updated with current spreadsheet data!")
                st.rerun()
                
        except Exception as e:
            st.error(f"❌ Error loading spreadsheet data: {e}")
            if DEV and DEBUG:
                import traceback
                st.write("**Full error traceback:**")
                st.code(traceback.format_exc())

with sync_col:
    if st.button("📋 Sync Sheet", key="bottom_sync", use_container_width=True):
        # Get current record data
        current_row = st.session_state.master_df.iloc[idx]
        whatsapp_number = current_row.get("whatsapp_number", "")

        # Prepare data to sync with correct column mappings (exclude resposta and standby)
        def format_list_field(field_value):
            """Convert list to comma-separated string"""
            if isinstance(field_value, list):
                return ", ".join(str(item) for item in field_value)
            elif isinstance(field_value, str):
                return field_value
            else:
                return ""

        def format_boolean_field(field_value):
            """Convert boolean to TRUE/FALSE string"""
            import numpy as np

            # Handle NaN values first
            if pd.isna(field_value):
                return "FALSE"
            # Handle both Python bool and numpy.bool_
            if isinstance(field_value, (bool, np.bool_)):
                return "TRUE" if bool(field_value) else "FALSE"
            elif isinstance(field_value, str):
                return (
                    "TRUE" if field_value.lower() in ["true", "1", "yes"] else "FALSE"
                )
            else:
                return "FALSE"

        def safe_get_field(row, field, default=""):
            """Safely get a field value, converting NaN to default."""
            value = row.get(field, default)
            if pd.isna(value):
                return default
            return value

        # Debug: Check boolean values before formatting
        stakeholder_val = st.session_state.get(f"stakeholder_input_{idx}", current_row.get("stakeholder", False))
        intermediador_val = st.session_state.get(f"intermediador_input_{idx}", current_row.get("intermediador", False))
        inventario_val = st.session_state.get(f"inventario_input_{idx}", current_row.get("inventario_flag", False))
        standby_val = st.session_state.get(f"standby_input_{idx}", current_row.get("standby", False))

        if DEV and DEBUG:
            st.write("Debug - Boolean values before sync:")
            st.write(f"stakeholder: {stakeholder_val} (type: {type(stakeholder_val)})")
            st.write(
                f"intermediador: {intermediador_val} (type: {type(intermediador_val)})"
            )
            st.write(
                f"inventario_flag: {inventario_val} (type: {type(inventario_val)})"
            )
            st.write(f"standby: {standby_val} (type: {type(standby_val)})")

        # Check for assigned property data
        assigned_property = None
        if "assigned_properties" in st.session_state and conversation_id in st.session_state.assigned_properties:
            assigned_property = st.session_state.assigned_properties[conversation_id]

        # SIMPLIFIED SYNC: Use the same logic as the working display system
        def detect_changed_fields_for_sync():
            """Detect changed fields using the same proven logic as the display system."""
            changed_fields = {}
            
            # Only proceed if we have original values stored (same as display system)
            if idx not in st.session_state.original_values:
                return changed_fields
                
            original = st.session_state.original_values[idx]
            current = st.session_state.master_df.iloc[idx].to_dict()
            
            # Field mapping from database field name to spreadsheet column name
            field_to_spreadsheet_mapping = {
                "classificacao": "Classificação do dono do número",
                "intencao": "status_manual", 
                "acoes_urblink": "Ações",
                "status_urblink": "status_manual_urb.link",
                "pagamento": "pagamento",
                "percepcao_valor_esperado": "percepcao_valor_esperado",
                "razao_standby": "standby_reason",
                "resposta": "resposta", 
                "obs": "OBS",
                "stakeholder": "stakeholder",
                "intermediador": "intermediador", 
                "inventario_flag": "imovel_em_inventario",
                "standby": "standby",
                "followup_date": "fup_date",  # Fix for follow-up date sync
            }
            
            # Use the same comparison logic as the display system
            for field in original:
                if field in current and not compare_values(original[field], current[field]):
                    # Map to spreadsheet column name if available
                    spreadsheet_field = field_to_spreadsheet_mapping.get(field, field)
                    
                    # Format the value appropriately for spreadsheet
                    current_value = current[field]
                    if isinstance(current_value, bool):
                        formatted_value = "TRUE" if current_value else "FALSE"
                    elif isinstance(current_value, list):
                        formatted_value = ", ".join(str(item) for item in current_value) if current_value else ""
                    else:
                        formatted_value = str(current_value) if current_value is not None else ""
                    
                    changed_fields[spreadsheet_field] = formatted_value
                    
                    if DEV and DEBUG:
                        st.write(f"🔄 Sync will update: {spreadsheet_field}")
                        st.write(f"   Original: {repr(original[field])}")
                        st.write(f"   Current: {repr(current[field])}")
                        st.write(f"   Formatted for sync: {repr(formatted_value)}")
            
            # Special handling for property assignment fields (always include if assigned_property exists)
            if assigned_property:
                property_fields = {
                    "endereco_bairro": format_address_field(assigned_property.get("BAIRRO", "")),
                    "endereco": format_address_field(assigned_property.get("ENDERECO", "")),
                    "endereco_complemento": format_address_field(assigned_property.get("COMPLEMENTO ENDERECO", "")),
                    "indice_cadastral_list": assigned_property.get("INDICE CADASTRAL", ""),
                }
                for field, value in property_fields.items():
                    if value:  # Only include non-empty property fields
                        changed_fields[field] = value
                        if DEV and DEBUG:
                            st.write(f"🏠 Property field: {field} = {value}")
            
            return changed_fields

        # Removed old debug code - real issue was widget initialization triggering update_field()
        
        # Get only the changed fields using the same logic as display system
        sync_data = detect_changed_fields_for_sync()
        
        # Show debug info about sync detection
        if DEV and DEBUG and sync_data:
            st.write(f"**Sync system detected {len(sync_data)} changed field(s):**")
            for field, value in sync_data.items():
                st.write(f"  {field}: {repr(value)}")
        
        # If no fields changed, show message and exit
        if not sync_data:
            st.info("ℹ️ No changes detected. Nothing to sync.")
        else:
            # Add essential fields for row identification and creation if needed
            # These are only added if the row doesn't exist in the spreadsheet
            essential_fields = {
                "cpf": current_row.get("cpf", ""),
                "Nome": current_row.get("display_name", ""),
                "nome_whatsapp": current_row.get("display_name", ""),
                "celular": format_phone_for_storage(whatsapp_number.split('@')[0] if '@' in whatsapp_number else whatsapp_number),
            }

            # Queue sync operation in background (partial update mode)
            try:
                operation_id = queue_sync_operation(sync_data, whatsapp_number, "report", essential_fields)
                
                # Mark as synced in the dataframe (optimistic update)
                st.session_state.master_df.at[idx, "sheet_synced"] = True
                
                # Show immediate feedback with more details
                if len(sync_data) > 0:
                    st.success(f"✅ Sync queued for {len(sync_data)} field(s)! (ID: {operation_id[:8]}...)")
                    # Show what fields will be synced
                    fields_list = ", ".join(f"`{field}`" for field in sync_data.keys())
                    st.info(f"📋 Syncing: {fields_list}")
                    
                    # Store operation ID for later status checking
                    if "recent_sync_operations" not in st.session_state:
                        st.session_state.recent_sync_operations = []
                    st.session_state.recent_sync_operations.append({
                        "operation_id": operation_id,
                        "timestamp": time.time(),
                        "fields_count": len(sync_data),
                        "fields": list(sync_data.keys())
                    })
                    # Keep only last 5 operations
                    st.session_state.recent_sync_operations = st.session_state.recent_sync_operations[-5:]
                else:
                    st.info("✅ Nothing to sync - all values are already up to date in the spreadsheet.")
                    
                st.info("📋 Check the sidebar for detailed progress and results.")
                
            except Exception as e:
                st.error(f"❌ Error queueing sync operation: {e}")
                if DEV and DEBUG:
                    import traceback
                    st.write("**Full error traceback:**")
                    st.code(traceback.format_exc())

        # Display recent sync results with detailed feedback
        if "recent_sync_operations" in st.session_state and st.session_state.recent_sync_operations:
            st.write("---")
            st.subheader("📊 Recent Sync Results")
            
            # Check status of recent operations and display results
            for i, operation_info in enumerate(reversed(st.session_state.recent_sync_operations[-3:])):  # Show last 3
                operation_id = operation_info["operation_id"]
                status = background_manager.get_operation_status(operation_id)
                
                # Debug information for troubleshooting
                if DEV and DEBUG:
                    st.write(f"🔍 Debug - Operation {operation_id[:8]}: {status}")
                
                if status:
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        # Format timestamp
                        elapsed = time.time() - operation_info["timestamp"]
                        if elapsed < 60:
                            time_str = f"{int(elapsed)}s ago"
                        else:
                            time_str = f"{int(elapsed/60)}m ago"
                        
                        if status["status"] == "completed":
                            result = status.get("result", {})
                            action = result.get("action", "unknown")
                            
                            if action == "updated":
                                updated_fields = result.get("updated_fields", [])
                                row_number = result.get("row_number", "?")
                                st.success(f"✅ **Sync completed** ({time_str})")
                                st.write(f"📋 Updated {len(updated_fields)} field(s) in spreadsheet row {row_number}")
                                if updated_fields:
                                    fields_str = ", ".join(f"`{field}`" for field in updated_fields)
                                    st.write(f"📝 Fields: {fields_str}")
                                    
                            elif action == "created":
                                row_number = result.get("row_number", "?")
                                updated_cells = result.get("updated_cells", 0)
                                st.success(f"✅ **New row created** ({time_str})")
                                st.write(f"📋 Created new row {row_number} with {updated_cells} cells") 
                                
                            elif action == "already_synced":
                                row_number = result.get("row_number", "?")
                                st.info(f"ℹ️ **Already synced** ({time_str})")
                                st.write(f"📋 Spreadsheet row {row_number} already had identical values")
                                
                            else:
                                st.success(f"✅ **Sync completed** ({time_str})")
                                st.write(f"📋 Action: {action}")
                                
                        elif status["status"] == "failed":
                            error = status.get("error", "Unknown error")
                            st.error(f"❌ **Sync failed** ({time_str})")
                            st.write(f"💥 Error: {error}")
                            
                        elif status["status"] in ["queued", "running"]:
                            # Check if operation is old enough to be considered completed
                            elapsed = time.time() - operation_info["timestamp"]
                            if elapsed > 30:  # If more than 30 seconds old, assume completed
                                st.success(f"✅ **Sync completed** ({time_str})")
                                st.write(f"📋 Sync completed successfully")
                                st.write(f"📝 Fields: {', '.join(f'`{field}`' for field in operation_info.get('fields', []))}")
                            else:
                                progress = status.get("progress", 0)
                                st.info(f"🔄 **Sync in progress** ({time_str})")
                                if progress > 0:
                                    st.progress(progress / 100)
                                
                    with col2:
                        # Show operation ID for debugging
                        if DEV:
                            st.write(f"ID: `{operation_id[:8]}...`")
                
                else:
                    # No status available - assume completed if old enough
                    elapsed = time.time() - operation_info["timestamp"]
                    if elapsed < 60:
                        time_str = f"{int(elapsed)}s ago"
                    else:
                        time_str = f"{int(elapsed/60)}m ago"
                    
                    if elapsed > 30:  # Assume completed after 30 seconds
                        st.success(f"✅ **Sync completed** ({time_str})")
                        st.write(f"📋 Sync completed successfully")
                        st.write(f"📝 Fields: {', '.join(f'`{field}`' for field in operation_info.get('fields', []))}")
                    else:
                        st.info(f"🔄 **Sync queued** ({time_str})")
                        st.write(f"📝 Fields: {', '.join(f'`{field}`' for field in operation_info.get('fields', []))}")
                
                if i < len(st.session_state.recent_sync_operations[-3:]) - 1:
                    st.write("")  # Add spacing between operations

with bot_next_col:
    # Use same navigation context logic as top buttons
    next_disabled = False
    if ("processor_navigation_context" in st.session_state and 
        st.session_state.processor_navigation_context.get("from_conversations_page", False)):
        
        nav_context = st.session_state.processor_navigation_context
        conversation_ids = nav_context.get("conversation_ids", [])
        current_conversation_id = row.get("conversation_id", row.get("whatsapp_number", ""))
        
        if current_conversation_id in conversation_ids:
            current_position = conversation_ids.index(current_conversation_id)
            next_disabled = (current_position >= len(conversation_ids) - 1)
        else:
            next_disabled = True  # Not in filtered list
    else:
        next_disabled = bool(idx >= len(df) - 1)  # Original behavior
    
    st.button(
        "Próximo ➡️",
        key="bottom_next",
        disabled=next_disabled,
        on_click=goto_next,
        use_container_width=True,
    )

# ─── BACKGROUND OPERATIONS SIDEBAR ─────────────────────────────────────────
# Sync global background operations to session state for UI updates
try:
    from services.background_operations import global_storage
    global_storage.sync_to_session_state()
    
    # Auto-refresh with rate limiting if there are running operations
    running_ops = get_running_operations()
    if running_ops:
        # Rate-limited refresh: only refresh every few seconds when operations are running
        current_time = time.time()
        last_refresh_key = "last_bg_ops_refresh"
        
        if last_refresh_key not in st.session_state:
            st.session_state[last_refresh_key] = 0
        
        # Refresh every 3 seconds when operations are running (only when auto-sync is enabled)
        if (st.session_state.get('auto_sync_enabled', False) and 
            current_time - st.session_state[last_refresh_key] > 3.0):
            st.session_state[last_refresh_key] = current_time
            st.rerun()
        
except Exception as e:
    if DEBUG:
        st.sidebar.error(f"Error syncing background operations: {e}")

# Render background operations status in sidebar
try:
    render_operations_sidebar()
except Exception as e:
    st.sidebar.error(f"Error displaying operations status: {e}")

# ─── PROPERTY MODAL ─────────────────────────────────────────────────────────
# Check if we need to show the property modal
if hasattr(
    st.session_state, "property_modal_data"
) and st.session_state.property_modal_data.get("show_modal", False):
    modal_data = st.session_state.property_modal_data

    # Create modal using dialog with wider width
    @st.dialog("Conversas Relacionadas", width="large")
    def show_property_modal():
        try:
            # Get conversations with same property
            related_conversations_df = find_conversations_with_same_property(
                modal_data["address"],
                modal_data["neighborhood"],
                modal_data.get("current_conversation_id"),
            )

            # Store current_idx in session state for the exclusion logic
            st.session_state.current_idx = modal_data.get("current_idx")

            if not related_conversations_df.empty:
                # Display each conversation with the new format
                for idx, conv_row in related_conversations_df.iterrows():
                    with st.container():
                        col1, col2 = st.columns([6, 1])

                        with col1:
                            # Build display string with only non-empty fields
                            display_parts = []

                            # Always include expected_name (or fallback to display_name)
                            expected_name = (
                                conv_row.get("expected_name", "")
                                if conv_row.get("expected_name", "")
                                and conv_row.get("expected_name", "").strip()
                                else conv_row.get("display_name", "")
                            )
                            display_parts.append(f"**{expected_name}**")

                            # Add classificacao if not empty
                            if (
                                conv_row.get("classificacao", "")
                                and conv_row.get("classificacao", "").strip()
                            ):
                                display_parts.append(conv_row.get("classificacao", ""))

                            # Add intencao if not empty
                            if conv_row.get("intencao", "") and conv_row.get("intencao", "").strip():
                                display_parts.append(conv_row.get("intencao", ""))

                            # Add formatted date if available
                            formatted_date = format_last_message_date(
                                conv_row.get("last_message_date", 0)
                            )
                            if formatted_date:
                                display_parts.append(formatted_date)

                            # Join all parts with " | "
                            display_text = " | ".join(display_parts)
                            st.write(display_text)

                        with col2:
                            if st.button(
                                "➡️ Ir", key=f"goto_conv_{conv_row.get('row_index', idx)}"
                            ):
                                # Navigate to this conversation
                                st.session_state.idx = conv_row.get("row_index", idx)
                                st.session_state.property_modal_data = {
                                    "show_modal": False
                                }
                                # Update URL with new conversation_id
                                new_conversation_id = conv_row.get(
                                    "conversation_id", conv_row.get("phone", "")
                                )
                                if new_conversation_id:
                                    st.query_params["conversation_id"] = (
                                        new_conversation_id
                                    )
                                st.rerun()

                        st.divider()

                if DEBUG:
                    st.write("**DEBUG: Related conversations data:**")
                    st.write(related_conversations_df)

            else:
                st.write("Nenhuma conversa relacionada encontrada.")

        except Exception as e:
            st.error(f"Erro ao buscar conversas relacionadas: {e}")
            if DEBUG:
                st.write("**DEBUG: Error details:**")
                st.exception(e)

        # Close modal button
        if st.button("❌ Fechar", key="close_property_modal"):
            st.session_state.property_modal_data = {"show_modal": False}
            st.rerun()

    # Show the modal
    show_property_modal()


# ─── PROPERTY MAP SECTION ──────────────────────────────────────────────────
def show_property_map():
    """Show interactive map of all properties owned by this person."""
    phone_number = row.get("whatsapp_number", row.get("phone_number", ""))

    if not phone_number:
        return

    # Use session state to cache properties for this phone number during the same session
    cache_key = f"properties_{phone_number}"

    if cache_key not in st.session_state:
        # Get properties for this phone number
        from services.mega_data_set_loader import get_properties_for_phone

        st.session_state[cache_key] = get_properties_for_phone(phone_number)

    properties = st.session_state[cache_key]

    if not properties:
        return

    # Import map functions (style selector moved to advanced options)
    from utils.property_map import (
        get_property_map_summary,
        render_property_map_streamlit,
    )

    # Show property summary
    summary = get_property_map_summary(properties)

    if summary:
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total de Propriedades", summary["total_properties"])
            st.metric("Com Dados Geográficos", summary["mappable_properties"])

        with col2:
            if summary["total_area_terreno"] > 0:
                st.metric(
                    "Área Total Terreno", f"{summary['total_area_terreno']:,.0f} m²"
                )
            if summary["total_area_construcao"] > 0:
                st.metric(
                    "Área Total Construção",
                    f"{summary['total_area_construcao']:,.0f} m²",
                )

        with col3:
            if summary["total_valor"] > 0:
                valor_formatado = (
                    f"R$ {summary['total_valor']:,.2f}".replace(",", "X")
                    .replace(".", ",")
                    .replace("X", ".")
                )
                st.metric("Valor Total (NET)", valor_formatado)

        # Show property types breakdown
        if summary["property_types"]:
            st.subheader("📊 Tipos de Propriedades")
            types_df = pd.DataFrame(
                list(summary["property_types"].items()), columns=["Tipo", "Quantidade"]
            )
            st.dataframe(types_df, hide_index=True, use_container_width=True)

        # Show neighborhoods breakdown
        if summary["neighborhoods"]:
            st.subheader("📍 Bairros")
            neighborhoods_df = pd.DataFrame(
                list(summary["neighborhoods"].items()), columns=["Bairro", "Quantidade"]
            )
            st.dataframe(neighborhoods_df, hide_index=True, use_container_width=True)

    # Render the interactive map with selected style
    try:
        render_property_map_streamlit(
            properties, map_style="Light", enable_extra_options=True
        )
    except Exception as e:
        st.error(f"Erro ao carregar mapa: {e}")
        st.info(
            "💡 Para ver o mapa, instale as dependências: `pip install folium streamlit-folium`"
        )

        # Show fallback property list
        st.subheader("📋 Lista de Propriedades")
        properties_data = []
        for prop in properties:
            properties_data.append(
                {
                    "Endereço": prop.get("ENDERECO", "N/A"),
                    "Bairro": prop.get("BAIRRO", "N/A"),
                    "Tipo": prop.get("TIPO CONSTRUTIVO", "N/A"),
                    "Área Terreno": prop.get("AREA TERRENO", "N/A"),
                    "Área Construção": prop.get("AREA CONSTRUCAO", "N/A"),
                    "Índice Cadastral": prop.get("INDICE CADASTRAL", "N/A"),
                    "CPF Proprietário": prop.get("DOCUMENTO PROPRIETARIO", "N/A"),
                    "Nome Proprietário": prop.get("NOME PROPRIETARIO PBH", "N/A"),
                    "Idade": prop.get("IDADE", "N/A"),
                    "Óbito Provável": prop.get("OBITO PROVAVEL", "N/A"),
                }
            )

        if properties_data:
            properties_df = pd.DataFrame(properties_data)
            st.dataframe(properties_df, hide_index=True, use_container_width=True)


    # Show property map if we have properties
    show_property_map()

    # ─── FOOTER CAPTION ─────────────────────────────────────────────────────────
    st.caption(
        f"Caso ID: {idx + 1} | WhatsApp: {row.get('whatsapp_number', row.get('phone_number', 'N/A'))} | {datetime.now():%H:%M:%S}"
    )

# ─── PROPERTY ASSIGNMENT POPUP ──────────────────────────────────────────────
# Property assignment popup - rendered at the end after all main content
try:
    if st.session_state.get("show_property_assignment", False):
        show_property_assignment_popup()
except Exception as e:
    st.error(f"Erro na funcionalidade de atribuição de propriedade: {e}")
    st.session_state.show_property_assignment = False

# Error handling removed for now - will be added back with proper try/except structure if needed
