"""Processor.py - Streamlit interface for WhatsApp Agent with authentication."""

import os
from datetime import datetime

import pandas as pd
import streamlit as st
import requests

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


# ──────────────────────────────────────────────────────────────────────────────
# PHONE NUMBER FORMATTING FUNCTION
# ──────────────────────────────────────────────────────────────────────────────

def format_phone_for_display(phone_number: str) -> str:
    """Format phone number to (XX) XXXXX-XXXX format for display."""
    if not phone_number:
        return ""
    
    # Remove any non-digit characters and @domain suffix
    clean_phone = phone_number.split('@')[0] if '@' in phone_number else phone_number
    clean_phone = ''.join(filter(str.isdigit, clean_phone))
    
    # Remove country code if present (assuming Brazilian numbers)
    if clean_phone.startswith('55') and len(clean_phone) > 10:
        clean_phone = clean_phone[2:]
    
    # Format as (XX) XXXXX-XXXX if we have at least 10 digits
    if len(clean_phone) >= 10:
        area_code = clean_phone[:2]
        if len(clean_phone) == 11:  # Mobile number
            number_part = clean_phone[2:7] + '-' + clean_phone[7:]
        else:  # Landline number (10 digits)
            number_part = clean_phone[2:6] + '-' + clean_phone[6:]
        return f"({area_code}) {number_part}"
    
    return phone_number  # Return original if can't format

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
    """Show the property assignment popup with cascading filters."""
    # Safety check - only show if we have a valid conversation loaded
    if not hasattr(st.session_state, 'current_conversation_id') or not st.session_state.current_conversation_id:
        st.error("Por favor, carregue uma conversa primeiro.")
        st.session_state.show_property_assignment = False
        st.rerun()
        return
    
    from services.mega_data_set_loader import load_mega_data_set, get_available_bairros
    
    # Create a modal-like container
    with st.container():
        st.markdown("### 🏢 Atribuir Propriedade")
        st.markdown("---")
        
        # Load mega data set
        try:
            mega_df = load_mega_data_set()
            if mega_df is None or mega_df.empty:
                st.error("Erro ao carregar o mega data set.")
                return
        except Exception as e:
            st.error(f"Erro ao carregar o mega data set: {e}")
            return
        
        # Get available bairros
        try:
            bairros = get_available_bairros()
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
        
        # Apply bairro filter
        filtered_df = mega_df.copy()
        if selected_bairros:
            filtered_df = filtered_df[filtered_df["BAIRRO"].isin(selected_bairros)]
            
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
            display_columns = ["BAIRRO", "NOME LOGRADOURO", "ENDERECO", "TIPO CONSTRUTIVO", "AREA TERRENO", "AREA CONSTRUCAO", "INDICE CADASTRAL", "DOCUMENTO PROPRIETARIO", "NOME PROPRIETARIO PBH", "IDADE", "OBITO PROVAVEL"]
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
        else:
            st.info("Nenhuma propriedade encontrada com os filtros aplicados.")
        
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
except:
    pass

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
def load_data():
    """Load the WhatsApp conversations DataFrame."""
    return get_dataframe()


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
            "resposta": conversation_data.get("OBS", ""),  # Maps to resposta field
            "Razao": conversation_data.get("standby_reason", ""),
            "acoes_urblink": parse_spreadsheet_list(conversation_data.get("Ações", "")),
            "status_urblink": conversation_data.get("status_manual_urb.link", ""),
            "obs": conversation_data.get("OBS_urb.link", ""),  # Additional observations
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
    else:
        return str(original) == str(current)


# ─── STATE INIT ─────────────────────────────────────────────────────────────
# Initialize session state
initialize_session_state()

# Work with master_df
df = st.session_state.master_df

# ─── AUTO-LOADING CONVERSATION WITH ERROR HANDLING ───────────────────────
# If auto_load_conversation is specified, find and load that conversation
if auto_load_conversation:
    try:
        # Search for the conversation by conversation_id or whatsapp_number
        if "conversation_id" in df.columns:
            matching_conversations = df[df["conversation_id"] == auto_load_conversation]
        else:
            matching_conversations = df[df["whatsapp_number"] == auto_load_conversation]

        if not matching_conversations.empty:
            # Found the conversation, set idx to its position
            conversation_idx = matching_conversations.index[0]
            st.session_state.idx = conversation_idx
            if DEBUG:
                st.success(f"✅ Successfully loaded conversation: {auto_load_conversation}")
        else:
            if DEBUG:
                st.warning(
                    f"⚠️ Conversation {auto_load_conversation} not found in current dataset"
                )
                
                # Show debug info for missing conversation
                with st.expander("🔍 Debug: Why conversation not found?", expanded=False):
                    st.write(f"**Looking for:** {auto_load_conversation}")
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
    st.progress((idx + 1) / len(df))
    st.caption(f"{idx + 1}/{len(df)} mensagens processadas")
st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)

# Dashboard navigation moved to bottom

st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)


# ─── NAVIGATION TOP ─────────────────────────────────────────────────────────
def goto_prev():
    """Go to the previous conversation."""
    # Cleanup sync for current conversation
    current_row = df.iloc[st.session_state.idx]
    current_conversation_id = current_row.get("conversation_id", current_row.get("whatsapp_number", ""))
    if current_conversation_id:
        cleanup_sync_on_exit(current_conversation_id)
    
    st.session_state.idx = max(st.session_state.idx - 1, 0)
    # Update URL with conversation_id
    new_idx = st.session_state.idx
    if new_idx < len(df):
        conversation_id = df.iloc[new_idx].get(
            "conversation_id", df.iloc[new_idx].get("whatsapp_number", "")
        )
        if conversation_id:
            st.query_params["conversation_id"] = conversation_id


def goto_next():
    """Go to the next conversation."""
    # Cleanup sync for current conversation
    current_row = df.iloc[st.session_state.idx]
    current_conversation_id = current_row.get("conversation_id", current_row.get("whatsapp_number", ""))
    if current_conversation_id:
        cleanup_sync_on_exit(current_conversation_id)
    
    st.session_state.idx = min(st.session_state.idx + 1, len(df) - 1)
    # Update URL with conversation_id
    new_idx = st.session_state.idx
    if new_idx < len(df):
        conversation_id = df.iloc[new_idx].get(
            "conversation_id", df.iloc[new_idx].get("whatsapp_number", "")
        )
        if conversation_id:
            st.query_params["conversation_id"] = conversation_id


nav_prev_col, nav_property_col, nav_archive_col, nav_next_col = st.columns([1, 1, 1, 1])
with nav_prev_col:
    st.button(
        "⬅️ Anterior",
        key="top_prev",
        disabled=bool(idx == 0),
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
            # Show loading state
            with st.spinner("Arquivando conversa..."):
                result = archive_conversation(phone_number, conversation_id)
            
            # Show result to user
            if result["success"]:
                st.success(result["message"])
                # Optionally refresh the page or navigate to next conversation
                st.rerun()
            else:
                st.error(f"{result['message']}")
                if "error" in result and DEBUG:
                    st.sidebar.error(f"Archive Debug: {result['error']}")
                    if "details" in result:
                        st.sidebar.json(result["details"])
        else:
            st.error("Número de telefone não encontrado para esta conversa.")
with nav_next_col:
    st.button(
        "Próximo ➡️",
        key="top_next",
        disabled=bool(idx >= len(df) - 1),
        on_click=goto_next,
        use_container_width=True,
    )

# ─── SYNC INITIALIZATION ────────────────────────────────────────────────────
# Initialize auto-sync for the current conversation
conversation_id = row.get("conversation_id", row.get("whatsapp_number", ""))

# Store current conversation ID in session state
st.session_state.current_conversation_id = conversation_id

# Debug conversation ID
if DEBUG:
    st.write(f"🔍 **Sync Debug:** conversation_id = {conversation_id}")
    st.write(f"🔍 **Row keys:** {list(row.keys()) if hasattr(row, 'keys') else 'No keys'}")

if conversation_id:
    # Setup auto-sync
    setup_conversation_sync(conversation_id)
    
    # Check for sync updates and refresh if needed
    if check_for_sync_updates(conversation_id):
        st.rerun()
    
    # Setup auto-refresh mechanism
    setup_auto_refresh()
    
    # Render sync header
    render_sync_header(conversation_id)
    
    # Render sync sidebar controls
    render_sync_sidebar(conversation_id)
else:
    if DEBUG:
        st.error("🚨 **Sync Error:** No conversation_id found!")
        st.write(f"Row content: {dict(row) if hasattr(row, 'keys') else str(row)}")

# ─── MAIN CONTENT LAYOUT ────────────────────────────────────────────────────
left_col, right_col = st.columns([1, 1])

with left_col:
    # ─── CONTACT SECTION ────────────────────────────────────────────────────────
    hl_words = build_highlights(row["display_name"], row["expected_name"])

    # Create contact info HTML with fixed height
    picture = row.get("PictureUrl")
    # Clean and validate picture URL
    if (
        picture
        and not pd.isna(picture)
        and str(picture).strip()
        and str(picture).strip().lower() not in ["none", "null", ""]
    ):
        picture = str(picture).strip()
        if DEBUG:
            dbg(f"Picture URL found: {picture[:50]}...")
    else:
        picture = None
        if DEBUG:
            dbg(f"No valid picture URL (raw value: {repr(row.get('PictureUrl'))})")

    display_name = (
        highlight(row["display_name"], hl_words)
        if HIGHLIGHT_ENABLE
        else row["display_name"]
    )
    expected_name = highlight(row["expected_name"], hl_words)
    familiares_list = parse_familiares_grouped(row["familiares"])
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

    # Build picture HTML separately to avoid f-string issues
    if picture:
        picture_html = f'<img src="{picture}" style="width: 80px; height: 80px; border-radius: 50%; object-fit: cover; border: 2px solid #ddd;" onerror="this.style.display=\'none\'; this.nextSibling.style.display=\'flex\';" /><div style="width: 80px; height: 80px; border-radius: 50%; background-color: #f0f0f0; display: none; align-items: center; justify-content: center; font-size: 32px; border: 2px solid #ddd;">👤</div>'
    else:
        picture_html = '<div style="width: 80px; height: 80px; border-radius: 50%; background-color: #f0f0f0; display: flex; align-items: center; justify-content: center; font-size: 32px; border: 2px solid #ddd;">👤</div>'

    contact_html = f"""
    <div style="height: 400px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 5px; background-color: #f9f9f9; margin-bottom: 10px;">
        <h3>👤 Informações Pessoais</h3>
        <div style="display: flex; align-items: flex-start; margin-bottom: 10px;">
            <div style="margin-right: 15px;">
                {picture_html}
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

    st.markdown(contact_html, unsafe_allow_html=True)

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
            from services.mega_data_set_loader import clean_phone_for_match

            clean_phone = clean_phone_for_match(phone_number)
            debug_info["clean_phone"] = clean_phone

            # Step 2: Debug spreadsheet mapping
            from services.spreadsheet import get_sheet_data

            sheet_data = get_sheet_data()

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
                        sheet_phone_clean = clean_phone_for_match(sheet_phone)

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

                # Step 3: Debug mega_data_set lookup
                if debug_info["cpf_found"]:
                    from services.mega_data_set_loader import (
                        find_properties_by_documento,
                        load_mega_data_set,
                    )

                    # Load mega_data_set to show what's available
                    mega_df = load_mega_data_set()
                    debug_info["mega_data_total_rows"] = len(mega_df)
                    debug_info["mega_data_columns"] = list(mega_df.columns)

                    # Find document column
                    doc_col = None
                    for col in mega_df.columns:
                        if col == "DOCUMENTO PROPRIETARIO":
                            doc_col = col
                            break

                    debug_info["mega_data_document_column"] = doc_col

                    if doc_col:
                        # Show CPF cleaning
                        from services.mega_data_set_loader import clean_document_number

                        clean_cpf = clean_document_number(debug_info["cpf_found"])
                        debug_info["clean_cpf"] = clean_cpf

                        # Check for matches
                        debug_info["mega_data_matches"] = []
                        checked_count = 0
                        for idx, mega_row in mega_df.iterrows():
                            row_cpf = clean_document_number(str(mega_row[doc_col]))
                            checked_count += 1

                            # Show first few comparisons
                            if checked_count <= 5:
                                debug_info["errors"].append(
                                    f"Row {idx}: '{mega_row[doc_col]}' -> '{row_cpf}' vs '{clean_cpf}'"
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
                        property_info = f"""
                        <div style="margin-bottom: 10px; padding: 8px; border-left: 3px solid #007bff; background-color: #f8f9fa; border-radius: 4px;">
                            <strong>{address}, {neighborhood}</strong><br>
                            <small>Terreno: {area_terreno_text} m² | Construção: {area_construcao_text} m²</small>
                            {f" | <em>{build_type}</em>" if build_type else ""}
                            {f" | Fração: {fraction_percent}" if fraction_percent != "N/A" else ""}
                            {f"<br><small style='color: #666;'>Cadastro: {indice_cadastral}</small>" if indice_cadastral else ""}
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
                                    "msg": msg_row["message_text"],
                                    "ts": datetime.fromtimestamp(
                                        msg_row["timestamp"]
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
            and pd.notna(row["conversation_history"])
            and row["conversation_history"]
        ):
            messages = parse_chat(row["conversation_history"])
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

st.markdown("---")

# ─── CLASSIFICAÇÃO & RESPOSTA ───────────────────────────────────────────────
st.subheader("📝 Classificação e Resposta")

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
    st.markdown(
        f"""
    <div style="margin-top: 25px;">
        <strong>📋 Racional usado pela AI classificadora:</strong><br>
        <div class='reason-box' style="margin-top: 5px; font-size: 0.85rem; max-height: 100px; overflow-y: auto;">
            {row['Razao']}
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
    st.rerun()

left_col, right_col = st.columns(2)

with left_col:
    # Classificação
    current_classificacao = row.get("classificacao", "")
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

    # Intenção
    current_intencao = row.get("intencao", "")
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
    # Resposta
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

    # OBS
    current_obs = row.get("obs", "")

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
                followup_amount = st.number_input(
                    "Quantidade",
                    min_value=1,
                    max_value=365,
                    value=st.session_state.get(f"followup_amount_{idx}", 1),
                    key=f"followup_amount_{idx}",
                )

            with followup_col2:
                followup_unit = st.selectbox(
                    "Período",
                    options=["dias", "semanas", "meses"],
                    index=["dias", "semanas", "meses"].index(
                        st.session_state.get(f"followup_unit_{idx}", "dias")
                    ),
                    key=f"followup_unit_{idx}",
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

    # Show modifications status
    if idx in st.session_state.original_values:
        original = st.session_state.original_values[idx]
        current = st.session_state.master_df.iloc[idx].to_dict()
        modified_fields = []
        for field in original:
            if field in current and not compare_values(original[field], current[field]):
                modified_fields.append(field)

        if modified_fields:
            st.info(f"📝 Campos modificados: {', '.join(modified_fields)}")
        else:
            if DEBUG:
                st.success("✅ Sem modificações")

        # Debug info (remove this later)
        if DEV and DEBUG:
            with st.expander("🔍 Debug Info"):
                st.write("**DataFrame columns:**")
                st.write(list(st.session_state.master_df.columns))
                st.write("**Original values:**")
                st.json(original)
                st.write("**Current values:**")
                current_debug = {k: v for k, v in current.items() if k in original}
                st.json(current_debug)
                st.write("**All current values for problematic fields:**")
                problematic_fields = [
                    "acoes_urblink",
                    "status_urblink",
                    "razao_standby",
                    "obs",
                    "stakeholder",
                    "intermediador",
                    "inventario_flag",
                    "standby",
                ]
                for field in problematic_fields:
                    if field in current:
                        st.write(
                            f"**{field}**: {current[field]} (type: {type(current[field])})"
                        )
                    else:
                        st.write(f"**{field}**: NOT IN CURRENT")
                st.write("**Field comparisons:**")
                for field in original:
                    if field in current:
                        orig_val = original[field]
                        curr_val = current[field]
                        is_equal = compare_values(orig_val, curr_val)
                        st.write(
                            f"**{field}**: {orig_val} → {curr_val} (Equal: {is_equal})"
                        )

# ─── NAVIGATION BOTTOM ──────────────────────────────────────────────────────
st.markdown("---")
bot_prev_col, dashboard_col, reset_col, sync_col, bot_next_col = st.columns(
    [1, 1, 1, 1, 1]
)
with bot_prev_col:
    st.button(
        "⬅️ Anterior",
        key="bottom_prev",
        disabled=bool(idx == 0),
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

        sync_data = {
            "Classificação do dono do número": st.session_state.get(
                f"classificacao_select_{idx}", safe_get_field(current_row, "classificacao")
            ),
            "status_manual": st.session_state.get(
                f"intencao_select_{idx}", safe_get_field(current_row, "intencao")
            ),
            "Ações": format_list_field(st.session_state.get(f"acoes_select_{idx}", current_row.get("acoes_urblink", []))),
            "status_manual_urb.link": st.session_state.get(f"status_select_{idx}", safe_get_field(current_row, "status_urblink")),
            "pagamento": ", ".join(st.session_state.get(
                f"pagamento_select_{idx}", [safe_get_field(current_row, "pagamento")]
            )),
            "percepcao_valor_esperado": st.session_state.get(
                f"percepcao_select_{idx}", safe_get_field(current_row, "percepcao_valor_esperado")
            ),
            "standby_reason": format_list_field(st.session_state.get(f"razao_select_{idx}", current_row.get("razao_standby", []))),
            "OBS": st.session_state.get(f"obs_input_{idx}", safe_get_field(current_row, "obs")),
            "stakeholder": format_boolean_field(stakeholder_val),
            "intermediador": format_boolean_field(intermediador_val),
            "imovel_em_inventario": format_boolean_field(inventario_val),
            "standby": format_boolean_field(standby_val),
            "fup_date": safe_get_field(current_row, "followup_date"),
            # Property assignment fields (with proper title case formatting)
            "endereco_bairro": format_address_field(assigned_property.get("BAIRRO", "")) if assigned_property else "",
            "endereco": format_address_field(assigned_property.get("ENDERECO", "")) if assigned_property else "",
            "endereco_complemento": format_address_field(assigned_property.get("COMPLEMENTO ENDERECO", "")) if assigned_property else "",
            "indice_cadastral_list": assigned_property.get("INDICE CADASTRAL", "") if assigned_property else "",
            # Required fields for new row creation
            "cpf": current_row.get("cpf", ""),
            "Nome": current_row.get("display_name", ""),
            "nome_whatsapp": current_row.get("display_name", ""),
            "celular": format_phone_for_storage(whatsapp_number.split('@')[0] if '@' in whatsapp_number else whatsapp_number),
            # Additional fields that might be in spreadsheet
            "imovel_anunciado": format_boolean_field(current_row.get("imovel_anunciado", False)),
            "Empresa": current_row.get("empresa", ""),
            "cnpj": current_row.get("cnpj", ""),
            "OBS_urb.link": current_row.get("obs_urblink", ""),
        }

        # Debug: Show formatted sync data
        if DEV and DEBUG:
            st.write("**Formatted sync data:**")
            for key, value in sync_data.items():
                st.write(f"  {key}: {repr(value)} (type: {type(value)})")

        # Sync to Google Sheet
        with st.spinner("Syncing to Google Sheet..."):
            try:
                result = sync_record_to_sheet(sync_data, whatsapp_number, "report")

                if result["success"]:
                    action = result["action"]
                    row_number = result["row_number"]
                    
                    if action == "created":
                        st.success(f"✅ New row created in Google Sheet! (Row #{row_number})")
                    elif action == "updated":
                        st.success(f"✅ Record updated in Google Sheet! (Row #{row_number})")
                    elif action == "already_synced":
                        st.info(f"ℹ️ Spreadsheet already has identical values (Row #{row_number})")
                    
                    # Mark as synced in the dataframe
                    st.session_state.master_df.at[idx, "sheet_synced"] = True
                    
                    # Show detailed field mapping table
                    if "field_mappings" in result and result["field_mappings"]:
                        st.subheader("📊 Sync Results")
                        mapping_data = []
                        for field, value in result["field_mappings"].items():
                            mapping_data.append({
                                "System Field": field,
                                "Value": str(value),
                                "Action": f"{action.title()} in Row #{row_number}"
                            })
                        
                        if mapping_data:
                            import pandas as pd
                            df_mapping = pd.DataFrame(mapping_data)
                            st.dataframe(df_mapping, use_container_width=True)
                    
                    if DEV and DEBUG:
                        st.write("**Full sync result:**", result)
                        
                else:
                    st.error(f"❌ Failed to sync to Google Sheet: {result.get('error', 'Unknown error')}")
                    if DEV and DEBUG:
                        st.write("**WhatsApp number for sync:**", whatsapp_number)
                        st.write("**Full sync result:**", result)

            except Exception as e:
                st.error(f"❌ Error during sync: {e}")
                if DEV and DEBUG:
                    import traceback

                    st.write("**Full error traceback:**")
                    st.code(traceback.format_exc())

with bot_next_col:
    st.button(
        "Próximo ➡️",
        key="bottom_next",
        disabled=bool(idx >= len(df) - 1),
        on_click=goto_next,
        use_container_width=True,
    )

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
                                conv_row["expected_name"]
                                if conv_row["expected_name"]
                                and conv_row["expected_name"].strip()
                                else conv_row["display_name"]
                            )
                            display_parts.append(f"**{expected_name}**")

                            # Add classificacao if not empty
                            if (
                                conv_row["classificacao"]
                                and conv_row["classificacao"].strip()
                            ):
                                display_parts.append(conv_row["classificacao"])

                            # Add intencao if not empty
                            if conv_row["intencao"] and conv_row["intencao"].strip():
                                display_parts.append(conv_row["intencao"])

                            # Add formatted date if available
                            formatted_date = format_last_message_date(
                                conv_row["last_message_date"]
                            )
                            if formatted_date:
                                display_parts.append(formatted_date)

                            # Join all parts with " | "
                            display_text = " | ".join(display_parts)
                            st.write(display_text)

                        with col2:
                            if st.button(
                                "➡️ Ir", key=f"goto_conv_{conv_row['row_index']}"
                            ):
                                # Navigate to this conversation
                                st.session_state.idx = conv_row["row_index"]
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
    phone_number = row["whatsapp_number"]

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
        f"Caso ID: {idx + 1} | WhatsApp: {row['whatsapp_number']} | {datetime.now():%H:%M:%S}"
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
