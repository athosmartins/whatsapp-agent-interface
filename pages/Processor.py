"""Processor.py - Streamlit interface for WhatsApp Agent with authentication."""

import copy
import os
from datetime import datetime

import pandas as pd
import streamlit as st

from auth.login_manager import simple_auth
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
from services.spreadsheet import sync_record_to_sheet
from services.voxuy_api import send_whatsapp_message
from services.mega_data_set_loader import get_properties_for_phone, format_property_for_display
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

def format_last_message_date(timestamp):
    """Format timestamp to format: 01/Jul/25 (10 dias)"""
    if pd.isna(timestamp) or timestamp == 0 or not timestamp or timestamp == '':
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
            dt = pd.to_datetime(timestamp, unit='s')
        else:
            dt = pd.to_datetime(timestamp)
        
        # Calculate days ago
        now = pd.Timestamp.now()
        days_ago = (now - dt).days
        
        # Portuguese month abbreviations
        months_pt_abbr = {
            1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
            5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
            9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
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

def find_conversations_with_same_property(current_property_address, current_property_neighborhood, current_conversation_id=None):
    """
    Find all conversations that have the same property (address + neighborhood).
    Returns a DataFrame with columns: classificacao, display_name, expected_name, phone, status
    """
    try:
        # Get all conversations data
        all_conversations_df = get_dataframe()
        
        if all_conversations_df.empty:
            return pd.DataFrame(columns=['classificacao', 'display_name', 'expected_name', 'phone', 'status'])
        
        # Find conversations with matching properties
        matching_conversations = []
        
        for idx, row in all_conversations_df.iterrows():
            # Skip current conversation - use the idx parameter passed from button click
            if hasattr(st.session_state, 'property_modal_data') and \
               st.session_state.property_modal_data.get('current_idx') == idx:
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
                if (prop_address and prop_neighborhood and 
                    prop_address.lower() == current_property_address.lower() and 
                    prop_neighborhood.lower() == current_property_neighborhood.lower()):
                    
                    # Add to matching conversations
                    matching_conversations.append({
                        'expected_name': row.get('nome', '') or row.get('name', '') or row.get('nome_proprietario', ''),
                        'classificacao': row.get('classificacao', '') or row.get('Classificação do dono do número', ''),
                        'display_name': row.get('display_name', ''),
                        'intencao': row.get('intencao', '') or row.get('Intenção', ''),
                        'last_message_date': row.get('last_message_timestamp', '') or row.get('last_message_time', '') or row.get('timestamp', ''),
                        'conversation_id': row.get('conversation_id', ''),  # Add this for navigation
                        'row_index': idx  # Add row index for navigation
                    })
                    found_match = True
                    break  # Found match, no need to check other properties for this conversation
            
            if found_match:
                continue  # Move to next conversation
        
        # Create DataFrame
        result_df = pd.DataFrame(matching_conversations)
        
        if DEBUG:
            print(f"DEBUG: Found {len(result_df)} conversations with same property")
            print(f"DEBUG: Property search: {current_property_address}, {current_property_neighborhood}")
        
        return result_df
        
    except Exception as e:
        if DEBUG:
            print(f"DEBUG: Error in find_conversations_with_same_property: {e}")
        return pd.DataFrame(columns=['classificacao', 'display_name', 'expected_name', 'phone', 'status'])

# ─── PAGE CONFIG (MUST BE FIRST) ────────────────────────────────────────
st.set_page_config(page_title="Processador de Conversas", page_icon="📱", layout="wide")

# ─── START BACKGROUND PRELOADER ─────────────────────────────────────────────
# Start background downloading of all critical files for smooth UX
if "preloader_started" not in st.session_state:
    st.session_state.preloader_started = True
    start_background_preload()

# ─── FLAGS ──────────────────────────────────────────────────────────────────
DEV = True  # Set based on your environment

# Debug mode check
DEBUG = st.sidebar.checkbox("Debug Mode", value=False)

# Display preloader status in sidebar
display_preloader_status()

# LOGIN_ENABLED flag - can be controlled via environment variable or hardcoded
LOGIN_ENABLED = True  # Default value

HIGHLIGHT_ENABLE = False

# Try to get from environment variable first
if "LOGIN_ENABLED" in os.environ:
    env_value = os.environ["LOGIN_ENABLED"].lower()
    LOGIN_ENABLED = env_value in ("true", "1", "yes", "on")
# Try to get from Streamlit secrets if available
elif hasattr(st, 'secrets') and "LOGIN_ENABLED" in st.secrets:
    LOGIN_ENABLED = st.secrets["LOGIN_ENABLED"]
# For DEV environment, you might want to disable login by default
elif DEV:
    LOGIN_ENABLED = False  # Disable login in DEV mode by default

# ─── AUTHENTICATION ─────────────────────────────────────────────────────
# Check authentication only if LOGIN_ENABLED is True
if LOGIN_ENABLED:
    if not simple_auth():
        st.stop()
else:
    # When login is disabled, show a warning in DEV mode
    if DEV:
        st.warning("🔓 Login is disabled (DEV mode)")

# ─── AUTHENTICATED APP STARTS HERE ──────────────────────────────────────
# Apply styles
st.markdown(STYLES, unsafe_allow_html=True)

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
        return datetime.fromtimestamp(timestamp).strftime('%H:%M')
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
                1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
                5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
                9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
            }
            
            # Portuguese weekday names
            weekdays_pt = {
                0: "Segunda-feira", 1: "Terça-feira", 2: "Quarta-feira",
                3: "Quinta-feira", 4: "Sexta-feira", 5: "Sábado", 6: "Domingo"
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
DEBUG = False

if DEV:
    DEBUG = st.sidebar.checkbox("🐛 Debug Mode", value=False)
    if DEBUG:
        debug_panel = st.sidebar.expander("🔍 Debug Log", expanded=False)
        
        # Add database info to debug panel
        db_info_panel = st.sidebar.expander("📊 Database Info", expanded=True)
        if db_info_panel:
            db_info = get_db_info()
            db_info_panel.write("**Database File Information:**")
            db_info_panel.write(f"📁 **Original filename:** {db_info['original_filename']}")
            db_info_panel.write(f"🕒 **Original modified:** {db_info['original_modified']}")
            db_info_panel.write(f"💾 **Local path:** {db_info['local_path']}")
            db_info_panel.write(f"📅 **Local modified:** {db_info['local_modified']}")
            db_info_panel.write(f"⏰ **File age:** {db_info['file_age']}")
            db_info_panel.write(f"📏 **File size:** {db_info['local_size']:,} bytes")
            
            # Show freshness status
            if db_info.get('is_stale', False):
                db_info_panel.warning("⚠️ Database file is older than 1 hour. Will auto-refresh on next load.")
            else:
                db_info_panel.success("✅ Database file is fresh (< 1 hour old)")
            
            db_info_panel.info("🔄 Database automatically refreshes when older than 1 hour")
            
            # Add DataFrame info
            if "master_df" in st.session_state:
                db_info_panel.write("**DataFrame Info:**")
                db_info_panel.write(f"📊 **Total records:** {len(st.session_state.master_df)}")
                db_info_panel.write(f"📝 **Available columns:** {list(st.session_state.master_df.columns)}")
                if "original_values" in st.session_state:
                    db_info_panel.write(f"✏️ **Modified records:** {len(st.session_state.original_values)}")


def dbg(message: str):
    """Write a debug message once to the sidebar panel."""
    if DEBUG and debug_panel and message not in logged_messages:
        logged_messages.add(message)
        debug_panel.write(message)


# ─── STATE MANAGEMENT FUNCTIONS ─────────────────────────────────────────
def initialize_session_state():
    """Initialize the session state with proper data structure."""
    # Check if we're coming from the Conversations page with specific conversation data
    if "processor_conversation_data" in st.session_state and st.session_state.processor_conversation_data:
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
                return [v.strip() for v in str(value).split(',') if v.strip()] if value else []
        
        # Helper function to parse boolean fields from spreadsheet
        def parse_spreadsheet_bool(value):
            if isinstance(value, bool):
                return value
            if pd.isna(value) or value is None:
                return False
            return str(value).lower() in ('true', '1', 'yes', 'sim', 'verdadeiro')
        
        # Map the conversation data to the expected deepseek_results format
        mapped_data = {
            'display_name': conversation_data.get('display_name', ''),
            'phone_number': conversation_data.get('phone_number', ''),
            'whatsapp_number': conversation_data.get('conversation_id', ''),
            'conversation_id': conversation_data.get('conversation_id', ''),
            'total_messages': conversation_data.get('total_messages', 0),
            'last_message_timestamp': conversation_data.get('last_message_timestamp', 0),
            # Add all the spreadsheet data that was merged
            'endereco': conversation_data.get('endereco', ''),
            'endereco_bairro': conversation_data.get('endereco_bairro', ''),
            'endereco_complemento': conversation_data.get('endereco_complemento', ''),
            'Nome': conversation_data.get('Nome', ''),
            'Classificação do dono do número': conversation_data.get('Classificação do dono do número', ''),
            'status': conversation_data.get('status', ''),
            'status_manual': conversation_data.get('status_manual', ''),
            # Initialize fields for classification - pre-load from spreadsheet where available
            'conversation_history': '',  # Will be loaded from messages
            'classificacao': conversation_data.get('Classificação do dono do número', ''),
            'intencao': conversation_data.get('status_manual', ''),  # Maps to intencao field
            'pagamento': conversation_data.get('pagamento', ''),
            'resposta': conversation_data.get('OBS', ''),  # Maps to resposta field
            'Razao': conversation_data.get('standby_reason', ''),
            'acoes_urblink': parse_spreadsheet_list(conversation_data.get('Ações', '')),
            'status_urblink': conversation_data.get('status_manual_urb.link', ''),
            'obs': conversation_data.get('OBS_urb.link', ''),  # Additional observations
            'stakeholder': parse_spreadsheet_bool(conversation_data.get('stakeholder', False)),
            'intermediador': parse_spreadsheet_bool(conversation_data.get('intermediador', False)),
            'inventario_flag': parse_spreadsheet_bool(conversation_data.get('imovel_em_inventario', False)),
            'standby': bool(conversation_data.get('standby_reason', '')),  # True if standby_reason exists
            'razao_standby': parse_spreadsheet_list(conversation_data.get('standby_reason', '')),
            'followup_date': conversation_data.get('fup_date', ''),
            'familiares': '',
            'IMOVEIS': '',
            'IDADE': '',
            'OBITO_PROVAVEL': '',
            'expected_name': conversation_data.get('Nome', ''),
            'percepcao_valor_esperado': conversation_data.get('percepcao_valor_esperado', ''),
            'imovel_em_inventario': conversation_data.get('imovel_em_inventario', ''),
            'PictureUrl': conversation_data.get('PictureUrl', ''),
        }
        
        # Create a dataframe with this single conversation
        st.session_state.master_df = pd.DataFrame([mapped_data])
        
        # Initialize display format for follow-up date if it exists
        if mapped_data.get('followup_date'):
            try:
                from datetime import datetime
                date_obj = datetime.strptime(mapped_data['followup_date'], '%Y-%m-%d').date()
                days_pt = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
                day_name = days_pt[date_obj.weekday()]
                display_format = f"{date_obj.strftime('%d/%m/%Y')} ({day_name})"
                st.session_state[f"followup_date_display_0"] = display_format
            except:
                pass  # If date format is invalid, just skip
        
        # Also set as original data
        st.session_state.original_db_data = st.session_state.master_df.copy()
        
        # Clear the conversation data from session state so it doesn't persist
        del st.session_state.processor_conversation_data
        
        st.info("📝 Processing conversation from Conversations page")
        
    else:
        # Normal initialization - load from deepseek_results
        if "master_df" not in st.session_state:
            st.session_state.master_df = load_data()
        
        # Initialize original_db_data (store the original database values)
        if "original_db_data" not in st.session_state:
            st.session_state.original_db_data = load_data()
    
    # Initialize original_values storage
    if "original_values" not in st.session_state:
        st.session_state.original_values = {}
    
    # Ensure all required columns exist in master_df
    required_columns = {
        'acoes_urblink': [],
        'status_urblink': "",
        'razao_standby': [],
        'obs': "",
        'stakeholder': False,
        'intermediador': False,
        'inventario_flag': False,
        'standby': False
    }
    
    for col, default_value in required_columns.items():
        if col not in st.session_state.master_df.columns:
            if isinstance(default_value, list):
                st.session_state.master_df[col] = [[]] * len(st.session_state.master_df)
            else:
                st.session_state.master_df[col] = [default_value] * len(st.session_state.master_df)
    
    # Also ensure original_db_data has the same columns
    for col, default_value in required_columns.items():
        if col not in st.session_state.original_db_data.columns:
            if isinstance(default_value, list):
                st.session_state.original_db_data[col] = [[]] * len(st.session_state.original_db_data)
            else:
                st.session_state.original_db_data[col] = [default_value] * len(st.session_state.original_db_data)
    
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
                    return [v.strip() for v in value.split(',') if v.strip()] if value else []
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
                return value.lower() in ['true', '1', 'yes', 'on']
            elif pd.isna(value) or value is None:
                return False
            else:
                return bool(value)
        
        st.session_state.original_values[idx] = {
            'classificacao': original_row.get("classificacao", "") or "",
            'intencao': original_row.get("intencao", "") or "",
            'acoes_urblink': parse_list_field(original_row.get("acoes_urblink")),
            'status_urblink': original_row.get("status_urblink", "") or "",
            'pagamento': original_row.get("pagamento", "") or "",
            'percepcao_valor_esperado': original_row.get("percepcao_valor_esperado", "") or "",
            'razao_standby': parse_list_field(original_row.get("razao_standby")),
            'resposta': original_row.get("resposta", "") or "",
            'obs': original_row.get("obs", "") or "",
            'stakeholder': parse_bool_field(original_row.get("stakeholder")),
            'intermediador': parse_bool_field(original_row.get("intermediador")),
            'inventario_flag': parse_bool_field(original_row.get("inventario_flag")),
            'standby': parse_bool_field(original_row.get("standby")),
            'followup_date': original_row.get("followup_date", "") or ""
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
            f"standby_input_{idx}"
        ]
        for key in widget_keys:
            if key in st.session_state:
                del st.session_state[key]

def update_field(idx, field, value):
    """Update a field value directly in master_df."""
    # Ensure the column exists in master_df
    if field not in st.session_state.master_df.columns:
        # Add missing column with default values
        if field in ['acoes_urblink', 'razao_standby']:
            st.session_state.master_df[field] = [[]] * len(st.session_state.master_df)
        elif field in ['stakeholder', 'intermediador', 'inventario_flag', 'standby']:
            st.session_state.master_df[field] = [False] * len(st.session_state.master_df)
        else:
            st.session_state.master_df[field] = [""] * len(st.session_state.master_df)
    
    # Convert column to object dtype if necessary to avoid dtype warnings
    if st.session_state.master_df[field].dtype != 'object':
        st.session_state.master_df[field] = st.session_state.master_df[field].astype('object')
    
    st.session_state.master_df.at[idx, field] = value

def compare_values(original, current):
    """Compare two values, handling lists and different types properly."""
    # Handle None values first
    if original is None:
        original = [] if isinstance(current, list) else (False if isinstance(current, bool) else "")
    if current is None:
        current = [] if isinstance(original, list) else (False if isinstance(original, bool) else "")
    
    # Handle NaN values for non-list types only
    try:
        if not isinstance(original, list) and pd.isna(original):
            original = [] if isinstance(current, list) else (False if isinstance(current, bool) else "")
    except (TypeError, ValueError):
        pass
    
    try:
        if not isinstance(current, list) and pd.isna(current):
            current = [] if isinstance(original, list) else (False if isinstance(original, bool) else "")
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

# Ensure idx is within bounds
st.session_state.idx = min(st.session_state.idx, len(df) - 1)
idx = st.session_state.idx

# Get current row
row = df.iloc[idx]

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
    st.session_state.idx = max(st.session_state.idx - 1, 0)


def goto_next():
    """Go to the next conversation."""
    st.session_state.idx = min(st.session_state.idx + 1, len(df) - 1)


nav_prev_col, _, nav_next_col = st.columns([1, 2, 1])
with nav_prev_col:
    st.button(
        "⬅️ Anterior",
        key="top_prev",
        disabled=bool(idx == 0),
        on_click=goto_prev,
        use_container_width=True,
    )
with nav_next_col:
    st.button(
        "Próximo ➡️",
        key="top_next",
        disabled=bool(idx >= len(df) - 1),
        on_click=goto_next,
        use_container_width=True,
    )

# ─── MAIN CONTENT LAYOUT ────────────────────────────────────────────────────
left_col, right_col = st.columns([1, 1])

with left_col:
    # ─── CONTACT SECTION ────────────────────────────────────────────────────────
    hl_words = build_highlights(row["display_name"], row["expected_name"])

    # Create contact info HTML with fixed height
    picture = row.get("PictureUrl")
    # Clean and validate picture URL
    if picture and not pd.isna(picture) and str(picture).strip() and str(picture).strip().lower() not in ['none', 'null', '']:
        picture = str(picture).strip()
        if DEBUG:
            dbg(f"Picture URL found: {picture[:50]}...")
    else:
        picture = None
        if DEBUG:
            dbg(f"No valid picture URL (raw value: {repr(row.get('PictureUrl'))})")
        
    display_name = highlight(row["display_name"], hl_words) if HIGHLIGHT_ENABLE else row["display_name"]
    expected_name = highlight(row["expected_name"], hl_words)
    familiares_list = parse_familiares_grouped(row["familiares"])
    age = row.get("IDADE")
    age_text = ""
    if pd.notna(age) and str(age).strip() and str(age).strip() != '':
        try:
            age_int = int(float(str(age).strip()))
            age_text = f"**{age_int} anos**"
        except (ValueError, TypeError):
            age_text = ""
    alive_status = "✝︎ Provável Óbito" if row.get("OBITO_PROVAVEL", False) else "🌟 Provável vivo"

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
                    <strong>Nome Esperado:</strong> {expected_name}
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
            'phone_number': phone_number,
            'spreadsheet_mapping': None,
            'cpf_found': None,
            'mega_data_properties': [],
            'errors': []
        }
        
        try:
            # Step 1: Debug phone cleaning
            from services.mega_data_set_loader import clean_phone_for_match
            clean_phone = clean_phone_for_match(phone_number)
            debug_info['clean_phone'] = clean_phone
            
            # Step 2: Debug spreadsheet mapping
            from services.spreadsheet import get_sheet_data
            sheet_data = get_sheet_data()
            
            if sheet_data:
                headers = sheet_data[0] if sheet_data else []
                debug_info['spreadsheet_headers'] = headers
                
                # Find column indices
                cpf_col_index = None
                phone_col_index = None
                
                for i, header in enumerate(headers):
                    header_lower = str(header).lower()
                    if any(term in header_lower for term in ['cpf', 'documento', 'doc']):
                        cpf_col_index = i
                    if any(term in header_lower for term in ['celular', 'phone', 'telefone', 'contato']):
                        phone_col_index = i
                
                debug_info['cpf_column_index'] = cpf_col_index
                debug_info['phone_column_index'] = phone_col_index
                
                # Search for matching row
                debug_info['spreadsheet_matches'] = []
                for row_idx, data_row in enumerate(sheet_data[1:], 1):  # Skip header
                    if phone_col_index is not None and phone_col_index < len(data_row):
                        sheet_phone = data_row[phone_col_index]
                        sheet_phone_clean = clean_phone_for_match(sheet_phone)
                        
                        if sheet_phone_clean == clean_phone:
                            cpf = data_row[cpf_col_index] if cpf_col_index is not None and cpf_col_index < len(data_row) else None
                            debug_info['spreadsheet_matches'].append({
                                'row_number': row_idx,
                                'original_phone': sheet_phone,
                                'cleaned_phone': sheet_phone_clean,
                                'cpf': cpf,
                                'full_row': data_row
                            })
                            debug_info['cpf_found'] = cpf
                            break
                
                # Step 3: Debug mega_data_set lookup
                if debug_info['cpf_found']:
                    from services.mega_data_set_loader import find_properties_by_documento, load_mega_data_set
                    
                    # Load mega_data_set to show what's available
                    mega_df = load_mega_data_set()
                    debug_info['mega_data_total_rows'] = len(mega_df)
                    debug_info['mega_data_columns'] = list(mega_df.columns)
                    
                    # Find document column
                    doc_col = None
                    for col in mega_df.columns:
                        if col == 'DOCUMENTO PROPRIETARIO':
                            doc_col = col
                            break
                    
                    debug_info['mega_data_document_column'] = doc_col
                    
                    if doc_col:
                        # Show CPF cleaning
                        from services.mega_data_set_loader import clean_document_number
                        clean_cpf = clean_document_number(debug_info['cpf_found'])
                        debug_info['clean_cpf'] = clean_cpf
                        
                        # Check for matches
                        debug_info['mega_data_matches'] = []
                        checked_count = 0
                        for idx, mega_row in mega_df.iterrows():
                            row_cpf = clean_document_number(str(mega_row[doc_col]))
                            checked_count += 1
                            
                            # Show first few comparisons
                            if checked_count <= 5:
                                debug_info['errors'].append(f"Row {idx}: '{mega_row[doc_col]}' -> '{row_cpf}' vs '{clean_cpf}'")
                            
                            if row_cpf == clean_cpf:
                                debug_info['mega_data_matches'].append({
                                    'row_index': idx,
                                    'original_cpf': mega_row[doc_col],
                                    'cleaned_cpf': row_cpf,
                                    'property_data': mega_row.to_dict()
                                })
                                debug_info['errors'].append(f"MATCH FOUND at row {idx}!")
                                break  # Found one match, that's enough
                        
                        debug_info['errors'].append(f"Checked {checked_count} rows in mega_data_set")
                    
                    # Get properties using the service
                    print(f"\nDEBUG: About to call find_properties_by_documento with CPF: '{debug_info['cpf_found']}'")
                    properties = find_properties_by_documento(debug_info['cpf_found'])
                    debug_info['mega_data_properties'] = properties
                    print(f"DEBUG: find_properties_by_documento returned {len(properties)} properties")
            
            else:
                debug_info['errors'].append("No spreadsheet data available")
                
        except Exception as e:
            debug_info['errors'].append(f"Debug error: {str(e)}")
            import traceback
            debug_info['traceback'] = traceback.format_exc()
        
        return debug_info
    
    # Get properties from mega_data_set using phone number
    phone_number = row.get('phone_number') or row.get('whatsapp_number', '')
    debug_info = None
    
    if phone_number:
        try:
            # Run comprehensive debugging
            if DEBUG:
                debug_info = debug_property_mapping(phone_number, row)
            
            properties_from_mega = get_properties_for_phone(phone_number)
            # Format properties for display
            imoveis = [format_property_for_display(prop) for prop in properties_from_mega]
            if DEBUG:
                print(f"DEBUG: Found {len(imoveis)} properties from mega_data_set for phone {phone_number}")
        except Exception as e:
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
            # Apply styling to the entire container
            # st.markdown("""
            #     <style>
            #     .imoveis-container {
            #         border: 1px solid #ddd;
            #         border-radius: 5px;
            #         padding: 10px;
            #         background-color: #f9f9f9;
            #         margin-bottom: 20px;
            #         max-height: 400px;
            #         overflow-y: auto;
            #     }
            #     </style>
            # """, unsafe_allow_html=True)
            
            # Add the custom CSS class
            st.markdown('<div class="imoveis-container">', unsafe_allow_html=True)
            
            if imoveis:
                for i, item in enumerate(imoveis):
                    if not isinstance(item, dict):
                        continue
                    
                    # Handle both old format (AREA TERRENO) and new format (area_terreno)
                    area_terreno = item.get("area_terreno") or item.get("AREA TERRENO", "?")
                    area_construcao = item.get("area_construcao") or item.get("AREA CONSTRUCAO", "?")
                    fraction = item.get("fracao_ideal") or item.get("FRACAO IDEAL", "")
                    build_type = item.get("tipo_construtivo") or item.get("TIPO CONSTRUTIVO", "").strip()
                    address = item.get("endereco") or item.get("ENDERECO", "?")
                    neighborhood = item.get("bairro") or item.get("BAIRRO", "?")
                    indice_cadastral = item.get("indice_cadastral") or item.get("INDICE CADASTRAL", "")
                    
                    # Format areas
                    area_terreno_text = fmt_num(area_terreno) if area_terreno else "?"
                    area_construcao_text = fmt_num(area_construcao) if area_construcao else "?"
                    
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
                            related_conversations_df = find_conversations_with_same_property(
                                address, neighborhood, row.get('conversation_id')
                            )
                            has_related_conversations = not related_conversations_df.empty
                            
                            if DEBUG:
                                if has_related_conversations:
                                    print(f"DEBUG: Found {len(related_conversations_df)} related conversations for {address}, {neighborhood}")
                                else:
                                    print(f"DEBUG: No related conversations for {address}, {neighborhood}")
                        except Exception as e:
                            if DEBUG:
                                print(f"DEBUG: Error checking related conversations: {e}")
                            has_related_conversations = False
                        
                        if has_related_conversations:
                            # Show active button when there are related conversations
                            if st.button("🔍", key=f"property_btn_{idx}_{i}"):
                                # Store property info in session state for modal
                                st.session_state.property_modal_data = {
                                    'address': address,
                                    'neighborhood': neighborhood,
                                    'current_conversation_id': row.get('conversation_id'),
                                    'current_idx': idx,  # Add current row index
                                    'show_modal': True
                                }
                                st.rerun()
                        else:
                            # Show greyed out button when no related conversations
                            st.button(
                                "🔍", 
                                key=f"property_btn_{idx}_{i}_disabled",
                                disabled=True,
                                help="Nenhuma conversa relacionada encontrada para esta propriedade"
                            )
            else:
                st.markdown('<div style="color: #888; font-style: italic;">Nenhum imóvel encontrado</div>', unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # ─── COMPREHENSIVE DEBUG INFORMATION ─────────────────────────────────────────
    if DEBUG and debug_info:
        with st.expander("🔍 **Property Mapping Debug Information**", expanded=True):
            st.markdown("### 📞 **Step 1: Phone Number Processing**")
            st.write(f"**Original phone:** `{debug_info['phone_number']}`")
            st.write(f"**Cleaned phone:** `{debug_info.get('clean_phone', 'N/A')}`")
            
            st.markdown("### 📊 **Step 2: Spreadsheet Lookup**")
            if debug_info.get('spreadsheet_headers'):
                st.write(f"**Spreadsheet headers:** {debug_info['spreadsheet_headers']}")
                st.write(f"**CPF column index:** {debug_info.get('cpf_column_index')}")
                st.write(f"**Phone column index:** {debug_info.get('phone_column_index')}")
                
                if debug_info.get('spreadsheet_matches'):
                    st.success(f"✅ **Found {len(debug_info['spreadsheet_matches'])} spreadsheet match(es)**")
                    for match in debug_info['spreadsheet_matches']:
                        st.write(f"**→ Row {match['row_number']}:** `{match['original_phone']}` → `{match['cleaned_phone']}` → CPF: `{match['cpf']}`")
                else:
                    st.error("❌ **No spreadsheet matches found**")
                    st.write("**Phone number not found in spreadsheet data**")
            else:
                st.error("❌ **No spreadsheet data available**")
                
            # Show general errors
            if debug_info.get('errors'):
                st.write("**Errors/Debug info:**")
                for error in debug_info['errors']:
                    st.write(f"- {error}")
            
            st.markdown("### 🏢 **Step 3: Mega Data Set Lookup**")
            if debug_info.get('cpf_found'):
                st.write(f"**CPF to search:** `{debug_info['cpf_found']}`")
                st.write(f"**Cleaned CPF:** `{debug_info.get('clean_cpf', 'N/A')}`")
                
                # Show data source status
                total_rows = debug_info.get('mega_data_total_rows', 'N/A')
                if total_rows != 'N/A' and int(total_rows) < 10000:
                    st.error(f"⚠️ **SAMPLE DATA DETECTED:** {total_rows} rows (should be 350k+)")
                    st.error("**This is NOT production data! See ENABLE_GOOGLE_DRIVE_API.md**")
                else:
                    st.success(f"✅ **Real mega data:** {total_rows} rows")
                
                st.write(f"**Mega data columns:** {debug_info.get('mega_data_columns', [])}")
                st.write(f"**Document column:** `{debug_info.get('mega_data_document_column', 'N/A')}`")
                
                if debug_info.get('mega_data_matches'):
                    st.success(f"✅ **Found {len(debug_info['mega_data_matches'])} property match(es)**")
                    for match in debug_info['mega_data_matches']:
                        property_data = match['property_data']
                        st.write(f"**→ Row {match['row_index']}:** `{match['original_cpf']}` → `{match['cleaned_cpf']}`")
                        st.write(f"   **Address:** {property_data.get('ENDERECO', 'N/A')}")
                        st.write(f"   **Neighborhood:** {property_data.get('BAIRRO', 'N/A')}")
                        st.write(f"   **Cadastral Index:** {property_data.get('INDICE CADASTRAL', 'N/A')}")
                else:
                    st.error("❌ **No property matches found in mega data set**")
                    st.error("CPF not found in mega data set")
                    
                    # Show debugging info
                    if debug_info.get('errors'):
                        st.write("**Debug info:**")
                        for error in debug_info['errors']:
                            st.write(f"- {error}")
                    st.error("❌ **No property matches found in mega data set**")
                    st.write("**CPF not found in mega data set**")
            else:
                st.warning("⚠️ **No CPF found to search properties**")
            
            st.markdown("### 📋 **Step 4: Final Results**")
            st.write(f"**Properties returned:** {len(debug_info.get('mega_data_properties', []))}")
            if debug_info.get('mega_data_properties'):
                for i, prop in enumerate(debug_info['mega_data_properties']):
                    st.write(f"**Property {i+1}:** {prop.get('ENDERECO', 'N/A')} - {prop.get('BAIRRO', 'N/A')}")
            
            if debug_info.get('errors'):
                st.markdown("### ⚠️ **Errors**")
                for error in debug_info['errors']:
                    st.error(error)
                    
            if debug_info.get('traceback'):
                st.markdown("### 🐛 **Traceback**")
                st.code(debug_info['traceback'])

with right_col:
    # ─── CHAT HISTORY ───────────────────────────────────────────────────────────
    # st.subheader("💬 Histórico da Conversa")
    
    # Parse the conversation history and display in WhatsApp style
    try:
        # First try to load messages from database if we have a conversation_id
        messages = []
        if 'conversation_id' in row.index and pd.notna(row['conversation_id']):
            try:
                messages_df = get_conversation_messages(row['conversation_id'])
                if not messages_df.empty:
                    # Convert database messages to the expected format
                    for _, msg_row in messages_df.iterrows():
                        sender = "Urb.Link" if msg_row.get('from_me', False) else row.get('display_name', 'Contact')
                        messages.append({
                            'sender': sender,
                            'msg': msg_row['message_text'],
                            'ts': datetime.fromtimestamp(msg_row['timestamp']).strftime('%d/%m/%Y %H:%M')
                        })
            except Exception as e:
                if DEBUG:
                    print(f"DEBUG: Could not load messages from database: {e}")
        
        # If no messages from database, fall back to parsed conversation history
        if not messages and 'conversation_history' in row.index and pd.notna(row['conversation_history']) and row['conversation_history']:
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
                    timestamp_str = msg['ts'].strip()
                    
                    # Debug: let's see what format we're dealing with
                    if DEBUG:
                        print(f"DEBUG: Parsing timestamp: '{timestamp_str}'")
                    
                    # Try various common formats
                    formats_to_try = [
                        '%d/%m/%Y %H:%M',      # 25/06/2025 15:30
                        '%Y-%m-%d %H:%M',      # 2025-06-25 15:30
                        '%d/%m/%Y %H:%M:%S',   # 25/06/2025 15:30:45
                        '%Y-%m-%d %H:%M:%S',   # 2025-06-25 15:30:45
                        '%H:%M',               # 15:30 (time only)
                        '%d/%m %H:%M',         # 25/06 15:30 (no year)
                    ]
                    
                    for fmt in formats_to_try:
                        try:
                            dt = datetime.strptime(timestamp_str, fmt)
                            if fmt == '%H:%M':
                                # If only time, assume today
                                dt = dt.replace(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
                            elif fmt == '%d/%m %H:%M':
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
                                    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
                                    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
                                    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
                                }
                                
                                # Portuguese weekday names
                                weekdays_pt = {
                                    0: "Segunda-feira", 1: "Terça-feira", 2: "Quarta-feira",
                                    3: "Quinta-feira", 4: "Sexta-feira", 5: "Sábado", 6: "Domingo"
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
                        msg_time = dt.strftime('%H:%M')
                    else:
                        # If all parsing fails, extract time manually
                        if ':' in timestamp_str:
                            time_part = timestamp_str.split()[-1] if ' ' in timestamp_str else timestamp_str
                            if ':' in time_part:
                                msg_time = time_part[:5]  # Get only HH:MM
                            else:
                                msg_time = timestamp_str
                        else:
                            msg_time = timestamp_str
                            
                except Exception as e:
                    # If timestamp parsing fails completely, use original
                    if DEBUG:
                        print(f"DEBUG: Timestamp parsing failed: {e}")
                    msg_time = msg['ts']
                
                # Determine if message is from business or contact
                is_from_me = msg["sender"] in ("Urb.Link", "Athos")
                
                # Process the message text but DON'T escape HTML tags (we want <strong> to work)
                clean_msg = bold_asterisks(msg['msg'])
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
                    chat_html += f'''<div style="display: flex; justify-content: flex-end; margin: 2px 0; width: 100%;">
                        <div style="background-color: #dcf8c6; padding: 8px 12px; border-radius: 18px; max-width: 400px; min-width: 120px; display: inline-block;">
                            <div style="display: inline-block; max-width: 100%;">{clean_msg}</div>
                            <div style="font-size: 11px; color: #666; text-align: right; margin-top: 2px;">{clean_time}</div>
                        </div>
                    </div>'''
                else:
                    # Message from contact (left side, white/light gray)
                    chat_html += f'''<div style="display: flex; justify-content: flex-start; margin: 2px 0; width: 100%;">
                        <div style="background-color: #ffffff; padding: 8px 12px; border-radius: 18px; max-width: 400px; min-width: 120px; border: 1px solid #e0e0e0; display: inline-block;">
                            <div style="display: inline-block; max-width: 100%;">{clean_msg}</div>
                            <div style="font-size: 11px; color: #666; text-align: right; margin-top: 2px;">{clean_time}</div>
                        </div>
                    </div>'''
            
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
    st.markdown(f"""
    <div style="margin-top: 25px;">
        <strong>📋 Racional usado pela AI classificadora:</strong><br>
        <div class='reason-box' style="margin-top: 5px; font-size: 0.85rem; max-height: 100px; overflow-y: auto;">
            {row['Razao']}
        </div>
    </div>
    """, unsafe_allow_html=True)

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
    current_classificacao = row.get('classificacao', '')
    classificacao_index = CLASSIFICACAO_OPTS.index(current_classificacao) if current_classificacao in CLASSIFICACAO_OPTS else 0
    classificacao_sel = st.selectbox(
        "🏷️ Classificação",
        CLASSIFICACAO_OPTS,
        index=classificacao_index,
        key=f"classificacao_select_{idx}",
        on_change=lambda: update_field(idx, 'classificacao', st.session_state[f"classificacao_select_{idx}"])
    )

    # Intenção
    current_intencao = row.get('intencao', '')
    intencao_index = INTENCAO_OPTS.index(current_intencao) if current_intencao in INTENCAO_OPTS else 0
    intencao_sel = st.selectbox(
        "🔍 Intenção",
        INTENCAO_OPTS,
        index=intencao_index,
        key=f"intencao_select_{idx}",
        on_change=lambda: update_field(idx, 'intencao', st.session_state[f"intencao_select_{idx}"])
    )

    # Ações Urb.Link
    current_acoes = row.get('acoes_urblink', [])
    if isinstance(current_acoes, str):
        import json
        try:
            current_acoes = json.loads(current_acoes) if current_acoes else []
        except:
            current_acoes = [v.strip() for v in current_acoes.split(',') if v.strip()] if current_acoes else []
    elif not isinstance(current_acoes, list):
        current_acoes = []
    
    def on_acoes_change():
        new_value = st.session_state[f"acoes_select_{idx}"]
        update_field(idx, 'acoes_urblink', new_value)
        dbg(f"Acoes updated to: {new_value}")
    
    acoes_sel = st.multiselect(
        "📞 Ações Urb.Link",
        ACOES_OPTS,
        default=current_acoes,
        key=f"acoes_select_{idx}",
        on_change=on_acoes_change
    )

    # Status Urb.Link
    status_opts = [""] + STATUS_URBLINK_OPTS
    current_status = row.get('status_urblink', '')
    status_index = status_opts.index(current_status) if current_status in status_opts else 0
    def on_status_change():
        new_value = st.session_state[f"status_select_{idx}"]
        update_field(idx, 'status_urblink', new_value)
        dbg(f"Status updated to: {new_value}")
    
    status_sel = st.selectbox(
        "🚦 Status Urb.Link",
        status_opts,
        index=status_index,
        key=f"status_select_{idx}",
        on_change=on_status_change
    )

    # Helper function to safely split comma-separated values
    def safe_split_csv(value):
        """Safely split a value into a list, handling various data types."""
        if pd.isna(value) or value is None:
            return []
        value_str = str(value).strip()
        return [v.strip() for v in value_str.split(",") if v.strip()] if value_str else []
    
    # Forma de Pagamento
    current_pagamento = row.get('pagamento', '')
    pag_default = safe_split_csv(current_pagamento)
    pagamento_sel = st.multiselect(
        "💳 Forma de Pagamento",
        PAGAMENTO_OPTS,
        default=pag_default,
        key=f"pagamento_select_{idx}",
        on_change=lambda: update_field(idx, 'pagamento', ", ".join(st.session_state[f"pagamento_select_{idx}"]))
    )

    # Percepção de Valor
    percepcao_opts = [""] + PERCEPCAO_OPTS
    current_percepcao = row.get('percepcao_valor_esperado', '')
    percepcao_index = percepcao_opts.index(current_percepcao) if current_percepcao in percepcao_opts else 0
    percepcao_sel = st.selectbox(
        "💎 Percepção de Valor",
        percepcao_opts,
        index=percepcao_index,
        key=f"percepcao_select_{idx}",
        on_change=lambda: update_field(idx, 'percepcao_valor_esperado', st.session_state[f"percepcao_select_{idx}"])
    )

    # Razão Stand-by
    current_razao = row.get('razao_standby', [])
    if isinstance(current_razao, str):
        import json
        try:
            current_razao = json.loads(current_razao) if current_razao else []
        except:
            current_razao = [v.strip() for v in current_razao.split(',') if v.strip()] if current_razao else []
    elif not isinstance(current_razao, list):
        current_razao = []
    
    def on_razao_change():
        new_value = st.session_state[f"razao_select_{idx}"]
        update_field(idx, 'razao_standby', new_value)
        dbg(f"Razao updated to: {new_value}")
    
    razao_sel = st.multiselect(
        "🤔 Razão Stand-by",
        STANDBY_REASONS,
        default=current_razao,
        key=f"razao_select_{idx}",
        on_change=on_razao_change
    )

with right_col:
    # Resposta
    current_resposta = row.get('resposta', '')
    resposta_input = st.text_area(
        "✏️ Resposta",
        value=current_resposta,
        height=180,
        key=f"resposta_input_{idx}",
        on_change=lambda: update_field(idx, 'resposta', st.session_state[f"resposta_input_{idx}"])
    )
    
    # Send button for the message
    if st.button("📤 Enviar Mensagem", key=f"send_btn_{idx}"):
        if resposta_input.strip():
            # Get phone number from the conversation data
            phone_number = row.get('phone_number', '')
            client_name = row.get('name', '')
            
            # Show loading spinner
            with st.spinner("Enviando mensagem..."):
                result = send_whatsapp_message(
                    phone_number=phone_number,
                    message_content=resposta_input,
                    client_name=client_name
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
                    st.json({
                        "phone_number": phone_number,
                        "message_content": resposta_input[:100] + "..." if len(resposta_input) > 100 else resposta_input,
                        "client_name": client_name
                    })
        else:
            st.warning("⚠️ Por favor, digite uma mensagem antes de enviar.")

    # OBS
    current_obs = row.get('obs', '')
    def on_obs_change():
        new_value = st.session_state[f"obs_input_{idx}"]
        update_field(idx, 'obs', new_value)
        dbg(f"OBS updated to: {new_value}")
    
    obs_input = st.text_area(
        "📋 OBS",
        value=current_obs,
        height=120,
        key=f"obs_input_{idx}",
        on_change=on_obs_change
    )

    # Checkboxes
    def parse_bool_value(value):
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value.lower() in ['true', '1', 'yes', 'on']
        elif pd.isna(value) or value is None:
            return False
        else:
            return bool(value)
    
    def on_stakeholder_change():
        new_value = st.session_state[f"stakeholder_input_{idx}"]
        update_field(idx, 'stakeholder', new_value)
        dbg(f"Stakeholder updated to: {new_value}")
    
    def on_intermediador_change():
        new_value = st.session_state[f"intermediador_input_{idx}"]
        update_field(idx, 'intermediador', new_value)
        dbg(f"Intermediador updated to: {new_value}")
    
    def on_inventario_change():
        new_value = st.session_state[f"inventario_input_{idx}"]
        update_field(idx, 'inventario_flag', new_value)
        dbg(f"Inventario updated to: {new_value}")
    
    def on_standby_change():
        new_value = st.session_state[f"standby_input_{idx}"]
        update_field(idx, 'standby', new_value)
        dbg(f"Standby updated to: {new_value}")
    
    # Create layout with checkboxes and calendar icon
    flags_col, calendar_col = st.columns([5, 1])
    
    with flags_col:
        current_stakeholder = parse_bool_value(row.get('stakeholder', False))
        stakeholder_input = st.checkbox(
            "Stakeholder", 
            value=current_stakeholder, 
            key=f"stakeholder_input_{idx}",
            on_change=on_stakeholder_change
        )

        current_intermediador = parse_bool_value(row.get('intermediador', False))
        intermediador_input = st.checkbox(
            "Intermediador", 
            value=current_intermediador, 
            key=f"intermediador_input_{idx}",
            on_change=on_intermediador_change
        )

        current_inventario = parse_bool_value(row.get('inventario_flag', False))
        inventario_input = st.checkbox(
            "Inventário", 
            value=current_inventario, 
            key=f"inventario_input_{idx}",
            on_change=on_inventario_change
        )

        current_standby = parse_bool_value(row.get('standby', False))
        standby_input = st.checkbox(
            "Stand-by", 
            value=current_standby, 
            key=f"standby_input_{idx}",
            on_change=on_standby_change
        )
    
    with calendar_col:
        # Calendar icon button for follow-up date
        current_followup = row.get('followup_date', '')
        current_followup_display = st.session_state.get(f"followup_date_display_{idx}", "")
        
        if current_followup:
            button_text = "📅✅"
            # Show user-friendly format in tooltip if available, otherwise convert ISO to display format
            if current_followup_display:
                button_help = f"Follow-up: {current_followup_display}"
            else:
                # Convert ISO format to display format for existing data
                try:
                    from datetime import datetime
                    date_obj = datetime.strptime(current_followup, '%Y-%m-%d').date()
                    days_pt = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
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
                    key=f"followup_amount_{idx}"
                )
            
            with followup_col2:
                followup_unit = st.selectbox(
                    "Período",
                    options=["dias", "semanas", "meses"],
                    index=["dias", "semanas", "meses"].index(st.session_state.get(f"followup_unit_{idx}", "dias")),
                    key=f"followup_unit_{idx}"
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
            iso_date = target_date.strftime('%Y-%m-%d')  # For spreadsheet (2025-12-28)
            days_pt = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
            day_name = days_pt[target_date.weekday()]
            display_date = f"{target_date.strftime('%d/%m/%Y')} ({day_name})"  # For display (28/12/2025 (Segunda))
            
            # Update the follow-up date automatically
            current_followup_display = st.session_state.get(f"followup_date_display_{idx}", "")
            current_followup_iso = st.session_state.get(f"followup_date_{idx}", "")
            
            if current_followup_display != display_date or current_followup_iso != iso_date:
                st.session_state[f"followup_date_display_{idx}"] = display_date
                st.session_state[f"followup_date_{idx}"] = iso_date
                update_field(idx, 'followup_date', iso_date)  # Store ISO format in dataframe
                    
            # Display calculated date
            if st.session_state.get(f"followup_date_display_{idx}"):
                st.success(f"📅 Follow-up agendado para: **{st.session_state[f'followup_date_display_{idx}']}**")
            
            # Action buttons
            button_col1, button_col2 = st.columns([1, 1])
            
            with button_col1:
                if st.button("Limpar", key=f"clear_followup_{idx}"):
                    st.session_state[f"followup_date_{idx}"] = ""
                    st.session_state[f"followup_date_display_{idx}"] = ""
                    update_field(idx, 'followup_date', "")
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
                problematic_fields = ['acoes_urblink', 'status_urblink', 'razao_standby', 'obs', 'stakeholder', 'intermediador', 'inventario_flag', 'standby']
                for field in problematic_fields:
                    if field in current:
                        st.write(f"**{field}**: {current[field]} (type: {type(current[field])})")
                    else:
                        st.write(f"**{field}**: NOT IN CURRENT")
                st.write("**Field comparisons:**")
                for field in original:
                    if field in current:
                        orig_val = original[field]
                        curr_val = current[field]
                        is_equal = compare_values(orig_val, curr_val)
                        st.write(f"**{field}**: {orig_val} → {curr_val} (Equal: {is_equal})")

# ─── NAVIGATION BOTTOM ──────────────────────────────────────────────────────
st.markdown("---")
bot_prev_col, dashboard_col, reset_col, sync_col, bot_next_col = st.columns([1, 1, 1, 1, 1])
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
        whatsapp_number = current_row.get('whatsapp_number', '')
        
        # Prepare data to sync with correct column mappings (exclude resposta and standby)
        def format_list_field(field_value):
            """Convert list to comma-separated string"""
            if isinstance(field_value, list):
                return ', '.join(str(item) for item in field_value)
            elif isinstance(field_value, str):
                return field_value
            else:
                return ''
        
        def format_boolean_field(field_value):
            """Convert boolean to TRUE/FALSE string"""
            import numpy as np
            # Handle NaN values first
            if pd.isna(field_value):
                return 'FALSE'
            # Handle both Python bool and numpy.bool_
            if isinstance(field_value, (bool, np.bool_, np.bool)):
                return 'TRUE' if bool(field_value) else 'FALSE'
            elif isinstance(field_value, str):
                return 'TRUE' if field_value.lower() in ['true', '1', 'yes'] else 'FALSE'
            else:
                return 'FALSE'
        
        def safe_get_field(row, field, default=''):
            """Safely get a field value, converting NaN to default."""
            value = row.get(field, default)
            if pd.isna(value):
                return default
            return value
        
        # Debug: Check boolean values before formatting
        stakeholder_val = current_row.get('stakeholder', False)
        intermediador_val = current_row.get('intermediador', False)
        inventario_val = current_row.get('inventario_flag', False)
        
        if DEV and DEBUG:
            st.write(f"Debug - Boolean values before sync:")
            st.write(f"stakeholder: {stakeholder_val} (type: {type(stakeholder_val)})")
            st.write(f"intermediador: {intermediador_val} (type: {type(intermediador_val)})")
            st.write(f"inventario_flag: {inventario_val} (type: {type(inventario_val)})")
        
        sync_data = {
            'Classificação do dono do número': safe_get_field(current_row, 'classificacao'),
            'status_manual': safe_get_field(current_row, 'intencao'),
            'Ações': format_list_field(current_row.get('acoes_urblink', [])),
            'status_manual_urb.link': safe_get_field(current_row, 'status_urblink'),
            'pagamento': safe_get_field(current_row, 'pagamento'),
            'percepcao_valor_esperado': safe_get_field(current_row, 'percepcao_valor_esperado'),
            'standby_reason': format_list_field(current_row.get('razao_standby', [])),
            'OBS': safe_get_field(current_row, 'obs'),
            'stakeholder': format_boolean_field(stakeholder_val),
            'intermediador': format_boolean_field(intermediador_val),
            'imovel_em_inventario': format_boolean_field(inventario_val),
            'fup_date': safe_get_field(current_row, 'followup_date')
        }
        
        # Debug: Show formatted sync data
        if DEV and DEBUG:
            st.write("**Formatted sync data:**")
            for key, value in sync_data.items():
                st.write(f"  {key}: {repr(value)} (type: {type(value)})")
        
        # Sync to Google Sheet
        with st.spinner('Syncing to Google Sheet...'):
            try:
                success = sync_record_to_sheet(sync_data, whatsapp_number, "report")
                
                if success:
                    st.success("✅ Record synced to Google Sheet!")
                    # Mark as synced in the dataframe
                    st.session_state.master_df.at[idx, 'sheet_synced'] = True
                else:
                    st.error("❌ Failed to sync to Google Sheet")
                    if DEV and DEBUG:
                        st.write("**WhatsApp number for sync:**", whatsapp_number)
                        st.write("**Sync attempt completed but returned False**")
                        
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
if hasattr(st.session_state, 'property_modal_data') and st.session_state.property_modal_data.get('show_modal', False):
    modal_data = st.session_state.property_modal_data
    
    # Create modal using dialog with wider width
    @st.dialog("Conversas Relacionadas", width="large")
    def show_property_modal():
        try:
            # Get conversations with same property
            related_conversations_df = find_conversations_with_same_property(
                modal_data['address'], 
                modal_data['neighborhood'], 
                modal_data.get('current_conversation_id')
            )
            
            # Store current_idx in session state for the exclusion logic
            st.session_state.current_idx = modal_data.get('current_idx')
            
            if not related_conversations_df.empty:
                # Display each conversation with the new format
                for idx, conv_row in related_conversations_df.iterrows():
                    with st.container():
                        col1, col2 = st.columns([6, 1])
                        
                        with col1:
                            # Build display string with only non-empty fields
                            display_parts = []
                            
                            # Always include expected_name (or fallback to display_name)
                            expected_name = conv_row['expected_name'] if conv_row['expected_name'] and conv_row['expected_name'].strip() else conv_row['display_name']
                            display_parts.append(f"**{expected_name}**")
                            
                            # Add classificacao if not empty
                            if conv_row['classificacao'] and conv_row['classificacao'].strip():
                                display_parts.append(conv_row['classificacao'])
                            
                            # Add intencao if not empty
                            if conv_row['intencao'] and conv_row['intencao'].strip():
                                display_parts.append(conv_row['intencao'])
                            
                            # Add formatted date if available
                            formatted_date = format_last_message_date(conv_row['last_message_date'])
                            if formatted_date:
                                display_parts.append(formatted_date)
                            
                            # Join all parts with " | "
                            display_text = " | ".join(display_parts)
                            st.write(display_text)
                        
                        with col2:
                            if st.button("➡️ Ir", key=f"goto_conv_{conv_row['row_index']}"):
                                # Navigate to this conversation
                                st.session_state.idx = conv_row['row_index']
                                st.session_state.property_modal_data = {'show_modal': False}
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
            st.session_state.property_modal_data = {'show_modal': False}
            st.rerun()
    
    # Show the modal
    show_property_modal()

# ─── PROPERTY MAP SECTION ──────────────────────────────────────────────────
def show_property_map():
    """Show interactive map of all properties owned by this person."""
    phone_number = row['whatsapp_number']
    
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
    
    st.header("🗺️ Mapa de Propriedades")
    st.write("Visualização geográfica de todas as propriedades associadas a este contato:")
    
    # Map style selector
    from utils.property_map import get_property_map_summary, render_property_map_streamlit, get_available_map_styles
    
    # Create columns for map controls
    col1, col2 = st.columns([3, 1])
    
    with col2:
        available_styles = get_available_map_styles()
        selected_style = st.selectbox(
            "🎨 Estilo do Mapa",
            options=list(available_styles.keys()),
            format_func=lambda x: available_styles[x],
            key=f"map_style_{phone_number}",
            index=0,  # Default to OpenStreetMap
            help="Escolha o estilo de visualização do mapa"
        )
        
        # Show style description
        style_descriptions = {
            "OpenStreetMap": "Mapa padrão com ruas e edifícios",
            "Satellite": "Imagem de satélite em alta resolução",
            "Terrain": "Mapa topográfico com relevo",
            "Streets": "Foco em vias e navegação",
            "Physical": "Características geográficas naturais",
            "Light": "Estilo claro e minimalista",
            "Dark": "Estilo escuro para menos brilho"
        }
        
        if selected_style in style_descriptions:
            st.caption(style_descriptions[selected_style])
    
    with col1:
        st.write("")  # Empty space for alignment
    
    # Show property summary
    summary = get_property_map_summary(properties)
    
    if summary:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total de Propriedades", summary['total_properties'])
            st.metric("Com Dados Geográficos", summary['mappable_properties'])
        
        with col2:
            if summary['total_area_terreno'] > 0:
                st.metric("Área Total Terreno", f"{summary['total_area_terreno']:,.0f} m²")
            if summary['total_area_construcao'] > 0:
                st.metric("Área Total Construção", f"{summary['total_area_construcao']:,.0f} m²")
        
        with col3:
            if summary['total_valor'] > 0:
                valor_formatado = f"R$ {summary['total_valor']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                st.metric("Valor Total (NET)", valor_formatado)
        
        # Show property types breakdown
        if summary['property_types']:
            st.subheader("📊 Tipos de Propriedades")
            types_df = pd.DataFrame(list(summary['property_types'].items()), columns=['Tipo', 'Quantidade'])
            st.dataframe(types_df, hide_index=True, use_container_width=True)
        
        # Show neighborhoods breakdown
        if summary['neighborhoods']:
            st.subheader("📍 Bairros")
            neighborhoods_df = pd.DataFrame(list(summary['neighborhoods'].items()), columns=['Bairro', 'Quantidade'])
            st.dataframe(neighborhoods_df, hide_index=True, use_container_width=True)
    
    # Render the interactive map with selected style
    try:
        render_property_map_streamlit(properties, map_style=selected_style)
    except Exception as e:
        st.error(f"Erro ao carregar mapa: {e}")
        st.info("💡 Para ver o mapa, instale as dependências: `pip install folium streamlit-folium`")
        
        # Show fallback property list
        st.subheader("📋 Lista de Propriedades")
        properties_data = []
        for prop in properties:
            properties_data.append({
                'Endereço': prop.get('ENDERECO', 'N/A'),
                'Bairro': prop.get('BAIRRO', 'N/A'),
                'Tipo': prop.get('TIPO CONSTRUTIVO', 'N/A'),
                'Área Terreno': prop.get('AREA TERRENO', 'N/A'),
                'Área Construção': prop.get('AREA CONSTRUCAO', 'N/A'),
                'Índice Cadastral': prop.get('INDICE CADASTRAL', 'N/A')
            })
        
        if properties_data:
            properties_df = pd.DataFrame(properties_data)
            st.dataframe(properties_df, hide_index=True, use_container_width=True)

# Show property map if we have properties
show_property_map()

# ─── FOOTER CAPTION ─────────────────────────────────────────────────────────
st.caption(
    f"Caso ID: {idx + 1} | WhatsApp: {row['whatsapp_number']} | {datetime.now():%H:%M:%S}"
)


