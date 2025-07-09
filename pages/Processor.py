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
from loaders.db_loader import get_dataframe, get_db_info
from services.spreadsheet import sync_record_to_sheet
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

# ─── PAGE CONFIG (MUST BE FIRST) ────────────────────────────────────────
st.set_page_config(page_title="Processador de Conversas", page_icon="📱", layout="wide")

# ─── FLAGS ──────────────────────────────────────────────────────────────────
DEV = True  # Set based on your environment

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


# ─── DEBUG PANEL (dev‐only) ─────────────────────────────────────────────────
DEBUG = False
debug_panel = None
logged_messages = set()

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


def dbg(message: str):
    """Write a debug message once to the sidebar panel."""
    if DEBUG and debug_panel and message not in logged_messages:
        logged_messages.add(message)
        debug_panel.write(message)


# ─── STATE MANAGEMENT FUNCTIONS ─────────────────────────────────────────
def initialize_session_state():
    """Initialize the session state with proper data structure."""
    # Initialize master_df if not exists
    if "master_df" not in st.session_state:
        st.session_state.master_df = load_data()
    
    # Initialize original_values storage
    if "original_values" not in st.session_state:
        st.session_state.original_values = {}
    
    # Initialize original_db_data (store the original database values)
    if "original_db_data" not in st.session_state:
        st.session_state.original_db_data = load_data()
    
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
            'standby': parse_bool_field(original_row.get("standby"))
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
st.title("📱 WhatsApp Agent Interface")
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

# ─── CONTACT SECTION ────────────────────────────────────────────────────────
hl_words = build_highlights(row["display_name"], row["expected_name"])

# Create contact info HTML with fixed height
picture = row.get("PictureUrl")
display_name = highlight(row["display_name"], hl_words) if HIGHLIGHT_ENABLE else row["display_name"]
expected_name = highlight(row["expected_name"], hl_words)
familiares_list = parse_familiares_grouped(row["familiares"])
age = row.get("IDADE")
age_text = f"**{int(age)} anos**" if pd.notna(age) else ""
alive_status = "✝︎ Provável Óbito" if row.get("OBITO_PROVAVEL", False) else "🌟 Provável vivo"

# Build familiares HTML
familiares_html = ""
for card in familiares_list:
    familiares_html += f"<li>{card}</li>"

contact_html = f"""
<div style="height: 150px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 5px; background-color: #f9f9f9; display: flex; align-items: flex-start;">
    <div style="flex: 1; margin-right: 10px;">
        {'<img src="' + picture + '" style="width: 80px; height: auto;" />' if picture else '👤'}
    </div>
    <div style="flex: 2; margin-right: 10px;">
        <strong>Nome no WhatsApp</strong><br>
        {display_name}
    </div>
    <div style="flex: 2; margin-right: 10px;">
        <strong>Nome Esperado</strong><br>
        {expected_name}
    </div>
    <div style="flex: 4; margin-right: 10px;">
        <strong>Familiares</strong><br>
        <ul style="margin: 0; padding-left: 20px;">{familiares_html}</ul>
    </div>
    <div style="flex: 2;">
        {age_text}<br>
        {alive_status}
    </div>
</div>
"""

st.markdown(contact_html, unsafe_allow_html=True)

# ─── IMÓVEIS ────────────────────────────────────────────────────────────────
imoveis = parse_imoveis(row.get("IMOVEIS"))
if isinstance(imoveis, dict):
    imoveis = [imoveis]
elif not isinstance(imoveis, list):
    imoveis = []

# Build imoveis HTML
imoveis_html = ""
if imoveis:
    for item in imoveis:
        if not isinstance(item, dict):
            continue
        area = fmt_num(item.get("AREA TERRENO", "?"))
        fraction = item.get("FRACAO IDEAL", "")
        try:
            fraction_percent = f"{int(round(float(fraction) * 100 if float(fraction) <= 1 else float(fraction)))}%"
        except (ValueError, TypeError):
            fraction_percent = str(fraction)
        build_type = item.get("TIPO CONSTRUTIVO", "").strip()
        address = item.get("ENDERECO", "?")
        neighborhood = item.get("BAIRRO", "?")
        imoveis_html += f'<div style="margin-bottom: 10px; padding: 5px; border-left: 3px solid #007bff;">{address}, {neighborhood} – <strong>Terreno: {area} m²</strong>{(" [" + build_type + "]") if build_type else ""} (Fração ideal: {fraction_percent})</div>'

imoveis_section = f"""
<div style="margin: 10px 0;">
    <h3>🏢 Imóveis</h3>
    <div style="height: 120px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 5px; background-color: #f9f9f9;">
        {imoveis_html if imoveis_html else '<div style="color: #888; font-style: italic;">Nenhum imóvel encontrado</div>'}
    </div>
</div>
"""

st.markdown(imoveis_section, unsafe_allow_html=True)

st.markdown("---")

# ─── CHAT HISTORY ───────────────────────────────────────────────────────────
chat_html = "<div class='chat-container'>"
for msg in parse_chat(row["conversation_history"]):
    msg_class = (
        "agent-message" if msg["sender"] in ("Urb.Link", "Athos") else "contact-message"
    )
    chat_html += f"<div class='{msg_class}'>{bold_asterisks(msg['msg'])}"
    chat_html += f"<div class='timestamp'>{msg['ts']}</div></div>"
chat_html += "</div>"
st.markdown(chat_html, unsafe_allow_html=True)

st.markdown("---")

# ─── RAZÃO ─────────────────────────────────────────────────────────────────
st.subheader("📋 Racional usado pela AI classificadora")
st.markdown(f"<div class='reason-box'>{row['Razao']}</div>", unsafe_allow_html=True)

st.markdown("---")

# ─── CLASSIFICAÇÃO & RESPOSTA ───────────────────────────────────────────────
st.subheader("📝 Classificação e Resposta")

# Presets dropdown (outside the form)
preset_selected = st.selectbox(
    "Respostas Prontas",
    options=list(PRESET_RESPONSES.keys()),
    format_func=lambda tag: tag or "-- selecione uma resposta pronta --",
    key=f"preset_key_{idx}",  # Unique key per record
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

    # Forma de Pagamento
    current_pagamento = row.get('pagamento', '')
    pag_default = [p.strip() for p in current_pagamento.split(",") if p.strip()] if current_pagamento else []
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
            # Handle both Python bool and numpy.bool_
            if isinstance(field_value, (bool, np.bool_)):
                return 'TRUE' if bool(field_value) else 'FALSE'
            elif isinstance(field_value, str):
                return 'TRUE' if field_value.lower() in ['true', '1', 'yes'] else 'FALSE'
            else:
                return 'FALSE'
        
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
            'Classificação do dono do número': current_row.get('classificacao', ''),
            'status_manual': current_row.get('intencao', ''),
            'Ações': format_list_field(current_row.get('acoes_urblink', [])),
            'status_manual_urb.link': current_row.get('status_urblink', ''),
            'pagamento': current_row.get('pagamento', ''),
            'percepcao_valor_esperado': current_row.get('percepcao_valor_esperado', ''),
            'standby_reason': format_list_field(current_row.get('razao_standby', [])),
            'OBS': current_row.get('obs', ''),
            'stakeholder': format_boolean_field(stakeholder_val),
            'intermediador': format_boolean_field(intermediador_val),
            'imovel_em_inventario': format_boolean_field(inventario_val)
        }
        
        # Sync to Google Sheet
        with st.spinner('Syncing to Google Sheet...'):
            success = sync_record_to_sheet(sync_data, whatsapp_number, "report")
            
        if success:
            st.success("✅ Record synced to Google Sheet!")
            # Mark as synced in the dataframe
            st.session_state.master_df.at[idx, 'sheet_synced'] = True
        else:
            st.error("❌ Failed to sync to Google Sheet")

with bot_next_col:
    st.button(
        "Próximo ➡️",
        key="bottom_next",
        disabled=bool(idx >= len(df) - 1),
        on_click=goto_next,
        use_container_width=True,
    )

# ─── FOOTER CAPTION ─────────────────────────────────────────────────────────
st.caption(
    f"Caso ID: {idx + 1} | WhatsApp: {row['whatsapp_number']} | {datetime.now():%H:%M:%S}"
)


