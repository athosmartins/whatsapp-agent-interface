# pages/Processor.py

# pylint: disable=invalid-name,broad-exception-caught,C0301,C0114,C0116,E0602,E1101

"""
Processor.py
Streamlit interface for the WhatsApp Agent with persistent cookie authentication
"""

from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
import time, os
import copy

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
from loaders.db_loader import get_dataframe
from auth.login_manager import simple_auth
from utils.styles import STYLES
from utils.ui_helpers import (
    apply_preset,
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
    LOGIN_ENABLED = os.environ["LOGIN_ENABLED"].lower() in ("true", "1", "yes", "on")
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
DEV = False  # Set based on your environment


# ─── DATA LOADER ────────────────────────────────────────────────────────────
@st.cache_data
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
    
    # Initialize index
    if "selected_idx" in st.session_state:
        st.session_state.idx = st.session_state.selected_idx
        del st.session_state.selected_idx
    elif "idx" not in st.session_state:
        st.session_state.idx = 0

def store_original_values(idx, row):
    """Store original values for a record if not already stored."""
    if idx not in st.session_state.original_values:
        st.session_state.original_values[idx] = {
            'classificacao': row.get("classificacao", ""),
            'intencao': row.get("intencao", ""),
            'acoes_urblink': row.get("acoes_urblink", []) if isinstance(row.get("acoes_urblink"), list) else [],
            'status_urblink': row.get("status_urblink", ""),
            'pagamento': row.get("pagamento", ""),
            'percepcao_valor_esperado': row.get("percepcao_valor_esperado", ""),
            'razao_standby': row.get("razao_standby", []) if isinstance(row.get("razao_standby"), list) else [],
            'resposta': row.get("resposta", ""),
            'obs': row.get("obs", ""),
            'stakeholder': row.get("stakeholder", False),
            'intermediador': row.get("intermediador", False),
            'inventario_flag': row.get("inventario_flag", False),
            'standby': row.get("standby", False)
        }

def reset_to_original(idx):
    """Reset all fields to original AI values."""
    if idx in st.session_state.original_values:
        original = st.session_state.original_values[idx]
        for field, value in original.items():
            if field in st.session_state.master_df.columns:
                st.session_state.master_df.at[idx, field] = value

def update_field(idx, field, value):
    """Update a field value directly in master_df."""
    if field in st.session_state.master_df.columns:
        st.session_state.master_df.at[idx, field] = value


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

# ─── DASHBOARD NAVIGATION ────────────────────────────────────────────────────
dashboard_col, reset_col, _ = st.columns([1, 1, 2])
with dashboard_col:
    if st.button("🏠 Back to Dashboard"):
        st.switch_page("app.py")

with reset_col:
    if st.button("🤖 Carregar Respostas da AI"):
        reset_to_original(idx)
        st.success("✅ Valores originais da AI carregados!")
        st.rerun()

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
        disabled=idx == 0,
        on_click=goto_prev,
        use_container_width=True,
    )
with nav_next_col:
    st.button(
        "Próximo ➡️",
        key="top_next",
        disabled=idx >= len(df) - 1,
        on_click=goto_next,
        use_container_width=True,
    )

# ─── CONTACT SECTION ────────────────────────────────────────────────────────
hl_words = build_highlights(row["display_name"], row["expected_name"])
col1, col2, col3, col4, col5 = st.columns([1, 2, 2, 4, 2])

with col1:
    picture = row.get("PictureUrl")
    if picture:
        st.image(picture, width=80)
    else:
        st.markdown("👤")

with col2:
    st.markdown("**Nome no WhatsApp**")
    if HIGHLIGHT_ENABLE:
        st.markdown(highlight(row["display_name"], hl_words), unsafe_allow_html=True)
    else:
        st.markdown(row["display_name"])

with col3:
    st.markdown("**Nome Esperado**")
    st.markdown(highlight(row["expected_name"], hl_words), unsafe_allow_html=True)

with col4:
    st.markdown("**Familiares**")
    for card in parse_familiares_grouped(row["familiares"]):
        st.markdown(f"- {card}")

with col5:
    age = row.get("IDADE")
    if pd.notna(age):
        st.markdown(f"**{int(age)} anos**")
    alive_status = (
        "✝︎ Provável Óbito" if row.get("OBITO_PROVAVEL", False) else "🌟 Provável vivo"
    )
    st.markdown(alive_status)

# ─── IMÓVEIS ────────────────────────────────────────────────────────────────
imoveis = parse_imoveis(row.get("IMOVEIS"))
if isinstance(imoveis, dict):
    imoveis = [imoveis]
elif not isinstance(imoveis, list):
    imoveis = []

if imoveis:
    st.subheader("🏢 Imóveis")
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
        st.markdown(
            f"{address}, {neighborhood} – "
            f"**Terreno: {area} m²**{(' [' + build_type + ']') if build_type else ''}"
            f" (Fração ideal: {fraction_percent})",
            unsafe_allow_html=True,
        )

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
    if not isinstance(current_acoes, list):
        current_acoes = []
    acoes_sel = st.multiselect(
        "📞 Ações Urb.Link",
        ACOES_OPTS,
        default=current_acoes,
        key=f"acoes_select_{idx}",
        on_change=lambda: update_field(idx, 'acoes_urblink', st.session_state[f"acoes_select_{idx}"])
    )

    # Status Urb.Link
    status_opts = [""] + STATUS_URBLINK_OPTS
    current_status = row.get('status_urblink', '')
    status_index = status_opts.index(current_status) if current_status in status_opts else 0
    status_sel = st.selectbox(
        "🚦 Status Urb.Link",
        status_opts,
        index=status_index,
        key=f"status_select_{idx}",
        on_change=lambda: update_field(idx, 'status_urblink', st.session_state[f"status_select_{idx}"])
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
    if not isinstance(current_razao, list):
        current_razao = []
    razao_sel = st.multiselect(
        "🤔 Razão Stand-by",
        STANDBY_REASONS,
        default=current_razao,
        key=f"razao_select_{idx}",
        on_change=lambda: update_field(idx, 'razao_standby', st.session_state[f"razao_select_{idx}"])
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
    obs_input = st.text_area(
        "📋 OBS",
        value=current_obs,
        height=120,
        key=f"obs_input_{idx}",
        on_change=lambda: update_field(idx, 'obs', st.session_state[f"obs_input_{idx}"])
    )

    # Checkboxes
    current_stakeholder = row.get('stakeholder', False)
    stakeholder_input = st.checkbox(
        "Stakeholder", 
        value=current_stakeholder, 
        key=f"stakeholder_input_{idx}",
        on_change=lambda: update_field(idx, 'stakeholder', st.session_state[f"stakeholder_input_{idx}"])
    )

    current_intermediador = row.get('intermediador', False)
    intermediador_input = st.checkbox(
        "Intermediador", 
        value=current_intermediador, 
        key=f"intermediador_input_{idx}",
        on_change=lambda: update_field(idx, 'intermediador', st.session_state[f"intermediador_input_{idx}"])
    )

    current_inventario = row.get('inventario_flag', False)
    inventario_input = st.checkbox(
        "Inventário", 
        value=current_inventario, 
        key=f"inventario_input_{idx}",
        on_change=lambda: update_field(idx, 'inventario_flag', st.session_state[f"inventario_input_{idx}"])
    )

    current_standby = row.get('standby', False)
    standby_input = st.checkbox(
        "Stand-by", 
        value=current_standby, 
        key=f"standby_input_{idx}",
        on_change=lambda: update_field(idx, 'standby', st.session_state[f"standby_input_{idx}"])
    )

    # Show modifications status
    if idx in st.session_state.original_values:
        original = st.session_state.original_values[idx]
        current = row.to_dict()
        modified_fields = []
        for field in original:
            if field in current and original[field] != current[field]:
                modified_fields.append(field)
        
        if modified_fields:
            st.info(f"📝 Campos modificados: {', '.join(modified_fields)}")
        else:
            st.success("✅ Sem modificações")

# ─── NAVIGATION BOTTOM ──────────────────────────────────────────────────────
st.markdown("---")
bot_prev_col, _, bot_next_col = st.columns([1, 2, 1])
with bot_prev_col:
    st.button(
        "⬅️ Anterior",
        key="bottom_prev",
        disabled=idx == 0,
        on_click=goto_prev,
        use_container_width=True,
    )
with bot_next_col:
    st.button(
        "Próximo ➡️",
        key="bottom_next",
        disabled=idx >= len(df) - 1,
        on_click=goto_next,
        use_container_width=True,
    )

# ─── FOOTER CAPTION ─────────────────────────────────────────────────────────
st.caption(
    f"Caso ID: {idx + 1} | WhatsApp: {row['whatsapp_number']} | {datetime.now():%H:%M:%S}"
)