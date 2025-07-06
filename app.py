# pylint: disable=invalid-name,broad-exception-caught,C0301,C0114,C0116,E0602,E1101

"""
app.py

Streamlit interface for the WhatsApp Agent with persistent cookie authentication
"""

from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
import time

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
from db_loader import get_dataframe
from login_manager import simple_auth
from styles import STYLES
from ui_helpers import (
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
st.set_page_config(page_title="WhatsApp Agent Interface", page_icon="📱", layout="wide")

# ─── AUTHENTICATION ─────────────────────────────────────────────────────
# Check authentication (this now includes cookie checking)
if not simple_auth():
    st.stop()

# ─── AUTHENTICATED APP STARTS HERE ──────────────────────────────────────
# Apply styles
st.markdown(STYLES, unsafe_allow_html=True)

# ─── FLAGS ──────────────────────────────────────────────────────────────────
HIGHLIGHT_ENABLE = False
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


# ─── STATE INIT ─────────────────────────────────────────────────────────────
if "df" not in st.session_state:
    st.session_state.df = load_data()
if "idx" not in st.session_state:
    st.session_state.idx = 0
if "resposta_text" not in st.session_state:
    st.session_state.resposta_text = ""

df = st.session_state.df
st.session_state.idx = min(st.session_state.idx, len(df) - 1)
idx = st.session_state.idx
row = df.iloc[idx]

# Prefill "Resposta" from the DataFrame, once per new idx
if (
    "last_prefill_idx" not in st.session_state
    or st.session_state.last_prefill_idx != idx
):
    st.session_state.resposta_text = row["resposta"]
    st.session_state.last_prefill_idx = idx

# Normalize odd column name
if "OBITO PROVAVEL" in df.columns and "OBITO_PROVAVEL" not in df.columns:
    df = df.rename(columns={"OBITO PROVAVEL": "OBITO_PROVAVEL"})

# ─── HEADER & PROGRESS ──────────────────────────────────────────────────────
st.title("📱 WhatsApp Agent Interface")
_, progress_col, _ = st.columns([1, 2, 1])
with progress_col:
    st.progress((idx + 1) / len(df))
    st.caption(f"{idx + 1}/{len(df)} mensagens processadas")
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
st.selectbox(
    "Respostas Prontas",
    options=list(PRESET_RESPONSES.keys()),
    format_func=lambda tag: tag or "-- selecione uma resposta pronta --",
    key="preset_key",
    on_change=apply_preset,
)

with st.form("main_form"):
    left_col, right_col = st.columns(2)

    with left_col:
        classificacao_sel = st.selectbox(
            "🏷️ Classificação",
            CLASSIFICACAO_OPTS,
            index=(
                CLASSIFICACAO_OPTS.index(row["classificacao"])
                if row["classificacao"] in CLASSIFICACAO_OPTS
                else 0
            ),
        )
        intencao_sel = st.selectbox(
            "🔍 Intenção",
            INTENCAO_OPTS,
            index=(
                INTENCAO_OPTS.index(row["intencao"])
                if row["intencao"] in INTENCAO_OPTS
                else 0
            ),
        )

        # ensure we only default to a list if row["acoes_urblink"] is actually a list
        acoes_default = (
            row.get("acoes_urblink")
            if isinstance(row.get("acoes_urblink"), list)
            else []
        )
        acoes_sel = st.multiselect(
            "📞 Ações Urb.Link",
            ACOES_OPTS,
            default=acoes_default,
            key="acoes_urblink",
        )

        status_opts = [""] + STATUS_URBLINK_OPTS
        current_status = row.get("status_urblink", "")
        # if DF has a real status use its index+1, otherwise stay at 0 (the blank)
        status_index = (
            status_opts.index(current_status) if current_status in status_opts else 0
        )

        status = st.selectbox(
            "🚦 Status Urb.Link",
            status_opts,
            index=status_index,
            key="status_urblink",
        )

        pag_default = row.get("pagamento") or ""
        pag_default = [p.strip() for p in pag_default.split(",") if p.strip()]
        pagamento_sel = st.multiselect(
            "💳 Forma de Pagamento",
            PAGAMENTO_OPTS,
            default=pag_default,
            key="pagamento",
        )

        percepcao_opts = [""] + PERCEPCAO_OPTS
        current_perc = row.get("percepcao_valor_esperado", "")
        percepcao_index = (
            percepcao_opts.index(current_perc) if current_perc in percepcao_opts else 0
        )

        percepcao_sel = st.selectbox(
            "💎 Percepção de Valor",
            percepcao_opts,
            index=percepcao_index,
            key="percepcao_valor_esperado",
        )

        razao_default = (
            row.get("razao_standby")
            if isinstance(row.get("razao_standby"), list)
            else []
        )

        razao_sel = st.multiselect(
            "🤔 Razão Stand-by",
            STANDBY_REASONS,
            default=razao_default,
            key="razao_standby",
        )

    with right_col:
        st.text_area(
            "✏️ Resposta",
            value=st.session_state.resposta_text,
            key="resposta_text",
            height=180,
        )

        st.text_area(
            "📋 OBS",
            value="",
            height=120,
            key="obs",
        )

        st.checkbox(
            "Stakeholder", value=row.get("stakeholder", False), key="stakeholder"
        )
        st.checkbox(
            "Intermediador", value=row.get("intermediador", False), key="intermediador"
        )
        st.checkbox(
            "Inventário", value=row.get("inventario_flag", False), key="inventario_flag"
        )
        st.checkbox("Stand-by", value=row.get("standby", False), key="standby")

        salvar = st.form_submit_button("💾 Salvar Alterações")
        if salvar:
            dbg("Saved changes")
            st.success("Alterações salvas (mock)")

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
