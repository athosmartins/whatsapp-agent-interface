# app.py – WhatsApp Agent Interface
# -----------------------------------------------------------------------------
import streamlit as st
import pandas as pd
from datetime import datetime
import re, json, ast
from io import StringIO

from db_loader    import get_dataframe
from login_manager import simple_auth


# ─── ENV + LOGIN ────────────────────────────────────────────────────────────
DEV            = st.secrets.get("ENV", "dev") == "dev"   # default → dev
LOGIN_ENABLED  = not DEV                                 # prod must log in

if LOGIN_ENABLED and not simple_auth():                  # prod ⇒ stop if not authed
    st.stop()

# ─── FLAGS ──────────────────────────────────────────────────────────────────
HIGHLIGHT_ENABLE = False      # flip to True to reactivate word highlights

# ─── DATA LOADER (cached) ───────────────────────────────────────────────────
@st.cache_data
def load_data():
    return get_dataframe()    # db_loader handles local vs cloud

# ─── OPTIONAL DEBUG PANEL (dev-only) ────────────────────────────────────────
DEBUG      = False
_dbg_panel = None
_logged: set[str] = set()

if DEV:
    DEBUG      = st.sidebar.checkbox("🐛 Debug Mode", value=False)
    _dbg_panel = st.sidebar.expander("🔍 Debug Log", expanded=False) if DEBUG else None

def dbg(msg: str):
    if DEBUG and msg not in _logged:
        _logged.add(msg)
        _dbg_panel.write(msg)


# ─── CSS, helper functions, rest of the app … ───────────────────────────────

st.markdown(
    """
<style>
#MainMenu, footer, header{visibility:hidden;}
.chat-container{background:#fafafa;border:1px solid #e6e9ef;border-radius:8px;padding:1rem;max-height:550px;overflow-y:auto;max-width:800px;margin-left:auto;margin-right:auto;}
.agent-message{background:#f0f0f0;padding:0.7rem 1rem;border-radius:1rem;margin:0.4rem 0;margin-left:25%;text-align:left;}
.contact-message{background:#2196f3;color:#fff;padding:0.7rem 1rem;border-radius:1rem;margin:0.4rem 0;margin-right:25%;text-align:left;}
.timestamp{font-size:0.7rem;color:#999;margin-top:0.25rem;text-align:right;}
.contact-message .timestamp{color:#e3f2fd;text-align:left;}
.highlighted{background:#e3f2fd;padding:2px 4px;border-radius:3px;}
.reason-box{background:#fff3cd;border:1px solid #ffc107;border-radius:0.5rem;padding:1rem;color:#856404;}
.family-grid{display:flex;flex-direction:column;gap:8px;}
.family-card{background:#fff;padding:8px 12px;border:1px solid #e6e9ef;border-radius:6px;font-size:0.85rem;line-height:1.3;}
</style>
""",
    unsafe_allow_html=True,
)



# ───────────────────────── HELPER FUNCTIONS ---------------------------------

from collections import OrderedDict
import re

# --------------------------------------------------------------------------- #
# Helper – Portuguese-ish proper-case (every word capitalised)                #
# --------------------------------------------------------------------------- #
def proper_case_pt(txt: str) -> str:
    return " ".join(w.capitalize() for w in txt.split())

# --------------------------------------------------------------------------- #
# Robust parser – handles "REL: name"  +  "name (REL)"  +  continuations      #
# --------------------------------------------------------------------------- #
def parse_familiares_grouped(raw: str) -> list[str]:
    """
    Example output:
        ["Mae: Maria De Lourdes Aguiar",
         "Irmaos: Francisco Aguiar, Idelbrando Luiz Aguiar, ...",
         "Filho: Leandro Alvarenga Aguiar",
         "Socio: Marcio Evandro De Aguiar, Renato Cesar De Aguiar, Marcio Jose De Aguiar",
         "Empregador: Fundacao Mineira De Educacao E Cultura"]
    """
    groups: OrderedDict[str, list[str]] = OrderedDict()
    current = None

    # Split on commas but keep commas inside parentheses intact
    tokens = re.split(r",(?![^()]*\))", raw)

    for tok in (t.strip() for t in tokens if t.strip()):
        # 1) "REL: name" pattern ------------------------------------------------
        if ":" in tok:
            rel, name = (p.strip() for p in tok.split(":", 1))
            current = rel.rstrip("sS").capitalize()          # IrmaoS → Irmao
            groups.setdefault(current, []).append(proper_case_pt(name))
            continue

        # 2) "name (REL)" pattern ----------------------------------------------
        m = re.match(r"(.+?)\s+\(([^)]+)\)\s*$", tok)
        if m:
            name, rel = m.group(1).strip(), m.group(2).strip()
            rel = rel.split("(")[0].rstrip("sS").capitalize()  # "IRMAO(A)" → "Irmao"
            current = rel
            groups.setdefault(current, []).append(proper_case_pt(name))
            continue

        # 3) Continuation token -------------------------------------------------
        if current is None:
            current = "Outros"
        groups.setdefault(current, []).append(proper_case_pt(tok))

    return [f"{rel}: {', '.join(nomes)}" for rel, nomes in groups.items()]



def build_highlights(*names: str) -> list[str]:
    words = []
    for n in names:
        if n:
            words.extend(str(n).split())
            words.append(str(n))
    return list({w.strip() for w in words if len(w) > 1})

def highlight(text: str, names: list[str]):
    """Return text with <span class="highlighted">…</span> tags, or plain text."""
    if not HIGHLIGHT_ENABLE:
        return text                 #  ←  early-exit: do nothing
    if not text:
        return text
    out = str(text)
    for n in names:
        out = re.sub(re.escape(n), f'<span class="highlighted">{n}</span>', out,
                     flags=re.I)
    return out


def bold_asterisks(text: str) -> str:
    """Convert *emphasis* markers to <strong> for HTML rendering."""
    return re.sub(r"\*([^*]+)\*", r"<strong>\1</strong>", text)


import re

def parse_chat(raw: str) -> list[dict]:
    """
    Keep only lines like:
        [YYYY-MM-DD hh:mm:ss] (Sender): Message…
    Accepts any mix of  " | "  and/or   newline  as delimiters.
    Returns a list of dicts: {'ts', 'sender', 'msg'}
    """
    if not raw:
        return []

    # Split on explicit pipe OR on a newline that starts a new bracketed block
    chunks = re.split(r"\s*\|\s*|\n(?=\[)", str(raw).strip())

    msgs = []
    for part in (c.strip() for c in chunks if c.strip()):
        m = re.match(r"\[(.*?)\]\s+\((.*?)\):(.*)", part, re.S)
        if not m:
            continue              # drop anything that isn’t format A
        ts, sender, msg = m.groups()
        msgs.append(
            {"ts": ts.strip(),
             "sender": sender.strip(),
             "msg":   msg.strip()}
        )
    return msgs


# ───────────────────────── STATE INIT ---------------------------------------
if "df" not in st.session_state:
    st.session_state.df = load_data()
if "idx" not in st.session_state:
    st.session_state.idx = 0
if "resposta_text" not in st.session_state:
    st.session_state.resposta_text = ""   # will be filled later

df   = st.session_state.df
idx  = st.session_state.idx = min(st.session_state.idx, len(df) - 1)
row  = df.iloc[idx]

# normalise the one odd column
if "OBITO PROVAVEL" in df.columns and "OBITO_PROVAVEL" not in df.columns:
    df = df.rename(columns={"OBITO PROVAVEL": "OBITO_PROVAVEL"})


# ── optional debug dump ────────────────────────────────────────────────
if DEBUG:
    st.write("Columns in DataFrame:", list(df.columns))
    st.write("RAW conv:", row.get("conversation_history"))

# ───────────────────────────────────────────────────────────────────────

# --- pre-fill "Resposta" every time we load a new row -----------------------
if "last_prefill_idx" not in st.session_state or st.session_state.last_prefill_idx != idx:
    st.session_state.resposta_text = row["resposta"]
    st.session_state.last_prefill_idx = idx

# ───────────────────────── HEADER & PROGRESS --------------------------------
st.title("📱 WhatsApp Agent Interface (DEV)")
_, bar_col, _ = st.columns([1, 2, 1])
with bar_col:
    st.progress((idx + 1) / len(df))
    st.caption(f"{idx + 1}/{len(df)} mensagens processadas")
st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)

# ───────────────────────── NAVIGATION (TOP) ------------------------------
def _goto_prev():
    if st.session_state.idx > 0:
        st.session_state.idx -= 1
        st.session_state.resposta_text = ""     # safe: runs pre-rerun
def _goto_next():
    if st.session_state.idx < len(st.session_state.df) - 1:
        st.session_state.idx += 1
        st.session_state.resposta_text = ""

nav_prev, nav_prog, nav_next = st.columns([1, 2, 1])
with nav_prev:
    st.button("⬅️ Anterior", disabled=idx == 0, on_click=_goto_prev, use_container_width=True)
with nav_next:
    st.button("Próximo ➡️", disabled=idx >= len(df) - 1, on_click=_goto_next, use_container_width=True)


# ───────────────────────── CONTACT SECTION ----------------------------------
hl = build_highlights(row["display_name"], row["expected_name"])

c1, c2, c3, c4, c5 = st.columns([1, 2, 2, 4, 2])
with c1:
    if row.get("PictureUrl"):
        st.image(row["PictureUrl"], width=80)
    else:
        st.markdown("👤")
with c2:
    st.markdown("**Nome no WhatsApp**")
    st.markdown(highlight(row["display_name"], hl), unsafe_allow_html=True)
with c3:
    st.markdown("**Nome Esperado**")
    st.markdown(highlight(row["expected_name"], hl), unsafe_allow_html=True)
with c4:
    st.markdown("**Familiares**")
    fam_cards = parse_familiares_grouped(row["familiares"])
    st.markdown(
        '<div class="family-grid">' +
        "".join(f'<div class="family-card">{highlight(card, hl)}</div>' for card in fam_cards) +
        '</div>',
        unsafe_allow_html=True
    )
with c5:
    idade = row.get("IDADE")
    obito = bool(row.get("OBITO_PROVAVEL", False))   # default = False if column missing

    if pd.notna(idade):
        st.markdown(f"**{int(idade)} anos**")

    st.markdown("✝︎ Provável Óbito" if obito else "🌟 Provável vivo")


# ───────────────────────── IMÓVEIS (sempre visível) -------------------------
import json, ast

def _parse_imoveis(raw):
    """Converte string/JSON/objeto em lista de dicts ou None."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, (list, dict)):
        return raw
    if isinstance(raw, str):
        txt = raw.strip()
        for loader in (json.loads, ast.literal_eval):
            try:
                return loader(txt)
            except Exception:
                continue
    return None

def _fmt(val):
    """Formata números sem zeros desnecessários."""
    if isinstance(val, (int, float)):
        return f"{val:g}"
    return str(val)

imv_obj = _parse_imoveis(row.get("IMOVEIS"))

# Normalizar para lista
if isinstance(imv_obj, dict):
    imv_obj = [imv_obj]

if isinstance(imv_obj, list) and imv_obj:
    st.subheader("🏢 Imóveis")
    lines = []
    for im in imv_obj:
        ender  = im.get("ENDERECO", "Endereço?")
        bairro = im.get("BAIRRO",   "Bairro?")
        # --- Terreno --------------------------------------------------------
        area_val = im.get("AREA TERRENO", "?")
        area_txt = f"<strong>Terreno: {_fmt(area_val)} m²</strong>"
        # --- Fração ideal como % inteiro ------------------------------------
        fi = im.get("FRACAO IDEAL", "")
        try:
            pct = int(round(float(fi) * 100 if float(fi) <= 1 else float(fi)))
            frac_txt = f"{pct}%"
        except Exception:
            frac_txt = str(fi)
        # --- Tipo construtivo ----------------------------------------------
        tipo = im.get("TIPO CONSTRUTIVO", "").strip()
        tipo_txt = f"[{tipo}]" if tipo else ""
        # --- Linha final ----------------------------------------------------
        lines.append(
            f"{ender}, {bairro} – {area_txt} {tipo_txt} (Fração ideal = {frac_txt})"
        )


    # Bullet-list bonitinha
    st.markdown(
        "<ul style='margin-top:-0.5rem'>" +
        "".join(f"<li>{ln}</li>" for ln in lines) +
        "</ul>",
        unsafe_allow_html=True,
    )

elif imv_obj is not None:
    # fallback
    st.subheader("🏢 Imóveis (formato desconhecido)")
    st.write(imv_obj)

# else: não mostra nada quando vazio
st.markdown("---")

# ───────────────────────── CHAT HISTORY -------------------------------------
chat_html = '<div class="chat-container">'
for m in parse_chat(row["conversation_history"]):
    cls = "agent-message" if m["sender"] in ("Urb.Link", "Athos") else "contact-message"
    msg_html = bold_asterisks(m["msg"])
    chat_html += f'<div class="{cls}">{msg_html}<div class="timestamp">{m["ts"]}</div></div>'
chat_html += '</div>'

st.markdown(chat_html, unsafe_allow_html=True)

st.markdown("---")
# ───────────────────────── RACIONAL -----------------------------------------
st.subheader("📋 Racional usado pela AI classificadora")
st.markdown(f'<div class="reason-box">{row["Razao"]}</div>', unsafe_allow_html=True)

st.markdown("---")

# ───────────────────────── CLASSIFICAÇÃO & RESPOSTA -------------------------
st.subheader("📝 Classificação e Resposta")

preset_responses = {
    "": "Selecione uma resposta pronta",
    "obrigado": "Muito obrigado pelo retorno! Tenha um ótimo dia 😊",
    "inteligencia": "Nós usamos algumas ferramentas de inteligência de mercado, como Serasa, que nos informam possíveis contatos de proprietários de imóveis.",
    "proposta": "Gostaria de apresentar uma proposta para seu imóvel. Quando seria um bom momento para conversarmos?",
    "followup": "Entendi sua posição. Entrarei em contato em breve caso surjam novas oportunidades."
}

acoes_opts   = ["Enviar proposta", "Agendar visita", "Aguardar retorno", "Marcar follow-up", "Descartar"]
pg_opts      = ["Dinheiro", "Permuta no local", "Permuta fora", "Permuta pronta"]
percep_opts  = ["Ótimo", "Bom", "Alto", "Inviável"]

with st.form("main_form"):
    left, right = st.columns(2)

    # LEFT COL ---------------------------------------------------------------
    with left:
        # class / intencao
        cls_opts = ["Proprietário", "Herdeiro / Futuro herdeiro", "Parente / Conhecido",
                    "Ex-proprietário", "Sem relação com imóvel", "Não identificado"]
        classificacao = st.selectbox(
            "Classificação",
            cls_opts,
            index=cls_opts.index(row["classificacao"]) if row["classificacao"] in cls_opts else 5
        )

        int_opts = ["Aberto a Proposta", "Aberto a Proposta, outros não", "Não receptivo a venda",
                    "Pretende vender no futuro", "Vendido para Construtora", "Está em negociação (FUP 30d)",
                    "Passou contato stakeholder", "Sem contato", "Entendendo situação", "N/A"]
        intencao = st.selectbox(
            "Intenção",
            int_opts,
            index=int_opts.index(row["intencao"]) if row["intencao"] in int_opts else 9
        )

        # Flags
        stakeholder   = st.checkbox("Stakeholder",   value=row.get("stakeholder", False))
        intermediador = st.checkbox("Intermediador", value=row.get("intermediador", False))
        inventario_f  = st.checkbox("Inventário",    value=row.get("inventario_flag", False))
        standby_f     = st.checkbox("Stand-by",      value=row.get("standby", False))

        acoes_sel = st.multiselect("Ações Urb.Link", acoes_opts, default=row.get("acoes_urblink", []))

        # NEW – Pagamento (multi) + Percepção (single) – always visible
        pg_current = [p.strip() for p in (row.get("pagamento") or "").split(",") if p.strip()]
        pagamento_sel = st.multiselect("Pagamento (múltiplo)", pg_opts, default=pg_current)

        percepcao_val = st.selectbox(
            "Percepção de Valor",
            percep_opts,
            index=percep_opts.index(row.get("percepcao_valor_esperado", "Bom"))
            if row.get("percepcao_valor_esperado") in percep_opts else 1
        )

    # RIGHT COL --------------------------------------------------------------
    with right:
        preset_key = st.selectbox("Respostas Prontas",
                                  list(preset_responses.keys()),
                                  format_func=lambda k: preset_responses[k])

        # ── Header (label + trash button) BEFORE the textarea ───────────────
        lbl_col, trash_col = st.columns([6, 1])
        with lbl_col:
            st.markdown("**Resposta**")
        with trash_col:
            clear_clicked = st.form_submit_button("🗑️", help="Apagar Resposta")

        # Clear logic happens BEFORE instantiating the text_area
        if clear_clicked:
            st.session_state.resposta_text = ""

        # Determine default text
        if st.session_state.resposta_text == "":
            default_resp = preset_responses[preset_key] if preset_key else row["resposta"]
        else:
            default_resp = st.session_state.resposta_text

        resposta = st.text_area(
            "Resposta (hidden)",         # a11y label
            value=default_resp,
            height=180,
            key="resposta_text",
            label_visibility="collapsed"
        )

    # FOOTER BUTTON (SAVE) ---------------------------------------------------
    salvar = st.form_submit_button("💾 Salvar Alterações")

    if salvar:
        # In real use: persist everything to DB — here we just mimic
        dbg(f"Saved → cls={classificacao}, int={intencao}, pg={pagamento_sel}, perc={percepcao_val}")
        st.success("Alterações salvas (mock)")

st.markdown("---")



st.caption(f"Caso ID: {idx + 1} | WhatsApp: {row['whatsapp_number']} | {datetime.now():%H:%M:%S}")
