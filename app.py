# app.py – WhatsApp Agent Interface (DEV-ready, full feature set)
# -----------------------------------------------------------------------------
import streamlit as st
import pandas as pd
from datetime import datetime
import re
from io import StringIO
from login_manager import simple_auth

# ───────────────────────── SETTINGS ─────────────────────────────────────────
LOGIN_ENABLED = False  # flip to True for production

# ───────────────────────── AUTH (optional) ──────────────────────────────────
if LOGIN_ENABLED and not simple_auth():
    st.stop()

# ───────────────────────── DEBUG TOGGLE ─────────────────────────────────────
DEBUG = st.sidebar.checkbox("🐛 Debug Mode", value=False)
_dbg_panel = st.sidebar.expander("🔍 Debug Log", expanded=False) if DEBUG else None
_logged: set[str] = set()

def dbg(msg: str):
    if DEBUG and msg not in _logged:
        _logged.add(msg)
        _dbg_panel.write(msg)

# ───────────────────────── SAMPLE DATA (replace with DB) ────────────────────
@st.cache_data
def load_sample() -> pd.DataFrame:
    """Return a single-row example matching the latest schema."""
    return pd.read_json(
        StringIO(
            r"""
            [
              {
                "PictureUrl": "https://pps.whatsapp.net/v/t61.24694-24/463235056_1330030495039995_619627974121266174_n.jpg",
                "last_message_timestamp": "2024-08-14 07:48:24",
                "whatsapp_number": "5531994716770",
                "display_name": "Lili",
                "expected_name": "Liliane Figueiredo Teixeira",
                "familiares": "MARIA DE LOURDES AGUIAR (MAE), FRANCISCO AGUIAR DA SILVA (IRMAO), IDELBRANDO LUIZ AGUIAR (IRMAO), LEANDRO ALVARENGA AGUIAR (FILHO), MARCIO EVANDRO DE AGUIAR (IRMAO(A)), RENATO CESAR DE AGUIAR (IRMAO(A)), FUNDACAO MINEIRA DE EDUCACAO E CULTURA (EMPREGADOR), MARCIO JOSE DE AGUIAR (SOCIO(A))",
                "conversation_history": "[2024-08-14 05:39:29] (Urb.Link): Oi, Liliane! | [2024-08-14 05:39:46] (Urb.Link): Meu nome é Athos, prazer em falar contigo. | [2024-08-14 05:40:01] (Urb.Link): Estou no mercado imobiliário há quase 20 anos e ajudo proprietários de imóveis na Zona Sul a vender suas imóveis para construtoras pelo melhor valor possível. | [2024-08-14 05:40:13] (Urb.Link): Tenho bom relacionamento com *sócios de mais de 30 construtoras em busca de terrenos no Carmo.* | [2024-08-14 05:40:33] (Urb.Link): Seu imóvel na *Rua Caldas 143* está no perfil que muitas destas empresas buscam. | [2024-08-14 05:40:54] (Urb.Link): Você teria interesse que eu *apresente seu imóvel para algumas destas empresas e traga propostas para você?* | [2024-08-14 07:25:02] (Contato): Quem te deu esse número? | [2024-08-14 07:25:13] (Contato): Bom dia | [2024-08-14 07:48:24] (Urb.Link): Bom dia! Usamos uma ferramenta de inteligência de mercado que identifica os proprietários de determinado imóvel e seus possíveis contato.",
                "classificacao": "Não identificado",
                "intencao": "N/A",
                "resposta": "Nós usamos algumas ferramentas de inteligência de mercado, como Serasa, que nos informam possíveis contatos de proprietários de imóveis.",
                "Razao": "O nome no WhatsApp está vazio e a resposta questiona como o número foi obtido, indicando que não há relação clara com o imóvel ou o nome esperado (Liliane Figueiredo Teixeira).",
                "pagamento": "Dinheiro",
                "percepcao_valor_esperado": "Alto",
                "imovel_em_inventario": true,
                "IMOVEIS": [
                    {"BAIRRO": "Gutierrez", "ENDERECO": "Rua Bernardino De Lima 29", "TIPO": "Casa", "AREA TERRENO": 375.0,"ANO CONSTRUCAO": 1979.0, "FRACAO IDEAL": 0.1299}
                ],
                "IDADE": 72,
                "OBITO_PROVAVEL": true,
                "stakeholder": false,
                "intermediador": false,
                "inventario_flag": false,
                "standby": false,
                "acoes_urblink": []
              }
            ]
            """
        )
    )

# ───────────────────────── CSS OVERRIDES ------------------------------------
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
    if not text:
        return text
    out = str(text)
    for n in names:
        out = re.sub(re.escape(n), f'<span class="highlighted">{n}</span>', out, flags=re.I)
    return out

def bold_asterisks(text: str) -> str:
    """Convert *emphasis* markers to <strong> for HTML rendering."""
    return re.sub(r"\*([^*]+)\*", r"<strong>\1</strong>", text)


def parse_chat(raw: str):
    msgs = []
    for part in raw.split(" | "):
        if "] (" not in part:
            continue
        ts_end = part.find("] (")
        ts = part[1:ts_end]
        sender = part[ts_end + 3 : part.find("):")]
        msg = part[part.find("):") + 2:].strip()
        msgs.append({"ts": ts, "sender": sender, "msg": msg})
    return msgs

# ───────────────────────── STATE INIT ---------------------------------------
if "df" not in st.session_state:
    st.session_state.df = load_sample()
if "idx" not in st.session_state:
    st.session_state.idx = 0
if "resposta_text" not in st.session_state:
    st.session_state.resposta_text = ""   # will be filled later

df   = st.session_state.df
idx  = st.session_state.idx = min(st.session_state.idx, len(df) - 1)
row  = df.iloc[idx]

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
    if pd.notna(row["IDADE"]):
        st.markdown(f"**{int(row['IDADE'])} anos**")
    st.markdown("✝︎ Provável Óbito" if row["OBITO_PROVAVEL"] else "🌟 Provável vivo")


# ───────────────────────── IMÓVEIS (always visible) -------------------------
if row.get("IMOVEIS"):
    st.subheader("🏢 Imóveis")
    st.table(pd.DataFrame(row["IMOVEIS"]))

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

# ───────────────────────── NAVIGATION ---------------------------------------
prev, _, nxt = st.columns([1, 2, 1])
with prev:
    if st.button("⬅️ Anterior", disabled=idx == 0):
        st.session_state.idx -= 1
        st.session_state.resposta_text = ""
        st.rerun()
with nxt:
    if st.button("Próximo ➡️", disabled=idx >= len(df) - 1):
        st.session_state.idx += 1
        st.session_state.resposta_text = ""
        st.rerun()

st.caption(f"Caso ID: {idx + 1} | WhatsApp: {row['whatsapp_number']} | {datetime.now():%H:%M:%S}")
