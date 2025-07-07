"""ui_helpers.py

Shared UI helper functions for the WhatsApp Agent Streamlit app:
– parsing/fmt for familiares, imóveis, chat
– text formatting (highlight, bold)
– preset‐response application
"""

import re
import json
import ast
from typing import Any, List, Dict
from collections import OrderedDict

import pandas as pd
import streamlit as st
from config import PRESET_RESPONSES

# toggle highlighting globally
HIGHLIGHT_ENABLE = False


def proper_case_pt(txt: str) -> str:
    """Capitalize each word in Portuguese‐style names."""
    return " ".join(w.capitalize() for w in txt.split())


def parse_familiares_grouped(raw: str) -> List[str]:
    """Turn a raw familiares string into grouped "Rel: name, name" entries."""
    groups: OrderedDict[str, List[str]] = OrderedDict()
    current = None
    tokens = re.split(r",(?![^()]*\))", raw or "")
    for tok in (t.strip() for t in tokens if t.strip()):
        if ":" in tok:
            rel, name = (p.strip() for p in tok.split(":", 1))
            current = rel.rstrip("sS").capitalize()
            groups.setdefault(current, []).append(proper_case_pt(name))
        else:
            m = re.match(r"(.+?)\s+\(([^)]+)\)$", tok)
            if m:
                name, rel = m.groups()
                rel = rel.split("(")[0].rstrip("sS").capitalize()
                current = rel
                groups.setdefault(current, []).append(proper_case_pt(name))
            else:
                if current is None:
                    current = "Outros"
                groups.setdefault(current, []).append(proper_case_pt(tok))

    return [f"{rel}: {', '.join(names)}" for rel, names in groups.items()]


def build_highlights(*names: str) -> List[str]:
    """From several name strings, build a deduped list of words to highlight."""
    words = []
    for n in names:
        if n:
            words += str(n).split() + [str(n)]
    return list({w for w in words if len(w) > 1})


def highlight(text: str, names: List[str]) -> str:
    """
    Wrap each occurrence of any name in <span class="highlighted">…</span>.
    Respects the global HIGHLIGHT_ENABLE flag.
    """
    if not HIGHLIGHT_ENABLE or not text:
        return text

    out = str(text)
    for n in names:
        out = re.sub(
            re.escape(n),
            f'<span class="highlighted">{n}</span>',
            out,
            flags=re.I
        )
    return out


def bold_asterisks(text: str) -> str:
    """Convert *emphasis* into <strong>…</strong> HTML."""
    return re.sub(r"\*([^*]+)\*", r"<strong>\1</strong>", text)


def parse_chat(raw: str) -> List[Dict[str, str]]:
    """
    Parse a Streamlit‐pasted chat log into
    [{'ts': timestamp, 'sender': name, 'msg': message}, …].
    """
    if not raw:
        return []
    chunks = re.split(r"\s*\|\s*|\n(?=\[)", str(raw).strip())
    msgs = []
    for part in (c.strip() for c in chunks if c.strip()):
        m = re.match(r"\[(.*?)\]\s+\((.*?)\):(.*)", part, re.S)
        if m:
            ts, sender, msg = m.groups()
            msgs.append({
                "ts": ts.strip(),
                "sender": sender.strip(),
                "msg": msg.strip()
            })
    return msgs


def parse_imoveis(raw: Any) -> List[Dict]:
    """
    Parse an IMOVEIS field that may be list/dict/JSON string into
    a list of dicts (possibly empty).
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        for loader in (json.loads, ast.literal_eval):
            try:
                result = loader(raw)
            except (ValueError, SyntaxError):
                continue
            if isinstance(result, dict):
                return [result]
            if isinstance(result, list):
                return result
    return []


def fmt_num(v: Any) -> str:
    """Format numbers without unnecessary trailing zeros."""
    if isinstance(v, (int, float)):
        return f"{v:g}"
    return str(v)


def apply_preset() -> None:
    """
    Callback for the "Respostas Prontas" selectbox:
    pull the chosen key from session and set sessão.resposta_text.
    """
    sel = st.session_state.get("preset_key", "")
    st.session_state.resposta_text = PRESET_RESPONSES.get(sel, "")
