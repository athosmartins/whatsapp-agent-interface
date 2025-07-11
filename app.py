"""Main dashboard application for WhatsApp conversation processor."""

import re
from datetime import datetime

import streamlit as st

from loaders.db_loader import get_dataframe
from utils.ui_helpers import parse_imoveis

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────
st.set_page_config(page_title="Home Dashboard", layout="wide")
st.title("🏠 Home Dashboard")

# Debug mode check
DEBUG = st.sidebar.checkbox("Debug Mode", value=False)


# ─── LOAD & CACHE DATA ────────────────────────────────────────────────────
@st.cache_data
def load_master_df():


    df = get_dataframe()
    # Ensure sync-status flags exist
    if "sheet_synced" not in df.columns:
        df["sheet_synced"] = False
    if "whatsapp_sent" not in df.columns:
        df["whatsapp_sent"] = False
    return df


# Initialize master_df if not exists
if "master_df" not in st.session_state:
    st.session_state.master_df = load_master_df()

# Work on the master_df
df = st.session_state.master_df.copy()

# Debug info
if DEBUG:
    st.sidebar.subheader("Debug Info")
    st.sidebar.write(f"Total records: {len(df)}")
    st.sidebar.write(f"Available columns: {list(df.columns)}")
    if "original_values" in st.session_state:
        st.sidebar.write(f"Modified records: {len(st.session_state.get('original_values', {}))}")


# ─── TRANSFORM COLUMNS ─────────────────────────────────────────────────────
# 1) Phone formatting: (XX) XXXXX-XXXX
def format_brazilian_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    # Remove country code if present
    if digits.startswith("55") and len(digits) > 10:
        digits = digits[2:]
    if len(digits) >= 10:
        area, rest = digits[:2], digits[2:]
        return f"({area}) {rest[:-4]}-{rest[-4:]}"
    return raw


# 2) Extract addresses from IMOVEIS
def extract_addresses(imoveis_raw):
    try:
        parsed = parse_imoveis(imoveis_raw)
        items = parsed if isinstance(parsed, list) else [parsed]
        return ", ".join(
            itm.get("ENDERECO", "?") for itm in items if isinstance(itm, dict)
        )
    except Exception:
        return ""


# Apply transformations
df["phone"] = df.get("whatsapp_number", "").apply(format_brazilian_phone)
df["ENDERECO"] = df.get("IMOVEIS", "").apply(extract_addresses)

# ─── SELECT & VALIDATE COLUMNS ─────────────────────────────────────────────
display_cols = [
    "phone",
    "ENDERECO",
    "display_name",
    "expected_name",
    "classificacao",
    "intencao",
    "pagamento",
    "percepcao_valor_esperado",
    "inventario_flag",
    "resposta",
    "sheet_synced",
]
# Keep only columns that exist
display_cols = [c for c in display_cols if c in df.columns]

# ─── BUILD GRID DATAFRAME ─────────────────────────────────────────────────
# Add index
if "_idx" not in df.columns:
    df["_idx"] = df.index

grid_cols = ["_idx", *display_cols]
grid_df = df[grid_cols].copy()

# ─── SHOW MODIFICATIONS STATUS ─────────────────────────────────────────────
if "original_values" in st.session_state and st.session_state["original_values"]:
    modified_records = []
    for idx, original in st.session_state["original_values"].items():
        current = st.session_state.master_df.iloc[idx]
        has_changes = False
        for field, orig_value in original.items():
            if field in current and current[field] != orig_value:
                has_changes = True
                break
        if has_changes:
            modified_records.append(idx)

    if modified_records:
        st.info(
            f"📝 Records with modifications: {len(modified_records)} ({', '.join(map(str, [r+1 for r in modified_records]))})"
        )

# ─── RENDER GRID ──────────────────────────────────────────────────────────
# Display the dataframe with clickable rows
event = st.dataframe(
    grid_df,
    hide_index=True,
    use_container_width=True,
    on_select="rerun",
    selection_mode="single-row",
)

# Handle row selection to navigate to processor
if hasattr(event, "selection") and event.selection and event.selection.get("rows"):
    selected_row_idx = event.selection["rows"][0]
    if selected_row_idx < len(grid_df):
        actual_idx = int(grid_df.iloc[selected_row_idx]["_idx"])
        st.session_state.selected_idx = actual_idx
        st.switch_page("pages/Processor.py")

# ─── BULK ACTIONS ─────────────────────────────────────────────────────────
st.subheader("Bulk Actions")
bulk_col1, bulk_col2, bulk_col3 = st.columns(3)

with bulk_col1:
    if st.button("💾 Save to Database"):
        # This would save the current state to the database
        st.info("Save to database functionality would be implemented here")

with bulk_col2:
    st.write("")  # Empty space where reload button was

with bulk_col3:
    export_format = st.selectbox("Export format", ["CSV", "Excel"], key="export_format")
    if st.button("📊 Export Current State"):
        if export_format == "CSV":
            csv = st.session_state.master_df.to_csv(index=False)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"whatsapp_data_{timestamp}.csv",
                mime="text/csv",
            )
        else:
            # Excel export would be implemented here
            st.info("Excel export would be implemented here")

# Record Actions section removed - now handled by clicking rows in the dataframe
st.write(
    "💡 **Tip:** Click on any row in the table above to open the processor for that record."
)
