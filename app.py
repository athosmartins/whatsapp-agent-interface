# app.py
import streamlit as st
from loaders.db_loader import get_dataframe
from utils.ui_helpers import parse_imoveis
import re

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────
st.set_page_config(page_title="Home Dashboard", layout="wide")
st.title("🏠 Home Dashboard")

# ─── LOAD & CACHE DATA ────────────────────────────────────────────────────
@st.cache_data
def load_master_df():
    df = get_dataframe()
    # Ensure sync-status flags exist
    if 'sheet_synced' not in df.columns:
        df['sheet_synced'] = False
    if 'whatsapp_sent' not in df.columns:
        df['whatsapp_sent'] = False
    return df

# Initialize master_df if not exists
if 'master_df' not in st.session_state:
    st.session_state.master_df = load_master_df()

# Work on the master_df
df = st.session_state.master_df.copy()

# ─── TRANSFORM COLUMNS ─────────────────────────────────────────────────────
# 1) Phone formatting: (XX) XXXXX-XXXX
def format_brazilian_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    # Remove country code if present
    if digits.startswith('55') and len(digits) > 10:
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
            itm.get('ENDERECO', '?') for itm in items if isinstance(itm, dict)
        )
    except Exception:
        return ''

# Apply transformations
df['phone'] = df.get('whatsapp_number', '').apply(format_brazilian_phone)
df['ENDERECO'] = df.get('IMOVEIS', '').apply(extract_addresses)

# ─── SELECT & VALIDATE COLUMNS ─────────────────────────────────────────────
display_cols = [
    'phone', 'display_name', 'expected_name',
    'classificacao', 'intencao',
    'pagamento', 'percepcao_valor_esperado',
    'inventario_flag', 'resposta', 'sheet_synced'
]
# Keep only columns that exist
display_cols = [c for c in display_cols if c in df.columns]

# ─── BUILD GRID DATAFRAME ─────────────────────────────────────────────────
# Add index
if '_idx' not in df.columns:
    df['_idx'] = df.index

grid_cols = ['_idx', *display_cols]
if 'ENDERECO' not in grid_cols:
    grid_cols.append('ENDERECO')

grid_df = df[grid_cols].copy()

# ─── SHOW MODIFICATIONS STATUS ─────────────────────────────────────────────
if 'original_values' in st.session_state and st.session_state['original_values']:
    modified_records = []
    for idx, original in st.session_state['original_values'].items():
        current = st.session_state.master_df.iloc[idx]
        has_changes = False
        for field, orig_value in original.items():
            if field in current and current[field] != orig_value:
                has_changes = True
                break
        if has_changes:
            modified_records.append(idx)
    
    if modified_records:
        st.info(f"📝 Records with modifications: {len(modified_records)} ({', '.join(map(str, [r+1 for r in modified_records]))})")

# ─── RENDER GRID ──────────────────────────────────────────────────────────
# Display the data editor
st.data_editor(
    grid_df,
    disabled=True,
    hide_index=True,
    use_container_width=True,
)

# ─── BULK ACTIONS ─────────────────────────────────────────────────────────
st.subheader("Bulk Actions")
bulk_col1, bulk_col2, bulk_col3 = st.columns(3)

with bulk_col1:
    if st.button("💾 Save to Database"):
        # This would save the current state to the database
        st.info("Save to database functionality would be implemented here")

with bulk_col2:
    if st.button("🔄 Reload Original Data"):
        if st.button("⚠️ Confirm Reload", key="confirm_reload"):
            # Clear all session state and reload
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.success("✅ Reloaded original data - all modifications lost!")
            st.rerun()

with bulk_col3:
    export_format = st.selectbox("Export format", ["CSV", "Excel"], key="export_format")
    if st.button("📊 Export Current State"):
        if export_format == "CSV":
            csv = st.session_state.master_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"whatsapp_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            # Excel export would be implemented here
            st.info("Excel export would be implemented here")

# Add action buttons below the grid
st.subheader("Record Actions")
st.write("Click to open processor for a record:")

# Create a grid of buttons
num_records = len(grid_df)
cols_per_row = 5
num_rows = (num_records + cols_per_row - 1) // cols_per_row

for row_idx in range(num_rows):
    cols = st.columns(cols_per_row)
    for col_idx in range(cols_per_row):
        record_num = row_idx * cols_per_row + col_idx
        if record_num < num_records:
            row = grid_df.iloc[record_num]
            record_idx = row['_idx'] if '_idx' in row else record_num
            display_name = row.get('display_name', f'Record {record_idx}')
            
            # Show if record has modifications
            has_modifications = False
            if 'original_values' in st.session_state and record_idx in st.session_state['original_values']:
                original = st.session_state['original_values'][record_idx]
                current = st.session_state.master_df.iloc[record_idx].to_dict()
                for field in original:
                    if field in current and original[field] != current[field]:
                        has_modifications = True
                        break
            
            button_text = f"{'📝 ' if has_modifications else ''}{display_name[:20]}{'...' if len(display_name) > 20 else ''}"
            
            with cols[col_idx]:
                if st.button(button_text, key=f"open_{record_idx}", use_container_width=True):
                    # Store the selected index in session state
                    st.session_state.selected_idx = record_idx
                    # Navigate to the processor page
                    st.switch_page("pages/Processor.py")