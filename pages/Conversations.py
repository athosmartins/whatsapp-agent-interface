"""Conversations page with filtering and conversation history display."""

import streamlit as st
import pandas as pd
from datetime import datetime
import re

from loaders.db_loader import get_conversations_summary, get_conversation_messages, get_conversation_details, get_conversations_with_sheets_data

# Page config
st.set_page_config(page_title="Conversations", layout="wide")
st.title("💬 Conversations")

# Load conversations summary with merged sheets data (optimized for filtering)
@st.cache_data(ttl=60)  # Cache for 1 minute only to see changes faster
def load_conversations_with_sheets():
    return get_conversations_with_sheets_data()

# Load conversation messages (cached per conversation)
@st.cache_data(ttl=300)
def load_conversation_messages(conversation_id: str):
    return get_conversation_messages(conversation_id)

# Load conversation details (cached per conversation)
@st.cache_data(ttl=300)
def load_conversation_details(conversation_id: str):
    return get_conversation_details(conversation_id)

# Format phone number for display
def format_phone_for_display(phone_number: str) -> str:
    """Format phone number to (XX) XXXXX-XXXX format."""
    if not phone_number:
        return ""
    
    # Extract just the numeric part
    clean_number = re.sub(r'\D', '', phone_number)
    
    # Remove @s.whatsapp.net suffix if present
    if '@' in phone_number:
        clean_number = phone_number.split('@')[0]
        clean_number = re.sub(r'\D', '', clean_number)
    
    # Remove country code if present (assuming Brazilian numbers)
    if clean_number.startswith('55') and len(clean_number) > 10:
        clean_number = clean_number[2:]
    
    # Format as (XX) XXXXX-XXXX
    if len(clean_number) >= 10:
        area_code = clean_number[:2]
        number = clean_number[2:]
        return f"({area_code}) {number[:-4]}-{number[-4:]}"
    
    return phone_number

# Format timestamp for display
def format_timestamp(timestamp):
    """Format timestamp to readable datetime."""
    if pd.isna(timestamp) or timestamp == 0:
        return "N/A"
    try:
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return "N/A"

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

# Initialize session state
if 'selected_conversation_id' not in st.session_state:
    st.session_state.selected_conversation_id = None

if 'filter_state' not in st.session_state:
    st.session_state.filter_state = {}

# Load data
try:
    conversations_df = load_conversations_with_sheets()
    
    if conversations_df.empty:
        st.warning("No conversations found in the database.")
        st.stop()
    
    # Format phone numbers for display
    conversations_df['formatted_phone'] = conversations_df['phone_number'].apply(format_phone_for_display)
    
    # Format timestamps
    conversations_df['last_message_date'] = conversations_df['last_message_timestamp'].apply(format_timestamp)
    
    # Filter section
    st.subheader("🔍 Filter Conversations")
    
    # Filter state is now managed by Streamlit widgets directly
    
    # Function to get available options based on current selections
    def get_filtered_options(current_df, exclude_filter=None):
        """Get available filter options based on current dataframe state"""
        try:
            options = {}
            
            # Display names
            if exclude_filter != 'display_names':
                display_names = []
                for name in current_df['display_name'].dropna().unique():
                    name_str = str(name).strip()
                    # Filter out garbage data: empty strings, single characters, phone numbers, etc.
                    if (name_str and 
                        len(name_str) > 1 and 
                        not name_str.isdigit() and 
                        name_str not in ['.', '..', '...'] and
                        not name_str.startswith('+') and
                        any(c.isalpha() for c in name_str)):  # Must contain at least one letter
                        display_names.append(name_str)
                
                # Sort alphabetically
                options['display_names'] = sorted(display_names, key=lambda x: x.lower())
            else:
                options['display_names'] = []
            
            # Phone numbers  
            if exclude_filter != 'phone_numbers':
                phone_numbers = [str(phone).strip() for phone in current_df['formatted_phone'].dropna().unique() if str(phone).strip()]
                options['phone_numbers'] = sorted(phone_numbers, key=lambda x: x.lower())
            else:
                options['phone_numbers'] = []
            
            # Bairros
            if exclude_filter != 'bairros' and 'endereco_bairro' in current_df.columns:
                bairros = [str(bairro).strip() for bairro in current_df['endereco_bairro'].dropna().unique() if str(bairro).strip()]
                options['bairros'] = sorted(bairros, key=lambda x: x.lower())
            else:
                options['bairros'] = []
            
            # Classificações
            if exclude_filter != 'classificacoes' and 'Classificação do dono do número' in current_df.columns:
                classificacoes = [str(cls).strip() for cls in current_df['Classificação do dono do número'].dropna().unique() if str(cls).strip()]
                options['classificacoes'] = sorted(classificacoes, key=lambda x: x.lower())
            else:
                options['classificacoes'] = []
            
            # Status
            if exclude_filter != 'statuses':
                status_col = None
                if 'status' in current_df.columns:
                    status_col = 'status'
                elif 'status_manual' in current_df.columns:
                    status_col = 'status_manual'
                
                if status_col:
                    statuses = [str(status).strip() for status in current_df[status_col].dropna().unique() if str(status).strip()]
                    options['statuses'] = sorted(statuses, key=lambda x: x.lower())
                else:
                    options['statuses'] = []
            else:
                options['statuses'] = []
            
            # Enderecos
            if 'endereco' in current_df.columns:
                # Filter out empty strings and whitespace-only entries
                endereco_values = current_df['endereco'].dropna().astype(str)
                endereco_values = endereco_values[endereco_values.str.strip() != '']
                enderecos = [str(endereco).strip() for endereco in endereco_values.unique()]
                options['enderecos'] = sorted(enderecos, key=lambda x: x.lower())
            else:
                options['enderecos'] = []
            
            # Complementos
            if exclude_filter != 'complementos' and 'endereco_complemento' in current_df.columns:
                complementos = [str(comp).strip() for comp in current_df['endereco_complemento'].dropna().unique() if str(comp).strip()]
                options['complementos'] = sorted(complementos, key=lambda x: x.lower())
            else:
                options['complementos'] = []
            
            return options
        except Exception as e:
            st.error(f"Error in get_filtered_options: {e}")
            return {
                'display_names': [],
                'phone_numbers': [],
                'bairros': [],
                'classificacoes': [],
                'statuses': [],
                'enderecos': [],
                'complementos': []
            }
    
    # Filter function removed - using widget values directly
    
    # Debug mode check
    DEBUG = st.sidebar.checkbox("Debug Mode", value=False)
    if DEBUG:
        st.sidebar.subheader("Debug Info")
        st.sidebar.write(f"Total conversations loaded: {len(conversations_df)}")
        st.sidebar.write(f"Available columns: {list(conversations_df.columns)}")
        # st.sidebar.write("Filter state:", st.session_state.filter_state)
        
        # Cache clear button
        if st.sidebar.button("Clear Cache"):
            st.cache_data.clear()
            st.rerun()
            
        # Debug sorting
        display_names_raw = [str(name).strip() for name in conversations_df['display_name'].dropna().unique() if str(name).strip()]
        
        # Filter out garbage data
        display_names_filtered = []
        for name in display_names_raw:
            if (name and 
                len(name) > 1 and 
                not name.isdigit() and 
                name not in ['.', '..', '...'] and
                not name.startswith('+') and
                any(c.isalpha() for c in name)):  # Must contain at least one letter
                display_names_filtered.append(name)
        
        display_names_sorted = sorted(display_names_filtered, key=lambda x: x.lower())
        st.sidebar.write("Raw names:", display_names_raw[:10])
        st.sidebar.write("Filtered names:", display_names_filtered[:10])
        st.sidebar.write("Sorted names:", display_names_sorted[:10])
        
        # Test endereco column specifically
        if 'endereco' in conversations_df.columns:
            endereco_count = len(conversations_df['endereco'].dropna())
            unique_enderecos = len(conversations_df['endereco'].dropna().unique())
            st.sidebar.write(f"Endereco data: {endereco_count} non-null values, {unique_enderecos} unique")
    
    try:
        # Get initial options for all filters
        all_options = get_filtered_options(conversations_df)
        
        # First row of filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Display name filter - always show all available display names
            available_display_names = get_filtered_options(conversations_df)['display_names']
            
            # Simple fix: Re-sort the options right before passing to multiselect
            # This ensures they start in alphabetical order
            available_display_names_sorted = sorted(available_display_names, key=lambda x: x.lower())
            
            selected_display_names = st.multiselect(
                "Display Name:",
                options=available_display_names_sorted,
                key="display_name_filter"
            )
    
        with col2:
            # Phone number filter - always show all available phone numbers
            available_phone_numbers = get_filtered_options(conversations_df)['phone_numbers']
            available_phone_numbers_sorted = sorted(available_phone_numbers, key=lambda x: x.lower())
            
            selected_phone_numbers = st.multiselect(
                "Phone Number:",
                options=available_phone_numbers_sorted,
                key="phone_filter"
            )
    
        with col3:
            # Classificação filter - always show all available classificacoes
            available_classificacoes = get_filtered_options(conversations_df)['classificacoes']
            available_classificacoes_sorted = sorted(available_classificacoes, key=lambda x: x.lower())
            
            selected_classificacoes = st.multiselect(
                "Classificação:",
                options=available_classificacoes_sorted,
                key="classificacao_filter"
            )
        
        # Second row of filters
        col4, col5, col6 = st.columns(3)
        
        with col4:
            # Bairro filter - always show all available bairros
            available_bairros = get_filtered_options(conversations_df)['bairros']
            available_bairros_sorted = sorted(available_bairros, key=lambda x: x.lower())
            
            selected_bairros = st.multiselect(
                "Bairro:",
                options=available_bairros_sorted,
                key="bairro_filter"
            )
        
        with col5:
            # Status filter - always show all available statuses
            available_statuses = get_filtered_options(conversations_df)['statuses']
            available_statuses_sorted = sorted(available_statuses, key=lambda x: x.lower())
            
            selected_statuses = st.multiselect(
                "Status:",
                options=available_statuses_sorted,
                key="status_filter"
            )
        
        with col6:
            # Endereco filter - cascade only from bairro selection
            if selected_bairros:
                # Filter endereco options based on selected bairros only
                current_df = conversations_df[conversations_df['endereco_bairro'].isin(selected_bairros)] if 'endereco_bairro' in conversations_df.columns else conversations_df
                available_enderecos = get_filtered_options(current_df)['enderecos']
                if DEBUG:
                    st.sidebar.write(f"Endereco Debug: {len(current_df)} rows after bairro filtering, {len(available_enderecos)} endereco options")
            else:
                # Show all endereco options if no bairro is selected
                available_enderecos = get_filtered_options(conversations_df)['enderecos']
                if DEBUG:
                    st.sidebar.write(f"Endereco Debug: No bairro filter, {len(available_enderecos)} endereco options from all data")
            
            available_enderecos_sorted = sorted(available_enderecos, key=lambda x: x.lower())
            
            selected_enderecos = st.multiselect(
                "Endereco:",
                options=available_enderecos_sorted,
                key="endereco_filter",
                help=f"Available options: {len(available_enderecos)}"
            )
        
        # Third row of filters
        col7, col8, col9 = st.columns(3)
        
        with col7:
            # Complemento filter - always show all available complementos
            available_complementos = get_filtered_options(conversations_df)['complementos']
            available_complementos_sorted = sorted(available_complementos, key=lambda x: x.lower())
            
            selected_complementos = st.multiselect(
                "Complemento:",
                options=available_complementos_sorted,
                key="complemento_filter"
            )
        
        with col8:
            # Clear filters button
            if st.button("🗑️ Clear All Filters", type="secondary"):
                # Clear all filter widget states
                for key in ['display_name_filter', 'phone_filter', 'classificacao_filter', 
                           'bairro_filter', 'status_filter', 'endereco_filter', 'complemento_filter']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        with col9:
            # Show active filters count (using the cleaned variable names)
            all_filters = [selected_display_names, selected_phone_numbers, selected_classificacoes, 
                          selected_bairros, selected_statuses, selected_enderecos, selected_complementos]
            active_filters = sum(1 for filter_list in all_filters if filter_list)
            if active_filters > 0:
                st.metric("Active Filters", active_filters)
    
    except Exception as e:
        st.error(f"Error in filter setup: {e}")
        if DEBUG:
            st.exception(e)
    
    # Apply filters using the widget values directly
    try:
        filtered_df = conversations_df.copy()
        
        # Apply display name filter
        if selected_display_names:
            filtered_df = filtered_df[filtered_df['display_name'].isin(selected_display_names)]
        
        # Apply phone number filter
        if selected_phone_numbers:
            filtered_df = filtered_df[filtered_df['formatted_phone'].isin(selected_phone_numbers)]
        
        # Apply bairro filter
        if selected_bairros and 'endereco_bairro' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['endereco_bairro'].isin(selected_bairros)]
        
        # Apply classificacao filter
        if selected_classificacoes and 'Classificação do dono do número' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['Classificação do dono do número'].isin(selected_classificacoes)]
        
        # Apply status filter
        if selected_statuses:
            status_col = 'status' if 'status' in filtered_df.columns else 'status_manual'
            if status_col in filtered_df.columns:
                filtered_df = filtered_df[filtered_df[status_col].isin(selected_statuses)]
        
        # Apply endereco filter
        if selected_enderecos and 'endereco' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['endereco'].isin(selected_enderecos)]
        
        # Apply complemento filter
        if selected_complementos and 'endereco_complemento' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['endereco_complemento'].isin(selected_complementos)]
    
    except Exception as e:
        st.error(f"Error applying filters: {e}")
        if DEBUG:
            st.exception(e)
        filtered_df = conversations_df.copy()  # Fallback to unfiltered data
    
    # Display filtered results
    st.subheader(f"📋 Filtered Results ({len(filtered_df)} conversations)")
    
    if filtered_df.empty:
        st.info("No conversations match the selected filters.")
    else:
        # Reset the index to ensure proper selection handling
        filtered_df = filtered_df.reset_index(drop=True)
        
        # Create display dataframe with required columns
        display_columns = ['display_name', 'formatted_phone', 'total_messages']
        display_column_names = ['Display Name', 'Phone Number', 'Messages']
        
        # Add expected_name - use 'Nome' from spreadsheet data if available
        if 'Nome' in filtered_df.columns:
            display_columns.insert(1, 'Nome')
            display_column_names.insert(1, 'Expected Name')
        elif 'expected_name' in filtered_df.columns:
            display_columns.insert(1, 'expected_name')
            display_column_names.insert(1, 'Expected Name')
        
        # Add sheets data columns if they exist
        if 'endereco_bairro' in filtered_df.columns:
            display_columns.append('endereco_bairro')
            display_column_names.append('Bairro')
        
        if 'endereco' in filtered_df.columns:
            display_columns.append('endereco')
            display_column_names.append('Endereco')
        
        if 'endereco_complemento' in filtered_df.columns:
            display_columns.append('endereco_complemento')
            display_column_names.append('Complemento')
        
        if 'Classificação do dono do número' in filtered_df.columns:
            display_columns.append('Classificação do dono do número')
            display_column_names.append('Classificacao')
        
        if 'status' in filtered_df.columns:
            display_columns.append('status')
            display_column_names.append('Status')
        elif 'status_manual' in filtered_df.columns:
            display_columns.append('status_manual')
            display_column_names.append('Status')
        
        # Only include columns that exist in the dataframe
        available_columns = [col for col in display_columns if col in filtered_df.columns]
        available_names = [display_column_names[i] for i, col in enumerate(display_columns) if col in filtered_df.columns]
        
        display_df = filtered_df[available_columns].copy()
        display_df.columns = available_names
        
        # Add conversation selector
        st.write("Select a conversation to view full history:")
        
        # Display conversations with clickable rows
        event = st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row"
        )
        
        # Handle row selection
        if hasattr(event, 'selection') and event.selection and event.selection.get('rows'):
            selected_row_idx = event.selection['rows'][0]
            if selected_row_idx < len(filtered_df):
                selected_conversation_id = filtered_df.iloc[selected_row_idx]['conversation_id']
                st.session_state.selected_conversation_id = selected_conversation_id
        
        # Show "Open in Processor" button if a conversation is selected
        if st.session_state.selected_conversation_id:
            selected_conv_matches = filtered_df[filtered_df['conversation_id'] == st.session_state.selected_conversation_id]
            if not selected_conv_matches.empty:
                selected_conv_info = selected_conv_matches.iloc[0]
                
                st.success(f"Selected: {selected_conv_info['display_name']} ({selected_conv_info['formatted_phone']})")
                
                if st.button("📝 Open in Processor", type="primary", use_container_width=True):
                    # Store the selected conversation data in session state for the Processor page
                    st.session_state.processor_conversation_id = st.session_state.selected_conversation_id
                    st.session_state.processor_conversation_data = selected_conv_info.to_dict()
                    
                    # Navigate to Processor page using Streamlit's page navigation
                    st.switch_page("pages/Processor.py")
            else:
                st.warning("Selected conversation not found in current filter. Please reselect.")

except Exception as e:
    st.error(f"Error loading conversations: {str(e)}")
    st.info("Please check if the database is available and contains the conversations table.")