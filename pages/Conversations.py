"""Conversations page with filtering and conversation history display."""

import streamlit as st
import pandas as pd
from datetime import datetime
import re

from loaders.db_loader import (
    get_conversation_messages,
    get_conversation_details,
    get_conversations_with_sheets_data,
)
from services.preloader import start_background_preload, display_preloader_status

# Import centralized phone utilities
from services.phone_utils import format_phone_for_display as format_phone_display

# Page config
st.set_page_config(page_title="Conversations", layout="wide")
st.title("💬 Conversations")

# Debug mode check (define early)
DEBUG = st.sidebar.checkbox("Debug Mode", value=False)

# ─── START BACKGROUND PRELOADER ─────────────────────────────────────────────
# Start background downloading of all critical files for smooth UX
if "preloader_started" not in st.session_state:
    st.session_state.preloader_started = True
    start_background_preload()

# Display preloader status in sidebar
display_preloader_status()


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
    # Use centralized phone utility for consistent behavior
    return format_phone_display(phone_number)


# Format timestamp for display
def format_timestamp(timestamp):
    """Format timestamp to readable datetime."""
    if pd.isna(timestamp) or timestamp == 0:
        return "N/A"
    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return "N/A"


def format_time_only(timestamp):
    """Format timestamp to show only HH:MM in BRT."""
    if pd.isna(timestamp) or timestamp == 0:
        return ""
    try:
        return datetime.fromtimestamp(timestamp).strftime("%H:%M")
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
                1: "Janeiro",
                2: "Fevereiro",
                3: "Março",
                4: "Abril",
                5: "Maio",
                6: "Junho",
                7: "Julho",
                8: "Agosto",
                9: "Setembro",
                10: "Outubro",
                11: "Novembro",
                12: "Dezembro",
            }

            # Portuguese weekday names
            weekdays_pt = {
                0: "Segunda-feira",
                1: "Terça-feira",
                2: "Quarta-feira",
                3: "Quinta-feira",
                4: "Sexta-feira",
                5: "Sábado",
                6: "Domingo",
            }

            day = dt.day
            month = months_pt[dt.month]
            year = dt.year
            weekday = weekdays_pt[dt.weekday()]

            return f"{day} {month}, {year} - {weekday}"
    except:
        return ""


# Initialize session state for persistent filters
if "selected_conversation_id" not in st.session_state:
    st.session_state.selected_conversation_id = None

# Initialize persistent filter state
if "conversations_filter_state" not in st.session_state:
    st.session_state.conversations_filter_state = {
        "display_name_filter": [],
        "phone_filter": [],
        "expected_name_filter": [],
        "cpf_filter": [],
        "classificacao_filter": [],
        "bairro_filter": [],
        "status_filter": [],
        "endereco_filter": [],
        "complemento_filter": [],
        "only_unarchived_filter": False,
        "only_unread_filter": False,
        "property_status_filter": [],
    }

# ─── ULTRA-FAST PROPERTY MAP FUNCTION ──────────────────────────────────────
# PERFORMANCE OPTIMIZATION: Using ultra-fast batch loader for 50x speed improvement
from services.ultra_fast_property_loader import ultra_fast_batch_load_properties


# Fallback function using original approach for compatibility
def fallback_batch_load_properties(conversation_data):
    """Fallback property loading using original individual approach."""
    print("🔄 Using fallback property loading approach...")
    from services.mega_data_set_loader import get_properties_for_phone

    phone_to_properties = {}

    for _, row in conversation_data.iterrows():
        phone_number = row["phone_number"]
        if phone_number:
            try:
                properties = get_properties_for_phone(phone_number)
                if properties:
                    phone_to_properties[phone_number] = properties
            except Exception as e:
                print(f"❌ Error getting properties for {phone_number}: {e}")
                continue

    return phone_to_properties


def show_filtered_conversations_map(filtered_df):
    """Show interactive map of all properties from filtered conversations."""

    if filtered_df.empty:
        return

    # Limit conversations for performance
    max_conversations = 200
    limited_df = filtered_df.head(max_conversations)

    if len(filtered_df) > max_conversations:
        st.warning(
            f"⚡ Mostrando propriedades dos primeiros {max_conversations} contatos para melhor performance. Total filtrado: {len(filtered_df)}"
        )

    # Removed visualization description as requested

    # Ultra-fast batch load all properties - 50x faster!
    import time

    start_time = time.time()

    try:
        with st.spinner("Carregando propriedades (modo ultra-rápido)..."):
            phone_to_properties = ultra_fast_batch_load_properties(limited_df)
        load_time = time.time() - start_time

        # If ultra-fast loading found no properties, try fallback
        if not phone_to_properties and len(limited_df) > 0:
            st.warning(
                "⚠️ Ultra-fast loader found no properties. Trying fallback approach..."
            )
            fallback_start = time.time()
            with st.spinner("Tentando abordagem alternativa..."):
                phone_to_properties = fallback_batch_load_properties(
                    limited_df.head(10)
                )  # Limit to 10 for testing
            fallback_time = time.time() - fallback_start
            if phone_to_properties:
                st.success(
                    f"✅ Fallback encontrou propriedades em {fallback_time:.2f} segundos"
                )
            else:
                st.info(
                    f"🔍 Fallback também não encontrou propriedades ({fallback_time:.2f}s)"
                )

        # Show performance improvement
        if phone_to_properties:
            st.success(f"⚡ Propriedades carregadas em {load_time:.2f} segundos")
        else:
            st.info(f"🔍 Busca concluída em {load_time:.2f} segundos")

    except Exception as e:
        load_time = time.time() - start_time
        st.error(f"❌ Erro ao carregar propriedades: {str(e)}")
        if DEBUG:
            st.exception(e)
        phone_to_properties = {}

    # Collect all properties with conversation context
    all_properties = []
    conversations_with_properties = 0

    for idx, row in limited_df.iterrows():
        phone_number = row["phone_number"]
        if phone_number and phone_number in phone_to_properties:
            conversations_with_properties += 1
            properties = phone_to_properties[phone_number]

            # Add conversation info to each property
            for prop in properties:
                prop_with_context = prop.copy()
                prop_with_context["_conversation_display_name"] = row.get(
                    "display_name", "N/A"
                )
                prop_with_context["_conversation_phone"] = row.get(
                    "formatted_phone", phone_number
                )
                prop_with_context["_conversation_id"] = row.get(
                    "conversation_id", "N/A"
                )
                all_properties.append(prop_with_context)

    if not all_properties:
        st.warning("⚠️ Nenhuma propriedade encontrada para os contatos filtrados.")

        # Show detailed analysis when no properties found
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Possíveis motivos:**")
            st.write("• Contatos não possuem CPF cadastrado no Google Sheets")
            st.write("• CPF não encontrado no mega_data_set")
            st.write("• Propriedades não possuem dados geográficos")

        with col2:
            st.write("**Estatísticas da busca:**")
            st.write(f"• Contatos processados: {len(limited_df)}")
            st.write(f"• Tempo de processamento: {load_time:.2f}s")
            if DEBUG:
                st.write(
                    f"• Telefones únicos: {len(set(limited_df['phone_number'].dropna()))}"
                )

        return

    # Removed summary statistics as requested

    # Show quick reference for conversation IDs if debug mode is enabled
    if DEBUG and all_properties:
        with st.expander("🆔 IDs das Conversas com Propriedades", expanded=False):
            conversation_ids = set()
            for prop in all_properties:
                conv_id = prop.get("_conversation_id")
                conv_name = prop.get("_conversation_display_name")
                if conv_id and conv_id != "N/A":
                    conversation_ids.add((conv_id, conv_name or "Nome não disponível"))

            if conversation_ids:
                st.write("**IDs para busca no Processor:**")
                for conv_id, conv_name in sorted(conversation_ids):
                    st.write(f"• `{conv_id}` - {conv_name}")
            else:
                st.write("Nenhum ID de conversa encontrado")

    # Import map function (style selector moved to advanced options)
    from utils.property_map import render_property_map_streamlit

    # Render the map with all properties
    try:
        st.write(f"🎯 Renderizando mapa com {len(all_properties)} propriedades...")

        if DEBUG:
            st.write("📋 Debug: Primeiras 3 propriedades:")
            for i, prop in enumerate(all_properties[:3]):
                st.write(
                    f"   {i+1}. {prop.get('ENDERECO', 'N/A')} - Geometry: {len(str(prop.get('GEOMETRY', '')))}"
                )

            # Debug map settings
            st.write("🗺️ Map Debug Info:")
            st.write(f"   - Default Zoom Level: 16")
            st.write(f"   - Total Properties: {len(all_properties)}")
            st.write(
                f"   - Properties with Geometry: {len([p for p in all_properties if p.get('GEOMETRY')])}"
            )

        # Add a container to isolate the map rendering
        map_container = st.container()
        with map_container:
            # Try a simple test first
            if DEBUG and len(all_properties) > 0:
                st.write("📊 Map render test:")
                st.write(f"   - Properties: {len(all_properties)}")
                st.write("   - Style: Light (default)")
                st.write(
                    f"   - First property: {all_properties[0].get('ENDERECO', 'N/A')}"
                )

            render_property_map_streamlit(
                all_properties,
                map_style="Light",  # Default style, can be changed in advanced options
                enable_extra_options=True,
                enable_style_selector=False,  # Style selector moved to advanced options
            )

        st.write("✅ Map render completed")

        # Add helpful info about processor navigation

        # Add JavaScript to handle navigation messages from map
        st.markdown(
            """
        <script>
        // Listen for messages from the map
        window.addEventListener('message', function(event) {
            if (event.data.type === 'navigate_to_processor') {
                const conversationId = event.data.conversation_id;
                
                // Store in session storage for processor page
                sessionStorage.setItem('processor_conversation_id', conversationId);
                sessionStorage.setItem('auto_search_processor', 'true');
                
                // Navigate to processor page
                window.location.href = window.location.origin + window.location.pathname + '?conversation_id=' + encodeURIComponent(conversationId);
            }
        });
        
        // Check if we need to auto-search in processor
        if (sessionStorage.getItem('auto_search_processor') === 'true') {
            const convId = sessionStorage.getItem('processor_conversation_id');
            if (convId) {
                // Clear the flags
                sessionStorage.removeItem('auto_search_processor');
                sessionStorage.removeItem('processor_conversation_id');
                
                // If we're on conversations page, navigate to processor
                if (window.location.pathname.includes('Conversations')) {
                    window.location.href = window.location.origin + window.location.pathname.replace('Conversations', 'Processor') + '?search=' + encodeURIComponent(convId);
                }
            }
        }
        </script>
        """,
            unsafe_allow_html=True,
        )

    except Exception as e:
        st.error(f"❌ Erro ao carregar mapa: {e}")
        if DEBUG:
            st.exception(e)
        st.info(
            "💡 Para ver o mapa, instale as dependências: `pip install folium streamlit-folium`"
        )

        # Show fallback property list
        if all_properties:
            st.subheader("📋 Lista de Propriedades")
            properties_data = []
            for prop in all_properties:
                properties_data.append(
                    {
                        "Contato": prop.get("_conversation_display_name", "N/A"),
                        "Telefone": prop.get("_conversation_phone", "N/A"),
                        "Endereço": prop.get("ENDERECO", "N/A"),
                        "Bairro": prop.get("BAIRRO", "N/A"),
                        "Tipo": prop.get("TIPO CONSTRUTIVO", "N/A"),
                        "Área Terreno": prop.get("AREA TERRENO", "N/A"),
                        "Valor NET": prop.get("NET VALOR", "N/A"),
                    }
                )

            if properties_data:
                properties_df = pd.DataFrame(properties_data)
                st.dataframe(properties_df, hide_index=True, use_container_width=True)


# Load data
try:
    conversations_df = load_conversations_with_sheets()

    if conversations_df.empty:
        st.warning("No conversations found in the database.")
        st.stop()

    # Format phone numbers for display
    conversations_df["formatted_phone"] = conversations_df["phone_number"].apply(
        format_phone_for_display
    )

    # Format timestamps
    conversations_df["last_message_date"] = conversations_df[
        "last_message_timestamp"
    ].apply(format_timestamp)

    # Debug: Show available columns in production
    if DEBUG:
        st.sidebar.write("Available columns:", list(conversations_df.columns))
        st.sidebar.write("DataFrame shape:", conversations_df.shape)
        if len(conversations_df) > 0:
            # Check if key columns exist
            key_columns = [
                "Nome",
                "CPF",
                "endereco_bairro",
                "endereco",
                "Classificação do dono do número",
            ]
            for col in key_columns:
                exists = col in conversations_df.columns
                st.sidebar.write(f"{col}: {'✅' if exists else '❌'}")

    # Check if we have merged data or just basic conversations
    has_sheets_data = any(
        col in conversations_df.columns
        for col in ["Nome", "CPF", "endereco_bairro", "Classificação do dono do número"]
    )

    if not has_sheets_data:
        st.warning(
            "⚠️ Google Sheets integration not available. Only basic conversation data is loaded. Some filters may be disabled."
        )
        st.info(f"📊 Loaded {len(conversations_df)} conversations (basic data only)")
    else:
        st.success(
            f"📊 Loaded {len(conversations_df)} conversations with integrated Google Sheets data"
        )

    # Filter section
    st.subheader("🔍 Filter Conversations")

    # Filter state is now managed by Streamlit widgets directly

    # Function to get available options based on current selections
    def get_filtered_options(current_df, exclude_filter=None):
        """Get available filter options based on current dataframe state"""
        try:
            options = {}

            # Display names
            if exclude_filter != "display_names":
                display_names = []
                for name in current_df["display_name"].dropna().unique():
                    name_str = str(name).strip()
                    # Filter out garbage data: empty strings, single characters, phone numbers, etc.
                    if (
                        name_str
                        and len(name_str) > 1
                        and not name_str.isdigit()
                        and name_str not in [".", "..", "..."]
                        and not name_str.startswith("+")
                        and any(c.isalpha() for c in name_str)
                    ):  # Must contain at least one letter
                        display_names.append(name_str)

                # Sort alphabetically
                options["display_names"] = sorted(
                    display_names, key=lambda x: x.lower()
                )
            else:
                options["display_names"] = []

            # Phone numbers
            if exclude_filter != "phone_numbers":
                phone_numbers = [
                    str(phone).strip()
                    for phone in current_df["formatted_phone"].dropna().unique()
                    if str(phone).strip()
                ]
                options["phone_numbers"] = sorted(
                    phone_numbers, key=lambda x: x.lower()
                )
            else:
                options["phone_numbers"] = []

            # Bairros
            if exclude_filter != "bairros" and "endereco_bairro" in current_df.columns:
                bairros = [
                    str(bairro).strip()
                    for bairro in current_df["endereco_bairro"].dropna().unique()
                    if str(bairro).strip()
                ]
                options["bairros"] = sorted(bairros, key=lambda x: x.lower())
            else:
                options["bairros"] = []

            # Expected Names
            if exclude_filter != "expected_names":
                expected_names = []
                # Try both 'Nome' (from spreadsheet) and 'expected_name' columns
                for col in ["Nome", "expected_name"]:
                    if col in current_df.columns:
                        names = [
                            str(name).strip()
                            for name in current_df[col].dropna().unique()
                            if str(name).strip()
                        ]
                        # Filter out garbage data similar to display names
                        for name in names:
                            if (
                                name
                                and len(name) > 1
                                and not name.isdigit()
                                and not name.startswith("+")
                                and any(c.isalpha() for c in name)
                            ):
                                expected_names.append(name)
                options["expected_names"] = sorted(
                    list(set(expected_names)), key=lambda x: x.lower()
                )
            else:
                options["expected_names"] = []

            # CPF
            if exclude_filter != "cpfs":
                cpfs = []
                # Look for CPF in spreadsheet columns
                for col in ["CPF", "cpf", "documento", "Documento"]:
                    if col in current_df.columns:
                        cpf_values = [
                            str(cpf).strip()
                            for cpf in current_df[col].dropna().unique()
                            if str(cpf).strip()
                        ]
                        # Filter valid CPF format (11 digits)
                        for cpf in cpf_values:
                            # Remove any formatting and check if it's 11 digits
                            clean_cpf = "".join(filter(str.isdigit, cpf))
                            if len(clean_cpf) == 11 and clean_cpf != "00000000000":
                                # Format CPF for display: 000.000.000-00
                                formatted_cpf = f"{clean_cpf[:3]}.{clean_cpf[3:6]}.{clean_cpf[6:9]}-{clean_cpf[9:]}"
                                cpfs.append(formatted_cpf)
                options["cpfs"] = sorted(list(set(cpfs)), key=lambda x: x.lower())
            else:
                options["cpfs"] = []

            # Classificações
            if (
                exclude_filter != "classificacoes"
                and "Classificação do dono do número" in current_df.columns
            ):
                classificacoes = [
                    str(cls).strip()
                    for cls in current_df["Classificação do dono do número"]
                    .dropna()
                    .unique()
                    if str(cls).strip()
                ]
                options["classificacoes"] = sorted(
                    classificacoes, key=lambda x: x.lower()
                )
            else:
                options["classificacoes"] = []

            # Status
            if exclude_filter != "statuses":
                status_col = None
                if "status" in current_df.columns:
                    status_col = "status"
                elif "status_manual" in current_df.columns:
                    status_col = "status_manual"

                if status_col:
                    statuses = [
                        str(status).strip()
                        for status in current_df[status_col].dropna().unique()
                        if str(status).strip()
                    ]
                    options["statuses"] = sorted(statuses, key=lambda x: x.lower())
                else:
                    options["statuses"] = []
            else:
                options["statuses"] = []

            # Enderecos
            if "endereco" in current_df.columns:
                # Filter out empty strings and whitespace-only entries
                endereco_values = current_df["endereco"].dropna().astype(str)
                endereco_values = endereco_values[endereco_values.str.strip() != ""]
                enderecos = [
                    str(endereco).strip() for endereco in endereco_values.unique()
                ]
                options["enderecos"] = sorted(enderecos, key=lambda x: x.lower())
            else:
                options["enderecos"] = []

            # Complementos
            if (
                exclude_filter != "complementos"
                and "endereco_complemento" in current_df.columns
            ):
                complementos = [
                    str(comp).strip()
                    for comp in current_df["endereco_complemento"].dropna().unique()
                    if str(comp).strip()
                ]
                options["complementos"] = sorted(complementos, key=lambda x: x.lower())
            else:
                options["complementos"] = []

            return options
        except Exception as e:
            st.error(f"Error in get_filtered_options: {e}")
            return {
                "display_names": [],
                "phone_numbers": [],
                "bairros": [],
                "expected_names": [],
                "cpfs": [],
                "classificacoes": [],
                "statuses": [],
                "enderecos": [],
                "complementos": [],
            }

    # Filter function removed - using widget values directly

    if DEBUG:
        st.sidebar.subheader("Debug Info")
        st.sidebar.write(f"Total conversations loaded: {len(conversations_df)}")
        st.sidebar.write(f"Available columns: {list(conversations_df.columns)}")

        # Debug archived and unread_count columns
        if "archived" in conversations_df.columns:
            archived_counts = conversations_df["archived"].value_counts()
            st.sidebar.write(f"Archived column values: {dict(archived_counts)}")
            st.sidebar.write(
                f"Archived column dtype: {conversations_df['archived'].dtype}"
            )
        else:
            st.sidebar.write("❌ 'archived' column not found")

        if "unread_count" in conversations_df.columns:
            unread_counts = conversations_df["unread_count"].value_counts().head(10)
            st.sidebar.write(f"Unread count values (top 10): {dict(unread_counts)}")
            st.sidebar.write(
                f"Unread count dtype: {conversations_df['unread_count'].dtype}"
            )
            unread_greater_than_0 = (conversations_df["unread_count"] > 0).sum()
            st.sidebar.write(
                f"Conversations with unread_count > 0: {unread_greater_than_0}"
            )
        else:
            st.sidebar.write("❌ 'unread_count' column not found")

        # Debug filter checkboxes
        st.sidebar.write(
            f"Only unarchived filter: {st.session_state.get('only_unarchived_filter', False)}"
        )
        st.sidebar.write(
            f"Only unread filter: {st.session_state.get('only_unread_filter', False)}"
        )

        # st.sidebar.write("Filter state:", st.session_state.filter_state)

        # Cache clear buttons
        if st.sidebar.button("Clear Streamlit Cache"):
            st.cache_data.clear()
            st.rerun()

        if st.sidebar.button("Clear Ultra-Fast Cache"):
            from services.ultra_fast_property_loader import clear_ultra_fast_cache

            clear_ultra_fast_cache()
            st.sidebar.success("Ultra-fast cache cleared!")
            st.rerun()

        # Debug sorting
        display_names_raw = [
            str(name).strip()
            for name in conversations_df["display_name"].dropna().unique()
            if str(name).strip()
        ]

        # Filter out garbage data
        display_names_filtered = []
        for name in display_names_raw:
            if (
                name
                and len(name) > 1
                and not name.isdigit()
                and name not in [".", "..", "..."]
                and not name.startswith("+")
                and any(c.isalpha() for c in name)
            ):  # Must contain at least one letter
                display_names_filtered.append(name)

        display_names_sorted = sorted(display_names_filtered, key=lambda x: x.lower())
        st.sidebar.write("Raw names:", display_names_raw[:10])
        st.sidebar.write("Filtered names:", display_names_filtered[:10])
        st.sidebar.write("Sorted names:", display_names_sorted[:10])

        # Test endereco column specifically
        if "endereco" in conversations_df.columns:
            endereco_count = len(conversations_df["endereco"].dropna())
            unique_enderecos = len(conversations_df["endereco"].dropna().unique())
            st.sidebar.write(
                f"Endereco data: {endereco_count} non-null values, {unique_enderecos} unique"
            )

    try:
        # Helper function to apply filters and get current dataset for cascading
        def get_current_filtered_dataset(exclude_filter_key=None):
            """
            Apply all current filter selections EXCEPT the one being calculated
            to get the dataset for cascading filter options.
            """
            current_df = conversations_df.copy()

            # Get current filter values from session state
            current_filters = {
                "display_names": st.session_state.get("display_name_filter", []),
                "phone_numbers": st.session_state.get("phone_filter", []),
                "expected_names": st.session_state.get("expected_name_filter", []),
                "cpfs": st.session_state.get("cpf_filter", []),
                "classificacoes": st.session_state.get("classificacao_filter", []),
                "bairros": st.session_state.get("bairro_filter", []),
                "statuses": st.session_state.get("status_filter", []),
                "enderecos": st.session_state.get("endereco_filter", []),
                "complementos": st.session_state.get("complemento_filter", []),
            }

            # Apply all filters except the one being excluded
            if (
                exclude_filter_key != "display_names"
                and current_filters["display_names"]
            ):
                current_df = current_df[
                    current_df["display_name"].isin(current_filters["display_names"])
                ]

            if (
                exclude_filter_key != "phone_numbers"
                and current_filters["phone_numbers"]
            ):
                current_df = current_df[
                    current_df["formatted_phone"].isin(current_filters["phone_numbers"])  # type: ignore
                ]

            if (
                exclude_filter_key != "expected_names"
                and current_filters["expected_names"]
            ):
                if "Nome" in current_df.columns:  # type: ignore
                    current_df = current_df[
                        current_df["Nome"].isin(current_filters["expected_names"])  # type: ignore
                    ]
                elif "expected_name" in current_df.columns:  # type: ignore
                    current_df = current_df[
                        current_df["expected_name"].isin(
                            current_filters["expected_names"]
                        )  # type: ignore
                    ]

            if exclude_filter_key != "cpfs" and current_filters["cpfs"]:
                # Apply CPF filter with format matching
                cpf_matched = False
                for col in ["CPF", "cpf", "documento", "Documento"]:
                    if col in current_df.columns and not cpf_matched:  # type: ignore

                        def format_cpf_for_match(cpf_val):
                            if pd.isna(cpf_val):
                                return ""
                            clean_cpf = "".join(filter(str.isdigit, str(cpf_val)))
                            if len(clean_cpf) == 11:
                                return f"{clean_cpf[:3]}.{clean_cpf[3:6]}.{clean_cpf[6:9]}-{clean_cpf[9:]}"
                            return ""

                        current_df["temp_formatted_cpf"] = current_df[col].apply(  # type: ignore
                            format_cpf_for_match
                        )
                        current_df = current_df[
                            current_df["temp_formatted_cpf"].isin(
                                current_filters["cpfs"]
                            )  # type: ignore
                        ]
                        current_df = current_df.drop("temp_formatted_cpf", axis=1)  # type: ignore
                        cpf_matched = True

            if (
                exclude_filter_key != "classificacoes"
                and current_filters["classificacoes"]
            ):
                if "Classificação do dono do número" in current_df.columns:  # type: ignore
                    current_df = current_df[
                        current_df["Classificação do dono do número"].isin(
                            current_filters["classificacoes"]
                        )  # type: ignore
                    ]

            if exclude_filter_key != "bairros" and current_filters["bairros"]:
                if "endereco_bairro" in current_df.columns:
                    current_df = current_df[
                        current_df["endereco_bairro"].isin(current_filters["bairros"])
                    ]

            if exclude_filter_key != "statuses" and current_filters["statuses"]:
                status_col = (
                    "status" if "status" in current_df.columns else "status_manual"
                )
                if status_col in current_df.columns:
                    current_df = current_df[
                        current_df[status_col].isin(current_filters["statuses"])
                    ]

            if exclude_filter_key != "enderecos" and current_filters["enderecos"]:
                if "endereco" in current_df.columns:
                    current_df = current_df[
                        current_df["endereco"].isin(current_filters["enderecos"])
                    ]

            if exclude_filter_key != "complementos" and current_filters["complementos"]:
                if "endereco_complemento" in current_df.columns:
                    current_df = current_df[
                        current_df["endereco_complemento"].isin(
                            current_filters["complementos"]
                        )
                    ]

            return current_df

        # Helper function for persistent multiselect filters
        def persistent_multiselect(
            label, options, filter_key, widget_key, help_text=None
        ):
            # CRITICAL FIX: Use widget state as the single source of truth to prevent value disappearing bug
            # Get current widget state if it exists, otherwise use persistent filter state
            if widget_key in st.session_state:
                # Widget state exists, use it as the source of truth
                current_selections = st.session_state[widget_key]
            else:
                # Widget doesn't exist yet, use persistent filter state
                current_selections = st.session_state.conversations_filter_state.get(
                    filter_key, []
                )
            
            # Combine available options with current selections to ensure they're always available
            all_options = list(set(options + current_selections))
            sorted_options = (
                sorted(all_options, key=lambda x: x.lower()) if all_options else []
            )

            # Create the widget with the current selections as default
            selected = st.multiselect(
                label,
                options=sorted_options,
                default=current_selections,
                key=widget_key,
                help=help_text,
            )
            
            # Update the persistent filter state with the widget's current value
            # This ensures both states stay in sync
            st.session_state.conversations_filter_state[filter_key] = selected
            return selected

        # Helper function for persistent checkbox filters
        def persistent_checkbox(label, filter_key, widget_key, help_text=None):
            current_value = st.session_state.conversations_filter_state.get(
                filter_key, False
            )
            selected = st.checkbox(
                label, value=current_value, key=widget_key, help=help_text
            )
            st.session_state.conversations_filter_state[filter_key] = selected
            return selected

        # === INFORMAÇÕES PESSOAIS ===
        st.markdown("### 👤 Informações Pessoais")
        if not has_sheets_data:
            st.info("ℹ️ Expected Name and CPF filters require Google Sheets integration")
        col1, col2, col3 = st.columns(3)

        with col1:
            # Display name filter - get options based on other filters
            current_df = get_current_filtered_dataset(
                exclude_filter_key="display_names"
            )
            available_display_names = get_filtered_options(current_df)["display_names"]

            selected_display_names = persistent_multiselect(
                "🔤 Display Name:",
                available_display_names,
                "display_name_filter",
                "display_name_filter",
            )

        with col2:
            # Expected Name filter - get options based on other filters
            current_df = get_current_filtered_dataset(
                exclude_filter_key="expected_names"
            )
            available_expected_names = get_filtered_options(current_df)[
                "expected_names"
            ]

            selected_expected_names = persistent_multiselect(
                "🙍‍♂️ Expected Name:",
                available_expected_names,
                "expected_name_filter",
                "expected_name_filter",
            )

        with col3:
            # CPF filter - get options based on other filters
            current_df = get_current_filtered_dataset(exclude_filter_key="cpfs")
            available_cpfs = get_filtered_options(current_df)["cpfs"]

            selected_cpfs = persistent_multiselect(
                "🔢 CPF:", available_cpfs, "cpf_filter", "cpf_filter"
            )

        # === CONTATO ===
        st.markdown("### 📞 Contato")
        col4 = st.columns(1)[0]

        with col4:
            # Phone number filter - get options based on other filters
            current_df = get_current_filtered_dataset(
                exclude_filter_key="phone_numbers"
            )
            available_phone_numbers = get_filtered_options(current_df)["phone_numbers"]

            selected_phone_numbers = persistent_multiselect(
                "📞 Phone Number:",
                available_phone_numbers,
                "phone_filter",
                "phone_filter",
            )

        # === IMÓVEL ===
        st.markdown("### 🏠 Imóvel")
        if not has_sheets_data:
            st.warning("🔗 Filters below require Google Sheets integration")
        col5, col6, col7 = st.columns(3)

        with col5:
            # Bairro filter - get options based on other filters
            current_df = get_current_filtered_dataset(exclude_filter_key="bairros")
            available_bairros = get_filtered_options(current_df)["bairros"]

            selected_bairros = persistent_multiselect(
                "🗺️ Bairro:", available_bairros, "bairro_filter", "bairro_filter"
            )

        with col6:
            # Endereco filter - get options based on other filters
            current_df = get_current_filtered_dataset(exclude_filter_key="enderecos")
            available_enderecos = get_filtered_options(current_df)["enderecos"]

            selected_enderecos = persistent_multiselect(
                "📍 Endereco:",
                available_enderecos,
                "endereco_filter",
                "endereco_filter",
            )

        with col7:
            # Complemento filter - get options based on other filters
            current_df = get_current_filtered_dataset(exclude_filter_key="complementos")
            available_complementos = get_filtered_options(current_df)["complementos"]

            selected_complementos = persistent_multiselect(
                "🚪 Complemento:",
                available_complementos,
                "complemento_filter",
                "complemento_filter",
            )

        # === QUALIFICAÇÃO ===
        st.markdown("### ✅ Qualificação")
        if not has_sheets_data:
            st.warning("🔗 Filters below require Google Sheets integration")
        col8, col9 = st.columns(2)

        with col8:
            # Classificação filter - get options based on other filters
            current_df = get_current_filtered_dataset(
                exclude_filter_key="classificacoes"
            )
            available_classificacoes = get_filtered_options(current_df)[
                "classificacoes"
            ]

            selected_classificacoes = persistent_multiselect(
                "✅ Classificação:",
                available_classificacoes,
                "classificacao_filter",
                "classificacao_filter",
            )

        with col9:
            # Status filter - get options based on other filters
            current_df = get_current_filtered_dataset(exclude_filter_key="statuses")
            available_statuses = get_filtered_options(current_df)["statuses"]

            selected_statuses = persistent_multiselect(
                "🎯 Status:", available_statuses, "status_filter", "status_filter"
            )

        # === CONVERSA ===
        st.markdown("### 💬 Conversa")
        col_conv1, col_conv2, col_conv3 = st.columns(3)

        with col_conv1:
            property_status_options = ["Sim", "Não"]
            property_status_filter = persistent_multiselect(
                "🏠 Propriedade Atribuída?",
                property_status_options,
                "property_status_filter",
                "property_status_filter",
                "Sim = Bairro e Endereço preenchidos, Não = Bairro ou Endereço em branco"
            )

        with col_conv2:
            only_unarchived = persistent_checkbox(
                "📁 Apenas não arquivadas",
                "only_unarchived_filter",
                "only_unarchived_filter",
                "Mostrar apenas conversas não arquivadas",
            )

        with col_conv3:
            only_unread = persistent_checkbox(
                "👁️ Apenas não lidas",
                "only_unread_filter",
                "only_unread_filter",
                "Mostrar apenas conversas com mensagens não lidas",
            )

        # === ACTIONS & METRICS ===
        col10, col11, col12 = st.columns(3)

        with col10:
            # Clear filters button
            if st.button("🗑️ Clear All Filters", type="secondary"):
                # Reset persistent filter state
                st.session_state.conversations_filter_state = {
                    "display_name_filter": [],
                    "phone_filter": [],
                    "expected_name_filter": [],
                    "cpf_filter": [],
                    "classificacao_filter": [],
                    "bairro_filter": [],
                    "status_filter": [],
                    "endereco_filter": [],
                    "complemento_filter": [],
                    "only_unarchived_filter": False,
                    "only_unread_filter": False,
                    "property_status_filter": [],
                }
                # Also clear widget states
                for key in [
                    "display_name_filter",
                    "phone_filter",
                    "expected_name_filter",
                    "cpf_filter",
                    "classificacao_filter",
                    "bairro_filter",
                    "status_filter",
                    "endereco_filter",
                    "complemento_filter",
                    "only_unarchived_filter",
                    "only_unread_filter",
                    "property_status_filter",
                ]:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()

        with col11:
            # Show active filters count (using the cleaned variable names)
            all_filters = [
                selected_display_names,
                selected_phone_numbers,
                selected_expected_names,
                selected_cpfs,
                selected_classificacoes,
                selected_bairros,
                selected_statuses,
                selected_enderecos,
                selected_complementos,
            ]
            # Add checkbox filters
            checkbox_filters = [only_unarchived, only_unread]
            active_filters = sum(1 for filter_list in all_filters if filter_list) + sum(
                1 for checkbox in checkbox_filters if checkbox
            )
            if active_filters > 0:
                st.metric("🔍 Active Filters", active_filters)

    except Exception as e:
        st.error(f"Error in filter setup: {e}")
        if DEBUG:
            st.exception(e)

    # Apply filters using the widget values directly
    try:
        filtered_df = conversations_df.copy()

        # Apply display name filter
        if selected_display_names:
            filtered_df = filtered_df[
                filtered_df["display_name"].isin(selected_display_names)
            ]

        # Apply phone number filter
        if selected_phone_numbers:
            filtered_df = filtered_df[
                filtered_df["formatted_phone"].isin(selected_phone_numbers)
            ]

        # Apply expected name filter
        if selected_expected_names:
            # Check both 'Nome' and 'expected_name' columns
            if "Nome" in filtered_df.columns:
                filtered_df = filtered_df[
                    filtered_df["Nome"].isin(selected_expected_names)
                ]
            elif "expected_name" in filtered_df.columns:
                filtered_df = filtered_df[
                    filtered_df["expected_name"].isin(selected_expected_names)
                ]

        # Apply CPF filter
        if selected_cpfs:
            # Look for CPF in various columns and match formatted values
            cpf_matched = False
            for col in ["CPF", "cpf", "documento", "Documento"]:
                if col in filtered_df.columns and not cpf_matched:
                    # Create formatted CPF column for matching
                    def format_cpf_for_match(cpf_val):
                        if pd.isna(cpf_val):
                            return ""
                        clean_cpf = "".join(filter(str.isdigit, str(cpf_val)))
                        if len(clean_cpf) == 11:
                            return f"{clean_cpf[:3]}.{clean_cpf[3:6]}.{clean_cpf[6:9]}-{clean_cpf[9:]}"
                        return ""

                    filtered_df["temp_formatted_cpf"] = filtered_df[col].apply(
                        format_cpf_for_match
                    )
                    filtered_df = filtered_df[
                        filtered_df["temp_formatted_cpf"].isin(selected_cpfs)
                    ]
                    filtered_df = filtered_df.drop("temp_formatted_cpf", axis=1)
                    cpf_matched = True

        # Apply bairro filter
        if selected_bairros and "endereco_bairro" in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df["endereco_bairro"].isin(selected_bairros)
            ]

        # Apply classificacao filter
        if (
            selected_classificacoes
            and "Classificação do dono do número" in filtered_df.columns
        ):
            filtered_df = filtered_df[
                filtered_df["Classificação do dono do número"].isin(
                    selected_classificacoes
                )
            ]

        # Apply status filter
        if selected_statuses:
            status_col = (
                "status" if "status" in filtered_df.columns else "status_manual"
            )
            if status_col in filtered_df.columns:
                filtered_df = filtered_df[
                    filtered_df[status_col].isin(selected_statuses)
                ]

        # Apply endereco filter
        if selected_enderecos and "endereco" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["endereco"].isin(selected_enderecos)]

        # Apply complemento filter
        if selected_complementos and "endereco_complemento" in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df["endereco_complemento"].isin(selected_complementos)
            ]

        # Apply conversation state filters
        if only_unarchived and "archived" in filtered_df.columns:
            before_count = len(filtered_df)
            # Show only unarchived conversations (archived = False or 0)
            filtered_df = filtered_df[filtered_df["archived"].isin([False, 0])]
            if DEBUG:
                st.sidebar.write(
                    f"Unarchived filter: {before_count} → {len(filtered_df)}"
                )

        if only_unread and "unread_count" in filtered_df.columns:
            before_count = len(filtered_df)
            # Show only conversations with unread messages (unread_count > 0)
            filtered_df = filtered_df[filtered_df["unread_count"] > 0]
            if DEBUG:
                st.sidebar.write(f"Unread filter: {before_count} → {len(filtered_df)}")
        
        # Apply property status filter
        if property_status_filter and len(property_status_filter) > 0:
            before_count = len(filtered_df)
            
            # Check if we need to filter by property completeness
            if len(property_status_filter) == 1:  # Only one option selected
                selected_option = property_status_filter[0]
                
                if selected_option == "Sim":
                    # Show only conversations with both endereco_bairro and endereco filled
                    if "endereco_bairro" in filtered_df.columns and "endereco" in filtered_df.columns:
                        filtered_df = filtered_df[
                            (filtered_df["endereco_bairro"].notna()) & 
                            (filtered_df["endereco_bairro"] != "") & 
                            (filtered_df["endereco"].notna()) & 
                            (filtered_df["endereco"] != "")
                        ]
                        if DEBUG:
                            st.sidebar.write(f"Property Status 'Sim' filter: {before_count} → {len(filtered_df)}")
                    else:
                        if DEBUG:
                            st.sidebar.write(f"⚠️ Property Status 'Sim' filter SKIPPED - Missing columns: endereco_bairro={('endereco_bairro' in filtered_df.columns)}, endereco={('endereco' in filtered_df.columns)}")
                        
                elif selected_option == "Não":
                    # Show only conversations with either endereco_bairro or endereco blank/empty OR BOTH blank/empty
                    if "endereco_bairro" in filtered_df.columns and "endereco" in filtered_df.columns:
                        filtered_df = filtered_df[
                            (filtered_df["endereco_bairro"].isna()) | 
                            (filtered_df["endereco_bairro"] == "") | 
                            (filtered_df["endereco"].isna()) | 
                            (filtered_df["endereco"] == "")
                        ]
                        if DEBUG:
                            st.sidebar.write(f"Property Status 'Não' filter: {before_count} → {len(filtered_df)}")
                    else:
                        if DEBUG:
                            st.sidebar.write(f"⚠️ Property Status 'Não' filter SKIPPED - Missing columns: endereco_bairro={('endereco_bairro' in filtered_df.columns)}, endereco={('endereco' in filtered_df.columns)}")
            
            # If both options are selected, show all conversations (no filtering)
            # This is the default behavior when both or neither options are selected
            if DEBUG:
                st.sidebar.write(f"Property status filter applied: {property_status_filter}")
                st.sidebar.write(f"Columns available: endereco_bairro={('endereco_bairro' in filtered_df.columns)}, endereco={('endereco' in filtered_df.columns)}")
            
            if DEBUG:
                st.sidebar.write(f"Property assigned filter: {before_count} → {len(filtered_df)}")

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
        display_columns = ["display_name", "formatted_phone", "total_messages"]
        display_column_names = ["Display Name", "Phone Number", "Messages"]

        # Add expected_name - use 'Nome' from spreadsheet data if available
        if "Nome" in filtered_df.columns:
            display_columns.insert(1, "Nome")
            display_column_names.insert(1, "Expected Name")
        elif "expected_name" in filtered_df.columns:
            display_columns.insert(1, "expected_name")
            display_column_names.insert(1, "Expected Name")

        # Add CPF column if available
        for cpf_col in ["CPF", "cpf", "documento", "Documento"]:
            if cpf_col in filtered_df.columns:
                display_columns.insert(
                    -1, cpf_col
                )  # Insert before the last column (Messages)
                display_column_names.insert(-1, "CPF")
                break

        # Add sheets data columns if they exist
        if "endereco_bairro" in filtered_df.columns:
            display_columns.append("endereco_bairro")
            display_column_names.append("Bairro")

        if "endereco" in filtered_df.columns:
            display_columns.append("endereco")
            display_column_names.append("Endereco")

        if "endereco_complemento" in filtered_df.columns:
            display_columns.append("endereco_complemento")
            display_column_names.append("Complemento")

        if "Classificação do dono do número" in filtered_df.columns:
            display_columns.append("Classificação do dono do número")
            display_column_names.append("Classificacao")

        if "status" in filtered_df.columns:
            display_columns.append("status")
            display_column_names.append("Status")
        elif "status_manual" in filtered_df.columns:
            display_columns.append("status_manual")
            display_column_names.append("Status")

        # Only include columns that exist in the dataframe
        available_columns = [
            col for col in display_columns if col in filtered_df.columns
        ]
        available_names = [
            display_column_names[i]
            for i, col in enumerate(display_columns)
            if col in filtered_df.columns
        ]

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
            selection_mode="single-row",
        )

        # Handle row selection
        if (
            hasattr(event, "selection")
            and event.selection
            and event.selection.get("rows")
        ):
            selected_row_idx = event.selection["rows"][0]
            if selected_row_idx < len(filtered_df):
                selected_conversation_id = filtered_df.iloc[selected_row_idx][
                    "conversation_id"
                ]
                st.session_state.selected_conversation_id = selected_conversation_id

        # Show "Open in Processor" button if a conversation is selected
        if st.session_state.selected_conversation_id:
            selected_conv_matches = filtered_df[
                filtered_df["conversation_id"]
                == st.session_state.selected_conversation_id
            ]
            if not selected_conv_matches.empty:
                selected_conv_info = selected_conv_matches.iloc[0]

                st.success(
                    f"Selected: {selected_conv_info['display_name']} ({selected_conv_info['formatted_phone']})"
                )

                if st.button(
                    "📝 Open in Processor", type="primary", use_container_width=True
                ):
                    # Store the selected conversation data in session state for the Processor page
                    st.session_state.processor_conversation_id = (
                        st.session_state.selected_conversation_id
                    )
                    st.session_state.processor_conversation_data = (
                        selected_conv_info.to_dict()
                    )
                    
                    # ★ NAVIGATION CONTEXT PRESERVATION - Store filtered conversations for processor navigation
                    st.session_state.processor_filtered_conversations = filtered_df.copy()
                    st.session_state.processor_navigation_context = {
                        'total_filtered': len(filtered_df),
                        'conversation_ids': filtered_df['conversation_id'].tolist(),
                        'from_conversations_page': True,
                        'applied_filters': {
                            'display_names': selected_display_names,
                            'phone_numbers': selected_phone_numbers,
                            'expected_names': selected_expected_names,
                            'cpfs': selected_cpfs,
                            'bairros': selected_bairros,
                            'classificacoes': selected_classificacoes,
                            'statuses': selected_statuses,
                            'enderecos': selected_enderecos,
                            'complementos': selected_complementos,
                            'only_unarchived': only_unarchived,
                            'only_unread': only_unread,
                            'property_status_filter': property_status_filter
                        }
                    }

                    # Get conversation_id for URL parameter
                    conversation_id = st.session_state.selected_conversation_id

                    # Store conversation_id for URL update after navigation
                    if conversation_id:
                        st.session_state.pending_conversation_id = conversation_id

                    # Navigate to Processor page
                    st.switch_page("pages/Processor.py")
            else:
                st.warning(
                    "Selected conversation not found in current filter. Please reselect."
                )

        # Show the map for filtered conversations
        if len(filtered_df) > 0:
            st.markdown("---")

            # Add toggle for map loading
            col1, col2 = st.columns([3, 1])
            with col1:
                pass  # Removed title as requested

            with col2:
                # Use session state to control map loading
                if f"map_loaded_{len(filtered_df)}" not in st.session_state:
                    st.session_state[f"map_loaded_{len(filtered_df)}"] = False

                if not st.session_state[f"map_loaded_{len(filtered_df)}"]:
                    load_map = st.button(
                        "🗺️ Carregar Mapa",
                        type="primary",
                        help=f"Carregar mapa com propriedades dos contatos filtrados (máx {200 if len(filtered_df) > 200 else len(filtered_df)} contatos)",
                    )
                    if load_map:
                        st.session_state[f"map_loaded_{len(filtered_df)}"] = True
                        st.rerun()
                else:
                    # Map is loaded, show reset button
                    if st.button("🔄 Recarregar Mapa", type="secondary"):
                        st.session_state[f"map_loaded_{len(filtered_df)}"] = False
                        st.rerun()

            # Show map if loaded
            if st.session_state.get(f"map_loaded_{len(filtered_df)}", False):
                # Pass debug mode to session state for map debugging
                st.session_state.debug_mode = DEBUG
                show_filtered_conversations_map(filtered_df)

except Exception as e:
    st.error(f"Error loading conversations: {str(e)}")
    st.info(
        "Please check if the database is available and contains the conversations table."
    )

# ─── BACKGROUND OPERATIONS SIDEBAR ─────────────────────────────────────────
# Sync global background operations to session state for UI updates
try:
    import time
    from services.background_operations import global_storage, get_running_operations, render_operations_sidebar
    
    global_storage.sync_to_session_state()
    
    # Auto-refresh with rate limiting if there are running operations
    running_ops = get_running_operations()
    if running_ops:
        # Rate-limited refresh: only refresh every few seconds when operations are running
        current_time = time.time()
        last_refresh_key = "last_bg_ops_refresh_conversations"
        
        if last_refresh_key not in st.session_state:
            st.session_state[last_refresh_key] = 0
        
        # Refresh every 3 seconds when operations are running
        if current_time - st.session_state[last_refresh_key] > 3.0:
            st.session_state[last_refresh_key] = current_time
            st.rerun()
        
except Exception as e:
    if DEBUG:
        st.sidebar.error(f"Error syncing background operations: {e}")

# Render background operations status in sidebar
try:
    render_operations_sidebar()
except Exception as e:
    st.sidebar.error(f"Error displaying operations status: {e}")
