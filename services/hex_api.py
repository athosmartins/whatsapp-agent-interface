"""
Hex API integration service for sending filtered datasets to Hex notebooks.
"""

try:
    import requests
except ImportError:
    requests = None

import streamlit as st
from config import (
    HEX_API_BASE_URL, 
    HEX_PROJECT_ID_PBH, 
    HEX_PROJECT_ID_LOCALIZE, 
    HEX_PROJECT_ID_MAIS_TELEFONES,
    HEX_PROJECT_ID_PESSOAS_REFERENCIA,
    HEX_PROJECT_ID_VOXUY,
    HEX_API_TOKEN
)


def apply_hex_button_style():
    """Apply red styling to Hex integration buttons."""
    button_style = """
    <style>
    .hex-button > button {
        width: 100% !important;
        background-color: #F8D7DA !important;
        color: #721C24 !important;
        border: 1px solid #F5C6CB !important;
        border-radius: 6px !important;
        padding: 0.5rem 1rem !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        text-align: center !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        min-height: 38px !important;
    }
    .hex-button > button:hover {
        background-color: #F5C6CB !important;
        border-color: #F1B0B7 !important;
    }
    .hex-button > button:active {
        background-color: #F1B0B7 !important;
        border-color: #EC999F !important;
    }
    </style>
    """
    st.markdown(button_style, unsafe_allow_html=True)


# Hex integration configurations
HEX_CONFIGS = {
    "descobrir_proprietario": {
        "project_id": HEX_PROJECT_ID_PBH,
        "input_params": lambda df: {"pbh_input": df.to_csv(index=False)},
        "button_text": "👱 Descobrir Proprietário [PBH]",
        "success_message": "Descobrir Proprietário",
        "session_key": "hex_last_run_url",
        "link_text": "Abrir no Hex"
    },
    "consultar_localize": {
        "project_id": HEX_PROJECT_ID_LOCALIZE,
        "input_params": lambda df: {"assertiva_input": df.to_csv(index=False)},
        "button_text": "📍 Localize [Assertiva]",
        "success_message": "Localize",
        "session_key": "localize_last_run_url",
        "link_text": "Abrir Localize no Hex"
    },
    "mais_telefones": {
        "project_id": HEX_PROJECT_ID_MAIS_TELEFONES,
        "input_params": lambda df: {"assertiva_input": df.to_csv(index=False)},
        "button_text": "📞 Mais Telefones [Assertiva]",
        "success_message": "Mais Telefones",
        "session_key": "mais_telefones_last_run_url",
        "link_text": "Abrir Mais Telefones no Hex"
    },
    "pessoas_referencia": {
        "project_id": HEX_PROJECT_ID_PESSOAS_REFERENCIA,
        "input_params": lambda df: {"production_input": df.to_csv(index=False)},
        "button_text": "👥 Pessoas de Referência [Assertiva]",
        "success_message": "Pessoas de Referência",
        "session_key": "pessoas_referencia_last_run_url",
        "link_text": "Abrir Pessoas de Referência no Hex"
    },
    "mandar_voxuy": {
        "project_id": HEX_PROJECT_ID_VOXUY,
        "input_params": lambda df, funnel="mega_data_set": {
            "production_input_dataframe": df.to_csv(index=False),
            "funnel": funnel
        },
        "button_text": "📱 Mandar Whatsapp [Voxuy]",
        "success_message": "Voxuy",
        "session_key": "voxuy_last_run_url",
        "link_text": "Abrir Voxuy no Hex"
    }
}


def render_hex_dropdown_interface(filtered_df, **kwargs):
    """
    Render a simplified Hex interface with dropdown + single execute button.
    
    Args:
        filtered_df: The DataFrame to send
        **kwargs: Additional parameters (e.g., funnel for Voxuy)
    """
    if not requests:
        st.warning("⚠️ Biblioteca 'requests' não disponível. Execute: pip install requests")
        return
    
    if not HEX_API_TOKEN:
        st.warning("⚠️ HEX_API_SECRET não configurado. Configure a variável de ambiente para habilitar integração com Hex.")
        return
    
    if filtered_df.empty:
        st.warning("⚠️ Nenhum dado filtrado para enviar.")
        return
    
    # Create dropdown with options
    options = list(HEX_CONFIGS.keys())
    option_labels = [HEX_CONFIGS[key]["button_text"] for key in options]
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        selected_idx = st.selectbox(
            "Selecione a ação:",
            options=range(len(options)),
            format_func=lambda x: option_labels[x],
            key="hex_action_selector"
        )
        selected_action = options[selected_idx]
    
    with col2:
        # Apply styling for execute button
        apply_hex_button_style()
        st.markdown('<div class="hex-button">', unsafe_allow_html=True)
        
        if st.button("🚀 Executar Ação"):
            config = HEX_CONFIGS[selected_action]
            
            with st.spinner(f"Enviando dados para {config['success_message']}..."):
                # Generate input parameters
                if selected_action == "mandar_voxuy":
                    input_params = config["input_params"](filtered_df, **kwargs)
                else:
                    input_params = config["input_params"](filtered_df)
                
                result = send_dataframe_to_hex_with_params(
                    filtered_df,
                    project_id=config["project_id"],
                    input_params=input_params
                )
                
                if result["success"]:
                    st.success(f"✅ Dados enviados com sucesso para {config['success_message']}!")
                    st.info(f"🔗 Visualizar execução: {result['run_url']}")
                    
                    # Store in session state
                    st.session_state[config["session_key"]] = result["run_url"]
                    
                    # Show clickable link
                    st.markdown(f"[🚀 {config['link_text']}]({result['run_url']})")
                    
                else:
                    st.error(f"❌ Erro ao enviar dados para {config['success_message']}: {result['error']}")
                    
                    # Show debug info if available
                    if st.session_state.get('debug_mode', False):
                        st.write("**Debug Info:**")
                        st.write(f"- Project ID: {config['project_id']}")
                        st.write(f"- DataFrame shape: {filtered_df.shape}")
                        st.write(f"- API Token configured: {bool(HEX_API_TOKEN)}")
                        if kwargs:
                            st.write(f"- Additional params: {kwargs}")
        
        st.markdown('</div>', unsafe_allow_html=True)

def render_hex_button(filtered_df, config_key, **kwargs):
    """
    Generic function to render any Hex integration button.
    
    Args:
        filtered_df: The DataFrame to send
        config_key: Key from HEX_CONFIGS
        **kwargs: Additional parameters (e.g., funnel for Voxuy)
    """
    if not requests:
        st.warning("⚠️ Biblioteca 'requests' não disponível. Execute: pip install requests")
        return
    
    if not HEX_API_TOKEN:
        st.warning("⚠️ HEX_API_SECRET não configurado. Configure a variável de ambiente para habilitar integração com Hex.")
        return
    
    if filtered_df.empty:
        st.warning("⚠️ Nenhum dado filtrado para enviar.")
        return
    
    config = HEX_CONFIGS[config_key]
    
    # Apply styling and render button with custom CSS class
    apply_hex_button_style()
    
    # Use custom CSS class for this button
    st.markdown('<div class="hex-button">', unsafe_allow_html=True)
    
    if st.button(config["button_text"]):
        with st.spinner(f"Enviando dados para {config['success_message']}..."):
            # Generate input parameters
            if config_key == "mandar_voxuy":
                input_params = config["input_params"](filtered_df, **kwargs)
            else:
                input_params = config["input_params"](filtered_df)
            
            result = send_dataframe_to_hex_with_params(
                filtered_df,
                project_id=config["project_id"],
                input_params=input_params
            )
            
            if result["success"]:
                st.success(f"✅ Dados enviados com sucesso para {config['success_message']}!")
                st.info(f"🔗 Visualizar execução: {result['run_url']}")
                
                # Store in session state
                st.session_state[config["session_key"]] = result["run_url"]
                
                # Show clickable link
                st.markdown(f"[🚀 {config['link_text']}]({result['run_url']})")
                
            else:
                st.error(f"❌ Erro ao enviar dados para {config['success_message']}: {result['error']}")
                
                # Show debug info if available
                if st.session_state.get('debug_mode', False):
                    st.write("**Debug Info:**")
                    st.write(f"- Project ID: {config['project_id']}")
                    st.write(f"- DataFrame shape: {filtered_df.shape}")
                    st.write(f"- API Token configured: {bool(HEX_API_TOKEN)}")
                    if kwargs:
                        st.write(f"- Additional params: {kwargs}")
    
    st.markdown('</div>', unsafe_allow_html=True)


def send_dataframe_to_hex(df, project_id=HEX_PROJECT_ID_PBH, input_param_name="pbh_input"):
    """
    Send a DataFrame to a Hex project as a CSV string.
    
    Args:
        df: pandas DataFrame to send
        project_id: Hex project ID
        input_param_name: Name of the input parameter in the Hex notebook
    
    Returns:
        dict: Response with success status, run_url, and any error messages
    """
    if not requests:
        return {
            "success": False,
            "error": "requests library not available. Install with: pip install requests",
            "run_url": None
        }
    
    if not HEX_API_TOKEN:
        return {
            "success": False,
            "error": "HEX_API_SECRET environment variable not set",
            "run_url": None
        }
    
    if df.empty:
        return {
            "success": False,
            "error": "DataFrame is empty",
            "run_url": None
        }
    
    try:
        # Convert DataFrame to CSV string
        csv_string = df.to_csv(index=False)
        
        # Prepare the input parameters
        inputs = {
            "inputParams": {
                input_param_name: csv_string
            }
        }
        
        # Make the API request
        response = requests.post(
            url=f"{HEX_API_BASE_URL}/projects/{project_id}/runs",
            headers={
                "Authorization": f"Bearer {HEX_API_TOKEN}",
            },
            json=inputs
        )
        
        if response.status_code == 201:
            run_data = response.json()
            run_url = run_data.get("runUrl", "")
            
            return {
                "success": True,
                "run_url": run_url,
                "error": None,
                "response_data": run_data
            }
        else:
            return {
                "success": False,
                "error": f"API request failed with status {response.status_code}: {response.text}",
                "run_url": None
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Exception occurred: {str(e)}",
            "run_url": None
        }


def send_dataframe_to_hex_with_params(df, project_id, input_params):
    """
    Send a DataFrame to a Hex project with custom input parameters.
    
    Args:
        df: pandas DataFrame to send
        project_id: Hex project ID
        input_params: Dictionary of input parameters for the Hex notebook
    
    Returns:
        dict: Response with success status, run_url, and any error messages
    """
    if not requests:
        return {
            "success": False,
            "error": "requests library not available. Install with: pip install requests",
            "run_url": None
        }
    
    if not HEX_API_TOKEN:
        return {
            "success": False,
            "error": "HEX_API_SECRET environment variable not set",
            "run_url": None
        }
    
    if df.empty:
        return {
            "success": False,
            "error": "DataFrame is empty",
            "run_url": None
        }
    
    try:
        # Make the API request
        response = requests.post(
            url=f"{HEX_API_BASE_URL}/projects/{project_id}/runs",
            headers={
                "Authorization": f"Bearer {HEX_API_TOKEN}",
            },
            json={"inputParams": input_params}
        )
        
        if response.status_code == 201:
            run_data = response.json()
            run_url = run_data.get("runUrl", "")
            
            return {
                "success": True,
                "run_url": run_url,
                "error": None,
                "response_data": run_data
            }
        else:
            return {
                "success": False,
                "error": f"API request failed with status {response.status_code}: {response.text}",
                "run_url": None
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Exception occurred: {str(e)}",
            "run_url": None
        }


# Legacy functions removed - now using render_hex_button() with configuration