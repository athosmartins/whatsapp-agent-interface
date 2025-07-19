"""
config.py

Application constants for the WhatsApp Agent Streamlit interface:
– classification and intent options
– preset responses
– standby reasons
– Urb.Link status options
– Hex API configuration
"""

import os

# Hex API Configuration
HEX_API_BASE_URL = 'https://app.hex.tech/api/v1'
HEX_PROJECT_ID_PBH = 'fee83bcb-80e8-4b56-9a06-ebf71e0e902e'
HEX_PROJECT_ID_LOCALIZE = 'e8e789bf-4844-4301-b16c-631a582ad416'
HEX_PROJECT_ID_MAIS_TELEFONES = '01964feb-a827-7008-b68d-940e0cd7ae9a'
HEX_PROJECT_ID_PESSOAS_REFERENCIA = '0196504b-b2ca-7009-9b52-4e946809e803'
HEX_PROJECT_ID_VOXUY = '019659de-7608-7111-9a73-5611ede14b10'

# Try to get API token from multiple sources (in order of preference)
def get_hex_api_token():
    # 1. Environment variable (most secure)
    token = os.getenv('HEX_API_SECRET')
    if token:
        return token
    
    # 2. Streamlit secrets (for Streamlit Cloud)
    try:
        import streamlit as st
        if hasattr(st, 'secrets'):
            # Try direct access first
            if 'HEX_API_SECRET' in st.secrets:
                return st.secrets['HEX_API_SECRET']
            # Try accessing from [hex] section
            if 'hex' in st.secrets and 'HEX_API_SECRET' in st.secrets['hex']:
                return st.secrets['hex']['HEX_API_SECRET']
    except:
        pass
    
    # 3. Local secrets file (fallback for development)
    try:
        import json
        with open('.secrets.json', 'r') as f:
            secrets = json.load(f)
            return secrets.get('HEX_API_SECRET', '')
    except:
        pass
    
    return ''

HEX_API_TOKEN = get_hex_api_token()

CLASSIFICACAO_OPTS = [
    "Proprietário",
    "Herdeiro / Futuro herdeiro",
    "Parente / Conhecido",
    "Ex-proprietário",
    "Sem relação com imóvel",
    "Não identificado",
]

INTENCAO_OPTS = [
    "Aberto a Proposta",
    "Aberto a Proposta, outros não",
    "Não receptivo a venda",
    "Pretende vender no futuro",
    "Vendido para Construtora",
    "Está em negociação (FUP 30d)",
    "Passou contato stakeholder",
    "Sem contato",
    "Entendendo situação",
    "N/A",
]

ACOES_OPTS = [
    "Monitorar CPF",
    "Monitorar CPF (do familiar)",
    "Monitorar CPF (do vizinho)",
    "Falha ao entrar em contato",
    "Falha ao entrar em contato com vizinho",
    "Em estudo",
    "Não faz sentido mandar",
]

PAGAMENTO_OPTS = [
    "Dinheiro",
    "Permuta no local",
    "Permuta fora",
    "Permuta pronta",
]

PERCEPCAO_OPTS = [
    "Ótimo",
    "Bom",
    "Alto",
    "Inviável",
]

PRESET_RESPONSES = {
    "": "",
    "Número errado": (
        "Ah sim, mil desculpas pelo engano! Tenha um ótimo dia 😊"
    ),
    "Não quero vender": (
        "Entendido! Obrigado pelo retorno, e tenha um ótimo dia 😊"
    ),
    "Como conseguiu contato?": (
        "Nós usamos algumas ferramentas de inteligência de mercado, "
        "como Serasa, que nos informam possíveis contatos de proprietários "
        "de imóveis em nossa região de interesse."
    ),
    "Falar com vizinhos": (
        "Certo!\n\n"
        "Como próximo passo, irei verificar se os vizinhos também têm interesse.\n\n"
        "Superada esta etapa, estaremos prontos para te fazer uma proposta."
    ),
    "Despedida": (
        "Entrarei em contato em breve caso surjam novas oportunidades."
    ),
}

STANDBY_REASONS = [
    "Valor",
    "Pagamento",
    "Tamanho",
    "Formato",
    "Ponto",
    "Inviável para prédio",
    "Situação complexa",
    "Falta de registro",
    "Só negociam junto com vizinho",
    "Mensagem errada",
]

STATUS_URBLINK_OPTS = [
    "Em negociação",
    "Sendo ignorado",
    "Aguardando",
    "Estudando",
    "Procurando vizinhos",
]
