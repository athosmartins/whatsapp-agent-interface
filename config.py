"""
config.py

Application constants for the WhatsApp Agent Streamlit interface:
– classification and intent options
– preset responses
– standby reasons
– Urb.Link status options
"""

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
