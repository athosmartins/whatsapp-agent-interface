import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import re
from typing import List, Dict

# Page configuration
st.set_page_config(
    page_title="WhatsApp Agent Interface",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for the interface
st.markdown("""
<style>
    /* Hide Streamlit default elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Custom styling */
    .stApp {
        background-color: #ffffff;
    }
    
    /* Compact contact info */
    .contact-container {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e6e9ef;
        margin-bottom: 1rem;
    }
    
    /* Chat container styling */
    .chat-container {
        background: #fafafa;
        border: 1px solid #e6e9ef;
        border-radius: 8px;
        padding: 1rem;
        max-height: 600px;
        overflow-y: auto;
        margin-bottom: 1rem;
    }
    
    /* Chat messages */
    .agent-message {
        background: #f0f0f0;
        padding: 0.75rem 1rem;
        border-radius: 1rem;
        margin: 0.5rem 0;
        margin-left: 25%;
        text-align: left;
    }
    
    .contact-message {
        background: #2196f3;
        color: white;
        padding: 0.75rem 1rem;
        border-radius: 1rem;
        margin: 0.5rem 0;
        margin-right: 25%;
        text-align: left;
    }
    
    .timestamp {
        font-size: 0.7rem;
        color: #999;
        margin-top: 0.25rem;
        text-align: right;
    }
    
    .contact-message .timestamp {
        text-align: left;
        color: #e3f2fd;
    }
    
    /* Highlighted names */
    .highlighted {
        background-color: #e3f2fd;
        padding: 2px 4px;
        border-radius: 3px;
    }
    
    /* Yellow reason box */
    .reason-box {
        background: #fff3cd;
        border: 1px solid #ffc107;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
        color: #856404;
    }
    
    /* Family cards */
    .family-card {
        background: white;
        padding: 0.5rem;
        border-radius: 4px;
        border: 1px solid #e6e9ef;
        margin: 0.25rem;
        font-size: 0.85rem;
        line-height: 1.3;
    }
</style>
""", unsafe_allow_html=True)

# Sample data - replace with your DataFrame loading
@st.cache_data
def load_sample_data():
    return pd.DataFrame([
        {
            "PictureUrl": "https://pps.whatsapp.net/v/t61.24694-24/463235056_1330030495039995_619627974121266174_n.jpg?stp=dst-jpg_s96x96_tt6&ccb=11-4&oh=01_Q5AaIZF-x4gNH-mhwJCbngLEUonP6YXUV463-oxBMhwFS09x&oe=67FF698A&_nc_sid=5e03e0&_nc_cat=109",
            "last_message_timestamp": "2024-08-14 7:48:24",
            "whatsapp_number": "+5531994716770",
            "display_name": "Lili",
            "expected_name": "Liliane Figueiredo Teixeira",
            "familiares": "MARIA DE LOURDES AGUIAR (MAE), FRANCISCO AGUIAR DA SILVA (IRMAO), IDELBRANDO LUIZ AGUIAR (IRMAO), LEANDRO ALVARENGA AGUIAR (FILHO), MARCIO EVANDRO DE AGUIAR (IRMAO(A)), RENATO CESAR DE AGUIAR (IRMAO(A)), FUNDACAO MINEIRA DE EDUCACAO E CULTURA (EMPREGADOR), MARCIO JOSE DE AGUIAR (SOCIO(A))",
            "conversation_history": "[2024-08-14 05:39:29] (Urb.Link): Oi, Liliane! | [2024-08-14 05:39:46] (Urb.Link): Meu nome é Athos, prazer em falar contigo. | [2024-08-14 05:40:01] (Urb.Link): Estou no mercado imobiliário há quase 20 anos e ajudo proprietários de imóveis na Zona Sul a vender suas imóveis para construtoras pelo melhor valor possível. | [2024-08-14 05:40:13] (Urb.Link): Tenho bom relacionamento com *sócios de mais de 30 construtoras em busca de terrenos no Carmo.* | [2024-08-14 05:40:33] (Urb.Link): Seu imóvel na *Rua Caldas 143* está no perfil que muitas destas empresas buscam. | [2024-08-14 05:40:54] (Urb.Link): Você teria interesse que eu *apresente seu imóvel para algumas destas empresas e traga propostas para você?* | [2024-08-14 07:25:02] (Contato): Quem te deu esse número? | [2024-08-14 07:25:13] (Contato): Bom dia | [2024-08-14 07:48:24] (Urb.Link): Bom dia! Usamos uma ferramenta de inteligência de mercado que identifica os proprietários de determinado imóvel e seus possíveis contato.",
            "classificacao": "Não identificado",
            "intencao": "N/A",
            "resposta": "Nós usamos algumas ferramentas de inteligência de mercado, como Serasa, que nos informam possíveis contatos de proprietários de imóveis.",
            "Razao": "O nome no WhatsApp está vazio e a resposta questiona como o número foi obtido, indicando que não há relação clara com o imóvel ou o nome esperado (Liliane Figueiredo Teixeira).",
            "pagamento": "",
            "percepcao_valor_esperado": "",
            "imovel_em_inventario": False,
            "IMOVEIS": [{'INDICE CADASTRAL': '103003A030A0010', 'BAIRRO': 'Gutierrez', 'ENDERECO': 'Rua Bernardino De Lima 29', 'COMPLEMENTO ENDERECO': None, 'TIPO CONSTRUTIVO': 'Casa', 'ANO CONSTRUCAO': 1979.0, 'AREA CONSTRUCAO': 319.49, 'AREA TERRENO': 375.0, 'FRACAO IDEAL': 1.0}],
            "IDADE": 72,
            "OBITO_PROVAVEL": True
        },
        {
            "PictureUrl": "",
            "last_message_timestamp": "2024-08-15 10:30:00",
            "whatsapp_number": "+5531987654321",
            "display_name": "João Silva",
            "expected_name": "João Carlos Silva",
            "familiares": "MARIA SILVA (ESPOSA), PEDRO SILVA (FILHO)",
            "conversation_history": "[2024-08-15 10:00:00] (Urb.Link): Boa tarde! Falo com João? | [2024-08-15 10:05:00] (Contato): Sim, sou eu. Do que se trata? | [2024-08-15 10:10:00] (Urb.Link): Estamos interessados em fazer uma proposta para seu imóvel | [2024-08-15 10:30:00] (Contato): Que interessante! Pode me falar mais?",
            "classificacao": "Proprietário",
            "intencao": "Aberto a Proposta",
            "resposta": "Perfeito! Vou agendar uma visita para apresentarmos nossa proposta pessoalmente.",
            "Razao": "Nome confere, demonstrou interesse imediato na proposta.",
            "pagamento": "Dinheiro,Permuta no local",
            "percepcao_valor_esperado": "Bom",
            "imovel_em_inventario": False,
            "IMOVEIS": [{'INDICE CADASTRAL': '103003A030A0011', 'BAIRRO': 'Centro', 'ENDERECO': 'Rua Principal 123', 'TIPO CONSTRUTIVO': 'Apartamento', 'ANO CONSTRUCAO': 1990.0, 'AREA CONSTRUCAO': 85.0, 'AREA TERRENO': 0.0, 'FRACAO IDEAL': 0.05}],
            "IDADE": 45,
            "OBITO_PROVAVEL": False
        }
    ])

# Helper functions
def format_timestamp(timestamp_str):
    """Convert timestamp to Brazilian format"""
    try:
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        weekdays = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
        months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
        
        return f"{dt.day:02d}/{months[dt.month-1]}/{dt.year} ({weekdays[dt.weekday()]}) {dt.hour:02d}h{dt.minute:02d}m{dt.second:02d}s"
    except:
        return timestamp_str

def highlight_similar_names(text, names_to_highlight):
    """Highlight similar names in text"""
    if not text:
        return text
    
    highlighted_text = text
    for name in names_to_highlight:
        if len(name) > 2:  # Only highlight names longer than 2 characters
            pattern = re.compile(re.escape(name), re.IGNORECASE)
            highlighted_text = pattern.sub(f'<span class="highlighted">{name}</span>', highlighted_text)
    
    return highlighted_text

def parse_familiares(familiares_str):
    """Parse familiares string into organized groups"""
    if not familiares_str:
        return {}
    
    familiares_list = familiares_str.split(', ')
    familiares_dict = {}
    
    for familiar in familiares_list:
        if '(' in familiar and ')' in familiar:
            name = familiar.split('(')[0].strip().title()
            relationship = familiar.split('(')[1].replace(')', '').strip().title()
            
            if relationship not in familiares_dict:
                familiares_dict[relationship] = []
            familiares_dict[relationship].append(name)
    
    return familiares_dict

def parse_conversation(conversation_str):
    """Parse conversation history into messages"""
    if not conversation_str:
        return []
    
    messages = []
    parts = conversation_str.split(' | ')
    
    for part in parts:
        if '] (' in part:
            timestamp_end = part.find('] (')
            timestamp = part[1:timestamp_end]
            
            sender_start = part.find('(') + 1
            sender_end = part.find('):')
            sender = part[sender_start:sender_end]
            
            message = part[sender_end + 2:].strip()
            
            messages.append({
                'timestamp': timestamp,
                'sender': sender,
                'message': message
            })
    
    return messages

# Initialize session state
if 'current_index' not in st.session_state:
    st.session_state.current_index = 0
if 'df' not in st.session_state:
    st.session_state.df = load_sample_data()

# Load current record
df = st.session_state.df
total_records = len(df)

# Ensure current_index is within bounds
if st.session_state.current_index >= total_records:
    st.session_state.current_index = 0

current_row = df.iloc[st.session_state.current_index]

# Title and Progress
st.title("📱 WhatsApp Agent Interface")

# Progress bar
progress_value = (st.session_state.current_index + 1) / total_records
st.progress(progress_value, text=f"Processando: {st.session_state.current_index + 1} de {total_records} mensagens")

st.markdown("---")

# Contact Information Section
st.subheader("👤 Informações de Contato")

# Get names for highlighting
display_name = current_row['display_name'] or ""
expected_name = current_row['expected_name'] or ""
all_names = [display_name, expected_name]

# Add individual words from names for highlighting
for name in all_names:
    if name:
        all_names.extend(name.split())

# Clean and deduplicate names
names_to_highlight = list(set([name.strip() for name in all_names if name and len(name) > 2]))

col1, col2, col3, col4, col5 = st.columns([1, 2, 2, 4, 2])

with col1:
    # Profile picture
    if current_row['PictureUrl']:
        st.image(current_row['PictureUrl'], width=80)
    else:
        st.markdown("👤")

with col2:
    st.markdown("**Nome no WhatsApp**")
    highlighted_display = highlight_similar_names(display_name, names_to_highlight)
    st.markdown(highlighted_display, unsafe_allow_html=True)

with col3:
    st.markdown("**Nome Esperado**")
    highlighted_expected = highlight_similar_names(expected_name, names_to_highlight)
    st.markdown(highlighted_expected, unsafe_allow_html=True)

with col4:
    st.markdown("**Familiares**")
    familiares_dict = parse_familiares(current_row['familiares'])
    
    # Display familiares in compact cards
    for relationship, names in familiares_dict.items():
        names_str = ", ".join(names)
        highlighted_names = highlight_similar_names(names_str, names_to_highlight)
        st.markdown(f'<div class="family-card"><strong>{relationship}:</strong> {highlighted_names}</div>', unsafe_allow_html=True)

with col5:
    # Age and óbito status
    if pd.notna(current_row['IDADE']):
        idade = int(current_row['IDADE'])
        st.markdown(f"**{idade} anos**")
        
        if current_row['OBITO_PROVAVEL']:
            st.markdown("✝︎ Provável Óbito")
        else:
            st.markdown("🌟 Provável vivo")
    
    # Imóveis button
    if st.button("🏢 Imóveis"):
        with st.expander("Informações dos Imóveis", expanded=True):
            if current_row['IMOVEIS']:
                for i, imovel in enumerate(current_row['IMOVEIS']):
                    st.markdown(f"**Imóvel {i+1}**")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.write(f"**Bairro:** {imovel.get('BAIRRO', 'N/A')}")
                        st.write(f"**Endereço:** {imovel.get('ENDERECO', 'N/A')}")
                        st.write(f"**Tipo:** {imovel.get('TIPO CONSTRUTIVO', 'N/A')}")
                        st.write(f"**Ano:** {imovel.get('ANO CONSTRUCAO', 'N/A')}")
                    with col_b:
                        st.write(f"**Área Construção:** {imovel.get('AREA CONSTRUCAO', 'N/A')} m²")
                        st.write(f"**Área Terreno:** {imovel.get('AREA TERRENO', 'N/A')} m²")
                        st.write(f"**Fração Ideal:** {imovel.get('FRACAO IDEAL', 'N/A')}")
                        st.write(f"**Índice Cadastral:** {imovel.get('INDICE CADASTRAL', 'N/A')}")

st.markdown("---")

# Conversation History
st.subheader("💬 Histórico da Conversa")

messages = parse_conversation(current_row['conversation_history'])

chat_html = '<div class="chat-container">'
for msg in messages:
    formatted_timestamp = format_timestamp(msg['timestamp'])
    
    if msg['sender'] in ['Urb.Link', 'Athos']:
        chat_html += f'''
        <div class="agent-message">
            {msg['message']}
            <div class="timestamp">{formatted_timestamp}</div>
        </div>
        '''
    else:
        chat_html += f'''
        <div class="contact-message">
            {msg['message']}
            <div class="timestamp">{formatted_timestamp}</div>
        </div>
        '''

chat_html += '</div>'
st.markdown(chat_html, unsafe_allow_html=True)

st.markdown("---")

# Razão Section (before classification)
st.subheader("📋 Razão")
st.markdown(f'<div class="reason-box">{current_row["Razao"]}</div>', unsafe_allow_html=True)

st.markdown("---")

# Form Section
st.subheader("📝 Classificação e Resposta")

with st.form("classification_form"):
    col1, col2 = st.columns(2)
    
    with col1:
        # Classificação
        classificacao_options = [
            'Proprietário', 'Herdeiro / Futuro herdeiro', 'Parente / Conhecido',
            'Ex-proprietário', 'Sem relação com imóvel', 'Não identificado'
        ]
        classificacao = st.selectbox(
            "Classificação",
            options=classificacao_options,
            index=classificacao_options.index(current_row['classificacao']) if current_row['classificacao'] in classificacao_options else 0
        )
        
        # Intenção
        intencao_options = [
            'Aberto a Proposta', 'Aberto a Proposta, outros não', 'Não receptivo a venda',
            'Pretende vender no futuro', 'Vendido para Construtora', 'Está em negociação (FUP 30d)',
            'Passou contato stakeholder', 'Sem contato', 'Entendendo situação', 'N/A'
        ]
        intencao = st.selectbox(
            "Intenção",
            options=intencao_options,
            index=intencao_options.index(current_row['intencao']) if current_row['intencao'] in intencao_options else 0
        )
        
        # Conditional fields
        if intencao == "Aberto a Proposta":
            st.markdown("**Campos Adicionais (Aberto a Proposta)**")
            
            # Pagamento checkboxes
            st.markdown("**Pagamento:**")
            current_pagamento = current_row['pagamento'].split(',') if current_row['pagamento'] else []
            
            pagamento_selected = []
            if st.checkbox("Dinheiro", value="Dinheiro" in current_pagamento):
                pagamento_selected.append("Dinheiro")
            if st.checkbox("Permuta no local", value="Permuta no local" in current_pagamento):
                pagamento_selected.append("Permuta no local")
            if st.checkbox("Permuta fora", value="Permuta fora" in current_pagamento):
                pagamento_selected.append("Permuta fora")
            if st.checkbox("Permuta pronta", value="Permuta pronta" in current_pagamento):
                pagamento_selected.append("Permuta pronta")
            
            # Percepção Valor Esperado
            percepcao_options = ["", "Ótimo", "Bom", "Alto", "Inviável"]
            percepcao_index = 0
            if current_row['percepcao_valor_esperado'] in percepcao_options:
                percepcao_index = percepcao_options.index(current_row['percepcao_valor_esperado'])
            
            percepcao_valor = st.selectbox(
                "Percepção Valor Esperado",
                options=percepcao_options,
                index=percepcao_index,
                format_func=lambda x: "-- Selecione --" if x == "" else x
            )
            
            # Imóvel em Inventário
            imovel_inventario = st.checkbox(
                "Imóvel em Inventário",
                value=current_row['imovel_em_inventario']
            )
    
    with col2:
        # Response management
        st.markdown("**Resposta**")
        
        # Response controls
        col_preset, col_clear, col_schedule = st.columns([2, 1, 1])
        
        with col_preset:
            preset_responses = {
                "": "Selecione uma resposta pronta",
                "obrigado": "Muito obrigado pelo retorno! Tenha um ótimo dia 😊",
                "inteligencia": "Nós usamos algumas ferramentas de inteligência de mercado, como Serasa, que nos informam possíveis contatos de proprietários de imóveis.",
                "proposta": "Gostaria de apresentar uma proposta para seu imóvel. Quando seria um bom momento para conversarmos?",
                "followup": "Entendi sua posição. Entrarei em contato em breve caso surjam novas oportunidades."
            }
            
            selected_preset = st.selectbox(
                "Respostas Prontas",
                options=list(preset_responses.keys()),
                format_func=lambda x: preset_responses[x]
            )
        
        with col_clear:
            clear_response = st.form_submit_button("🚫 Não enviar", help="Limpa o campo de resposta")
        
        with col_schedule:
            schedule_response = st.form_submit_button("📅 Agendar", help="Agenda o envio da resposta")
        
        # Response textarea
        initial_response = current_row['resposta']
        if selected_preset and selected_preset != "":
            initial_response = preset_responses[selected_preset]
        if clear_response:
            initial_response = ""
            
        resposta = st.text_area(
            "Digite a resposta:",
            value=initial_response,
            height=150,
            help="Digite a resposta que será enviada ao contato"
        )
        
        if schedule_response:
            st.info("📅 Funcionalidade de agendamento será implementada na próxima versão")
    
    # Submit button
    submitted = st.form_submit_button("💾 Salvar Alterações", type="primary", use_container_width=True)
    
    if submitted:
        st.success("✅ Alterações salvas com sucesso!")
        # Here you would save the data back to your DataFrame/database

# Navigation
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])

with col1:
    if st.button("⬅️ Anterior", use_container_width=True, disabled=(st.session_state.current_index == 0)):
        st.session_state.current_index -= 1
        st.rerun()

with col3:
    if st.button("Próximo ➡️", use_container_width=True, disabled=(st.session_state.current_index >= total_records - 1)):
        st.session_state.current_index += 1
        st.rerun()

# Footer info
st.markdown("---")
st.caption(f"Caso ID: {st.session_state.current_index + 1} | WhatsApp: {current_row['whatsapp_number']} | Última atualização: {datetime.now().strftime('%H:%M:%S')}")