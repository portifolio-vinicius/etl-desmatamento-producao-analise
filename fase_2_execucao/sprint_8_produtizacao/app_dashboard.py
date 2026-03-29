import streamlit as st
import pandas as pd
import os
import json

# Configuração da página
st.set_page_config(
    page_title="Dashboard: Desmatamento e Eficiência", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilo Customizado
st.markdown("""
    <style>
    .main {
        background-color: #f5f7f9;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data
def load_data():
    # Caminhos para os arquivos Gold
    df_ica = pd.read_parquet('data/03_gold/ica_ranking.parquet')
    df_fiscal = pd.read_parquet('data/03_gold/fiscalizacao_series_temporais.parquet')
    df_reincidentes = pd.read_parquet('data/03_gold/reincidentes_embargos.parquet')
    df_impacto = pd.read_parquet('data/03_gold/impacto_embargo_producao.parquet')
    
    with open('data/03_gold/resumo_sprint6.json', 'r') as f:
        resumo_sprint6 = json.load(f)
        
    return df_ica, df_fiscal, df_reincidentes, df_impacto, resumo_sprint6

# Sidebar
st.sidebar.title("🌿 Monitoramento Ambiental")
st.sidebar.markdown("Sábado-TE-Analise-Dados")
page = st.sidebar.radio("Navegação:", [
    "1. Visão Geral (KPIs)", 
    "2. O Paradoxo do Lucro (Eficiência)", 
    "3. Fiscalização (Reincidência)",
    "4. Impacto na Produção (Análise Temporal)"
])

# Carregamento dos dados
try:
    df_ica, df_fiscal, df_reincidentes, df_impacto, resumo6 = load_data()
except Exception as e:
    st.error(f"Erro ao carregar dados Gold: {e}")
    st.stop()

# ---------------------------------------------------------
# Página 1: Visão Geral
# ---------------------------------------------------------
if page == "1. Visão Geral (KPIs)":
    st.title("📊 Desmatamento vs Eficiência Agropecuária")
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Municípios com Embargos", f"{resumo6['municipios_com_embargos_periodo']}")
    with col2:
        st.metric("Total Embargos (21-23)", f"{resumo6['total_embargos_periodo']}")
    with col3:
        st.metric("Δ Bovinos Médio", f"{resumo6['impacto_producao']['delta_bovinos_medio_pct']:.2f}%")
    with col4:
        st.metric("Infratores Reincidentes", f"{resumo6['reincidencia']['total_infratores_reincidentes']}")
    
    st.subheader("Destaques da Análise")
    st.info(f"""
    - **Correlação Nula:** O desmatamento não impulsiona o crescimento do PIB Agropecuário (Correlação: -0.01).
    - **Reincidência Crítica:** {resumo6['reincidencia']['total_infratores_reincidentes']} CPFs/CNPJs acumulam múltiplos embargos.
    - **Foco Territorial:** {resumo6['status_desmatamento']['pct_direto_desmatamento']:.1f}% dos embargos são diretamente ligados à degradação florestal.
    """)
    
    if os.path.exists('data/03_gold/visualizacoes/resumo_visual.png'):
        st.image('data/03_gold/visualizacoes/resumo_visual.png', caption="Storytelling Visual do Projeto")

# ---------------------------------------------------------
# Página 2: O Paradoxo do Lucro
# ---------------------------------------------------------
elif page == "2. O Paradoxo do Lucro (Eficiência)":
    st.title("🔴 O Paradoxo do Lucro: Eficiência e ICA")
    st.markdown("""
    O **Índice de Custo Ambiental (ICA)** mede o 'custo' de degradação para cada R$ 1.000 de riqueza gerada.
    Municípios com ICA alto degradam muito e geram pouco valor econômico real.
    """)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Distribuição do ICA por Município")
        if os.path.exists('data/03_gold/visualizacoes/distribuicao_ica.png'):
            st.image('data/03_gold/visualizacoes/distribuicao_ica.png')
    
    with col2:
        st.subheader("Top 10 Municípios Ineficientes")
        st.dataframe(df_ica.sort_values(by='ica', ascending=False).head(10)[['municipio', 'ica']])

    st.subheader("Exploração de Dados (Top 50 Ranking ICA)")
    st.dataframe(df_ica.sort_values(by='ica', ascending=False).head(50), use_container_width=True)

# ---------------------------------------------------------
# Página 3: Fiscalização
# ---------------------------------------------------------
elif page == "3. Fiscalização (Reincidência)":
    st.title("⚖️ Fiscalização e Concentração da Ilegalidade")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Evolução Temporal de Embargos")
        if os.path.exists('data/03_gold/visualizacoes/serie_temporal_embargos.png'):
            st.image('data/03_gold/visualizacoes/serie_temporal_embargos.png')
    
    with col2:
        st.subheader("Top 20 Infratores Reincidentes")
        if os.path.exists('data/03_gold/visualizacoes/top20_reincidentes.png'):
            st.image('data/03_gold/visualizacoes/top20_reincidentes.png')
        else:
            st.bar_chart(df_reincidentes.head(20).set_index('cpf_cnpj_e')['num_embargos'])

    st.subheader("Dados Detalhados de Reincidência")
    st.dataframe(df_reincidentes.head(100), use_container_width=True)

# ---------------------------------------------------------
# Página 4: Impacto na Produção
# ---------------------------------------------------------
elif page == "4. Impacto na Produção (Análise Temporal)":
    st.title("🚜 Impacto na Produção: Antes vs Depois dos Embargos")
    st.markdown("""
    Esta análise compara o VAB Agropecuário e o Rebanho Bovino nos 2 anos anteriores e 2 anos posteriores ao embargo.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Impacto no VAB e Bovinos (Boxplot)")
        if os.path.exists('data/03_gold/visualizacoes/impacto_producao_boxplot.png'):
            st.image('data/03_gold/visualizacoes/impacto_producao_boxplot.png')
            
    with col2:
        st.subheader("Distribuição do Δ Bovinos (%)")
        if os.path.exists('data/03_gold/visualizacoes/delta_bovinos_histogram.png'):
            st.image('data/03_gold/visualizacoes/delta_bovinos_histogram.png')

    st.subheader("Análise por Município (Impacto Consolidado)")
    st.dataframe(df_impacto.sort_values(by='delta_bovinos_pct', ascending=False), use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.info("""
**Metodologia:**
Cruzamento de dados do IBAMA (Embargos), IBGE (PIB e PPM) e INPE (PRODES).
Periodo: 2021-2023.
""")
