"""
Dashboard Streamlit — não execute com `python app_dashboard.py`.
Na raiz do repositório: streamlit run fase_2_execucao/sprint_8_produtizacao/app_dashboard.py
"""
import sys

from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx

if get_script_run_ctx(suppress_warning=True) is None:
    print(
        "\nEste arquivo é um app Streamlit. Use o comando abaixo na raiz do repositório:\n\n"
        "  streamlit run fase_2_execucao/sprint_8_produtizacao/app_dashboard.py\n",
        file=sys.stderr,
    )
    sys.exit(1)

import streamlit as st
import pandas as pd
import json
from pathlib import Path

# Caminhos relativos à raiz do clone (streamlit costuma ser iniciado de lá)
_ROOT = Path(__file__).resolve().parents[2]
_GOLD = _ROOT / "data" / "03_gold"
_VIZ = _GOLD / "visualizacoes"

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
    # Caminhos para os arquivos Gold (sempre relativos à raiz do repositório)
    df_ica = pd.read_parquet(_GOLD / "ica_ranking.parquet")
    df_fiscal = pd.read_parquet(_GOLD / "fiscalizacao_series_temporais.parquet")
    df_reincidentes = pd.read_parquet(_GOLD / "reincidentes_embargos.parquet")
    df_impacto = pd.read_parquet(_GOLD / "impacto_embargo_producao.parquet")
    df_alerta = pd.read_parquet(_GOLD / "lista_alerta_compliance.parquet")

    with open(_GOLD / "resumo_sprint6.json", "r", encoding="utf-8") as f:
        resumo_sprint6 = json.load(f)
        
    return df_ica, df_fiscal, df_reincidentes, df_impacto, df_alerta, resumo_sprint6

# Sidebar
st.sidebar.title("🌿 Monitoramento Ambiental")
st.sidebar.markdown("Sábado-TE-Analise-Dados")
page = st.sidebar.radio("Navegação:", [
    "1. Visão Geral (KPIs)", 
    "2. O Paradoxo do Lucro (Eficiência)", 
    "3. Compliance e Risco (Score)",
    "4. Impacto na Produção (Análise Temporal)",
    "5. Custo de Oportunidade (Simulador)"
])

# Carregamento dos dados
try:
    df_ica, df_fiscal, df_reincidentes, df_impacto, df_alerta, resumo6 = load_data()
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
        st.metric("Risco Crítico (Compliance)", f"{len(df_alerta[df_alerta['nivel_risco'].str.contains('Crítico|Alto')])}")
    
    st.subheader("Destaques da Análise")
    st.info(f"""
    - **Correlação Nula:** O desmatamento não impulsiona o crescimento do PIB Agropecuário (Correlação: -0.01).
    - **Reincidência Crítica:** {resumo6['reincidencia']['total_infratores_reincidentes']} CPFs/CNPJs acumulam múltiplos embargos.
    - **Score de Risco:** Identificamos {len(df_alerta[df_alerta['nivel_risco'].str.contains('Crítico|Alto')])} infratores de alto risco para o mercado (trava de crédito).
    - **Foco Territorial:** {resumo6['status_desmatamento']['pct_direto_desmatamento']:.1f}% dos embargos são diretamente ligados à degradação florestal.
    """)
    
    if (_VIZ / "resumo_visual.png").exists():
        st.image(str(_VIZ / "resumo_visual.png"), caption="Storytelling Visual do Projeto")

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
        if (_VIZ / "distribuicao_ica.png").exists():
            st.image(str(_VIZ / "distribuicao_ica.png"))
    
    with col2:
        st.subheader("Top 10 Municípios Ineficientes")
        # Verificar se municipio está em df_ica
        cols = ['municipio', 'ica'] if 'municipio' in df_ica.columns else ['cod_ibge', 'ica']
        st.dataframe(df_ica.sort_values(by='ica', ascending=False).head(10)[cols])

    st.subheader("Exploração de Dados (Top 50 Ranking ICA)")
    st.dataframe(df_ica.sort_values(by='ica', ascending=False).head(50), use_container_width=True)

# ---------------------------------------------------------
# Página 3: Compliance e Risco
# ---------------------------------------------------------
elif page == "3. Compliance e Risco (Score)":
    st.title("⚖️ Compliance e Score de Risco Socioambiental")
    st.markdown("""
    O **Compliance Risk Score** é uma métrica consolidada que avalia o risco de um infrator para a cadeia global de suprimentos.
    O score (0-100) combina **Volume de Embargos**, **Frequência (Recorrência)** e **Severidade (Área Total)**.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Infratores por Nível de Risco")
        risco_count = df_alerta['nivel_risco'].value_counts().reset_index()
        st.bar_chart(risco_count.set_index('nivel_risco'))
    
    with col2:
        st.subheader("Top 20 Infratores de Alto Risco")
        st.bar_chart(df_alerta.head(20).set_index('cpf_cnpj_e')['compliance_risk_score'])

    st.subheader("Lista de Alerta: Detalhes para Auditoria (Top 100)")
    st.dataframe(df_alerta.head(100)[['cpf_cnpj_e', 'num_embargos', 'area_total_ha', 'compliance_risk_score', 'nivel_risco', 'uf_principal']], use_container_width=True)

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
        if (_VIZ / "impacto_producao_boxplot.png").exists():
            st.image(str(_VIZ / "impacto_producao_boxplot.png"))
            
    with col2:
        st.subheader("Distribuição do Δ Bovinos (%)")
        if (_VIZ / "delta_bovinos_histogram.png").exists():
            st.image(str(_VIZ / "delta_bovinos_histogram.png"))

    st.subheader("Análise por Município (Impacto Consolidado)")
    st.dataframe(df_impacto.sort_values(by='delta_bovinos_pct', ascending=False), use_container_width=True)

# ---------------------------------------------------------
# Página 5: Custo de Oportunidade
# ---------------------------------------------------------
elif page == "5. Custo de Oportunidade (Simulador)":
    st.title("💰 Simulador de Custo de Oportunidade Territorial")
    st.markdown("""
    Quanto o Brasil deixa de ganhar ao manter áreas embargadas/degradadas em vez de intensificar a produção em áreas já abertas?
    Este simulador projeta o ganho potencial da conversão de pastagens de baixa produtividade para agricultura de precisão.
    """)
    
    with st.sidebar.expander("⚙️ Parâmetros do Simulador"):
        area_recuperavel = st.slider("Área para Conversão (Hectares)", 100, 1000000, 100000)
        eficiencia_agri = st.number_input("Eficiência Agrícola (R$/ha)", value=8240)
        custo_pastagem = st.number_input("Retorno Pastagem Atual (R$/ha)", value=800)
    
    ganho_agri = area_recuperavel * eficiencia_agri
    ganho_pasto = area_recuperavel * custo_pastagem
    delta_ganho = ganho_agri - ganho_pasto
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Ganho Potencial (Agricultura)", f"R$ {ganho_agri/1e6:,.1f} Mi")
    with col2:
        st.metric("Retorno Atual (Pasto)", f"R$ {ganho_pasto/1e6:,.1f} Mi")
    with col3:
        st.metric("Custo de Oportunidade", f"R$ {delta_ganho/1e6:,.1f} Mi", delta=f"{((ganho_agri/ganho_pasto)-1)*100:.0f}%")

    st.subheader("Análise de Viabilidade")
    st.info(f"""
    **Insight Estratégico:** Ao converter **{area_recuperavel:,} ha** de pastagem degradada para agricultura de alta produtividade, 
    o incremento na riqueza regional seria de **R$ {delta_ganho/1e6:,.1f} milhões**.
    
    Este valor é **{delta_ganho/ganho_pasto:.1f}x superior** ao retorno atual, provando que a **intensificação** 
    é financeiramente superior à **expansão por desmatamento**, que possui correlação nula com o crescimento do VAB.
    """)
    
    # Gráfico simples de barras
    chart_data = pd.DataFrame({
        'Cenário': ['Pastagem Atual', 'Agricultura Intensiva'],
        'Valor (R$ Mi)': [ganho_pasto/1e6, ganho_agri/1e6]
    })
    st.bar_chart(chart_data.set_index('Cenário'))

st.sidebar.markdown("---")
st.sidebar.info("""
**Metodologia:**
Cruzamento de dados do IBAMA (Embargos), IBGE (PIB e PPM) e INPE (PRODES).
Periodo: 2021-2023.
""")
