# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.6: Dimensão de Municípios (Tabela de Referência)
# 
# **Objetivo:** Criar tabela única de municípios com código IBGE, nome e UF
# 
# **Fontes:** PAM, IBAMA e API IBGE
# 
# **Saída:** `data/02_silver/dim_municipio.parquet`

# %%
import pandas as pd
from pathlib import Path
import requests

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
PAM_SILVER = BASE_DIR / 'data/02_silver/pam_consolidado.parquet'
IBAMA_SILVER = BASE_DIR / 'data/02_silver/embargos_por_municipio_ano.parquet'
DIMENSAO_SILVER_DIR = BASE_DIR / 'data/02_silver'

# Garantir diretório de saída
DIMENSAO_SILVER_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Saída: {DIMENSAO_SILVER_DIR}")

# %% [markdown]
# ## Coleta de Municípios Únicos

# %%
print("📖 Coletando municípios únicos das fontes Silver...")

municipios_unicos = set()

# PAM (usa chave_municipio = municipio_uf)
if PAM_SILVER.exists():
    df_pam = pd.read_parquet(PAM_SILVER, columns=['municipio', 'uf'])
    df_pam['chave'] = df_pam['municipio'] + ' - ' + df_pam['uf']
    municipios_unicos.update(df_pam['chave'].unique())
    print(f"  ✓ PAM: {df_pam['chave'].nunique()} municípios")

# IBAMA (tem cod_munici)
if IBAMA_SILVER.exists():
    df_ibama = pd.read_parquet(IBAMA_SILVER, columns=['cod_munici'])
    # Aqui precisaremos cruzar o código com a API do IBGE se não tivermos o nome
    print(f"  ✓ IBAMA: {df_ibama['cod_munici'].nunique()} códigos únicos")

print(f"Total de municípios únicos coletados (PAM): {len(municipios_unicos)}")

# %% [markdown]
# ## Integração com API IBGE

# %%
def buscar_municipios_ibge():
    """Busca lista completa de municípios da API do IBGE."""
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    response = requests.get(url)
    if response.status_code == 200:
        dados = response.json()
        df = pd.DataFrame([
            {
                'cod_ibge': int(m['id']),
                'municipio': m['nome'],
                'uf': m['microrregiao']['mesorregiao']['UF']['sigla'],
                'regiao': m['microrregiao']['mesorregiao']['UF']['regiao']['nome']
            }
            for m in dados
        ])
        return df
    return None

print("\n🌐 Consultando API do IBGE...")
df_ibge = buscar_municipios_ibge()

if df_ibge is not None:
    print(f"  ✓ API IBGE: {len(df_ibge)} municípios carregados")
    
    # Criar chave de join
    df_ibge['chave_municipio'] = df_ibge['municipio'].str.upper() + ' - ' + df_ibge['uf'].str.upper()
    
    # Salvar
    saida_parquet = DIMENSAO_SILVER_DIR / 'dim_municipio.parquet'
    df_ibge.to_parquet(saida_parquet, index=False)
    print(f"\n✅ Dimensão Município salva em: {saida_parquet}")
else:
    print("\n❌ Erro ao consultar API do IBGE")
