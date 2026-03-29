# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.6: Dimensão de Municípios
# 
# **Objetivo:** Criar tabela dimensão unificada de municípios com códigos IBGE
# 
# **Saída:** `dimensao/data/silver/dim_municipio.parquet`

# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import requests

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
PAM_SILVER = BASE_DIR / 'pam/data/silver/pam_consolidado.parquet'
PIB_SILVER = BASE_DIR / 'pib/data/silver/pib_vab_consolidado.parquet'
PPM_SILVER = BASE_DIR / 'ppm/data/silver/ppm_consolidado.parquet'
IBAMA_SILVER = BASE_DIR / 'ibama/data/silver/embargos_por_municipio_ano.parquet'
DIMENSAO_SILVER_DIR = BASE_DIR / 'dimensao/data/silver'

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
    for _, row in df_pam.drop_duplicates().iterrows():
        municipios_unicos.add((row['municipio'], row['uf']))
    print(f"  PAM: {df_pam[['municipio', 'uf']].drop_duplicates().shape[0]} municípios")

# PIB (tem cod_ibge mas não tem nome)
if PIB_SILVER.exists():
    print(f"  PIB: {PIB_SILVER.stat().st_size / 1024 / 1024:.2f} MB (será integrado depois)")

# IBAMA (tem cod_munici)
if IBAMA_SILVER.exists():
    df_ibama = pd.read_parquet(IBAMA_SILVER, columns=['cod_munici'])
    print(f"  IBAMA: {df_ibama['cod_munici'].nunique():,} municípios com código")

print(f"\n📊 Total de pares (municipio, UF) únicos do PAM: {len(municipios_unicos):,}")

# %% [markdown]
# ## Obter Códigos IBGE via API

# %%
# Usar API do IBGE para obter códigos
# https://servicodados.ibge.gov.br/api/docs/localidades?versao=1#api-municipios-uf

print("\n🌐 Buscando códigos IBGE na API do IBGE...")

def obter_municipios_ibge():
    """Obtém todos os municípios com código IBGE via API alternativa."""
    try:
        # API alternativa: IBGE Malha Municipal
        response = requests.get(
            'https://servicodados.ibge.gov.br/api/v1/localidades/municipios?details=true',
            timeout=60
        )
        response.raise_for_status()
        dados = response.json()
        
        municipios_map = {}
        for mun in dados:
            nome = mun.get('nome', '')
            uf = mun.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('sigla', '')
            codigo = mun.get('id')
            if nome and uf and codigo:
                municipios_map[(nome, uf)] = codigo
        
        return municipios_map
    except Exception as e:
        print(f"  ⚠️ Erro na API: {e}")
        # Fallback: tentar API mais simples
        try:
            response = requests.get(
                'https://brasilapi.com.br/api/ibge/municipios/v1',
                timeout=60
            )
            response.raise_for_status()
            dados = response.json()
            
            municipios_map = {}
            for mun in dados:
                nome = mun.get('nome', '')
                uf = mun.get('uf', '')
                codigo = mun.get('codigo_ibge')
                if nome and uf and codigo:
                    municipios_map[(nome, uf)] = codigo
            
            print(f"  ✅ BrasilAPI: {len(municipios_map):,} municípios obtidos")
            return municipios_map
        except Exception as e2:
            print(f"  ⚠️ Erro BrasilAPI: {e2}")
            return {}

municipios_ibge = obter_municipios_ibge()
print(f"  ✅ {len(municipios_ibge):,} municípios obtidos da API IBGE")

# %%
# Criar DataFrame com mapeamento
print("\n🔨 Criando tabela de mapeamento...")

mapeamento = []
for municipio, uf in municipios_unicos:
    cod_ibge = municipios_ibge.get((municipio, uf))
    if cod_ibge is None:
        # Tentar variação do nome (remover acentos, etc.)
        # Por enquanto, usar None
        print(f"  ⚠️ Sem código para: {municipio} - {uf}")
    mapeamento.append({
        'municipio': municipio,
        'uf': uf,
        'cod_ibge': cod_ibge
    })

df_mapeamento = pd.DataFrame(mapeamento)

# %% [markdown]
# ## Adicionar Informações Geográficas

# %%
# Mapeamento de UF
UF_MAP = {
    11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
    21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL',
    28: 'SE', 29: 'BA', 31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP', 41: 'PR',
    42: 'SC', 43: 'RS', 50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
}
UF_INVERSO = {v: k for k, v in UF_MAP.items()}

df_mapeamento['uf_codigo'] = df_mapeamento['uf'].map(UF_INVERSO)

# Regiões
REGIOES_MAP = {
    'RO': 'Norte', 'AC': 'Norte', 'AM': 'Norte', 'RR': 'Norte', 'PA': 'Norte', 'AP': 'Norte', 'TO': 'Norte',
    'MA': 'Nordeste', 'PI': 'Nordeste', 'CE': 'Nordeste', 'RN': 'Nordeste', 'PB': 'Nordeste', 
    'PE': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste', 'BA': 'Nordeste',
    'MG': 'Sudeste', 'ES': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'SC': 'Sul', 'RS': 'Sul',
    'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'DF': 'Centro-Oeste'
}

df_mapeamento['regiao'] = df_mapeamento['uf'].map(REGIOES_MAP)

# Amazônia Legal
AMAZONIA_LEGAL_UFS = ['AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO', 'MA', 'MT']
df_mapeamento['amazonia_legal'] = df_mapeamento['uf'].isin(AMAZONIA_LEGAL_UFS)

# %% [markdown]
# ## Validação

# %%
print("=" * 60)
print("📊 VALIDAÇÃO")
print("=" * 60)

print(f"\n📋 Shape: {df_mapeamento.shape[0]:,} × {df_mapeamento.shape[1]}")

print("\n🔍 Nulos:")
nulos = df_mapeamento.isnull().sum()
for col, qtd in nulos.items():
    if qtd > 0:
        print(f"  {col}: {qtd:,} ({qtd/len(df_mapeamento)*100:.1f}%)")

print("\n📊 Distribuição por UF:")
print(df_mapeamento.groupby('uf').size().sort_index())

print("\n📊 Amazônia Legal:", df_mapeamento['amazonia_legal'].sum(), "municípios")

# %% [markdown]
# ## Exportação

# %%
schema = pa.schema([
    ('municipio', pa.string()),
    ('uf', pa.string()),
    ('cod_ibge', pa.int64()),
    ('uf_codigo', pa.int64()),
    ('regiao', pa.string()),
    ('amazonia_legal', pa.bool_())
])

output_path = DIMENSAO_SILVER_DIR / 'dim_municipio.parquet'
print(f"\n💾 Exportando: {output_path}")

df_export = df_mapeamento.copy()
df_export['cod_ibge'] = df_export['cod_ibge'].fillna(-1).astype('int64')
df_export['uf_codigo'] = df_export['uf_codigo'].fillna(-1).astype('int64')
df_export['amazonia_legal'] = df_export['amazonia_legal'].fillna(False).astype('bool')

table = pa.Table.from_pandas(df_export, schema=schema, preserve_index=False)
pq.write_table(table, output_path, compression='snappy')

print(f"✅ Exportado! Tamanho: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

# %%
print("\n📋 Amostra:")
df_mapeamento.head(20)
