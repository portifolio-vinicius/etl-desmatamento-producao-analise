# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.4: Padronização IBAMA (Embargos Ambientais)
# 
# **Objetivo:** Agregar embargos do IBAMA por município e ano
# 
# **Entrada:** `ibama/data/bronze/ibama_embargos/embargos_ibama_tabular.parquet`
# 
# **Saída:** `ibama/data/silver/embargos_por_municipio_ano.parquet`
# 
# **Schema de Saída:**
# - `cod_munici` (int): Código IBGE do município
# - `ano` (int): Ano do embargo
# - `num_embargos` (int): Quantidade de embargos
# - `area_desmatada_ha` (float): Área desmatada (hectares)
# - `area_embargada_ha` (float): Área embargada (hectares)

# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
IBAMA_BRONZE_DIR = BASE_DIR / 'notebooks/etl-dados-necessarios/ibama/data/bronze/ibama_embargos'
IBAMA_SILVER_DIR = BASE_DIR / 'ibama/data/silver'

# Garantir diretório de saída
IBAMA_SILVER_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Bronze: {IBAMA_BRONZE_DIR}")
print(f"📂 Silver: {IBAMA_SILVER_DIR}")

# %% [markdown]
# ## Leitura dos Dados

# %%
# Ler arquivo tabular (não-geográfico para análise)
arquivo_tabular = IBAMA_BRONZE_DIR / 'embargos_ibama_tabular.parquet'
print(f"📖 Lendo {arquivo_tabular.name}...")

df_ibama = pd.read_parquet(arquivo_tabular)
print(f"📊 Total de registros: {len(df_ibama):,}")

# %%
# Verificar schema
print("\n📋 Schema original:")
print(df_ibama.dtypes)

print(f"\n📋 Colunas disponíveis ({len(df_ibama.columns)}):")
for i, col in enumerate(df_ibama.columns, 1):
    print(f"  {i:2d}. {col}")

# %% [markdown]
# ## Análise Exploratória Inicial

# %%
print("=" * 60)
print("📊 ANÁLISE EXPLORATÓRIA")
print("=" * 60)

print("\n🔍 Nulos por coluna (top 20):")
nulos = df_ibama.isnull().sum().sort_values(ascending=False).head(20)
for col, qtd in nulos.items():
    pct = (qtd / len(df_ibama)) * 100
    print(f"  {col}: {qtd:,} ({pct:.2f}%)")

# %%
# Verificar campos chave
print("\n🔑 Campos para agregação:")
print(f"  cod_munici únicos: {df_ibama['cod_munici'].nunique():,}")
print(f"  dat_embarg (amostra): {df_ibama['dat_embarg'].dropna().head(3).tolist()}")

# %% [markdown]
# ## Transformação de Dados

# %%
# Extrair ano da data do embargo
print("🔄 Extraindo ano da data do embargo...")

# Converter dat_embarg para datetime
df_ibama['dat_embarg'] = pd.to_datetime(df_ibama['dat_embarg'], format='%d/%m/%y %H:%M:%S', errors='coerce')

# Extrair ano
df_ibama['ano'] = df_ibama['dat_embarg'].dt.year

print(f"  Anos encontrados: {sorted(df_ibama['ano'].dropna().unique())}")

# %%
# Selecionar colunas relevantes para agregação
print("\n🔄 Selecionando colunas relevantes...")

colunas_relevantes = ['cod_munici', 'ano', 'qtd_area_d', 'qtd_area_e']
df_selecionado = df_ibama[colunas_relevantes].copy()

print(f"  Colunas: {list(df_selecionado.columns)}")

# %% [markdown]
# ## Agregação por Município e Ano

# %%
# Agrupar por município e ano
print("\n🔄 Agregando por município e ano...")

df_agregado = df_selecionado.groupby(['cod_munici', 'ano']).agg(
    num_embargos=('cod_munici', 'count'),
    area_desmatada_ha=('qtd_area_d', 'sum'),
    area_embargada_ha=('qtd_area_e', 'sum')
).reset_index()

print(f"📊 Total de registros após agregação: {len(df_agregado):,}")

# %%
# Ordenar dados
df_agregado = df_agregado.sort_values(['cod_munici', 'ano']).reset_index(drop=True)

# %% [markdown]
# ## Validação da Qualidade

# %%
print("=" * 60)
print("📊 VALIDAÇÃO DA QUALIDADE DOS DADOS")
print("=" * 60)

print(f"\n📋 Shape: {df_agregado.shape[0]:,} linhas × {df_agregado.shape[1]} colunas")

print("\n🔍 Nulos por coluna:")
nulos = df_agregado.isnull().sum()
for col, qtd in nulos.items():
    pct = (qtd / len(df_agregado)) * 100
    print(f"  {col}: {qtd:,} ({pct:.2f}%)")

print("\n📅 Amplitude temporal:")
print(f"  Anos: {int(df_agregado['ano'].min())} - {int(df_agregado['ano'].max())}")
print(f"  Anos únicos: {sorted(df_agregado['ano'].dropna().unique().astype(int))}")

print("\n🏙️ Municípios:")
print(f"  Total único: {df_agregado['cod_munici'].nunique():,}")

print("\n📊 Estatísticas descritivas:")
print(df_agregado.describe())

print("\n📊 Top 10 municípios com mais embargos (total):")
top_municipios = df_agregado.groupby('cod_munici')['num_embargos'].sum().nlargest(10)
for cod, qtd in top_municipios.items():
    print(f"  {cod}: {qtd:,} embargos")

print("\n📊 Top 10 anos com mais embargos:")
top_anos = df_agregado.groupby('ano')['num_embargos'].sum().nlargest(10)
for ano, qtd in top_anos.items():
    print(f"  {int(ano)}: {qtd:,} embargos")

print("\n📊 Área total desmatada e embargada:")
print(f"  Área desmatada total: {df_agregado['area_desmatada_ha'].sum():,.2f} ha")
print(f"  Área embargada total: {df_agregado['area_embargada_ha'].sum():,.2f} ha")

# %% [markdown]
# ## Exportação para Camada Silver

# %%
# Definir schema otimizado
schema = pa.schema([
    ('cod_munici', pa.int64()),
    ('ano', pa.int64()),
    ('num_embargos', pa.int64()),
    ('area_desmatada_ha', pa.float64()),
    ('area_embargada_ha', pa.float64())
])

# Exportar
output_path = IBAMA_SILVER_DIR / 'embargos_por_municipio_ano.parquet'
print(f"\n💾 Exportando para: {output_path}")

# Converter para tipos não-nullable
df_export = df_agregado.copy()
df_export['cod_munici'] = df_export['cod_munici'].fillna(-1).astype('int64')
df_export['ano'] = df_export['ano'].fillna(-1).astype('int64')
df_export['num_embargos'] = df_export['num_embargos'].fillna(0).astype('int64')
df_export['area_desmatada_ha'] = df_export['area_desmatada_ha'].fillna(0).astype('float64')
df_export['area_embargada_ha'] = df_export['area_embargada_ha'].fillna(0).astype('float64')

table = pa.Table.from_pandas(df_export, schema=schema, preserve_index=False)
pq.write_table(table, output_path, compression='snappy')

print(f"✅ Exportado com sucesso!")
print(f"📦 Tamanho do arquivo: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

# %% [markdown]
# ## Amostra dos Dados

# %%
print("\n📋 Amostra (20 primeiros registros):")
df_agregado.head(20)

# %%
print("\n📋 Amostra (anos recentes 2020-2024):")
df_recente = df_agregado[df_agregado['ano'] >= 2020]
df_recente.head(20)

# %% [markdown]
# ## Resumo da ETL 1.4
# 
# ✅ **Concluído:**
# - Leitura de embargos_ibama_tabular.parquet ({len(df_ibama):,} registros)
# - Conversão de dat_embarg para datetime
# - Extração de ano da data
# - Agregação por cod_munici + ano
# - Schema padronizado com 5 colunas
# - Exportação para `ibama/data/silver/embargos_por_municipio_ano.parquet`
# 
# **Próximo passo:** ETL 1.5 - Padronização COMEX
