# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.5: Padronização COMEX (Exportações/Importações)
# 
# **Objetivo:** Agregar dados do COMEX por UF e ano (com filtro de commodities)
# 
# **Entrada:** `comex/data/bronze/comex_stat/EXP_*.parquet` e `IMP_*.parquet`
# 
# **Saída:** `comex/data/silver/comex_por_uf_ano.parquet`
# 
# **Schema de Saída:**
# - `uf` (str): UF (28 estados + "ND")
# - `ano` (int): Ano de referência
# - `tipo_operacao` (str): Exportação ou Importação
# - `ncm` (str): Código NCM (8 dígitos)
# - `commodity` (str): Nome da commodity (ou "Outros")
# - `vob_fob_usd` (float): Valor FOB/FOB em USD
# - `peso_kg` (float): Peso líquido em kg

# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
COMEX_BRONZE_DIR = BASE_DIR / 'notebooks/etl-dados-necessarios/comex/data/bronze/comex_stat'
COMEX_SILVER_DIR = BASE_DIR / 'comex/data/silver'

# Garantir diretório de saída
COMEX_SILVER_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Bronze: {COMEX_BRONZE_DIR}")
print(f"📂 Silver: {COMEX_SILVER_DIR}")

# %% [markdown]
# ## Mapeamento NCM-Commodity

# %%
# Definir mapeamento de NCMs para commodities principais
COMMODITIES_MAP = {
    # Soja e derivados
    '12010000': 'Soja',
    '12011000': 'Soja',
    '12019000': 'Soja',
    
    # Milho
    '10051000': 'Milho',
    '10059000': 'Milho',
    
    # Carne bovina
    '02011000': 'Carne Bovina',
    '02012000': 'Carne Bovina',
    '02013000': 'Carne Bovina',
    '02021000': 'Carne Bovina',
    '02022000': 'Carne Bovina',
    '02023000': 'Carne Bovina',
    
    # Madeira e derivados (capítulos 44)
    '44010000': 'Madeira',
    '44020000': 'Madeira',
    '44030000': 'Madeira',
    '44040000': 'Madeira',
    '44050000': 'Madeira',
    '44060000': 'Madeira',
    '44070000': 'Madeira',
    '44080000': 'Madeira',
    '44090000': 'Madeira',
    
    # Celulose
    '47031100': 'Celulose',
    '47032100': 'Celulose',
    '47032900': 'Celulose',
    
    # Café
    '09011100': 'Café',
    '09011200': 'Café',
    '09012100': 'Café',
    '09012200': 'Café',
    
    # Açúcar
    '17011300': 'Açúcar',
    '17011400': 'Açúcar',
    '17019100': 'Açúcar',
    '17019900': 'Açúcar',
}

def mapear_commodity(ncm: str) -> str:
    """
    Mapeia código NCM para commodity.
    Retorna 'Outros' se não estiver no mapeamento.
    """
    if pd.isna(ncm):
        return 'Outros'
    ncm_str = str(ncm).zfill(8)  # Garantir 8 dígitos
    return COMMODITIES_MAP.get(ncm_str, 'Outros')

# %% [markdown]
# ## Leitura dos Dados

# %%
# Listar todos os arquivos Parquet
arquivos_exp = sorted(COMEX_BRONZE_DIR.glob('EXP_*.parquet'))
arquivos_imp = sorted(COMEX_BRONZE_DIR.glob('IMP_*.parquet'))

print(f"📄 Arquivos de Exportação: {len(arquivos_exp)}")
print(f"📄 Arquivos de Importação: {len(arquivos_imp)}")

# %%
# Ler e concatenar exportações
print("\n📖 Lendo exportações...")
dfs_exp = []
for arquivo in arquivos_exp:
    print(f"  {arquivo.name}...", end=' ')
    df = pd.read_parquet(arquivo)
    df['tipo_operacao'] = 'Exportação'
    dfs_exp.append(df)
    print(f"{len(df):,} registros")

df_exportacao = pd.concat(dfs_exp, ignore_index=True)
print(f"\n✅ Total exportações: {len(df_exportacao):,} registros")

# %%
# Ler e concatenar importações
print("\n📖 Lendo importações...")
dfs_imp = []
for arquivo in arquivos_imp:
    print(f"  {arquivo.name}...", end=' ')
    df = pd.read_parquet(arquivo)
    df['tipo_operacao'] = 'Importação'
    dfs_imp.append(df)
    print(f"{len(df):,} registros")

df_importacao = pd.concat(dfs_imp, ignore_index=True)
print(f"\n✅ Total importações: {len(df_importacao):,} registros")

# %%
# Concatenar tudo
df_comex = pd.concat([df_exportacao, df_importacao], ignore_index=True)
print(f"\n📊 Total COMEX: {len(df_comex):,} registros")

# %% [markdown]
# ## Validação do Schema

# %%
print("\n📋 Colunas disponíveis:")
for i, col in enumerate(df_comex.columns, 1):
    print(f"  {i:2d}. {col}")

# %%
# Verificar campos relevantes
print("\n🔑 Campos relevantes:")
campos_relevantes = ['CO_ANO', 'SG_UF_NCM', 'CO_NCM', 'VL_FOB', 'KG_LIQUIDO', 'CO_PAIS', 'tipo_operacao']
for campo in campos_relevantes:
    if campo in df_comex.columns:
        nulos = df_comex[campo].isnull().sum()
        pct = (nulos / len(df_comex)) * 100
        unicos = df_comex[campo].nunique()
        print(f"  {campo}: {unicos} únicos, {nulos:,} nulos ({pct:.2f}%)")
    else:
        print(f"  {campo}: ❌ NÃO ENCONTRADO")

# %% [markdown]
# ## Transformação de Dados

# %%
# Selecionar colunas relevantes
print("🔄 Selecionando colunas...")

colunas_selecionadas = ['CO_ANO', 'SG_UF_NCM', 'CO_NCM', 'VL_FOB', 'KG_LIQUIDO', 'tipo_operacao']
df_selecionado = df_comex[colunas_selecionadas].copy()

# Renomear colunas
df_selecionado.columns = ['ano', 'uf', 'ncm', 'vl_fob', 'kg_liquido', 'tipo_operacao']

print(f"  Colunas: {list(df_selecionado.columns)}")

# %%
# Mapear commodities
print("\n🔄 Mapeando commodities...")
df_selecionado['commodity'] = df_selecionado['ncm'].apply(mapear_commodity)

print(f"\n📊 Distribuição de commodities:")
print(df_selecionado['commodity'].value_counts().head(15))

# %% [markdown]
# ## Agregação por UF, Ano e Commodity

# %%
# Agrupar por UF, ano, tipo_operacao e commodity
print("\n🔄 Agregando dados...")

df_agregado = df_selecionado.groupby(['uf', 'ano', 'tipo_operacao', 'commodity']).agg(
    vob_fob_usd=('vl_fob', 'sum'),
    peso_kg=('kg_liquido', 'sum'),
    num_operacoes=('ncm', 'count')
).reset_index()

print(f"📊 Total de registros após agregação: {len(df_agregado):,}")

# %%
# Ordenar dados
df_agregado = df_agregado.sort_values(['uf', 'ano', 'tipo_operacao', 'commodity']).reset_index(drop=True)

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
print(f"  Anos: {df_agregado['ano'].min()} - {df_agregado['ano'].max()}")

print("\n📍 UFs:")
print(f"  Total: {df_agregado['uf'].nunique()}")
print(f"  UFs: {sorted(df_agregado['uf'].unique())}")

print("\n📊 Operações por tipo:")
for tipo in df_agregado['tipo_operacao'].unique():
    total = df_agregado[df_agregado['tipo_operacao'] == tipo]['num_operacoes'].sum()
    valor = df_agregado[df_agregado['tipo_operacao'] == tipo]['vob_fob_usd'].sum()
    print(f"  {tipo}: {total:,.0f} operações, USD {valor:,.0f}")

print("\n📊 Top 10 commodities (exportação - USD):")
df_exp = df_agregado[df_agregado['tipo_operacao'] == 'Exportação']
top_commodities = df_exp.groupby('commodity')['vob_fob_usd'].sum().nlargest(10)
for comm, valor in top_commodities.items():
    print(f"  {comm}: USD {valor:,.0f}")

# %% [markdown]
# ## Exportação para Camada Silver

# %%
# Definir schema otimizado
schema = pa.schema([
    ('uf', pa.string()),
    ('ano', pa.int64()),
    ('tipo_operacao', pa.string()),
    ('commodity', pa.string()),
    ('vob_fob_usd', pa.float64()),
    ('peso_kg', pa.float64()),
    ('num_operacoes', pa.int64())
])

# Exportar
output_path = COMEX_SILVER_DIR / 'comex_por_uf_ano.parquet'
print(f"\n💾 Exportando para: {output_path}")

# Converter tipos
df_export = df_agregado.copy()
df_export['ano'] = df_export['ano'].fillna(-1).astype('int64')
df_export['num_operacoes'] = df_export['num_operacoes'].fillna(0).astype('int64')
df_export['vob_fob_usd'] = df_export['vob_fob_usd'].fillna(0).astype('float64')
df_export['peso_kg'] = df_export['peso_kg'].fillna(0).astype('float64')

table = pa.Table.from_pandas(df_export, schema=schema, preserve_index=False)
pq.write_table(table, output_path, compression='snappy')

print(f"✅ Exportado com sucesso!")
print(f"📦 Tamanho do arquivo: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

# %% [markdown]
# ## Amostra dos Dados

# %%
print("\n📋 Amostra (30 primeiros registros):")
df_agregado.head(30)

# %%
print("\n📋 Exportações de Soja por UF (2024):")
df_soja_2024 = df_agregado[
    (df_agregado['commodity'] == 'Soja') & 
    (df_agregado['ano'] == 2024) & 
    (df_agregado['tipo_operacao'] == 'Exportação')
]
df_soja_2024.sort_values('vob_fob_usd', ascending=False).head(15)

# %% [markdown]
# ## Resumo da ETL 1.5
# 
# ✅ **Concluído:**
# - Leitura de {len(arquivos_exp) + len(arquivos_imp)} arquivos Parquet
# - {len(df_exportacao):,} registros de exportação
# - {len(df_importacao):,} registros de importação
# - Mapeamento de NCMs para commodities
# - Agregação por UF + ano + commodity
# - Schema padronizado com 7 colunas
# - Exportação para `comex/data/silver/comex_por_uf_ano.parquet`
# 
# **⚠️ Limitação:** COMEX não possui código municipal, apenas UF
# 
# **Próximo passo:** ETL 1.6 - Dimensão de Municípios
