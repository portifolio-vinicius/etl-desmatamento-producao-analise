# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.7: Série Histórica Comum (2020-2023)
# 
# **Objetivo:** Criar base analítica unificada com todas as fontes integradas
# 
# **Nota:** PAM usa chave_municipio (nome+UF), enquanto PIB/PPM/IBAMA usam cod_ibge

# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
PAM_SILVER = BASE_DIR / 'data/02_silver/pam_consolidado.parquet'
PIB_SILVER = BASE_DIR / 'data/02_silver/pib_vab_consolidado.parquet'
PPM_SILVER = BASE_DIR / 'data/02_silver/ppm_consolidado.parquet'
IBAMA_SILVER = BASE_DIR / 'data/02_silver/embargos_por_municipio_ano.parquet'
COMEX_SILVER = BASE_DIR / 'data/02_silver/comex_por_uf_ano.parquet'

DADOS_ANALITICOS_DIR = BASE_DIR / 'data/02_silver'
DADOS_ANALITICOS_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Saída: {DADOS_ANALITICOS_DIR}")

# %% [markdown]
# ## Período Comum

# %%
ANOS_SERIE = [2020, 2021, 2022, 2023]
print(f"📅 Série histórica: {ANOS_SERIE}")

# %% [markdown]
# ## Carregar Dados

# %%
print("\n📖 Carregando dados Silver...")

# PAM (tem chave_municipio = municipio_uf)
df_pam = pd.read_parquet(PAM_SILVER)
print(f"  PAM: {len(df_pam):,} registros, {df_pam['chave_municipio'].nunique():,} municípios")

# PIB (tem cod_ibge)
df_pib = pd.read_parquet(PIB_SILVER)
print(f"  PIB: {len(df_pib):,} registros, {df_pib['cod_ibge'].nunique():,} municípios")

# PPM (tem cod_ibge)
df_ppm = pd.read_parquet(PPM_SILVER)
print(f"  PPM: {len(df_ppm):,} registros, {df_ppm['cod_ibge'].nunique():,} municípios")

# IBAMA (tem cod_munici)
df_ibama = pd.read_parquet(IBAMA_SILVER)
print(f"  IBAMA: {len(df_ibama):,} registros, {df_ibama['cod_munici'].nunique():,} municípios")

# COMEX (tem uf, não tem município)
df_comex = pd.read_parquet(COMEX_SILVER)
print(f"  COMEX: {len(df_comex):,} registros (nível UF)")

# %% [markdown]
# ## Filtrar Período Comum

# %%
print("\n✂️ Filtrando 2020-2023...")

df_pam = df_pam[df_pam['ano'].isin(ANOS_SERIE)]
df_pib = df_pib[df_pib['ano'].isin(ANOS_SERIE)]
df_ppm = df_ppm[df_ppm['ano'].isin(ANOS_SERIE)]
df_ibama = df_ibama[df_ibama['ano'].isin(ANOS_SERIE)]
df_comex = df_comex[df_comex['ano'].isin(ANOS_SERIE)]

print(f"  PAM: {len(df_pam):,} registros")
print(f"  PIB: {len(df_pib):,} registros")
print(f"  PPM: {len(df_ppm):,} registros")
print(f"  IBAMA: {len(df_ibama):,} registros")
print(f"  COMEX: {len(df_comex):,} registros")

# %% [markdown]
# ## Agregar PAM

# %%
print("\n🔄 Agregando PAM...")

# Agrupar por município e ano, somando todas as culturas
df_pam_agg = df_pam.groupby(['chave_municipio', 'municipio', 'uf', 'ano']).agg(
    area_plantada_ha=('area_plantada_ha', 'sum'),
    area_colhida_ha=('area_colhida_ha', 'sum'),
    valor_producao_mil_reais=('valor_producao_mil_reais', 'sum')
).reset_index()

print(f"  PAM agregado: {len(df_pam_agg):,} registros")

# %% [markdown]
# ## Pivotar PPM

# %%
print("\n🔄 Pivotando PPM...")

# Pivotar: categorias → colunas
df_ppm_wide = df_ppm.pivot_table(
    index=['cod_ibge', 'ano'],
    columns='categoria',
    values='efetivo_cabecas',
    aggfunc='sum'
).reset_index()

# Renomear colunas
colunas_renomeadas = {}
for col in df_ppm_wide.columns:
    if col not in ['cod_ibge', 'ano']:
        nome_simples = str(col).split(',')[0].strip().lower().replace(' ', '_')
        colunas_renomeadas[col] = f"{nome_simples}_cabecas"

df_ppm_wide = df_ppm_wide.rename(columns=colunas_renomeadas)
print(f"  PPM pivotado: {len(df_ppm_wide):,} registros, {len(df_ppm_wide.columns)} colunas")

# %% [markdown]
# ## Criar Base Comum

# %%
print("\n🔨 Criando base comum...")

# Usar PIB como base principal (tem cod_ibge)
municipios = df_pib['cod_ibge'].unique()
print(f"  Municípios base (PIB): {len(municipios):,}")

# Criar produto cartesiano município × ano
base_comum = pd.MultiIndex.from_product(
    [municipios, ANOS_SERIE],
    names=['cod_ibge', 'ano']
).to_frame().reset_index(drop=True)

print(f"  Base comum: {len(base_comum):,} registros")

# %% [markdown]
# ## Realizar Joins

# %%
print("\n🔗 Joins...")

df_base = base_comum.copy()

# Join PIB
print(f"  PIB... ", end='')
df_base = df_base.merge(df_pib, on=['cod_ibge', 'ano'], how='left')
print(f"✅ {len(df_base):,}")

# Join PPM
print(f"  PPM... ", end='')
df_base = df_base.merge(df_ppm_wide, on=['cod_ibge', 'ano'], how='left')
print(f"✅ {len(df_base):,}")

# Join IBAMA
print(f"  IBAMA... ", end='')
# IBAMA usa cod_munici, renomear para cod_ibge
df_ibama_renamed = df_ibama.rename(columns={'cod_munici': 'cod_ibge'})
df_base = df_base.merge(df_ibama_renamed, on=['cod_ibge', 'ano'], how='left')
print(f"✅ {len(df_base):,}")

# PAM não tem cod_ibge, vamos adicionar separado
print(f"  PAM... ⚠️ separado (sem cod_ibge)")

# COMEX é nível UF, adicionar separado
print(f"  COMEX... ⚠️ separado (nível UF)")

# %% [markdown]
# ## Preencher Nulos

# %%
print("\n🔧 Preenchendo nulos...")

colunas_numericas = ['vab_agro_mil_reais', 'num_embargos', 'area_desmatada_ha', 'area_embargada_ha']
for col in df_ppm_wide.columns:
    if col.endswith('_cabecas'):
        colunas_numericas.append(col)

for col in colunas_numericas:
    if col in df_base.columns:
        df_base[col] = df_base[col].fillna(0)

# %% [markdown]
# ## Ordenar

# %%
df_base = df_base.sort_values(['cod_ibge', 'ano']).reset_index(drop=True)

# %% [markdown]
# ## Validação

# %%
print("=" * 60)
print("📊 VALIDAÇÃO")
print("=" * 60)

print(f"\n📋 Shape: {df_base.shape[0]:,} × {df_base.shape[1]}")

print("\n🔍 Nulos:")
nulos = df_base.isnull().sum()
for col, qtd in nulos.items():
    if qtd > 0:
        print(f"  {col}: {qtd:,} ({qtd/len(df_base)*100:.1f}%)")

print("\n📅 Anos:", sorted(df_base['ano'].unique()))
print("🏙️ Municípios:", df_base['cod_ibge'].nunique())

print("\n📊 VAB Agro total por ano:")
for ano in ANOS_SERIE:
    total = df_base[df_base['ano'] == ano]['vab_agro_mil_reais'].sum()
    print(f"  {ano}: R$ {total:,.0f} mil")

# %% [markdown]
# ## Exportação

# %%
# Schema dinâmico
colunas_schema = []
for col in df_base.columns:
    if col in ['cod_ibge', 'ano']:
        colunas_schema.append((col, pa.int64()))
    elif col.endswith('_cabecas') or col.endswith('_ha') or col.endswith('_mil_reais'):
        colunas_schema.append((col, pa.float64()))
    elif col.endswith('_pct'):
        colunas_schema.append((col, pa.float64()))
    else:
        colunas_schema.append((col, pa.float64()))

schema = pa.schema(colunas_schema)

output_path = DADOS_ANALITICOS_DIR / 'serie_historica_2020_2023.parquet'
print(f"\n💾 Exportando: {output_path}")

df_export = df_base.copy()
df_export['cod_ibge'] = df_export['cod_ibge'].fillna(-1).astype('int64')
df_export['ano'] = df_export['ano'].astype('int64')

table = pa.Table.from_pandas(df_export, schema=schema, preserve_index=False)
pq.write_table(table, output_path, compression='snappy')

print(f"✅ Exportado! Tamanho: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

# %%
print("\n📋 Amostra:")
df_base.head(10)

# %% [markdown]
# ## Resumo ETL 1.7
# 
# ✅ **Concluído:**
# - Base analítica com {len(df_base):,} registros (município × ano)
# - PIB Agro: integrado por cod_ibge
# - PPM: pivotado (categorias → colunas)
# - IBAMA: integrado por cod_munici
# - PAM: disponível separadamente (chave_municipio)
# - COMEX: disponível separadamente (nível UF)
# 
# **Próximo:** Sprint 2 - Análises Econômicas
