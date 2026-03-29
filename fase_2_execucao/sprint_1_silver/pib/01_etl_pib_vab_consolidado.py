# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.2: Padronização PIB VAB Agropecuária
# 
# **Objetivo:** Transformar dados do formato "long" (SIDRA/IBGE) para formato consolidado
# 
# **Entrada:** `pib/data/bronze/pib_vab_agro/ano=YYYY/dados.parquet`
# 
# **Saída:** `pib/data/silver/pib_vab_consolidado.parquet`
# 
# **Schema de Saída:**
# - `cod_ibge` (int): Código IBGE do município (7 dígitos)
# - `ano` (int): Ano de referência (2010-2023)
# - `vab_agro_mil_reais` (float): Valor Adicionado Bruto da Agropecuária (R$ mil)

# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import re
from tqdm import tqdm

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
PIB_BRONZE_DIR = BASE_DIR / 'data/01_bronze/pib/pib_vab_agro'
PIB_SILVER_DIR = BASE_DIR / 'data/02_silver'

# Garantir diretório de saída
PIB_SILVER_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Bronze: {PIB_BRONZE_DIR}")
print(f"📂 Silver: {PIB_SILVER_DIR}")

# %% [markdown]
# ## Funções de Transformação

# %%
def extrair_codigo_ibge(d1c_value: str) -> int:
    """
    Extrai os 7 dígitos do código IBGE do campo D1C.
    Exemplo: "Município (Código)|3500105" -> 3500105
    """
    if pd.isna(d1c_value):
        return None
    match = re.search(r'(\d{7})', str(d1c_value))
    return int(match.group(1)) if match else None


def converter_valor(v_value: str) -> float:
    """
    Converte campo V (Valor) de string para float.
    ".." representa valores ausentes.
    """
    if pd.isna(v_value) or v_value == '..':
        return None
    try:
        return float(v_value)
    except (ValueError, TypeError):
        return None


def processar_ano(ano: int, df_ano: pd.DataFrame) -> pd.DataFrame:
    """
    Processa dados de um ano específico.
    """
    # Extrair código IBGE
    df_ano['cod_ibge'] = df_ano['D1C'].apply(extrair_codigo_ibge)
    
    # Converter valor VAB
    df_ano['vab_agro_mil_reais'] = df_ano['V'].apply(converter_valor)
    
    # Selecionar colunas relevantes
    df_processado = df_ano[['cod_ibge', 'vab_agro_mil_reais']].copy()
    df_processado['ano'] = ano
    
    return df_processado

# %% [markdown]
# ## Leitura e Processamento dos Dados

# %%
# Listar todos os diretórios de ano
diretorios_ano = sorted([d for d in PIB_BRONZE_DIR.iterdir() if d.is_dir() and d.name.startswith('ano=')])
print(f"📄 Encontrados {len(diretorios_ano)} anos de dados")

# %%
# Processar cada ano
anos_processados = []

for dir_ano in tqdm(diretorios_ano, desc="Processando anos PIB"):
    try:
        # Extrair ano do nome do diretório
        ano = int(dir_ano.name.split('=')[1])
        
        # Ler arquivo Parquet do ano
        arquivo_parquet = dir_ano / 'dados.parquet'
        if not arquivo_parquet.exists():
            print(f"⚠️ Arquivo não encontrado: {arquivo_parquet}")
            continue
            
        df_ano = pd.read_parquet(arquivo_parquet)
        
        # Processar
        df_processado = processar_ano(ano, df_ano)
        anos_processados.append(df_processado)
        
        print(f"  {ano}: {len(df_processado):,} municípios")
        
    except Exception as e:
        print(f"⚠️ Erro ao processar {dir_ano.name}: {e}")

print(f"\n✅ {len(anos_processados)} anos processados com sucesso")

# %%
# Concatenar todos os anos
print("🔄 Concatenando anos...")
df_pib_completo = pd.concat(anos_processados, ignore_index=True)
print(f"📊 Total de registros: {len(df_pib_completo):,}")

# %%
# Ordenar dados
df_pib_completo = df_pib_completo.sort_values(['cod_ibge', 'ano']).reset_index(drop=True)

# %% [markdown]
# ## Validação da Qualidade

# %%
print("=" * 60)
print("📊 VALIDAÇÃO DA QUALIDADE DOS DADOS")
print("=" * 60)

print(f"\n📋 Shape: {df_pib_completo.shape[0]:,} linhas × {df_pib_completo.shape[1]} colunas")

print("\n🔍 Nulos por coluna:")
nulos = df_pib_completo.isnull().sum()
for col, qtd in nulos.items():
    pct = (qtd / len(df_pib_completo)) * 100
    print(f"  {col}: {qtd:,} ({pct:.2f}%)")

print("\n📅 Amplitude temporal:")
print(f"  Anos: {df_pib_completo['ano'].min()} - {df_pib_completo['ano'].max()}")
print(f"  Anos únicos: {sorted(df_pib_completo['ano'].unique())}")

print("\n🏙️ Municípios:")
print(f"  Total único: {df_pib_completo['cod_ibge'].nunique():,}")

print("\n📊 Estatísticas descritivas (VAB Agro):")
print(df_pib_completo['vab_agro_mil_reais'].describe())

print("\n📊 VAB Total por ano (R$ mil):")
vab_por_ano = df_pib_completo.groupby('ano')['vab_agro_mil_reais'].sum()
for ano, valor in vab_por_ano.items():
    print(f"  {ano}: R$ {valor:,.0f} mil")

# %% [markdown]
# ## Exportação para Camada Silver

# %%
# Definir schema otimizado
schema = pa.schema([
    ('cod_ibge', pa.int64()),
    ('ano', pa.int64()),
    ('vab_agro_mil_reais', pa.float64())
])

# Exportar
output_path = PIB_SILVER_DIR / 'pib_vab_consolidado.parquet'
print(f"\n💾 Exportando para: {output_path}")

# Converter para int64 padrão (remover NA antes)
df_export = df_pib_completo.copy()
df_export['cod_ibge'] = df_export['cod_ibge'].fillna(-1).astype('int64')
df_export['ano'] = df_export['ano'].astype('int64')

table = pa.Table.from_pandas(df_export, schema=schema, preserve_index=False)
pq.write_table(table, output_path, compression='snappy')

print(f"✅ Exportado com sucesso!")
print(f"📦 Tamanho do arquivo: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

# %% [markdown]
# ## Amostra dos Dados

# %%
print("\n📋 Amostra (20 primeiros registros):")
df_pib_completo.head(20)

# %%
# Verificar municípios com série completa
municipios_com_serie_completa = df_pib_completo.groupby('cod_ibge').size()
municipios_com_serie_completa = municipios_com_serie_completa[municipios_com_serie_completa == len(anos_processados)]
print(f"\n🏙️ Municípios com série completa ({len(anos_processados)} anos): {len(municipios_com_serie_completa):,}")

# %% [markdown]
# ## Resumo da ETL 1.2
# 
# ✅ **Concluído:**
# - Leitura de {len(diretorios_ano)} anos (2010-2023)
# - Extração de código IBGE via regex `(\d{{7}})`
# - Conversão de valores ".." → null
# - Consolidação em arquivo único
# - Schema padronizado com 3 colunas
# - Exportação para `pib/data/silver/pib_vab_consolidado.parquet`
# 
# **Próximo passo:** ETL 1.3 - Padronização PPM
