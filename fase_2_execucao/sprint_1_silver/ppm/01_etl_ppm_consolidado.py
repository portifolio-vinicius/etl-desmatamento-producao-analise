# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.3: Padronização PPM (Pecuária Municipal)
# 
# **Objetivo:** Consolidar efetivo de rebanhos por município e categoria
# 
# **Entrada:** `ppm/data/bronze/ppm_*/ano=YYYY/*.parquet`
# 
# **Saída:** `ppm/data/silver/ppm_consolidado.parquet`
# 
# **Schema de Saída:**
# - `cod_ibge` (int): Código IBGE do município (7 dígitos)
# - `ano` (int): Ano de referência (2021-2024)
# - `categoria` (str): Tipo de rebanho (Bovino, Suíno, Caprino, etc.)
# - `efetivo_cabecas` (int): Quantidade de cabeças

# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import re
from tqdm import tqdm

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
PPM_BRONZE_DIR = BASE_DIR / 'data/01_bronze/ppm'
PPM_SILVER_DIR = BASE_DIR / 'data/02_silver'

# Garantir diretório de saída
PPM_SILVER_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Bronze: {PPM_BRONZE_DIR}")
print(f"📂 Silver: {PPM_SILVER_DIR}")

# %% [markdown]
# ## Funções de Transformação

# %%
def extrair_codigo_ibge(d1c_value: str) -> int:
    """
    Extrai os 7 dígitos do código IBGE do campo D1C.
    """
    if pd.isna(d1c_value):
        return None
    match = re.search(r'(\d{7})', str(d1c_value))
    return int(match.group(1)) if match else None


def converter_efetivo(v_value: str) -> int:
    """
    Converte campo V (Efetivo) de string para int.
    ".." representa valores ausentes.
    """
    if pd.isna(v_value) or v_value == '..':
        return None
    try:
        return int(float(v_value))
    except (ValueError, TypeError):
        return None


def extrair_categoria(nome_categoria: str) -> str:
    """
    Extrai nome limpo da categoria.
    Exemplo: "Bovino, leiteiro, ordenhadas" -> "Bovino"
    """
    if pd.isna(nome_categoria):
        return "Desconhecido"
    # Pegar apenas a primeira parte (antes da vírgula)
    return str(nome_categoria).split(',')[0].strip()


def processar_categoria(categoria_nome: str, df_categoria: pd.DataFrame) -> pd.DataFrame:
    """
    Processa dados de uma categoria de rebanho.
    """
    # Extrair código IBGE
    df_categoria['cod_ibge'] = df_categoria['D1C'].apply(extrair_codigo_ibge)
    
    # Extrair ano do campo D3C
    df_categoria['ano'] = df_categoria['D3C'].apply(
        lambda x: int(re.search(r'(\d{4})', str(x)).group(1)) if pd.notna(x) and re.search(r'(\d{4})', str(x)) else None
    )
    
    # Converter efetivo
    df_categoria['efetivo_cabecas'] = df_categoria['V'].apply(converter_efetivo)
    
    # Adicionar categoria
    df_categoria['categoria'] = categoria_nome
    
    # Selecionar colunas relevantes
    df_processado = df_categoria[['cod_ibge', 'ano', 'categoria', 'efetivo_cabecas']].copy()
    
    return df_processado

# %% [markdown]
# ## Leitura e Processamento dos Dados

# %%
# Listar todas as categorias de rebanho
categorias = sorted([d.name for d in PPM_BRONZE_DIR.iterdir() if d.is_dir() and d.name.startswith('ppm_')])
print(f"📄 Encontradas {len(categorias)} categorias de rebanho:")
for cat in categorias:
    print(f"  - {cat}")

# %%
# Processar cada categoria
categorias_processadas = []
estatísticas_por_categoria = []

for categoria in tqdm(categorias, desc="Processando categorias PPM"):
    try:
        dir_categoria = PPM_BRONZE_DIR / categoria
        
        # Listar todos os anos dentro da categoria
        diretorios_ano = [d for d in dir_categoria.iterdir() if d.is_dir() and d.name.startswith('ano=')]
        
        if not diretorios_ano:
            print(f"  ⚠️ Sem dados para {categoria}")
            continue
        
        # Ler todos os anos da categoria
        dfs_ano = []
        for dir_ano in diretorios_ano:
            ano = int(dir_ano.name.split('=')[1])
            
            # Listar arquivos parquet dentro do ano
            arquivos_parquet = list(dir_ano.glob('*.parquet'))
            for arquivo in arquivos_parquet:
                df = pd.read_parquet(arquivo)
                df['ano'] = ano
                dfs_ano.append(df)
        
        if not dfs_ano:
            continue
            
        # Concatenar todos os anos da categoria
        df_categoria_completo = pd.concat(dfs_ano, ignore_index=True)
        
        # Processar
        df_processado = processar_categoria(categoria, df_categoria_completo)
        categorias_processadas.append(df_processado)
        
        # Estatísticas
        total_efetivo = df_processado['efetivo_cabecas'].sum()
        municipios = df_processado['cod_ibge'].nunique()
        estatísticas_por_categoria.append({
            'categoria': categoria,
            'municipios': municipios,
            'total_efetivo': total_efetivo,
            'anos': len(diretorios_ano)
        })
        
        print(f"  {categoria}: {municipios:,} municípios, {total_efetivo:,.0f} cabeças")
        
    except Exception as e:
        print(f"⚠️ Erro ao processar {categoria}: {e}")

print(f"\n✅ {len(categorias_processadas)} categorias processadas com sucesso")

# %%
# Mostrar estatísticas consolidadas
print("\n📊 Resumo por categoria:")
df_stats = pd.DataFrame(estatísticas_por_categoria)
print(df_stats.to_string(index=False))

# %%
# Concatenar todas as categorias
print("\n🔄 Concatenando categorias...")
df_ppm_completo = pd.concat(categorias_processadas, ignore_index=True)
print(f"📊 Total de registros: {len(df_ppm_completo):,}")

# %%
# Ordenar dados
df_ppm_completo = df_ppm_completo.sort_values(['cod_ibge', 'ano', 'categoria']).reset_index(drop=True)

# %% [markdown]
# ## Validação da Qualidade

# %%
print("=" * 60)
print("📊 VALIDAÇÃO DA QUALIDADE DOS DADOS")
print("=" * 60)

print(f"\n📋 Shape: {df_ppm_completo.shape[0]:,} linhas × {df_ppm_completo.shape[1]} colunas")

print("\n🔍 Nulos por coluna:")
nulos = df_ppm_completo.isnull().sum()
for col, qtd in nulos.items():
    pct = (qtd / len(df_ppm_completo)) * 100
    print(f"  {col}: {qtd:,} ({pct:.2f}%)")

print("\n📅 Amplitude temporal:")
print(f"  Anos: {df_ppm_completo['ano'].min()} - {df_ppm_completo['ano'].max()}")
print(f"  Anos únicos: {sorted(df_ppm_completo['ano'].unique())}")

print("\n🏙️ Municípios:")
print(f"  Total único: {df_ppm_completo['cod_ibge'].nunique():,}")

print("\n🐄 Categorias:")
print(f"  Total: {df_ppm_completo['categoria'].nunique()}")
print(f"  Categorias: {sorted(df_ppm_completo['categoria'].unique())}")

print("\n📊 Efetivo total por categoria (2024):")
df_2024 = df_ppm_completo[df_ppm_completo['ano'] == 2024]
for cat in df_2024['categoria'].unique():
    total = df_2024[df_2024['categoria'] == cat]['efetivo_cabecas'].sum()
    print(f"  {cat}: {total:,.0f} cabeças")

# %% [markdown]
# ## Exportação para Camada Silver

# %%
# Definir schema otimizado
schema = pa.schema([
    ('cod_ibge', pa.int64()),
    ('ano', pa.int64()),
    ('categoria', pa.string()),
    ('efetivo_cabecas', pa.int64())
])

# Exportar
output_path = PPM_SILVER_DIR / 'ppm_consolidado.parquet'
print(f"\n💾 Exportando para: {output_path}")

# Converter para tipos não-nullable
df_export = df_ppm_completo.copy()
df_export['cod_ibge'] = df_export['cod_ibge'].fillna(-1).astype('int64')
df_export['ano'] = df_export['ano'].fillna(-1).astype('int64')
df_export['efetivo_cabecas'] = df_export['efetivo_cabecas'].fillna(0).astype('int64')

table = pa.Table.from_pandas(df_export, schema=schema, preserve_index=False)
pq.write_table(table, output_path, compression='snappy')

print(f"✅ Exportado com sucesso!")
print(f"📦 Tamanho do arquivo: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

# %% [markdown]
# ## Amostra dos Dados

# %%
print("\n📋 Amostra (30 primeiros registros):")
df_ppm_completo.head(30)

# %%
# Verificar bovinos (categoria mais importante)
df_bovinos = df_ppm_completo[df_ppm_completo['categoria'].str.contains('Bovino', case=False, na=False)]
print(f"\n🐄 Bovinos - Total de registros: {len(df_bovinos):,}")
print(f"🐄 Bovinos - Total de cabeças (2024): {df_bovinos[df_bovinos['ano']==2024]['efetivo_cabecas'].sum():,.0f}")

# %% [markdown]
# ## Resumo da ETL 1.3
# 
# ✅ **Concluído:**
# - Leitura de {len(categorias)} categorias de rebanho
# - Extração de código IBGE via regex `(\d{{7}})`
# - Conversão de efetivo ".." → null
# - Consolidação de todas as categorias
# - Schema padronizado com 4 colunas
# - Exportação para `ppm/data/silver/ppm_consolidado.parquet`
# 
# **Próximo passo:** ETL 1.4 - Padronização IBAMA
