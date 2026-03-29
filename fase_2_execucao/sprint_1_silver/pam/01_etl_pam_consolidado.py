# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # ETL 1.1: Padronização PAM (Produção Agrícola Municipal)
# 
# **Objetivo:** Transformar dados do formato "long" (SIDRA/IBGE) para formato "wide" consolidado
# 
# **Entrada:** `pam/data/bronze/pam/D1C=Município (Código)/*.parquet`
# 
# **Saída:** `pam/data/silver/pam_consolidado.parquet`
# 
# **Nota:** O código IBGE será obtido via API do IBGE na ETL de Dimensão (1.6)

# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import re
from tqdm import tqdm

# Configurar caminhos
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
PAM_BRONZE_DIR = BASE_DIR / 'data/01_bronze/pam/pam/D1C=Município (Código)'
PAM_SILVER_DIR = BASE_DIR / 'data/02_silver'

# Garantir diretório de saída
PAM_SILVER_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Bronze: {PAM_BRONZE_DIR}")
print(f"📂 Silver: {PAM_SILVER_DIR}")

# %% [markdown]
# ## Funções de Transformação

# %%
def extrair_ano(d3c_value: str) -> int:
    """Extrai o ano do campo D3C."""
    if pd.isna(d3c_value) or str(d3c_value) == 'Ano (Código)':
        return None
    match = re.search(r'(\d{4})', str(d3c_value))
    return int(match.group(1)) if match else None


def converter_valor(v_value: str) -> float:
    """Converte campo V (Valor) de string para float."""
    if pd.isna(v_value) or v_value == '..':
        return None
    try:
        return float(v_value)
    except (ValueError, TypeError):
        return None


def mapear_variavel(d2n_value: str) -> str:
    """Mapeia o código D2N para nome da variável."""
    mapping = {
        'Área plantada': 'area_plantada_ha',
        'Área plantada - percentual do total geral': 'area_plantada_pct',
        'Área colhida': 'area_colhida_ha',
        'Área colhida - percentual do total geral': 'area_colhida_pct',
        'Quantidade produzida': 'producao_toneladas',
        'Rendimento médio da produção': 'rendimento_medio_kg_ha',
        'Valor da produção': 'valor_producao_mil_reais',
        'Valor da produção - percentual do total geral': 'valor_producao_pct',
        'Área destinada à colheita': 'area_destinada_colheita_ha',
        'Área destinada à colheita - percentual do total geral': 'area_destinada_colheita_pct'
    }
    return mapping.get(str(d2n_value), str(d2n_value).replace(' ', '_').replace('-', '').lower())


def limpar_dados(df_chunk: pd.DataFrame) -> pd.DataFrame:
    """Limpa dados removendo linhas de cabeçalho."""
    # Filtrar linhas de dados (remover cabeçalhos codificados)
    df_limpo = df_chunk[~df_chunk['D3C'].str.contains('Ano\\s*\\(Código\\)', regex=True, na=False)].copy()
    df_limpo = df_limpo[~df_limpo['NC'].str.contains('Nível', na=False)].copy()
    df_limpo = df_limpo[df_limpo['D1N'] != 'Município'].copy()
    return df_limpo


def extrair_municipio_uf(d1n_value: str) -> tuple:
    """
    Extrai nome do município e UF do campo D1N.
    Ex: "Alta Floresta D'Oeste - RO" -> ("Alta Floresta D'Oeste", "RO")
    """
    if pd.isna(d1n_value):
        return (None, None)
    partes = str(d1n_value).rsplit(' - ', 1)
    if len(partes) == 2:
        return (partes[0].strip(), partes[1].strip())
    return (str(d1n_value).strip(), None)

# %% [markdown]
# ## Leitura e Processamento dos Dados

# %%
arquivos_parquet = list(PAM_BRONZE_DIR.glob('*.parquet'))
print(f"📄 Encontrados {len(arquivos_parquet)} arquivos Parquet")

# %%
chunks_processados = []

for arquivo in tqdm(arquivos_parquet, desc="Processando chunks PAM"):
    try:
        df_chunk = pd.read_parquet(arquivo)
        df_limpo = limpar_dados(df_chunk)
        
        if len(df_limpo) == 0:
            continue
        
        # Extrair município e UF
        df_limpo[['municipio', 'uf']] = pd.DataFrame(
            df_limpo['D1N'].apply(extrair_municipio_uf).tolist(),
            index=df_limpo.index
        )
        
        # Extrair ano
        df_limpo['ano'] = df_limpo['D3C'].apply(extrair_ano)
        
        # Tipo de lavoura
        df_limpo['tipo_lavoura'] = 'Temporária'  # Simplificação - todos neste diretório são temporários
        
        # Produto
        df_limpo['produto'] = df_limpo['D4N']
        
        # Variável
        df_limpo['variavel'] = df_limpo['D2N'].apply(mapear_variavel)
        
        # Valor
        df_limpo['valor'] = df_limpo['V'].apply(converter_valor)
        
        # Criar chave única município+UF
        df_limpo['chave_municipio'] = df_limpo['municipio'] + '_' + df_limpo['uf']
        
        df_processado = df_limpo[['chave_municipio', 'municipio', 'uf', 'ano', 'tipo_lavoura', 'produto', 'variavel', 'valor']].copy()
        chunks_processados.append(df_processado)
        
    except Exception as e:
        print(f"⚠️ Erro ao processar {arquivo.name}: {e}")

print(f"\n✅ {len(chunks_processados)} chunks processados com sucesso")

# %%
print("🔄 Concatenando chunks...")
df_pam_completo = pd.concat(chunks_processados, ignore_index=True)
print(f"📊 Total de registros: {len(df_pam_completo):,}")

# %% [markdown]
# ## Pivotamento: Long → Wide

# %%
print("🔄 Pivotando dados (Long → Wide)...")

df_wide = df_pam_completo.pivot_table(
    index=['chave_municipio', 'municipio', 'uf', 'ano', 'tipo_lavoura', 'produto'],
    columns='variavel',
    values='valor',
    aggfunc='first'
).reset_index()

df_wide.columns.name = None

print(f"📊 Registros após pivotamento: {len(df_wide):,}")

# %%
# Ajustar tipos
print("🔧 Ajustando tipos de dados...")

df_wide['ano'] = df_wide['ano'].astype('Int64')

colunas_valor = ['area_plantada_ha', 'area_colhida_ha', 'producao_toneladas', 'valor_producao_mil_reais']
for col in colunas_valor:
    if col in df_wide.columns:
        df_wide[col] = df_wide[col].astype('float64')

df_wide = df_wide.sort_values(['chave_municipio', 'ano', 'produto']).reset_index(drop=True)

# %% [markdown]
# ## Validação

# %%
print("=" * 60)
print("📊 VALIDAÇÃO")
print("=" * 60)

print(f"\n📋 Shape: {df_wide.shape[0]:,} × {df_wide.shape[1]}")

print("\n🔍 Nulos:")
for col in df_wide.columns:
    nulos = df_wide[col].isnull().sum()
    if nulos > 0:
        print(f"  {col}: {nulos:,} ({nulos/len(df_wide)*100:.1f}%)")

print("\n📅 Anos:", sorted(df_wide['ano'].dropna().unique().astype(int)))
print("🏙️ Municípios únicos:", df_wide['chave_municipio'].nunique())
print("🌱 Produtos únicos:", df_wide['produto'].nunique())

# %% [markdown]
# ## Exportação

# %%
# Criar schema dinâmico baseado nas colunas reais
colunas_schema = []
for col in df_wide.columns:
    if col in ['ano']:
        colunas_schema.append((col, pa.int64()))
    elif col in ['chave_municipio', 'municipio', 'uf', 'tipo_lavoura', 'produto']:
        colunas_schema.append((col, pa.string()))
    else:
        colunas_schema.append((col, pa.float64()))

schema = pa.schema(colunas_schema)

output_path = PAM_SILVER_DIR / 'pam_consolidado.parquet'
print(f"\n💾 Exportando: {output_path}")
print(f"📋 Colunas no schema: {schema.names}")

df_export = df_wide.copy()
df_export['ano'] = df_export['ano'].fillna(-1).astype('int64')

table = pa.Table.from_pandas(df_export, schema=schema, preserve_index=False)
pq.write_table(table, output_path, compression='snappy')

print(f"✅ Exportado! Tamanho: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

# %%
print("\n📋 Amostra:")
df_wide.head(15)
