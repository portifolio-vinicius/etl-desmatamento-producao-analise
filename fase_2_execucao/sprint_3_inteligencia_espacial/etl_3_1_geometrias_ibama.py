"""
ETL 3.1: Processamento de Geometrias IBAMA
=========================================

Este script carrega o GeoParquet bruto do IBAMA, limpa e padroniza as geometrias,
e salva na camada Silver para análises espaciais (buffers, spillover).

Autor: Pipeline de Análise de Desmatamento
Data: 29/03/2026
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
from datetime import datetime

# Configuração
BASE_DIR = Path(__file__).parent.parent.parent
BRONZE_FILE = BASE_DIR / 'data' / '01_bronze' / 'ibama' / 'ibama_embargos' / 'embargos_ibama_full.geoparquet'
SILVER_DIR = BASE_DIR / 'data' / '02_silver' / 'espacial'
SILVER_FILE = SILVER_DIR / 'embargos_com_geometria.parquet'

# Criar diretório se não existir
SILVER_DIR.mkdir(parents=True, exist_ok=True)

def main():
    print("=" * 80)
    print("ETL 3.1: PROCESSAMENTO DE GEOMETRIAS IBAMA")
    print("=" * 80)
    
    if not BRONZE_FILE.exists():
        print(f"❌ Erro: Arquivo não encontrado em {BRONZE_FILE}")
        return

    # 1. Carregar GeoParquet
    print(f"📂 Carregando {BRONZE_FILE.name}...")
    gdf = gpd.read_parquet(BRONZE_FILE)
    print(f"   Registros totais: {len(gdf):,}")

    # 2. Filtrar geometrias nulas
    print("🧹 Limpando dados...")
    gdf = gdf[gdf['geometry'].notnull()].copy()
    print(f"   Registros com geometria válida: {len(gdf):,}")

    # 3. Padronizar códigos e datas
    print("📅 Padronizando datas e códigos...")
    
    # Extrair ano do embargo
    # O formato original é 'dd/mm/yy HH:MM:SS' ou 'yyyy-mm-dd'
    try:
        gdf['dat_embarg_dt'] = pd.to_datetime(gdf['dat_embarg'], errors='coerce')
        gdf['ano_embargo'] = gdf['dat_embarg_dt'].dt.year
    except Exception as e:
        print(f"   ⚠️ Erro ao converter data: {e}. Tentando formato específico...")
        gdf['dat_embarg_dt'] = pd.to_datetime(gdf['dat_embarg'], format='%d/%m/%y %H:%M:%S', errors='coerce')
        gdf['ano_embargo'] = gdf['dat_embarg_dt'].dt.year

    # Filtrar anos válidos (ex: > 1980)
    gdf = gdf[gdf['ano_embargo'] > 1980].copy()
    gdf['ano_embargo'] = gdf['ano_embargo'].astype(int)

    # 4. Calcular área em Hectares (se necessário)
    # A projeção original costuma ser WGS84 (EPSG:4326). 
    # Para cálculo de área preciso projetar (ex: Sirgas 2000 Albers - EPSG:5880)
    print("📐 Calculando áreas reais (hectares)...")
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    
    # Projetar para cálculo de área (Sirgas 2000 / Brazil Polyconic ou Albers)
    # Usando Sirgas 2000 / Brazil Albers (EPSG:5880) para medição de área em todo o Brasil
    gdf_projets = gdf.to_crs(epsg=5880)
    gdf['area_geo_ha'] = gdf_projets.geometry.area / 10000 # m² to ha
    
    # Comparar com área declarada (qtd_area_e)
    # Preencher nulos de qtd_area_e com area_geo_ha
    gdf['area_final_ha'] = gdf['qtd_area_e'].fillna(gdf['area_geo_ha'])

    # 5. Selecionar colunas relevantes para reduzir tamanho
    cols_relevantes = [
        'num_tad', 'seq_tad', 'cod_munici', 'municipio', 'uf', 
        'ano_embargo', 'dat_embarg', 'sit_desmat', 'area_final_ha', 
        'geometry'
    ]
    gdf_silver = gdf[cols_relevantes].copy()

    # 6. Salvar GeoParquet na camada Silver
    print(f"💾 Salvando em {SILVER_FILE}...")
    gdf_silver.to_parquet(SILVER_FILE, index=False)
    
    print("-" * 60)
    print(f"📊 Resumo Silver:")
    print(f"   - Registros: {len(gdf_silver):,}")
    print(f"   - Municípios: {gdf_silver['cod_munici'].nunique():,}")
    print(f"   - Área total embargada: {gdf_silver['area_final_ha'].sum():,.2f} ha")
    print("=" * 80)
    print("ETL 3.1 CONCLUÍDA COM SUCESSO")

if __name__ == '__main__':
    main()
