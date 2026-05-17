#!/usr/bin/env python
# coding: utf-8

"""
ETL PRODES Silver: Unificação e Padronização de Dados de Desmatamento Anual

Objetivo: Unificar dados PRODES de 2008-2017 com schema padronizado para camada Silver

Entrada: data/01_bronze/prodes/prodes_desmatamento_*.parquet
Saída: data/02_silver/prodes_consolidado.parquet
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import logging
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from utils.caminhos import repo_root
from utils.logging_config import configurar_logging

# Configurar logging
configurar_logging(nome_arquivo="etl_prodes_silver.log", nivel=logging.INFO)
logger = logging.getLogger("etl_prodes_silver")

BASE_DIR = repo_root()
PRODES_BRONZE_DIR = BASE_DIR / 'data' / '01_bronze' / 'prodes'
PRODES_SILVER_DIR = BASE_DIR / 'data' / '02_silver'

# Garantir diretório de saída
PRODES_SILVER_DIR.mkdir(parents=True, exist_ok=True)

logger.info("=" * 60)
logger.info("ETL PRODES SILVER: Iniciando processamento")
logger.info("=" * 60)
logger.info(f"Bronze: {PRODES_BRONZE_DIR}")
logger.info(f"Silver: {PRODES_SILVER_DIR}")

def padronizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza tipos de dados do PRODES.
    
    Args:
        df: DataFrame com dados brutos
        
    Returns:
        DataFrame com tipos padronizados
    """
    # Converter year para inteiro
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    
    # Converter area_km para float
    if 'area_km' in df.columns:
        df['area_km'] = pd.to_numeric(df['area_km'], errors='coerce')
    
    # Converter image_date para datetime
    if 'image_date' in df.columns:
        df['image_date'] = pd.to_datetime(df['image_date'], errors='coerce')
    
    # Converter publish_year para datetime
    if 'publish_year' in df.columns:
        df['publish_year'] = pd.to_datetime(df['publish_year'], errors='coerce')
    
    # Padronizar campos de texto
    campos_texto = ['state', 'main_class', 'class_name', 'sub_class', 'satellite', 'sensor']
    for campo in campos_texto:
        if campo in df.columns:
            df[campo] = df[campo].astype(str)
    
    return df


def validar_qualidade(df: pd.DataFrame) -> dict:
    """
    Valida qualidade dos dados PRODES.
    
    Args:
        df: DataFrame para validar
        
    Returns:
        Dicionário com métricas de qualidade
    """
    metricas = {
        'total_registros': len(df),
        'anos_distintos': df['year'].nunique() if 'year' in df.columns else 0,
        'estados_distintos': df['state'].nunique() if 'state' in df.columns else 0,
        'area_total_km': df['area_km'].sum() if 'area_km' in df.columns else 0,
        'nulos_year': df['year'].isnull().sum() if 'year' in df.columns else 0,
        'nulos_area': df['area_km'].isnull().sum() if 'area_km' in df.columns else 0,
        'nulos_state': df['state'].isnull().sum() if 'state' in df.columns else 0,
    }
    
    return metricas


# Leitura e Processamento dos Dados
arquivos_parquet = sorted(PRODES_BRONZE_DIR.glob('prodes_desmatamento_*.parquet'))
arquivos_parquet = [f for f in arquivos_parquet if 'anual' not in f.name]  # Excluir arquivo consolidado antigo

logger.info(f"Encontrados {len(arquivos_parquet)} arquivos Parquet")

chunks_processados = []

for arquivo in arquivos_parquet:
    try:
        logger.info(f"Processando: {arquivo.name}")
        df_chunk = pd.read_parquet(arquivo)
        df_padronizado = padronizar_tipos(df_chunk)
        chunks_processados.append(df_padronizado)
        logger.info(f"  {len(df_padronizado)} registros processados")
        
    except Exception as e:
        logger.error(f"Erro ao processar {arquivo.name}: {e}")

logger.info(f"{len(chunks_processados)} arquivos processados com sucesso")

# Concatenar todos os chunks
logger.info("Concatenando dados...")
df_prodes_completo = pd.concat(chunks_processados, ignore_index=True)
logger.info(f"Total de registros: {len(df_prodes_completo):,}")

# Validar qualidade
logger.info("=" * 60)
logger.info("VALIDAÇÃO DE QUALIDADE")
logger.info("=" * 60)
metricas = validar_qualidade(df_prodes_completo)
for chave, valor in metricas.items():
    logger.info(f"{chave}: {valor}")

# Ordenar por ano e estado
if 'year' in df_prodes_completo.columns and 'state' in df_prodes_completo.columns:
    df_prodes_completo = df_prodes_completo.sort_values(['year', 'state']).reset_index(drop=True)

# Exportar para Silver
output_path = PRODES_SILVER_DIR / 'prodes_consolidado.parquet'
logger.info(f"Exportando: {output_path}")

df_prodes_completo.to_parquet(
    output_path,
    engine='pyarrow',
    compression='snappy',
    index=False
)

tamanho_mb = output_path.stat().st_size / 1024 / 1024
logger.info(f"Exportado! Tamanho: {tamanho_mb:.2f} MB")

logger.info("=" * 60)
logger.info("ETL PRODES SILVER: Concluído com sucesso")
logger.info("=" * 60)
