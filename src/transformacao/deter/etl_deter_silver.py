#!/usr/bin/env python
# coding: utf-8

"""
ETL DETER Silver: Unificação e Padronização de Alertas de Desmatamento

Objetivo: Unificar dados DETER de 2016-2017 com schema padronizado para camada Silver

Entrada: data/01_bronze/deter/deter_alertas_*.parquet
Saída: data/02_silver/deter_consolidado.parquet
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
configurar_logging(nome_arquivo="etl_deter_silver.log", nivel=logging.INFO)
logger = logging.getLogger("etl_deter_silver")

BASE_DIR = repo_root()
DETER_BRONZE_DIR = BASE_DIR / 'data' / '01_bronze' / 'deter'
DETER_SILVER_DIR = BASE_DIR / 'data' / '02_silver'

# Garantir diretório de saída
DETER_SILVER_DIR.mkdir(parents=True, exist_ok=True)

logger.info("=" * 60)
logger.info("ETL DETER SILVER: Iniciando processamento")
logger.info("=" * 60)
logger.info(f"Bronze: {DETER_BRONZE_DIR}")
logger.info(f"Silver: {DETER_SILVER_DIR}")

def padronizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza tipos de dados do DETER.
    
    Args:
        df: DataFrame com dados brutos
        
    Returns:
        DataFrame com tipos padronizados
    """
    # Converter view_date para datetime
    if 'view_date' in df.columns:
        df['view_date'] = pd.to_datetime(df['view_date'], errors='coerce')
    
    # Converter publish_month para datetime
    if 'publish_month' in df.columns:
        df['publish_month'] = pd.to_datetime(df['publish_month'], errors='coerce')
    
    # Converter area_km2 para float
    if 'area_km2' in df.columns:
        df['area_km2'] = pd.to_numeric(df['area_km2'], errors='coerce')
    
    # Converter areauckm para float (se existir)
    if 'areauckm' in df.columns:
        df['areauckm'] = pd.to_numeric(df['areauckm'], errors='coerce')
    
    # Padronizar código de município
    if 'codigo_municipio_ibge' in df.columns:
        df['codigo_municipio_ibge'] = pd.to_numeric(
            df['codigo_municipio_ibge'], 
            errors='coerce'
        ).astype('Int64')
    
    # Padronizar campos de texto
    campos_texto = ['municipio', 'estado', 'classname', 'sensor', 'satellite']
    for campo in campos_texto:
        if campo in df.columns:
            df[campo] = df[campo].astype(str)
    
    return df


def validar_qualidade(df: pd.DataFrame) -> dict:
    """
    Valida qualidade dos dados DETER.
    
    Args:
        df: DataFrame para validar
        
    Returns:
        Dicionário com métricas de qualidade
    """
    metricas = {
        'total_registros': len(df),
        'anos_distintos': df['view_date'].dt.year.nunique() if 'view_date' in df.columns else 0,
        'estados_distintos': df['estado'].nunique() if 'estado' in df.columns else 0,
        'municipios_distintos': df['codigo_municipio_ibge'].nunique() if 'codigo_municipio_ibge' in df.columns else 0,
        'area_total_km2': df['area_km2'].sum() if 'area_km2' in df.columns else 0,
        'nulos_view_date': df['view_date'].isnull().sum() if 'view_date' in df.columns else 0,
        'nulos_area': df['area_km2'].isnull().sum() if 'area_km2' in df.columns else 0,
        'nulos_municipio': df['codigo_municipio_ibge'].isnull().sum() if 'codigo_municipio_ibge' in df.columns else 0,
    }
    
    return metricas


# Leitura e Processamento dos Dados
arquivos_parquet = sorted(DETER_BRONZE_DIR.glob('deter_alertas_*.parquet'))
arquivos_parquet = [f for f in arquivos_parquet if 'diarios' not in f.name]  # Excluir arquivo consolidado antigo

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
df_deter_completo = pd.concat(chunks_processados, ignore_index=True)
logger.info(f"Total de registros: {len(df_deter_completo):,}")

# Validar qualidade
logger.info("=" * 60)
logger.info("VALIDAÇÃO DE QUALIDADE")
logger.info("=" * 60)
metricas = validar_qualidade(df_deter_completo)
for chave, valor in metricas.items():
    logger.info(f"{chave}: {valor}")

# Ordenar por data e estado
if 'view_date' in df_deter_completo.columns and 'estado' in df_deter_completo.columns:
    df_deter_completo = df_deter_completo.sort_values(['view_date', 'estado']).reset_index(drop=True)

# Exportar para Silver
output_path = DETER_SILVER_DIR / 'deter_consolidado.parquet'
logger.info(f"Exportando: {output_path}")

df_deter_completo.to_parquet(
    output_path,
    engine='pyarrow',
    compression='snappy',
    index=False
)

tamanho_mb = output_path.stat().st_size / 1024 / 1024
logger.info(f"Exportado! Tamanho: {tamanho_mb:.2f} MB")

logger.info("=" * 60)
logger.info("ETL DETER SILVER: Concluído com sucesso")
logger.info("=" * 60)
