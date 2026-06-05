"""
Script para download de dados do CEPEA usando biblioteca agrobr
e conversão para formato Parquet seguindo arquitetura medallion.
"""

import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from agrobr import cepea

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Caminhos
CAMINHO_BRONZE = Path("data/01_bronze/cepea")
CAMINHO_SILVER = Path("data/02_silver/cepea")

# Criar diretórios
CAMINHO_BRONZE.mkdir(parents=True, exist_ok=True)
CAMINHO_SILVER.mkdir(parents=True, exist_ok=True)


async def baixar_indicador_cepea(produto: str, data_inicio: str, data_fim: str = None):
    """
    Baixa indicador diário do CEPEA para um produto específico.
    
    Args:
        produto: Nome do produto (soja, milho, boi, café, etc.)
        data_inicio: Data inicial no formato YYYY-MM-DD
        data_fim: Data final no formato YYYY-MM-DD (opcional)
    
    Returns:
        DataFrame com dados do indicador
    """
    logger.info(f"Baixando dados CEPEA para {produto} a partir de {data_inicio}")
    
    try:
        df = await cepea.indicador(produto, inicio=data_inicio, fim=data_fim)
        logger.info(f"Download concluído: {len(df)} registros")
        return df
    except Exception as e:
        logger.error(f"Erro ao baixar dados para {produto}: {e}")
        raise


def salvar_parquet_bronze(df: pd.DataFrame, produto: str):
    """
    Salva DataFrame em formato Parquet na camada Bronze.
    
    Args:
        df: DataFrame com dados
        produto: Nome do produto
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_bronze = CAMINHO_BRONZE / f"{produto}_{timestamp}.parquet"
    
    df.to_parquet(arquivo_bronze, index=False)
    logger.info(f"Salvo na camada Bronze: {arquivo_bronze}")
    return arquivo_bronze


def processar_silver(df: pd.DataFrame, produto: str):
    """
    Processa dados para camada Silver (limpeza e validação).
    
    Args:
        df: DataFrame bruto
        produto: Nome do produto
    
    Returns:
        DataFrame processado
    """
    logger.info("Processando dados para camada Silver")
    
    # Converter colunas de data
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'])
    
    # Remover duplicatas
    df = df.drop_duplicates()
    
    # Ordenar por data
    if 'data' in df.columns:
        df = df.sort_values('data')
    
    # Adicionar metadados
    df['produto'] = produto
    df['fonte'] = 'CEPEA'
    df['data_ingestao'] = datetime.now()
    
    logger.info(f"Processamento concluído: {len(df)} registros")
    return df


def salvar_parquet_silver(df: pd.DataFrame, produto: str):
    """
    Salva DataFrame em formato Parquet na camada Silver.
    
    Args:
        df: DataFrame processado
        produto: Nome do produto
    """
    arquivo_silver = CAMINHO_SILVER / f"{produto}.parquet"
    
    df.to_parquet(arquivo_silver, index=False)
    logger.info(f"Salvo na camada Silver: {arquivo_silver}")
    return arquivo_silver


async def pipeline_cepea(produto: str, data_inicio: str, data_fim: str = None):
    """
    Pipeline completo: download -> bronze -> silver.
    
    Args:
        produto: Nome do produto
        data_inicio: Data inicial
        data_fim: Data final (opcional)
    """
    try:
        # Download
        df = await baixar_indicador_cepea(produto, data_inicio, data_fim)
        
        # Bronze
        arquivo_bronze = salvar_parquet_bronze(df, produto)
        
        # Silver
        df_silver = processar_silver(df, produto)
        arquivo_silver = salvar_parquet_silver(df_silver, produto)
        
        logger.info(f"Pipeline concluído com sucesso para {produto}")
        return arquivo_silver
        
    except Exception as e:
        logger.error(f"Erro no pipeline para {produto}: {e}")
        raise


async def main():
    """
    Função principal - baixa dados para múltiplos produtos.
    """
    # Configuração
    produtos = ['soja', 'milho', 'boi']  # Adicionar mais produtos conforme necessário
    data_inicio = '2020-01-01'
    
    logger.info(f"Iniciando download para {len(produtos)} produtos")
    
    # Baixar para cada produto
    for produto in produtos:
        try:
            await pipeline_cepea(produto, data_inicio)
        except Exception as e:
            logger.error(f"Falha no produto {produto}: {e}")
            continue
    
    logger.info("Download concluído")


if __name__ == "__main__":
    asyncio.run(main())
