"""
Script otimizado para download de dados do CEPEA usando biblioteca agrobr
com paralelismo e chunking para acelerar o processo.

Recursos computacionais:
- 8 CPUs disponíveis
- 11GB RAM (limitada, cuidado com uso de memória)
- Estratégia: downloads paralelos com rate limiting + chunking por período
"""

import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
from agrobr import cepea
from concurrent.futures import ProcessPoolExecutor
import os

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

# Configurações de paralelismo
MAX_CONCURRENT_DOWNLOADS = 3  # Limite de downloads simultâneos (rate limiting)
MAX_WORKERS = 4  # Workers para processamento CPU-bound (Silver)
CHUNK_SIZE_YEARS = 1  # Tamanho do chunk em anos


def dividir_periodo_em_chunks(data_inicio: str, data_fim: str, anos_por_chunk: int = 1):
    """
    Divide um período em chunks menores para download paralelo.
    
    Args:
        data_inicio: Data inicial (YYYY-MM-DD)
        data_fim: Data final (YYYY-MM-DD)
        anos_por_chunk: Quantos anos por chunk
    
    Returns:
        Lista de tuplas (inicio, fim) para cada chunk
    """
    inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
    fim = datetime.strptime(data_fim, "%Y-%m-%d")
    
    chunks = []
    chunk_inicio = inicio
    
    while chunk_inicio < fim:
        chunk_fim = min(chunk_inicio + timedelta(days=365 * anos_por_chunk), fim)
        chunks.append((
            chunk_inicio.strftime("%Y-%m-%d"),
            chunk_fim.strftime("%Y-%m-%d")
        ))
        chunk_inicio = chunk_fim + timedelta(days=1)
    
    return chunks


async def baixar_indicador_cepea_com_rate_limit(
    produto: str, 
    data_inicio: str, 
    data_fim: str = None,
    semaforo: asyncio.Semaphore = None
):
    """
    Baixa indicador diário do CEPEA com rate limiting.
    
    Args:
        produto: Nome do produto
        data_inicio: Data inicial
        data_fim: Data final (opcional)
        semaforo: Semaphore para rate limiting
    
    Returns:
        DataFrame com dados
    """
    if semaforo:
        async with semaforo:
            return await _baixar_indicador_cepea(produto, data_inicio, data_fim)
    else:
        return await _baixar_indicador_cepea(produto, data_inicio, data_fim)


async def _baixar_indicador_cepea(produto: str, data_inicio: str, data_fim: str = None):
    """
    Função interna de download sem rate limiting.
    """
    logger.info(f"Baixando dados CEPEA para {produto}: {data_inicio} a {data_fim or 'atual'}")
    
    try:
        df = await cepea.indicador(produto, inicio=data_inicio, fim=data_fim)
        logger.info(f"Download concluído: {len(df)} registros")
        return df
    except Exception as e:
        logger.error(f"Erro ao baixar dados para {produto} ({data_inicio}): {e}")
        raise


def salvar_parquet_bronze(df: pd.DataFrame, produto: str, chunk_id: str = None):
    """
    Salva DataFrame em formato Parquet na camada Bronze.
    
    Args:
        df: DataFrame com dados
        produto: Nome do produto
        chunk_id: Identificador do chunk (opcional)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if chunk_id:
        arquivo_bronze = CAMINHO_BRONZE / f"{produto}_chunk_{chunk_id}_{timestamp}.parquet"
    else:
        arquivo_bronze = CAMINHO_BRONZE / f"{produto}_{timestamp}.parquet"
    
    df.to_parquet(arquivo_bronze, index=False)
    logger.info(f"Salvo na camada Bronze: {arquivo_bronze}")
    return arquivo_bronze


def processar_silver(df: pd.DataFrame, produto: str):
    """
    Processa dados para camada Silver (limpeza e validação).
    Esta função é CPU-bound, pode ser paralelizada.
    
    Args:
        df: DataFrame bruto
        produto: Nome do produto
    
    Returns:
        DataFrame processado
    """
    logger.info(f"Processando dados para camada Silver: {len(df)} registros")
    
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


def consolidar_chunks_silver(produto: str):
    """
    Consolida múltiplos chunks da camada Bronze em um único arquivo Silver.
    
    Args:
        produto: Nome do produto
    """
    logger.info(f"Consolidando chunks para {produto}")
    
    # Listar todos os chunks do produto
    chunks = list(CAMINHO_BRONZE.glob(f"{produto}_chunk_*.parquet"))
    
    if not chunks:
        logger.warning(f"Nenhum chunk encontrado para {produto}")
        return None
    
    # Ler e concatenar todos os chunks
    dfs = []
    for chunk in chunks:
        df = pd.read_parquet(chunk)
        dfs.append(df)
        logger.info(f"Lido chunk: {chunk.name} ({len(df)} registros)")
    
    # Concatenar
    df_consolidado = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total consolidado: {len(df_consolidado)} registros")
    
    # Processar Silver
    df_silver = processar_silver(df_consolidado, produto)
    
    # Salvar
    arquivo_silver = salvar_parquet_silver(df_silver, produto)
    
    # Opcional: remover chunks após consolidação
    # for chunk in chunks:
    #     chunk.unlink()
    
    return arquivo_silver


async def baixar_produto_com_chunks(
    produto: str,
    data_inicio: str,
    data_fim: str = None,
    anos_por_chunk: int = CHUNK_SIZE_YEARS,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS
):
    """
    Baixa dados de um produto dividindo em chunks e usando paralelismo.
    
    Args:
        produto: Nome do produto
        data_inicio: Data inicial
        data_fim: Data final (opcional)
        anos_por_chunk: Anos por chunk
        max_concurrent: Máximo de downloads simultâneos
    """
    if not data_fim:
        data_fim = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Iniciando download para {produto} com chunking ({anos_por_chunk} anos/chunk)")
    
    # Dividir período em chunks
    chunks = dividir_periodo_em_chunks(data_inicio, data_fim, anos_por_chunk)
    logger.info(f"Período dividido em {len(chunks)} chunks")
    
    # Criar semáforo para rate limiting
    semaforo = asyncio.Semaphore(max_concurrent)
    
    # Criar tarefas para cada chunk
    tarefas = []
    for i, (chunk_inicio, chunk_fim) in enumerate(chunks):
        chunk_id = f"{i+1}_{chunk_inicio}_{chunk_fim}"
        tarefa = baixar_indicador_cepea_com_rate_limit(
            produto, chunk_inicio, chunk_fim, semaforo
        )
        tarefas.append((tarefa, chunk_id))
    
    # Executar downloads em paralelo
    resultados = []
    for tarefa, chunk_id in tarefas:
        try:
            df = await tarefa
            # Salvar chunk imediatamente
            salvar_parquet_bronze(df, produto, chunk_id)
            resultados.append(df)
        except Exception as e:
            logger.error(f"Falha no chunk {chunk_id}: {e}")
            continue
    
    logger.info(f"Download concluído para {produto}: {len(resultados)} chunks baixados")
    
    # Consolidar chunks em Silver
    arquivo_silver = consolidar_chunks_silver(produto)
    
    return arquivo_silver


async def baixar_multiplos_produtos_paralelo(
    produtos: list,
    data_inicio: str,
    data_fim: str = None,
    max_concurrent_produtos: int = 2
):
    """
    Baixa dados para múltiplos produtos em paralelo.
    
    Args:
        produtos: Lista de produtos
        data_inicio: Data inicial
        data_fim: Data final (opcional)
        max_concurrent_produtos: Máximo de produtos simultâneos
    """
    logger.info(f"Iniciando download paralelo para {len(produtos)} produtos")
    
    # Criar semáforo para produtos
    semaforo_produtos = asyncio.Semaphore(max_concurrent_produtos)
    
    async def baixar_com_limitacao(produto):
        async with semaforo_produtos:
            return await baixar_produto_com_chunks(produto, data_inicio, data_fim)
    
    # Executar em paralelo
    tarefas = [baixar_com_limitacao(p) for p in produtos]
    resultados = await asyncio.gather(*tarefas, return_exceptions=True)
    
    # Reportar resultados
    for produto, resultado in zip(produtos, resultados):
        if isinstance(resultado, Exception):
            logger.error(f"Falha no produto {produto}: {resultado}")
        else:
            logger.info(f"Sucesso no produto {produto}: {resultado}")
    
    logger.info("Download paralelo concluído")


async def main():
    """
    Função principal com configurações otimizadas.
    """
    # Configuração
    produtos = ['soja', 'milho', 'boi', 'cafe']  # Adicionar mais conforme necessário
    data_inicio = '2020-01-01'
    data_fim = '2024-12-31'  # Opcional, padrão é data atual
    
    logger.info(f"=== Configuração ===")
    logger.info(f"Produtos: {produtos}")
    logger.info(f"Período: {data_inicio} a {data_fim}")
    logger.info(f"CPUs disponíveis: {os.cpu_count()}")
    logger.info(f"Max downloads simultâneos: {MAX_CONCURRENT_DOWNLOADS}")
    logger.info(f"Max produtos simultâneos: 2")
    logger.info(f"Chunk size: {CHUNK_SIZE_YEARS} anos")
    logger.info(f"===================")
    
    # Executar download paralelo
    await baixar_multiplos_produtos_paralelo(
        produtos=produtos,
        data_inicio=data_inicio,
        data_fim=data_fim,
        max_concurrent_produtos=2
    )
    
    logger.info("Pipeline concluído")


if __name__ == "__main__":
    asyncio.run(main())
