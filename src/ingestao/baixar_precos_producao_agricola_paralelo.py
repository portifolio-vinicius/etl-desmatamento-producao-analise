"""
Download otimizado de preços e produção agrícola com paralelismo.

Este script baixa dados de múltiplas fontes (CONAB, estimativas de preços)
de forma paralela, respeitando limites computacionais (8 CPUs, 11GB RAM).

Fontes:
- CONAB série histórica: Produção por UF-safra (via agrobr)
- Estimativas de preços: Médias anuais baseadas em Farmnews e variações

Estratégia de paralelismo:
- Downloads simultâneos de múltiplos produtos (max 3)
- Rate limiting para evitar bloqueios
- Salvamento imediato em chunks (Parquet)
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações de paralelismo (otimizadas para 8 CPUs, 11GB RAM)
MAX_CONCURRENT_DOWNLOADS = 3  # Limite de downloads simultâneos
MAX_THREADS = 4  # Limite de threads para processamento

# Caminhos
DIR_BRONZE = Path("data/01_bronze/precos_producao")
DIR_SILVER = Path("data/02_silver/precos_producao")
DIR_BRONZE.mkdir(parents=True, exist_ok=True)
DIR_SILVER.mkdir(parents=True, exist_ok=True)

# Produtos para download
PRODUTOS_CONAB = ['soja', 'milho', 'trigo', 'arroz', 'algodao']
PERIODO_ANOS = list(range(2020, 2024))  # 2020-2023


async def baixar_serie_historica_conab(produto: str, inicio: int, fim: int) -> pd.DataFrame:
    """
    Baixa série histórica da CONAB para um produto.
    
    Args:
        produto: Nome do produto (ex: 'soja')
        inicio: Ano inicial
        fim: Ano final
        
    Returns:
        DataFrame com dados de produção por UF-safra
    """
    from agrobr import conab
    
    logger.info(f"Baixando CONAB série histórica: {produto} ({inicio}-{fim})")
    
    try:
        df = await conab.serie_historica(produto, inicio=inicio, fim=fim)
        logger.info(f"✓ CONAB {produto}: {len(df)} registros")
        return df
    except Exception as e:
        logger.error(f"✗ Erro CONAB {produto}: {e}")
        return pd.DataFrame()


def salvar_parquet_bronze(df: pd.DataFrame, produto: str, tipo: str) -> Path:
    """
    Salva DataFrame na camada Bronze.
    
    Args:
        df: DataFrame a salvar
        produto: Nome do produto
        tipo: Tipo de dado ('producao' ou 'precos')
        
    Returns:
        Caminho do arquivo salvo
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para {produto}_{tipo}")
        return None
    
    arquivo = DIR_BRONZE / f"{produto}_{tipo}.parquet"
    df.to_parquet(arquivo, index=False)
    logger.info(f"Salvo Bronze: {arquivo}")
    return arquivo


def criar_estimativas_precos_anuais() -> pd.DataFrame:
    """
    Cria estimativas de preços médios anuais baseadas em múltiplas fontes.
    
    Fontes:
    - Farmnews: Dados confirmados 2023-2024
    - Variações percentuais: Para estimar 2020-2022
    - Recorde mensal: Para calibrar estimativas
    
    Returns:
        DataFrame com preços médios anuais por produto
    """
    logger.info("Criando estimativas de preços anuais")
    
    # Dados confirmados do Farmnews
    dados_confirmados = {
        2023: {
            'milho': 66.0,  # R$/saca
            'boi_gordo': 255.1,  # R$/arroba
        },
        2024: {
            'milho': 64.2,  # R$/saca
            'boi_gordo': 258.0,  # R$/arroba
        }
    }
    
    # Estimativas baseadas em variações percentuais
    # Milho: 2023 teve queda de 25.1% vs 2022
    # Soja: 2024 teve queda de 11% vs 2023, recorde em 2021-2022
    estimativas = {
        2020: {
            'milho': 75.0,  # Estimativa conservadora
            'soja': 140.0,  # Estimativa baseada em tendência
            'boi_gordo': 220.0,  # Estimativa conservadora
        },
        2021: {
            'milho': 88.0,  # Período de alta
            'soja': 160.0,  # Recorde mensal R$ 177 em abril
            'boi_gordo': 280.0,  # Período de alta
        },
        2022: {
            'milho': 88.1,  # Cálculo: 66.0 / 0.749
            'soja': 175.0,  # Recorde mensal R$ 179 em janeiro
            'boi_gordo': 300.0,  # Pico do ciclo
        },
        2023: dados_confirmados[2023],
        2024: dados_confirmados[2024],
    }
    
    # Converter para DataFrame
    registros = []
    for ano, precos in estimativas.items():
        for produto, valor in precos.items():
            registros.append({
                'ano': ano,
                'produto': produto,
                'preco_medio_rs': valor,
                'unidade': 'saca' if produto in ['milho', 'soja'] else 'arroba',
                'fonte': 'farmnews_estimado' if ano < 2023 else 'farmnews_confirmado'
            })
    
    df = pd.DataFrame(registros)
    logger.info(f"✓ Estimativas criadas: {len(df)} registros")
    return df


def processar_silver_producao(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Processa dados de produção para camada Silver.
    
    Transformações:
    - Padronização de colunas
    - Cálculo de métricas derivadas
    - Filtragem de dados inválidos
    
    Args:
        df_bronze: DataFrame da camada Bronze
        
    Returns:
        DataFrame processado para Silver
    """
    if df_bronze.empty:
        return df_bronze
    
    logger.info(f"Processando produção para Silver: {len(df_bronze)} registros")
    
    # Padronizar colunas
    df = df_bronze.copy()
    
    # Extrair ano da safra (formato "2020/21" -> 2020)
    if 'safra' in df.columns:
        df['ano_safra'] = df['safra'].str.split('/').str[0].astype(int)
    
    # Calcular produtividade se não existir
    if 'producao_mil_ton' in df.columns and 'area_plantada_mil_ha' in df.columns:
        df['produtividade_kg_ha'] = (
            df['producao_mil_ton'] * 1000 / df['area_plantada_mil_ha'] * 1000
        )
    
    logger.info(f"✓ Processamento concluído: {len(df)} registros")
    return df


def consolidar_silver() -> pd.DataFrame:
    """
    Consolida todos os dados da camada Silver em um único DataFrame.
    
    Returns:
        DataFrame consolidado com todos os produtos
    """
    logger.info("Consolidando dados da camada Silver")
    
    arquivos = list(DIR_SILVER.glob("*.parquet"))
    if not arquivos:
        logger.warning("Nenhum arquivo encontrado na camada Silver")
        return pd.DataFrame()
    
    dfs = []
    for arquivo in arquivos:
        try:
            df = pd.read_parquet(arquivo)
            df['fonte_arquivo'] = arquivo.stem
            dfs.append(df)
            logger.info(f"Lido: {arquivo.name} ({len(df)} registros)")
        except Exception as e:
            logger.error(f"Erro ao ler {arquivo}: {e}")
    
    if not dfs:
        return pd.DataFrame()
    
    df_consolidado = pd.concat(dfs, ignore_index=True)
    logger.info(f"✓ Consolidado: {len(df_consolidado)} registros")
    return df_consolidado


async def baixar_produto_conab_com_rate_limit(
    produto: str,
    semaforo: asyncio.Semaphore
) -> Tuple[str, pd.DataFrame]:
    """
    Baixa dados CONAB com rate limiting.
    
    Args:
        produto: Nome do produto
        semaforo: Semaphore para controle de concorrência
        
    Returns:
        Tupla (produto, DataFrame)
    """
    async with semaforo:
        df = await baixar_serie_historica_conab(
            produto,
            min(PERIODO_ANOS),
            max(PERIODO_ANOS)
        )
        return produto, df


async def baixar_multiplos_produtos_conab() -> Dict[str, pd.DataFrame]:
    """
    Baixa dados CONAB para múltiplos produtos em paralelo.
    
    Returns:
        Dicionário {produto: DataFrame}
    """
    logger.info(f"Iniciando download CONAB paralelo: {len(PRODUTOS_CONAB)} produtos")
    
    semaforo = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    tarefas = [
        baixar_produto_conab_com_rate_limit(produto, semaforo)
        for produto in PRODUTOS_CONAB
    ]
    
    resultados = await asyncio.gather(*tarefas)
    
    dados_por_produto = {produto: df for produto, df in resultados}
    
    logger.info(f"✓ Download CONAB concluído: {len(dados_por_produto)} produtos")
    return dados_por_produto


def processar_produto_thread(
    produto: str,
    df: pd.DataFrame,
    executor: ThreadPoolExecutor
) -> None:
    """
    Processa um produto em thread separada.
    
    Args:
        produto: Nome do produto
        df: DataFrame com dados brutos
        executor: ThreadPoolExecutor
    """
    logger.info(f"Processando {produto} em thread")
    
    # Salvar Bronze
    arquivo_bronze = salvar_parquet_bronze(df, produto, 'producao')
    
    # Processar Silver
    df_silver = processar_silver_producao(df)
    if not df_silver.empty:
        arquivo_silver = DIR_SILVER / f"{produto}_producao.parquet"
        df_silver.to_parquet(arquivo_silver, index=False)
        logger.info(f"Salvo Silver: {arquivo_silver}")


def processar_produtos_paralelo(dados_por_produto: Dict[str, pd.DataFrame]) -> None:
    """
    Processa múltiplos produtos em paralelo usando threads.
    
    Args:
        dados_por_produto: Dicionário {produto: DataFrame}
    """
    logger.info(f"Processando {len(dados_por_produto)} produtos em paralelo")
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futuros = {
            executor.submit(
                processar_produto_thread,
                produto,
                df,
                executor
            ): produto
            for produto, df in dados_por_produto.items()
            if not df.empty
        }
        
        for futuro in as_completed(futuros):
            produto = futuros[futuro]
            try:
                futuro.result()
                logger.info(f"✓ Processamento concluído: {produto}")
            except Exception as e:
                logger.error(f"✗ Erro processando {produto}: {e}")


async def pipeline_precos_producao() -> None:
    """
    Pipeline principal para download e processamento de preços e produção.
    """
    logger.info("=" * 60)
    logger.info("INICIANDO PIPELINE PREÇOS E PRODUÇÃO")
    logger.info("=" * 60)
    
    # Passo 1: Baixar dados CONAB (paralelo)
    logger.info("\n[1/4] Baixando dados CONAB série histórica...")
    dados_conab = await baixar_multiplos_produtos_conab()
    
    # Passo 2: Processar dados CONAB (paralelo com threads)
    logger.info("\n[2/4] Processando dados CONAB...")
    processar_produtos_paralelo(dados_conab)
    
    # Passo 3: Criar estimativas de preços
    logger.info("\n[3/4] Criando estimativas de preços...")
    df_precos = criar_estimativas_precos_anuais()
    salvar_parquet_bronze(df_precos, 'estimativas', 'precos')
    
    # Copiar para Silver (preços não precisam de processamento complexo)
    arquivo_silver = DIR_SILVER / "estimativas_precos.parquet"
    df_precos.to_parquet(arquivo_silver, index=False)
    logger.info(f"Salvo Silver: {arquivo_silver}")
    
    # Passo 4: Consolidar Silver
    logger.info("\n[4/4] Consolidando camada Silver...")
    df_consolidado = consolidar_silver()
    
    if not df_consolidado.empty:
        arquivo_consolidado = DIR_SILVER / "precos_producao_consolidado.parquet"
        df_consolidado.to_parquet(arquivo_consolidado, index=False)
        logger.info(f"✓ Consolidado salvo: {arquivo_consolidado}")
    
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE CONCLUÍDO COM SUCESSO")
    logger.info("=" * 60)


async def main():
    """Função principal."""
    await pipeline_precos_producao()


if __name__ == "__main__":
    asyncio.run(main())
