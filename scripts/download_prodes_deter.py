#!/usr/bin/env python
# coding: utf-8

"""
Script para download de dados PRODES e DETER via API WFS do INPE/TerraBrasilis.

Este script executa o download de dados históricos de desmatamento (PRODES 2008-2023)
e alertas DETER (2016-2023), salvando-os em formato Parquet na camada Bronze.
"""

import sys
from pathlib import Path
import logging
import pandas as pd
from tqdm import tqdm

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestao.extratores import ExtratorPRODES, ExtratorDETER
from utils.logging_config import configurar_logging
from utils.caminhos import repo_root, CaminhosDados


def salvar_dataframe_parquet(
    df: pd.DataFrame,
    caminho_arquivo: Path,
    logger: logging.Logger
) -> None:
    """
    Salva DataFrame em formato Parquet.
    
    Args:
        df: DataFrame para salvar
        caminho_arquivo: Caminho do arquivo Parquet
        logger: Logger para registro
    """
    try:
        caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
        
        # Remover coluna geometry se existir (dados tabulares apenas)
        if "geometry" in df.columns:
            df = df.drop(columns=["geometry"])
        
        df.to_parquet(caminho_arquivo, engine="pyarrow", compression="snappy", index=False)
        logger.info(f"Salvo: {caminho_arquivo} ({len(df)} registros)")
            
    except Exception as e:
        logger.error(f"Erro ao salvar {caminho_arquivo}: {str(e)}")
        raise


def download_prodes(anos: list, logger: logging.Logger) -> None:
    """
    Baixa dados PRODES para os anos especificados.
    
    Args:
        anos: Lista de anos para download
        logger: Logger para registro
    """
    logger.info("=" * 60)
    logger.info("INICIANDO DOWNLOAD PRODES")
    logger.info("=" * 60)
    
    extrator = ExtratorPRODES(anos=anos, chunk_size=1000)
    
    # Diretório de destino
    bronze_dir = repo_root() / "data" / "01_bronze" / "prodes"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    # Extrair dados por ano
    for ano in anos:
        logger.info(f"\nBaixando PRODES ano {ano}...")
        
        dfs_ano = []
        for df_chunk in extrator.extrair_ano(ano):
            if not df_chunk.empty:
                dfs_ano.append(df_chunk)
        
        if dfs_ano:
            df_completo = pd.concat(dfs_ano, ignore_index=True)
            
            # Salvar arquivo anual
            arquivo_ano = bronze_dir / f"prodes_desmatamento_{ano}.parquet"
            salvar_dataframe_parquet(df_completo, arquivo_ano, logger)
        else:
            logger.warning(f"Nenhum dado encontrado para PRODES ano {ano}")
    
    logger.info("\nDownload PRODES concluído")


def download_deter(anos: list, logger: logging.Logger) -> None:
    """
    Baixa dados DETER para os anos especificados.
    
    Args:
        anos: Lista de anos para download
        logger: Logger para registro
    """
    logger.info("=" * 60)
    logger.info("INICIANDO DOWNLOAD DETER")
    logger.info("=" * 60)
    
    extrator = ExtratorDETER(anos=anos, chunk_size=1000)
    
    # Diretório de destino
    bronze_dir = repo_root() / "data" / "01_bronze" / "deter"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    # Extrair dados por ano
    for ano in anos:
        logger.info(f"\nBaixando DETER ano {ano}...")
        
        dfs_ano = []
        for df_chunk in extrator.extrair_ano(ano):
            if not df_chunk.empty:
                dfs_ano.append(df_chunk)
        
        if dfs_ano:
            df_completo = pd.concat(dfs_ano, ignore_index=True)
            
            # Salvar arquivo anual
            arquivo_ano = bronze_dir / f"deter_alertas_{ano}.parquet"
            salvar_dataframe_parquet(df_completo, arquivo_ano, logger)
        else:
            logger.warning(f"Nenhum dado encontrado para DETER ano {ano}")
    
    logger.info("\nDownload DETER concluído")


def main():
    """
    Função principal para execução dos downloads.
    """
    # Configurar logging
    configurar_logging(nome_arquivo="download_prodes_deter.log", nivel=logging.INFO)
    logger = logging.getLogger("download_prodes_deter")
    
    logger.info("Iniciando downloads PRODES e DETER")
    
    # Anos para download (conforme plano Sprint 1: 2000-2017)
    # Ajustado para anos disponíveis nas APIs:
    # PRODES: 2008-2023
    # DETER: 2016-2023
    anos_prodes = list(range(2008, 2018))  # 2008-2017 conforme plano
    anos_deter = list(range(2016, 2018))   # 2016-2017 (DETER histórico limitado)
    
    try:
        # Download PRODES
        download_prodes(anos_prodes, logger)
        
        # Download DETER
        download_deter(anos_deter, logger)
        
        logger.info("\n" + "=" * 60)
        logger.info("TODOS OS DOWNLOADS CONCLUÍDOS COM SUCESSO")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Erro fatal durante downloads: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
