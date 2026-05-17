#!/usr/bin/env python
# coding: utf-8

"""
Script para download de dados DETER via API WFS do INPE/TerraBrasilis.

Este script executa o download de dados de alertas DETER (2016-2017),
salvando-os em formato Parquet na camada Bronze.
"""

import sys
from pathlib import Path
import logging
import pandas as pd

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestao.extratores import ExtratorDETER
from utils.logging_config import configurar_logging
from utils.caminhos import repo_root


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


def main():
    """
    Função principal para execução do download DETER.
    """
    # Configurar logging
    configurar_logging(nome_arquivo="download_deter.log", nivel=logging.INFO)
    logger = logging.getLogger("download_deter")
    
    logger.info("=" * 60)
    logger.info("INICIANDO DOWNLOAD DETER")
    logger.info("=" * 60)
    
    extrator = ExtratorDETER(anos=list(range(2016, 2018)), chunk_size=1000)
    
    # Diretório de destino
    bronze_dir = repo_root() / "data" / "01_bronze" / "deter"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    # Extrair dados por ano
    for ano in range(2016, 2018):
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
    
    logger.info("\n" + "=" * 60)
    logger.info("DOWNLOAD DETER CONCLUÍDO")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
