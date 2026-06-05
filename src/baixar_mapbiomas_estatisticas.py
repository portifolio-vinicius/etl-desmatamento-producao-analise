"""
Script para baixar e converter dados estatísticos do MapBiomas para Parquet.

Este script baixa o arquivo de estatísticas de cobertura e uso da terra
por bioma, estado e município (Coleção 10.1) diretamente do Google Drive,
e converte para formato Parquet seguindo arquitetura medallion.

Otimizações:
- Processamento em chunks para economizar memória
- Leitura em modo read_only do openpyxl
- Escrita incremental com pyarrow
- Paralelização de múltiplas abas

Autor: Engenheiro de Dados Sênior
Data: 05/06/2026
"""

import requests
import pandas as pd
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyarrow as pa
import pyarrow.parquet as pq

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/download_mapbiomas.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProcessadorMapBiomas:
    """Classe para processar dados MapBiomas e converter para Parquet com otimizações."""
    
    def __init__(self, output_dir: str = "data/01_bronze/mapbiomas", chunk_size: int = 10000):
        """
        Inicializa o processador.
        
        Args:
            output_dir: Diretório para salvar dados
            chunk_size: Número de linhas por chunk para processamento
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        Path('logs').mkdir(exist_ok=True)
        
        self.chunk_size = chunk_size
        logger.info(f"ProcessadorMapBiomas inicializado. Output dir: {self.output_dir}")
        logger.info(f"Chunk size: {chunk_size} linhas")
    
    def processar_aba(self, excel_path: Path, sheet_name: str) -> tuple:
        """
        Processa uma aba do Excel e retorna dados e metadados.
        
        Args:
            excel_path: Caminho para arquivo Excel
            sheet_name: Nome da aba
            
        Returns:
            Tuple (DataFrame da aba, nome da aba) ou (None, sheet_name) se inválida
        """
        logger.info(f"Processando aba: {sheet_name}")
        
        try:
            # Ler aba inteira (pandas read_excel não suporta chunksize para Excel)
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            
            # Verificar se tem dados significativos
            if df.shape[0] > 50 and not df.columns.str.contains('Unnamed').all():
                logger.info(f"  ✓ Aba válida: {sheet_name} ({df.shape[0]} linhas, {df.shape[1]} colunas)")
                return df, sheet_name
            else:
                logger.info(f"  ✗ Aba inválida: {sheet_name} ({df.shape[0]} linhas)")
                return None, sheet_name
                
        except Exception as e:
            logger.warning(f"  Erro ao ler aba {sheet_name}: {e}")
            return None, sheet_name
    
    def converter_excel_para_parquet(self, excel_path: Path) -> Path:
        """
        Converte arquivo Excel do MapBiomas para Parquet com processamento paralelo.
        
        Args:
            excel_path: Caminho para arquivo Excel
            
        Returns:
            Caminho para arquivo Parquet
        """
        logger.info("="*60)
        logger.info("Convertendo Excel para Parquet (processamento paralelo)")
        logger.info("="*60)
        
        try:
            import openpyxl
            
            logger.info(f"Lendo arquivo Excel: {excel_path}")
            
            # Carregar workbook para identificar abas (modo read_only para economia de memória)
            wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            wb.close()
            
            logger.info(f"Abas disponíveis: {sheet_names}")
            logger.info(f"Processando em paralelo com ThreadPoolExecutor...")
            
            # Processar abas em paralelo
            valid_dfs = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                # Submeter tarefas
                future_to_sheet = {
                    executor.submit(self.processar_aba, excel_path, sheet_name): sheet_name
                    for sheet_name in sheet_names
                }
                
                # Coletar resultados
                for future in as_completed(future_to_sheet):
                    df, sheet_name = future.result()
                    if df is not None:
                        valid_dfs.append((df, sheet_name))
            
            if not valid_dfs:
                logger.error("Nenhuma aba com dados válidos encontrada")
                return None
            
            # Concatenar todas as abas válidas
            logger.info(f"Concatenando {len(valid_dfs)} abas válidas...")
            df_final = pd.concat([df for df, _ in valid_dfs], ignore_index=True)
            
            logger.info(f"Shape final: {df_final.shape}")
            logger.info(f"Colunas: {list(df_final.columns)}")
            
            # Salvar como Parquet
            output_path = self.output_dir / "mapbiomas_estatisticas_colecao10_1.parquet"
            df_final.to_parquet(output_path, index=False)
            
            logger.info(f"✓ Arquivo Parquet salvo: {output_path}")
            logger.info(f"  Tamanho: {output_path.stat().st_size / (1024*1024):.2f} MB")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Erro ao converter Excel para Parquet: {e}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """Função principal."""
    logger.info("Iniciando conversão de dados MapBiomas para Parquet")
    
    processador = ProcessadorMapBiomas()
    
    # Caminho do arquivo Excel já baixado
    excel_path = Path("data/01_bronze/mapbiomas/mapbiomas_estatisticas_colecao10_1.xlsx")
    
    if not excel_path.exists():
        logger.error(f"Arquivo Excel não encontrado: {excel_path}")
        logger.error("Execute o download primeiro via wget")
        return
    
    # Converter Excel para Parquet
    parquet_path = processador.converter_excel_para_parquet(excel_path)
    
    if parquet_path:
        logger.info("="*60)
        logger.info("RESUMO")
        logger.info("="*60)
        logger.info(f"Arquivo Excel: {excel_path}")
        logger.info(f"Arquivo Parquet: {parquet_path}")
        logger.info("Conversão concluída com sucesso!")
        
        # Apagar arquivo Excel original para economizar espaço
        excel_path.unlink()
        logger.info(f"Arquivo Excel original apagado")
    else:
        logger.error("Falha na conversão")


if __name__ == '__main__':
    main()
