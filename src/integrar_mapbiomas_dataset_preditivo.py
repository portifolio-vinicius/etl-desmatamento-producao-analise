"""
Script para integrar dados MapBiomas ao dataset preditivo.

Processamento otimizado com pandas:
- Filtra para Amazônia Legal
- Transforma para formato long (melt)
- Integra com dataset preditivo usando merge
- Processamento em memória eficiente

Autor: Engenheiro de Dados Sênior
Data: 05/06/2026
"""

import pandas as pd
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/integracao_mapbiomas.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IntegradorMapBiomas:
    """Classe para integrar dados MapBiomas ao dataset preditivo."""
    
    ESTADOS_AMAZONIA_LEGAL = ['AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO', 'MA', 'MT']
    
    def __init__(self):
        """Inicializa o integrador."""
        Path('logs').mkdir(exist_ok=True)
        
        self.path_mapbiomas = Path("data/01_bronze/mapbiomas/mapbiomas_estatisticas_colecao10_1.parquet")
        self.path_dataset = Path("data/04_modelagem/dataset_preditivo_consolidado.parquet")
        self.path_output = Path("data/04_modelagem/dataset_preditivo_com_mapbiomas.parquet")
        
        logger.info("IntegradorMapBiomas inicializado")
    
    def carregar_e_filtrar_mapbiomas(self) -> pd.DataFrame:
        """
        Carrega dados MapBiomas e filtra para Amazônia Legal.
        
        Returns:
            DataFrame filtrado
        """
        logger.info("="*60)
        logger.info("Carregando e filtrando dados MapBiomas")
        logger.info("="*60)
        
        try:
            # Carregar Parquet
            df = pd.read_parquet(self.path_mapbiomas)
            
            logger.info(f"Dados carregados: {df.shape}")
            
            # Filtrar para Amazônia Legal
            df_amazonia = df[df['state_acronym'].isin(self.ESTADOS_AMAZONIA_LEGAL)].copy()
            
            logger.info(f"✓ Dados MapBiomas filtrados: {df_amazonia.shape}")
            logger.info(f"  Estados: {sorted(df_amazonia['state_acronym'].unique())}")
            logger.info(f"  Municípios: {df_amazonia['municipality'].nunique()}")
            
            return df_amazonia
            
        except Exception as e:
            logger.error(f"Erro ao carregar MapBiomas: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def transformar_para_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma dados de wide para long (melt por ano).
        
        Args:
            df: DataFrame em formato wide
            
        Returns:
            DataFrame em formato long
        """
        logger.info("="*60)
        logger.info("Transformando para formato long (melt)")
        logger.info("="*60)
        
        try:
            # Identificar colunas de ano (1985-2024)
            colunas_id = ['country', 'biome', 'state', 'state_acronym', 'municipality', 
                         'class_id', 'class_level_0', 'class_level_1', 'class_level_2', 
                         'class_level_3', 'class_level_4']
            
            colunas_ano = [col for col in df.columns if col.isdigit() and 1985 <= int(col) <= 2024]
            
            logger.info(f"Colunas ID: {len(colunas_id)}")
            logger.info(f"Colunas de ano: {len(colunas_ano)} ({colunas_ano[0]} a {colunas_ano[-1]})")
            
            # Melt
            df_long = df.melt(
                id_vars=colunas_id,
                value_vars=colunas_ano,
                var_name='ano',
                value_name='area_ha'
            )
            
            # Converter ano para inteiro
            df_long['ano'] = df_long['ano'].astype('int16')
            
            # Remover linhas sem área
            df_long = df_long.dropna(subset=['area_ha'])
            
            logger.info(f"✓ Transformação concluída: {df_long.shape}")
            logger.info(f"  Período: {df_long['ano'].min()} a {df_long['ano'].max()}")
            
            return df_long
            
        except Exception as e:
            logger.error(f"Erro ao transformar: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def integrar_com_dataset_preditivo(self, df_mapbiomas: pd.DataFrame) -> pd.DataFrame:
        """
        Integra dados MapBiomas com dataset preditivo usando merge.
        
        Args:
            df_mapbiomas: DataFrame MapBiomas em formato long
            
        Returns:
            DataFrame integrado
        """
        logger.info("="*60)
        logger.info("Integrando com dataset preditivo")
        logger.info("="*60)
        
        try:
            # Carregar dataset preditivo
            logger.info(f"Carregando dataset preditivo: {self.path_dataset}")
            df_dataset = pd.read_parquet(self.path_dataset)
            
            logger.info(f"Dataset preditivo: {df_dataset.shape}")
            logger.info(f"  Período: {df_dataset['ano'].min()} a {df_dataset['ano'].max()}")
            
            # Filtrar MapBiomas para o período do dataset (2020-2023)
            df_mapbiomas_filtrado = df_mapbiomas[
                df_mapbiomas['ano'].between(2020, 2023)
            ].copy()
            
            logger.info(f"MapBiomas filtrado (2020-2023): {df_mapbiomas_filtrado.shape}")
            
            # Selecionar colunas relevantes do MapBiomas
            cols_mapbiomas = ['municipality', 'state_acronym', 'ano', 'class_id', 
                            'class_level_0', 'class_level_1', 'class_level_2', 'area_ha']
            df_mapbiomas_merge = df_mapbiomas_filtrado[cols_mapbiomas].copy()
            
            # Renomear colunas para merge
            df_mapbiomas_merge = df_mapbiomas_merge.rename(columns={
                'municipality': 'municipio',
                'state_acronym': 'uf'
            })
            
            # Merge usando pandas
            logger.info("Executando merge com pandas...")
            df_integrado = df_dataset.merge(
                df_mapbiomas_merge,
                on=['municipio', 'uf', 'ano'],
                how='left'
            )
            
            # Renomear colunas MapBiomas
            df_integrado = df_integrado.rename(columns={
                'class_id': 'mapbiomas_class_id',
                'class_level_0': 'mapbiomas_classe_0',
                'class_level_1': 'mapbiomas_classe_1',
                'class_level_2': 'mapbiomas_classe_2',
                'area_ha': 'mapbiomas_area_ha'
            })
            
            logger.info(f"✓ Integração concluída: {df_integrado.shape}")
            logger.info(f"  Colunas: {len(df_integrado.columns)}")
            logger.info(f"  Novas colunas: mapbiomas_class_id, mapbiomas_classe_0, mapbiomas_classe_1, mapbiomas_classe_2, mapbiomas_area_ha")
            
            return df_integrado
            
        except Exception as e:
            logger.error(f"Erro ao integrar: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def salvar_dataset_integrado(self, df: pd.DataFrame):
        """
        Salva dataset integrado em formato Parquet.
        
        Args:
            df: DataFrame integrado
        """
        logger.info("="*60)
        logger.info("Salvando dataset integrado")
        logger.info("="*60)
        
        try:
            df.to_parquet(self.path_output, index=False)
            
            tamanho_mb = self.path_output.stat().st_size / (1024*1024)
            
            logger.info(f"✓ Dataset salvo: {self.path_output}")
            logger.info(f"  Shape: {df.shape}")
            logger.info(f"  Tamanho: {tamanho_mb:.2f} MB")
            logger.info(f"  Colunas novas: mapbiomas_classe_0, mapbiomas_classe_1, mapbiomas_classe_2, mapbiomas_area_ha")
            
        except Exception as e:
            logger.error(f"Erro ao salvar: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Função principal."""
    logger.info("Iniciando integração MapBiomas ao dataset preditivo")
    
    integrador = IntegradorMapBiomas()
    
    # 1. Carregar e filtrar MapBiomas
    df_mapbiomas = integrador.carregar_e_filtrar_mapbiomas()
    if df_mapbiomas is None:
        logger.error("Falha ao carregar MapBiomas")
        return
    
    # 2. Transformar para formato long
    df_mapbiomas_long = integrador.transformar_para_long(df_mapbiomas)
    if df_mapbiomas_long is None:
        logger.error("Falha ao transformar para long")
        return
    
    # 3. Integrar com dataset preditivo
    df_integrado = integrador.integrar_com_dataset_preditivo(df_mapbiomas_long)
    if df_integrado is None:
        logger.error("Falha na integração")
        return
    
    # 4. Salvar dataset integrado
    integrador.salvar_dataset_integrado(df_integrado)
    
    logger.info("="*60)
    logger.info("INTEGRAÇÃO CONCLUÍDA COM SUCESSO")
    logger.info("="*60)


if __name__ == '__main__':
    main()
