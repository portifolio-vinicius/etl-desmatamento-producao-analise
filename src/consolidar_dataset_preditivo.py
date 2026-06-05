"""
Script para consolidar e otimizar dados para análise preditiva.

Este script combina dados da camada Silver em um único dataset otimizado
para modelagem preditiva, focando em features relevantes para prever
desmatamento e impacto socioambiental.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, List

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def carregar_dados_silver(caminho_base: Path) -> Dict[str, pd.DataFrame]:
    """
    Carrega os principais arquivos da camada Silver.
    
    Args:
        caminho_base: Caminho base do diretório data/02_silver
        
    Returns:
        Dicionário com DataFrames carregados
    """
    logger.info("Carregando dados da camada Silver...")
    
    dados = {}
    
    # Dataset principal já consolidado
    dados['serie_historica'] = pd.read_parquet(
        caminho_base / 'serie_historica_2020_2023.parquet'
    )
    logger.info(f"serie_historica: {dados['serie_historica'].shape}")
    
    # Dimensão de municípios
    dados['dim_municipio'] = pd.read_parquet(
        caminho_base / 'dim_municipio.parquet'
    )
    logger.info(f"dim_municipio: {dados['dim_municipio'].shape}")
    
    # IDHM interpolado
    dados['idhm'] = pd.read_parquet(
        caminho_base / 'idhm_municipal_interpolado.parquet'
    )
    logger.info(f"idhm: {dados['idhm'].shape}")
    
    # DETER consolidado (alertas em tempo real)
    try:
        dados['deter'] = pd.read_parquet(
            caminho_base / 'deter_consolidado.parquet'
        )
        logger.info(f"deter: {dados['deter'].shape}")
    except Exception as e:
        logger.warning(f"Não foi possível carregar DETER: {e}")
        dados['deter'] = None
    
    # PRODES consolidado (desmatamento anual)
    try:
        dados['prodes'] = pd.read_parquet(
            caminho_base / 'prodes_consolidado.parquet'
        )
        logger.info(f"prodes: {dados['prodes'].shape}")
    except Exception as e:
        logger.warning(f"Não foi possível carregar PRODES: {e}")
        dados['prodes'] = None
    
    # Embargos por município ano
    dados['embargos'] = pd.read_parquet(
        caminho_base / 'embargos_por_municipio_ano.parquet'
    )
    logger.info(f"embargos: {dados['embargos'].shape}")
    
    # Dados meteorológicos CHIRPS
    try:
        dados['chirps'] = pd.read_parquet(
            caminho_base / 'chirps_municipal/chirps_amazonia_2020_2023.parquet'
        )
        logger.info(f"chirps: {dados['chirps'].shape}")
    except Exception as e:
        logger.warning(f"Não foi possível carregar CHIRPS: {e}")
        dados['chirps'] = None
    
    return dados


def criar_dataset_preditivo(dados: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Cria dataset consolidado para análise preditiva.
    
    Args:
        dados: Dicionário com DataFrames da camada Silver
        
    Returns:
        DataFrame consolidado e otimizado para modelagem
    """
    logger.info("Criando dataset preditivo...")
    
    # Iniciar com série histórica que já tem dados consolidados
    df = dados['serie_historica'].copy()
    
    # Filtrar apenas códigos IBGE válidos (remover -1)
    df = df[df['cod_ibge'] > 0].copy()
    logger.info(f"Após filtro cod_ibge válido: {df.shape}")
    
    # Adicionar dimensão de municípios
    df = df.merge(
        dados['dim_municipio'][['cod_ibge', 'municipio', 'uf', 'regiao']],
        on='cod_ibge',
        how='left'
    )
    
    # Adicionar IDHM
    df = df.merge(
        dados['idhm'],
        on=['cod_ibge', 'ano'],
        how='left'
    )
    
    # Enriquecer com dados de embargos históricos
    embargos_hist = dados['embargos'].groupby('cod_munici').agg({
        'num_embargos': 'sum',
        'area_desmatada_ha': 'sum',
        'area_embargada_ha': 'sum'
    }).reset_index()
    embargos_hist.columns = ['cod_ibge', 'embargos_historicos_total', 
                             'area_desmatada_historica_ha', 'area_embargada_historica_ha']
    
    df = df.merge(
        embargos_hist,
        on='cod_ibge',
        how='left'
    )
    
    # Enriquecer com dados meteorológicos CHIRPS
    if dados['chirps'] is not None:
        logger.info("Integrando dados CHIRPS...")
        df = df.merge(
            dados['chirps'][['ano', 'precipitacao_total_mm', 
                            'precipitacao_media_diaria_mm', 'estacao_chuva']],
            on=['ano'],
            how='left'
        )
        logger.info("Dados CHIRPS integrados com sucesso")
    
    # Criar features derivadas
    logger.info("Criando features derivadas...")
    
    # Features temporais
    df['ano_inicio_analise'] = df.groupby('cod_ibge')['ano'].transform('min')
    df['anos_obs'] = df['ano'] - df['ano_inicio_analise']
    
    # Features de pecuária (focar em bovinos que são mais relevantes)
    df['tem_bovinos'] = (df['ppm_bovinos_cabecas'] > 0).astype(int)
    df['log_bovinos'] = np.log1p(df['ppm_bovinos_cabecas'])
    
    # Features de desmatamento
    df['tem_desmatamento'] = (df['area_desmatada_ha'] > 0).astype(int)
    df['log_area_desmatada'] = np.log1p(df['area_desmatada_ha'])
    df['log_area_embargada'] = np.log1p(df['area_embargada_ha'])
    
    # Features de embargos
    df['tem_embargos'] = (df['num_embargos'] > 0).astype(int)
    df['log_num_embargos'] = np.log1p(df['num_embargos'])
    
    # Features econômicas
    df['tem_vab'] = (df['vab_agro_mil_reais'] > 0).astype(int)
    df['log_vab'] = np.log1p(df['vab_agro_mil_reais'])
    
    # Features de desenvolvimento humano
    df['idhm_categoria'] = pd.cut(
        df['idhm'],
        bins=[0, 0.5, 0.6, 0.7, 1.0],
        labels=['Muito Baixo', 'Baixo', 'Médio', 'Alto']
    )
    
    # Features de risco (combinação de fatores)
    df['risco_desmatamento'] = (
        (df['tem_desmatamento'] * 0.4) +
        (df['tem_embargos'] * 0.3) +
        (df['tem_bovinos'] * 0.2) +
        ((df['idhm'] < 0.6).astype(int) * 0.1)
    )
    
    # Features de pressão econômica
    df['pressao_economica'] = (
        (df['log_vab'] / df['log_vab'].max()) * 0.5 +
        (df['log_bovinos'] / df['log_bovinos'].max()) * 0.5
    )
    
    # Tratar valores nulos
    logger.info("Tratando valores nulos...")
    
    # Preencher IDHM com mediana por município
    df['idhm'] = df.groupby('cod_ibge')['idhm'].transform(
        lambda x: x.fillna(x.median())
    )
    
    # Preencher valores nulos restantes
    colunas_numericas = df.select_dtypes(include=[np.number]).columns
    for col in colunas_numericas:
        df[col] = df[col].fillna(0)
    
    # Remover colunas redundantes ou com muitos zeros
    colunas_remover = [
        'ppm_asininos_cabecas', 'ppm_bubalinos_cabecas', 
        'ppm_caprinos_cabecas', 'ppm_codornas_cabecas',
        'ppm_equinos_cabecas', 'ppm_galinaceos_total_cabecas',
        'ppm_galinhas_cabecas', 'ppm_muar_cabecas',
        'ppm_ovinos_cabecas', 'ppm_suinos_matrizes_cabecas',
        'ppm_suinos_total_cabecas'
    ]
    
    for col in colunas_remover:
        if col in df.columns:
            df = df.drop(columns=[col])
    
    logger.info(f"Dataset final shape: {df.shape}")
    logger.info(f"Colunas finais: {list(df.columns)}")
    
    return df


def otimizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Otimiza tipos de dados para reduzir uso de memória.
    
    Args:
        df: DataFrame a ser otimizado
        
    Returns:
        DataFrame com tipos otimizados
    """
    logger.info("Otimizando tipos de dados...")
    
    # Otimizar tipos inteiros
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    # Otimizar tipos float
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    # Otimizar tipos categóricos
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # Se cardinalidade < 50%
            df[col] = df[col].astype('category')
    
    return df


def main():
    """Função principal para executar a consolidação."""
    
    # Definir caminhos
    caminho_projeto = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
    caminho_silver = caminho_projeto / 'data' / '02_silver'
    caminho_modelagem = caminho_projeto / 'data' / '04_modelagem'
    
    # Criar diretório de modelagem se não existir
    caminho_modelagem.mkdir(parents=True, exist_ok=True)
    
    # Carregar dados
    dados = carregar_dados_silver(caminho_silver)
    
    # Criar dataset preditivo
    df_preditivo = criar_dataset_preditivo(dados)
    
    # Otimizar tipos
    df_preditivo = otimizar_tipos(df_preditivo)
    
    # Salvar dataset consolidado
    caminho_saida = caminho_modelagem / 'dataset_preditivo_consolidado.parquet'
    df_preditivo.to_parquet(caminho_saida, index=False)
    
    logger.info(f"Dataset salvo em: {caminho_saida}")
    logger.info(f"Shape final: {df_preditivo.shape}")
    logger.info(f"Tamanho do arquivo: {caminho_saida.stat().st_size / 1024 / 1024:.2f} MB")
    
    # Salvar metadados
    metadados = {
        'shape': df_preditivo.shape,
        'colunas': list(df_preditivo.columns),
        'tipos': df_preditivo.dtypes.to_dict(),
        'memory_usage_mb': df_preditivo.memory_usage(deep=True).sum() / 1024 / 1024,
        'periodo': f"{df_preditivo['ano'].min()}-{df_preditivo['ano'].max()}",
        'n_municipios': df_preditivo['cod_ibge'].nunique(),
        'n_ufs': df_preditivo['uf'].nunique()
    }
    
    import json
    with open(caminho_modelagem / 'metadados_dataset.json', 'w') as f:
        json.dump(metadados, f, indent=2, default=str)
    
    logger.info("Metadados salvos em: data/04_modelagem/metadados_dataset.json")
    logger.info("Consolidação concluída com sucesso!")


if __name__ == '__main__':
    main()
