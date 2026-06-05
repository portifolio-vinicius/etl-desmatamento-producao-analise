"""
Integra dados de preços e produção agrícola ao dataset preditivo.

Este script combina:
- Dataset preditivo existente (data/04_modelagem/dataset_preditivo_com_mapbiomas.parquet)
- Dados de produção CONAB (data/02_silver/precos_producao/)
- Estimativas de preços anuais (data/02_silver/precos_producao/estimativas_precos.parquet)

Cria variáveis derivadas para análise de pressão econômica sobre desmatamento.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Caminhos
DATASET_PREDITIVO = Path("data/04_modelagem/dataset_preditivo_com_mapbiomas.parquet")
DIR_PRECOS_PRODUCAO = Path("data/02_silver/precos_producao")
DATASET_OUTPUT = Path("data/04_modelagem/dataset_preditivo_com_precos.parquet")


def carregar_dados() -> tuple:
    """
    Carrega todos os dados necessários.
    
    Returns:
        Tupla (dataset_preditivo, precos_anuais, producao_conab)
    """
    logger.info("Carregando dados...")
    
    # Dataset preditivo
    logger.info(f"  - Dataset preditivo: {DATASET_PREDITIVO}")
    df_dataset = pd.read_parquet(DATASET_PREDITIVO)
    logger.info(f"    ✓ {len(df_dataset)} linhas, {df_dataset.shape[1]} colunas")
    
    # Preços anuais
    arquivo_precos = DIR_PRECOS_PRODUCAO / "estimativas_precos.parquet"
    if arquivo_precos.exists():
        logger.info(f"  - Preços anuais: {arquivo_precos}")
        df_precos = pd.read_parquet(arquivo_precos)
        logger.info(f"    ✓ {len(df_precos)} registros")
    else:
        logger.warning("  ✗ Arquivo de preços não encontrado")
        df_precos = pd.DataFrame()
    
    # Produção CONAB
    arquivos_conab = list(DIR_PRECOS_PRODUCAO.glob("*_producao.parquet"))
    df_producao_list = []
    for arquivo in arquivos_conab:
        if arquivo.name != "estimativas_precos.parquet":
            logger.info(f"  - Produção CONAB: {arquivo}")
            df = pd.read_parquet(arquivo)
            df['produto'] = arquivo.stem.replace('_producao', '')
            df_producao_list.append(df)
            logger.info(f"    ✓ {len(df)} registros")
    
    if df_producao_list:
        df_producao = pd.concat(df_producao_list, ignore_index=True)
    else:
        logger.warning("  ✗ Nenhum arquivo de produção CONAB encontrado")
        df_producao = pd.DataFrame()
    
    return df_dataset, df_precos, df_producao


def pivotar_precos_anuais(df_precos: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma preços de formato longo para largo (uma coluna por produto).
    
    Args:
        df_precos: DataFrame com preços em formato longo
        
    Returns:
        DataFrame com preços em formato largo (ano x produto)
    """
    if df_precos.empty:
        return pd.DataFrame()
    
    logger.info("Pivotando preços anuais...")
    
    # Pivotar para formato largo
    df_pivot = df_precos.pivot(
        index='ano',
        columns='produto',
        values='preco_medio_rs'
    ).reset_index()
    
    # Renomear colunas
    df_pivot.columns = [f"preco_{col}_rs" if col != 'ano' else col 
                       for col in df_pivot.columns]
    
    logger.info(f"  ✓ {len(df_pivot)} anos, {df_pivot.shape[1]-1} produtos")
    return df_pivot


def criar_indicadores_pressao_economica(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria indicadores derivados de pressão econômica.
    
    Indicadores:
    - ano_boom_soja: Dummy para anos de preços altos de soja (> 150)
    - ano_boom_milho: Dummy para anos de preços altos de milho (> 80)
    - pressao_agro_alta: Dummy para pressão agrícola alta
    - indice_pressao_preco: Índice combinado de preços
    
    Args:
        df: Dataset com preços
        
    Returns:
        DataFrame com indicadores adicionais
    """
    logger.info("Criando indicadores de pressão econômica...")
    
    df = df.copy()
    
    # Dummies para anos de boom
    if 'preco_soja_rs' in df.columns:
        df['ano_boom_soja'] = (df['preco_soja_rs'] > 150).astype(int)
    
    if 'preco_milho_rs' in df.columns:
        df['ano_boom_milho'] = (df['preco_milho_rs'] > 80).astype(int)
    
    # Pressão agrícola alta (qualquer produto em boom)
    cols_boom = [col for col in df.columns if col.startswith('ano_boom_')]
    if cols_boom:
        df['pressao_agro_alta'] = df[cols_boom].max(axis=1).astype(int)
    
    # Índice de pressão de preços (normalizado)
    cols_preco = [col for col in df.columns if col.startswith('preco_') and col.endswith('_rs')]
    if cols_preco:
        # Normalizar por média histórica
        for col in cols_preco:
            media = df[col].mean()
            if media > 0:
                df[f"{col}_norm"] = df[col] / media
        
        cols_norm = [col for col in df.columns if col.endswith('_norm')]
        if cols_norm:
            df['indice_pressao_preco'] = df[cols_norm].mean(axis=1)
    
    logger.info(f"  ✓ {len([c for c in df.columns if c not in df.columns])} indicadores criados")
    return df


def integrar_producao_por_uf(
    df_dataset: pd.DataFrame,
    df_producao: pd.DataFrame
) -> pd.DataFrame:
    """
    Integra dados de produção por UF ao dataset.
    
    Args:
        df_dataset: Dataset preditivo
        df_producao: Dados de produção CONAB
        
    Returns:
        Dataset com produção por UF
    """
    if df_producao.empty:
        logger.warning("Dados de produção vazios, pulando integração")
        return df_dataset
    
    logger.info("Integrando produção por UF...")
    
    # Preparar dados de produção
    df_prod = df_producao.copy()
    
    # Extrair ano da safra se necessário
    if 'safra' in df_prod.columns:
        df_prod['ano'] = df_prod['safra'].str.split('/').str[0].astype(int)
    
    # Pivotar produção por produto
    if 'produto' in df_prod.columns and 'producao_mil_ton' in df_prod.columns:
        df_prod_pivot = df_prod.pivot_table(
            index=['uf', 'ano'],
            columns='produto',
            values='producao_mil_ton',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Renomear colunas
        df_prod_pivot.columns = [
            f"producao_{col}_mil_ton" if col not in ['uf', 'ano'] else col
            for col in df_prod_pivot.columns
        ]
        
        # Merge com dataset
        df_result = df_dataset.merge(
            df_prod_pivot,
            on=['uf', 'ano'],
            how='left'
        )
        
        logger.info(f"  ✓ Produção integrada: {df_prod_pivot.shape[1]-2} produtos")
        return df_result
    
    logger.warning("Colunas necessárias não encontradas em dados de produção")
    return df_dataset


def pipeline_integracao() -> None:
    """
    Pipeline principal de integração.
    """
    logger.info("=" * 60)
    logger.info("INICIANDO INTEGRAÇÃO PREÇOS E PRODUÇÃO")
    logger.info("=" * 60)
    
    # Passo 1: Carregar dados
    logger.info("\n[1/5] Carregando dados...")
    df_dataset, df_precos, df_producao = carregar_dados()
    
    # Passo 2: Pivotar preços
    logger.info("\n[2/5] Pivotando preços anuais...")
    df_precos_pivot = pivotar_precos_anuais(df_precos)
    
    # Passo 3: Merge preços com dataset
    logger.info("\n[3/5] Merge preços com dataset...")
    if not df_precos_pivot.empty:
        df_dataset = df_dataset.merge(df_precos_pivot, on='ano', how='left')
        logger.info(f"  ✓ Preços integrados: {df_dataset.shape[1]} colunas")
    else:
        logger.warning("  ✗ Preços não integrados (DataFrame vazio)")
    
    # Passo 4: Integrar produção por UF
    logger.info("\n[4/5] Integrando produção por UF...")
    df_dataset = integrar_producao_por_uf(df_dataset, df_producao)
    
    # Passo 5: Criar indicadores derivados
    logger.info("\n[5/5] Criando indicadores de pressão econômica...")
    df_dataset = criar_indicadores_pressao_economica(df_dataset)
    
    # Salvar dataset atualizado
    logger.info(f"\nSalvando dataset atualizado: {DATASET_OUTPUT}")
    df_dataset.to_parquet(DATASET_OUTPUT, index=False)
    
    logger.info(f"  ✓ Dataset salvo: {len(df_dataset)} linhas, {df_dataset.shape[1]} colunas")
    
    # Resumo
    logger.info("\n" + "=" * 60)
    logger.info("INTEGRAÇÃO CONCLUÍDA")
    logger.info("=" * 60)
    logger.info(f"Linhas: {len(df_dataset)}")
    logger.info(f"Colunas: {df_dataset.shape[1]}")
    logger.info(f"Novas colunas de preços: {len([c for c in df_dataset.columns if c.startswith('preco_')])}")
    logger.info(f"Novas colunas de produção: {len([c for c in df_dataset.columns if c.startswith('producao_')])}")
    logger.info(f"Novos indicadores: {len([c for c in df_dataset.columns if c in ['ano_boom_soja', 'ano_boom_milho', 'pressao_agro_alta', 'indice_pressao_preco']])}")


def main():
    """Função principal."""
    pipeline_integracao()


if __name__ == "__main__":
    main()
