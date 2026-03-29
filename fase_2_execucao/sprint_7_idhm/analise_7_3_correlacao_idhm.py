import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging

# Configuração de Log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def analisar_correlacao_idhm():
    silver_dir = Path("data/02_silver")
    gold_dir = Path("data/03_gold")
    viz_dir = gold_dir / "visualizacoes"
    viz_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Carregar Dados
    logging.info("Carregando dados Silver...")
    idhm = pd.read_parquet(silver_dir / "idhm_municipal_interpolado.parquet")
    serie = pd.read_parquet(silver_dir / "serie_historica_2020_2023.parquet")
    
    # 2. Join
    # série já tem cod_ibge, ano, area_desmatada_ha, vab_agro_mil_reais
    df = serie.merge(idhm, on=['cod_ibge', 'ano'], how='inner')
    
    logging.info(f"Dados integrados: {len(df)} registros para análise.")
    
    # 3. Cálculo de Correlação (Spearman - mais robusto para outliers)
    corr_desmat = df[['area_desmatada_ha', 'idhm']].corr(method='spearman').iloc[0, 1]
    corr_vab = df[['vab_agro_mil_reais', 'idhm']].corr(method='spearman').iloc[0, 1]
    
    logging.info(f"Correlação Spearman Desmatamento x IDHM: {corr_desmat:.4f}")
    logging.info(f"Correlação Spearman VAB Agro x IDHM: {corr_vab:.4f}")
    
    # 4. Visualização: Scatter Plot
    plt.figure(figsize=(12, 8))
    sns.scatterplot(data=df[df['area_desmatada_ha'] > 0], 
                    x='area_desmatada_ha', y='idhm', 
                    alpha=0.5, size='vab_agro_mil_reais', sizes=(20, 200))
    plt.xscale('log')
    plt.title('Paradoxo Socioeconômico: Desmatamento vs IDHM (2020-2023)\n(Municípios da Amazônia Legal)', fontsize=14)
    plt.xlabel('Área Desmatada Acumulada (ha) - Escala Log', fontsize=12)
    plt.ylabel('IDHM Interpolado', fontsize=12)
    plt.grid(True, which="both", ls="-", alpha=0.2)
    
    viz_path = viz_dir / "scatter_desmatamento_idhm.png"
    plt.savefig(viz_path)
    logging.info(f"Gráfico salvo em {viz_path}")
    
    # 5. Salvar Resultados Gold
    resultados = {
        'correlacao_spearman_desmat_idhm': corr_desmat,
        'correlacao_spearman_vab_idhm': corr_vab,
        'interpretacao': "Correlação próxima de zero sugere que o desmatamento não impulsiona o desenvolvimento humano local." if abs(corr_desmat) < 0.1 else "Análise de tendência em curso."
    }
    
    df_res = pd.DataFrame([resultados])
    df_res.to_parquet(gold_dir / "correlacao_idhm_desmatamento.parquet", index=False)
    logging.info("Resultados da correlação salvos em data/03_gold/correlacao_idhm_desmatamento.parquet")

if __name__ == "__main__":
    analis_correlacao_idhm = analisar_correlacao_idhm()
