import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging

# Configuração de Log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def classificar_quadrantes():
    silver_dir = Path("data/02_silver")
    gold_dir = Path("data/03_gold")
    viz_dir = gold_dir / "visualizacoes"
    
    # 1. Carregar Dados Integrados
    logging.info("Carregando dados para tipologia...")
    idhm = pd.read_parquet(silver_dir / "idhm_municipal_interpolado.parquet")
    serie = pd.read_parquet(silver_dir / "serie_historica_2020_2023.parquet")
    dim_mun = pd.read_parquet(silver_dir / "dim_municipio.parquet")
    
    df = serie.merge(idhm, on=['cod_ibge', 'ano'], how='inner')
    df = df.merge(dim_mun[['cod_ibge', 'municipio', 'uf']], on='cod_ibge', how='left')
    
    # Focar no último ano disponível (2023) para a tipologia
    df_2023 = df[df['ano'] == 2023].copy()
    
    # 2. Definir Medianas como Pontos de Corte
    med_desmat = df_2023['area_desmatada_ha'].median()
    med_idhm = df_2023['idhm'].median()
    
    logging.info(f"Mediana Desmatamento: {med_desmat:.2f} ha | Mediana IDHM: {med_idhm:.4f}")
    
    # 3. Classificação em Quadrantes
    def categorizar(row):
        if row['area_desmatada_ha'] >= med_desmat and row['idhm'] >= med_idhm:
            return 'Alto Desmatamento / Alto IDHM'
        elif row['area_desmatada_ha'] >= med_desmat and row['idhm'] < med_idhm:
            return 'Alto Desmatamento / Baixo IDHM (Paradoxo)'
        elif row['area_desmatada_ha'] < med_desmat and row['idhm'] >= med_idhm:
            return 'Baixo Desmatamento / Alto IDHM'
        else:
            return 'Baixo Desmatamento / Baixo IDHM'
            
    df_2023['quadrante'] = df_2023.apply(categorizar, axis=1)
    
    # 4. Estatísticas por Quadrante
    resumo = df_2023.groupby('quadrante').agg({
        'cod_ibge': 'count',
        'area_desmatada_ha': 'mean',
        'vab_agro_mil_reais': 'mean',
        'idhm': 'mean'
    }).reset_index()
    
    logging.info("\nResumo por Quadrante (2023):")
    logging.info(resumo)
    
    # 5. Visualização: Scatter Plot de Quadrantes
    plt.figure(figsize=(14, 10))
    sns.scatterplot(data=df_2023, x='area_desmatada_ha', y='idhm', 
                    hue='quadrante', style='quadrante', alpha=0.6, s=100,
                    palette='viridis')
    
    plt.axvline(med_desmat, color='red', linestyle='--', alpha=0.5)
    plt.axhline(med_idhm, color='red', linestyle='--', alpha=0.5)
    
    plt.xscale('log')
    plt.title('Tipologia Municipal: Desmatamento vs IDHM (2023)', fontsize=16)
    plt.xlabel('Área Desmatada (ha) - Escala Log', fontsize=12)
    plt.ylabel('IDHM Interpolado', fontsize=12)
    plt.legend(title='Quadrantes', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3)
    
    viz_path = viz_dir / "quadrantes_desmatamento_idhm.png"
    plt.savefig(viz_path, bbox_inches='tight')
    logging.info(f"Gráfico de quadrantes salvo em {viz_path}")
    
    # 6. Salvar Gold
    df_2023.to_parquet(gold_dir / "tipologia_municipal_quadrantes.parquet", index=False)
    
    # Top 10 Municípios no Paradoxo (Alto Desmatamento / Baixo IDHM)
    paradoxo = df_2023[df_2023['quadrante'] == 'Alto Desmatamento / Baixo IDHM (Paradoxo)'].sort_values(by='area_desmatada_ha', ascending=False)
    logging.info("\nTop 10 Municípios no 'Paradoxo do Desmatamento':")
    logging.info(paradoxo[['municipio', 'uf', 'area_desmatada_ha', 'idhm']].head(10))

if __name__ == "__main__":
    classificar_quadrantes()
