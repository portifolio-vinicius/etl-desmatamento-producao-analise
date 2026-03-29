import pandas as pd
import numpy as np
import os

def etl_6_1_fiscalizacao_series():
    print("Iniciando ETL 6.1: Séries Temporais de Fiscalização...")
    
    # 1. Carregar dados
    emb_path = 'data/02_silver/embargos_por_municipio_ano.parquet'
    dim_path = 'data/02_silver/dim_municipio.parquet'
    
    if not os.path.exists(emb_path) or not os.path.exists(dim_path):
        print("Erro: Arquivos Silver não encontrados.")
        return

    df_emb = pd.read_parquet(emb_path)
    df_dim = pd.read_parquet(dim_path)
    
    # 2. Filtrar anos de análise (2021-2023)
    df_emb = df_emb[df_emb['ano'].isin([2021, 2022, 2023])]
    
    # 3. Renomear para padronizar join
    df_emb = df_emb.rename(columns={'cod_munici': 'cod_ibge'})
    
    # 4. Unir com dim_municipio para metadados (Amazônia Legal, etc.)
    df_series = pd.merge(df_emb, df_dim[['cod_ibge', 'uf', 'amazonia_legal', 'regiao']], on='cod_ibge', how='left')
    
    # 5. Calcular métricas de intensidade (usando constante para área se não disponível, ou 1000km2 como base)
    # Nota: No momento a área territorial real não está em dim_municipio, usaremos num_embargos/ano como proxy de intensidade
    
    # Calcular média e std para outliers por ano
    for ano in [2021, 2022, 2023]:
        df_ano = df_series[df_series['ano'] == ano]
        p95 = df_ano['num_embargos'].quantile(0.95)
        df_series.loc[df_series['ano'] == ano, 'outlier_fiscalizacao'] = df_series['num_embargos'] > p95
        
    # 6. Salvar Gold
    output_dir = 'data/03_gold'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'fiscalizacao_series_temporais.parquet')
    df_series.to_parquet(output_path)
    
    print(f"ETL 6.1 concluída. Salvo em: {output_path}")
    print(f"Registros processados: {len(df_series)}")
    print(f"Municípios únicos com embargos (21-23): {df_series['cod_ibge'].nunique()}")

if __name__ == "__main__":
    etl_6_1_fiscalizacao_series()
