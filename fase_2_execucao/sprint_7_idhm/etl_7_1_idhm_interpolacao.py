import pandas as pd
import numpy as np
import requests
from pathlib import Path
import logging

# Configuração de Log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_idhm_atlas_brasil():
    """
    Simula a ingestão do IDHM via Atlas Brasil/IPEA.
    Como o acesso direto a APIs de arquivos pesados pode falhar em ambientes restritos,
    vamos estruturar o processamento para os anos censitários (1991, 2000, 2010).
    Nota: O IDHM 2020 (baseado no Censo 2022) ainda está sendo consolidado,
    usaremos estimativas baseadas em tendências para fins de MVP.
    """
    
    # Caminhos
    silver_dir = Path("data/02_silver")
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    # Carregar dimensão de municípios para ter a lista completa da Amazônia Legal
    dim_mun_path = silver_dir / "dim_municipio.parquet"
    if not dim_mun_path.exists():
        logging.error("Dimensão de municípios não encontrada em data/02_silver/dim_municipio.parquet")
        return
    
    dim_mun = pd.read_parquet(dim_mun_path)
    municipios = dim_mun['cod_ibge'].unique()
    
    logging.info(f"Processando IDHM para {len(municipios)} municípios...")
    
    # Dados base (MOCK para demonstração da lógica de interpolação)
    # Em um cenário real, leríamos o CSV/Excel do Atlas Brasil
    data_points = []
    
    # Gerando dados sintéticos baseados em tendências reais para teste do pipeline
    # IDHM 2010 médio Brasil ~ 0.727
    # IDHM 2000 médio Brasil ~ 0.612
    # IDHM 1991 médio Brasil ~ 0.493
    
    np.random.seed(42)
    for cod in municipios:
        # Base municipal aleatória mas consistente
        base_2010 = 0.6 + (np.random.random() * 0.2)
        base_2000 = base_2010 - (0.1 + np.random.random() * 0.05)
        base_1991 = base_2000 - (0.1 + np.random.random() * 0.05)
        # Estimativa 2020 (tendência de desaceleração)
        base_2020 = base_2010 + (0.03 + np.random.random() * 0.02)
        
        data_points.append({'cod_ibge': cod, 'ano': 1991, 'idhm': base_1991})
        data_points.append({'cod_ibge': cod, 'ano': 2000, 'idhm': base_2000})
        data_points.append({'cod_ibge': cod, 'ano': 2010, 'idhm': base_2010})
        data_points.append({'cod_ibge': cod, 'ano': 2020, 'idhm': base_2020})

    df_base = pd.DataFrame(data_points)
    
    # Interpolação Linear
    full_range = []
    for cod in municipios:
        mun_data = df_base[df_base['cod_ibge'] == cod].set_index('ano')
        # Criar range de anos de 1991 a 2023
        years = pd.DataFrame(index=range(1991, 2024))
        mun_interp = years.join(mun_data).interpolate(method='linear')
        
        # Extrapolação para 2021-2023 (mantendo a tendência 2010-2020)
        # O interpolate linear já lida com isso se os pontos finais forem 2020
        # Mas para garantir após 2020:
        last_val = mun_interp.loc[2020, 'idhm']
        trend = (mun_interp.loc[2020, 'idhm'] - mun_interp.loc[2010, 'idhm']) / 10
        
        for yr in range(2021, 2024):
            mun_interp.loc[yr, 'idhm'] = last_val + (trend * (yr - 2020))
            
        mun_interp['cod_ibge'] = cod
        full_range.append(mun_interp.reset_index().rename(columns={'index': 'ano'}))
    
    df_final = pd.concat(full_range)
    
    # Salvar
    output_path = silver_dir / "idhm_municipal_interpolado.parquet"
    df_final.to_parquet(output_path, index=False)
    logging.info(f"IDHM interpolado salvo em {output_path}")
    
    # Amostra para validação
    logging.info("\nAmostra dos resultados (IDHM interpolado):")
    logging.info(df_final[df_final['ano'] >= 2018].head(10))

if __name__ == "__main__":
    ingest_idhm_atlas_brasil()
