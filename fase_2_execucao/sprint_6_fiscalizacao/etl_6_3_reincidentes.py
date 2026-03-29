import pandas as pd
import numpy as np
import os

def etl_6_3_reincidentes():
    print("Iniciando ETL 6.3: Recorrência de Embargados...")
    
    # 1. Carregar dados Bronze (granulares)
    bronze_path = 'data/01_bronze/ibama/ibama_embargos/embargos_ibama_tabular.parquet'
    
    if not os.path.exists(bronze_path):
        print("Erro: Arquivo Bronze do IBAMA não encontrado.")
        return

    df = pd.read_parquet(bronze_path)
    
    # 2. Extrair ano do embargo
    df['ano'] = pd.to_datetime(df['dat_embarg'], format='%d/%m/%y %H:%M:%S', errors='coerce').dt.year
    
    # 3. Filtrar CPF/CNPJ válidos
    df_offenders = df[df['cpf_cnpj_e'].notnull() & (df['cpf_cnpj_e'] != 'nan')]
    
    # 4. Agrupar por infrator
    reincidentes = df_offenders.groupby('cpf_cnpj_e').agg(
        num_embargos=('num_tad', 'count'),
        anos_ativos=('ano', 'nunique'),
        area_total_ha=('qtd_area_e', 'sum'),
        uf_principal=('uf', lambda x: x.mode().iloc[0] if not x.mode().empty else 'ND'),
        municipio_principal=('municipio', lambda x: x.mode().iloc[0] if not x.mode().empty else 'ND')
    ).reset_index()
    
    # 5. Calcular Taxa de Recorrência
    reincidentes['recurrence_rate'] = reincidentes['num_embargos'] / reincidentes['anos_ativos']
    
    # 6. Filtrar apenas quem tem mais de 1 embargo (reincidentes reais)
    reincidentes = reincidentes[reincidentes['num_embargos'] > 1].sort_values(by='num_embargos', ascending=False)
    
    # 7. Salvar Gold
    output_path = 'data/03_gold/reincidentes_embargos.parquet'
    reincidentes.to_parquet(output_path)
    
    print(f"ETL 6.3 concluída. Salvo em: {output_path}")
    print(f"Total de infratores únicos identificados: {len(df_offenders['cpf_cnpj_e'].unique())}")
    print(f"Total de reincidentes ( > 1 embargo): {len(reincidentes)}")
    print(f"Top reincidente: {reincidentes.iloc[0]['cpf_cnpj_e']} com {reincidentes.iloc[0]['num_embargos']} embargos.")

if __name__ == "__main__":
    etl_6_3_reincidentes()
