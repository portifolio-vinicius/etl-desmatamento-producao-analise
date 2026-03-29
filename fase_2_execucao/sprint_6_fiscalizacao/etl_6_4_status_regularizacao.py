import pandas as pd
import numpy as np
import os
import json

def etl_6_4_status_regularizacao():
    print("Iniciando ETL 6.4: Situação do Desmatamento...")
    
    # 1. Carregar dados Bronze
    bronze_path = 'data/01_bronze/ibama/ibama_embargos/embargos_ibama_tabular.parquet'
    
    if not os.path.exists(bronze_path):
        print("Erro: Arquivo Bronze do IBAMA não encontrado.")
        return

    df = pd.read_parquet(bronze_path)
    
    # 2. Analisar campo sit_desmat
    # Mapear conforme observado na validação: D e N
    mapeamento = {
        'D': 'Desmatamento / Degradação',
        'N': 'Não Desmatamento / Outros'
    }
    
    dist_situacao = df['sit_desmat'].value_counts().reset_index()
    dist_situacao.columns = ['situacao', 'contagem']
    dist_situacao['descricao'] = dist_situacao['situacao'].map(mapeamento)
    dist_situacao['pct'] = dist_situacao['contagem'] / dist_situacao['contagem'].sum() * 100
    
    # 3. Analisar por ano (2020-2023)
    df['ano'] = pd.to_datetime(df['dat_embarg'], format='%d/%m/%y %H:%M:%S', errors='coerce').dt.year
    dist_ano = df[df['ano'].isin([2020, 2021, 2022, 2023])].groupby(['ano', 'sit_desmat']).size().unstack(fill_value=0)
    
    # 4. Salvar Gold
    output_path = 'data/03_gold/status_regularizacao_embargos.parquet'
    dist_situacao.to_parquet(output_path)
    
    # Resumo para o JSON do Sprint 6
    resumo_gold = {
        'distribuicao_geral': dist_situacao.to_dict(orient='records'),
        'por_ano_2020_2023': dist_ano.to_dict()
    }
    
    resumo_path = 'data/03_gold/resumo_status_embargos.json'
    with open(resumo_path, 'w', encoding='utf-8') as f:
        json.dump(resumo_gold, f, indent=4, ensure_ascii=False)
    
    print(f"ETL 6.4 concluída. Salvo em: {output_path}")
    print(f"Distribuição sit_desmat: \n{dist_situacao}")

if __name__ == "__main__":
    etl_6_4_status_regularizacao()
