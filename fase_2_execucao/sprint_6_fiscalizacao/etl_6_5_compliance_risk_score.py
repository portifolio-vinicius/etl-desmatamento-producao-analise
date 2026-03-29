import pandas as pd
import numpy as np
import os
from pathlib import Path

def generate_risk_score():
    print("="*60)
    print("ETL 6.5: COMPLIANCE RISK SCORE (ALERTA DE REINCIDENTES)")
    print("="*60)
    
    # 1. Carregar dados de reincidentes (gerados na ETL 6.3)
    input_path = 'data/03_gold/reincidentes_embargos.parquet'
    if not os.path.exists(input_path):
        print(f"Erro: Arquivo {input_path} não encontrado. Execute a ETL 6.3 primeiro.")
        return

    df = pd.read_parquet(input_path)
    print(f"✓ Registros de reincidentes carregados: {len(df)}")

    # 2. Definir Pesos para o Score (Total 100)
    # - Volume (num_embargos): 40%
    # - Frequência (recurrence_rate): 30%
    # - Severidade (area_total_ha): 30%
    
    # Normalização (Log para tratar outliers como o de 191 embargos)
    df['score_volume'] = np.log1p(df['num_embargos']) / np.log1p(df['num_embargos'].max()) * 40
    df['score_frequencia'] = np.log1p(df['recurrence_rate']) / np.log1p(df['recurrence_rate'].max()) * 30
    
    # Severidade - Capar área em 10.000ha para não distorcer o score
    area_cap = 10000
    df['area_capped'] = df['area_total_ha'].clip(upper=area_cap)
    df['score_severidade'] = np.log1p(df['area_capped']) / np.log1p(area_cap) * 30
    
    # 3. Score Final (0-100)
    df['compliance_risk_score'] = df['score_volume'] + df['score_frequencia'] + df['score_severidade']
    
    # Arredondar
    df['compliance_risk_score'] = df['compliance_risk_score'].round(2)
    
    # 4. Classificação de Risco
    def classify_risk(score):
        if score >= 80: return 'Crítico (Bloqueio Imediato)'
        if score >= 50: return 'Alto (Auditoria Requerida)'
        if score >= 20: return 'Médio (Monitoramento)'
        return 'Baixo'
        
    df['nivel_risco'] = df['compliance_risk_score'].apply(classify_risk)
    
    # 5. Ordenar por Risco
    df = df.sort_values(by='compliance_risk_score', ascending=False)
    
    # 6. Salvar Gold (Lista de Alerta)
    output_path = 'data/03_gold/lista_alerta_compliance.parquet'
    df.to_parquet(output_path, index=False)
    
    # CSV para facilitar visualização humana
    df.head(1000).to_csv('data/03_gold/lista_alerta_top1000.csv', index=False)
    
    print(f"✓ Lista de Alerta salva em: {output_path}")
    print("\n" + "="*60)
    print("RESUMO DA LISTA DE ALERTA")
    print("="*60)
    print(df['nivel_risco'].value_counts())
    print("\nTOP 5 INFRATORES DE MAIOR RISCO:")
    print(df[['cpf_cnpj_e', 'num_embargos', 'area_total_ha', 'compliance_risk_score', 'nivel_risco']].head(5))
    print("="*60)

if __name__ == "__main__":
    generate_risk_score()
