import pandas as pd
import numpy as np
import os
import json

def sprint6_analise_final():
    print("Iniciando Análise Final do Sprint 6...")
    
    # 1. Carregar artefatos Gold
    try:
        df_fiscal = pd.read_parquet('data/03_gold/fiscal_series_temporais.parquet') # Ops, checar nome
    except:
        df_fiscal = pd.read_parquet('data/03_gold/fiscalizacao_series_temporais.parquet')
        
    df_impacto = pd.read_parquet('data/03_gold/impacto_embargo_producao.parquet')
    df_reincidentes = pd.read_parquet('data/03_gold/reincidentes_embargos.parquet')
    
    with open('data/03_gold/resumo_status_embargos.json', 'r') as f:
        status_embargos = json.load(f)
    
    # 2. Consolidar métricas
    resumo = {
        "periodo_analise": "2021-2023",
        "municipios_com_embargos_periodo": int(df_fiscal['cod_ibge'].nunique()),
        "total_embargos_periodo": int(df_fiscal['num_embargos'].sum()),
        "area_total_embargada_ha": float(df_fiscal['area_embargada_ha'].sum()),
        
        "impacto_producao": {
            "municipios_analisados": len(df_impacto),
            "delta_bovinos_medio_pct": float(df_impacto['delta_bovinos_pct'].mean()),
            "delta_vab_medio_pct": float(df_impacto['delta_vab_pct'].mean()),
            "conclusao_impacto": "Redução detectada" if df_impacto['delta_bovinos_pct'].mean() < 0 else "Estabilidade/Aumento"
        },
        
        "reincidencia": {
            "total_infratores_reincidentes": len(df_reincidentes),
            "top_10_reincidentes_avg_embargos": float(df_reincidentes.head(10)['num_embargos'].mean()),
            "max_embargos_unico_cpf": int(df_reincidentes['num_embargos'].max())
        },
        
        "status_desmatamento": {
            "pct_direto_desmatamento": float(next(item['pct'] for item in status_embargos['distribuicao_geral'] if item['situacao'] == 'D'))
        }
    }
    
    # 3. Salvar Resumo Executivo Gold
    output_path = 'data/03_gold/resumo_sprint6.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(resumo, f, indent=4, ensure_ascii=False)
        
    print(f"Análise Final concluída. Resumo salvo em: {output_path}")
    print("\n--- DESTAQUES ---")
    print(f"Impacto Médio Bovinos: {resumo['impacto_producao']['delta_bovinos_medio_pct']:.2f}%")
    print(f"Reincidentes Identificados: {resumo['reincidencia']['total_infratores_reincidentes']}")
    print(f"Embargos Diretos Desmatamento: {resumo['status_desmatamento']['pct_direto_desmatamento']:.1f}%")

if __name__ == "__main__":
    sprint6_analise_final()
