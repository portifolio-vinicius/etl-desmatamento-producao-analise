import pandas as pd
import numpy as np
import os
from scipy import stats

def etl_6_2_impacto_producao():
    print("Iniciando ETL 6.2: Impacto na Produção (Antes vs Depois)...")
    
    # 1. Carregar dados
    pam_path = 'data/02_silver/pam_consolidado.parquet'
    ppm_path = 'data/02_silver/ppm_consolidado.parquet'
    emb_path = 'data/02_silver/embargos_por_municipio_ano.parquet'
    dim_path = 'data/02_silver/dim_municipio.parquet'
    
    if not all(os.path.exists(p) for p in [pam_path, ppm_path, emb_path, dim_path]):
        print("Erro: Arquivos Silver não encontrados.")
        return

    df_pam = pd.read_parquet(pam_path)
    df_ppm = pd.read_parquet(ppm_path)
    df_emb = pd.read_parquet(emb_path)
    df_dim = pd.read_parquet(dim_path)

    # 2. Corrigir chaves
    # PAM usa chave_municipio (Nome_UF). Requer converter para cod_ibge usando dim_municipio
    # Note: No momento dim_municipio tem municipio vazio, vamos tentar usar o pib_vab para extrair a ponte Nome_UF -> cod_ibge
    pib_path = 'data/02_silver/pib_vab_consolidado.parquet'
    df_pib = pd.read_parquet(pib_path)
    # Tentar reconstruir a ponte do PIB (já que PIB tem cod_ibge mas não UF direto na silver? Vamos ver)
    # Ops, pib_vab_consolidado.parquet tem cod_ibge, ano, vab_agro_mil_reais
    # Vamos usar a serie_historica que já tem cod_ibge e possivelmente os outros dados
    
    serie_path = 'data/02_silver/serie_historica_2020_2023.parquet'
    df_serie = pd.read_parquet(serie_path)
    # A serie já tem: cod_ibge, ano, vab, ppm_bovinos, num_embargos...
    
    # Focar na serie_historica para facilitar, mas precisamos de PAM nela
    # Como PAM não está na serie, vamos apenas processar PPM e PIB da serie, e tentar PAM se possível
    
    # Identificar primeiro ano de embargo por município (2021 ou 2022)
    df_emb_analise = df_emb[df_emb['ano'].isin([2021, 2022])].rename(columns={'cod_munici': 'cod_ibge'})
    primeiro_embargo = df_emb_analise.groupby('cod_ibge')['ano'].min().reset_index()
    primeiro_embargo.columns = ['cod_ibge', 'ano_0']
    
    # Join com dados da serie
    resultados = []
    for _, row in primeiro_embargo.iterrows():
        cod = row['cod_ibge']
        ano0 = int(row['ano_0'])
        
        # Antes (ano0 - 1) vs Depois (ano0 + 1)
        dados_mun = df_serie[df_serie['cod_ibge'] == cod]
        
        antes = dados_mun[dados_mun['ano'] == (ano0 - 1)]
        depois = dados_mun[dados_mun['ano'] == (ano0 + 1)]
        
        if not antes.empty and not depois.empty:
            res = {
                'cod_ibge': cod,
                'ano_embargo': ano0,
                'vab_antes': antes['vab_agro_mil_reais'].values[0],
                'vab_depois': depois['vab_agro_mil_reais'].values[0],
                'bovinos_antes': antes['ppm_bovinos_cabecas'].values[0],
                'bovinos_depois': depois['ppm_bovinos_cabecas'].values[0]
            }
            # Delta pct
            res['delta_vab_pct'] = ((res['vab_depois'] - res['vab_antes']) / res['vab_antes'] * 100) if res['vab_antes'] > 0 else 0
            res['delta_bovinos_pct'] = ((res['bovinos_depois'] - res['bovinos_antes']) / res['bovinos_antes'] * 100) if res['bovinos_antes'] > 0 else 0
            
            # Nova métrica: Sucesso (Redução de rebanho)
            res['sucesso_embargo'] = 1 if res['delta_bovinos_pct'] < -1 else 0 # Margem de 1%
            res['aumento_pos_embargo'] = 1 if res['delta_bovinos_pct'] > 1 else 0
            
            resultados.append(res)
            
    df_impacto = pd.DataFrame(resultados)
    
    if not df_impacto.empty:
        # Teste t pareado (apenas se houver amostra suficiente)
        # Nota: VAB 2022/2023 é 0 na serie atual, então o delta_vab será distorcido se ano_embargo for 2021 ou 2022
        # Vamos focar no impacto em Bovinos que tem dados reais 2021-2023
        t_stat, p_val = stats.ttest_rel(df_impacto['bovinos_antes'], df_impacto['bovinos_depois'])
        print(f"P-valor (Bovinos Antes vs Depois): {p_val:.4f}")
        
    # Salvar Gold
    output_path = 'data/03_gold/impacto_embargo_producao.parquet'
    df_impacto.to_parquet(output_path)
    print(f"ETL 6.2 concluída. Salvo em: {output_path}")
    print(f"Municípios com análise Antes/Depois: {len(df_impacto)}")

if __name__ == "__main__":
    etl_6_2_impacto_producao()
