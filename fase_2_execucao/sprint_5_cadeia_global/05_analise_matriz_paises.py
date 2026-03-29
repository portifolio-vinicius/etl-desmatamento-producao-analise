import pandas as pd
import os

def analyze_matriz_paises():
    """
    Identifica os principais países compradores de commodities usando dados Bronze.
    """
    print("Iniciando análise de Matriz de Destino (Países)...")
    
    # 1. Carregar Tabelas de Referência
    ncm_ref = pd.read_parquet('data/02_silver/ncm_commodity_reference.parquet')
    pais_ref = pd.read_parquet('data/02_silver/pais_reference.parquet')
    
    # Lista de NCMs de interesse
    ncms_interesse = ncm_ref['CO_NCM'].tolist()
    
    # 2. Processar Dados Bronze (2023 e 2024)
    # Como os dados são grandes, processamos as colunas necessárias e filtramos cedo
    frames = []
    for ano in [2023, 2024]:
        path = f'data/01_bronze/comex/comex_stat/EXP_{ano}.parquet'
        if os.path.exists(path):
            print(f"Processando Bronze {ano}...")
            # Carregar apenas colunas necessárias
            df = pd.read_parquet(path, columns=['CO_ANO', 'CO_NCM', 'CO_PAIS', 'VL_FOB', 'KG_LIQUIDO'])
            
            # Garantir tipos
            df['CO_NCM'] = df['CO_NCM'].astype(str)
            
            # Filtrar por NCMs mapeados
            df_filtered = df[df['CO_NCM'].isin(ncms_interesse)].copy()
            
            # Agregar por Ano, NCM e País
            df_agg = df_filtered.groupby(['CO_ANO', 'CO_NCM', 'CO_PAIS']).agg({
                'VL_FOB': 'sum',
                'KG_LIQUIDO': 'sum'
            }).reset_index()
            
            frames.append(df_agg)
        else:
            print(f"Aviso: Arquivo {path} não encontrado.")
            
    if not frames:
        print("Erro: Nenhum dado bronze encontrado.")
        return
        
    df_full = pd.concat(frames)
    
    # 3. Join com Referências
    # Join NCM -> Commodity
    df_full = df_full.merge(ncm_ref[['CO_NCM', 'commodity']], on='CO_NCM', how='left')
    
    # Join País -> Nome do País
    df_full = df_full.merge(pais_ref, on='CO_PAIS', how='left')
    
    # Preencher países não mapeados
    df_full['nome_pais'] = df_full['nome_pais'].fillna(df_full['CO_PAIS'].apply(lambda x: f'Outro ({x})'))
    
    # 4. Agregação Final por País-Commodity
    matriz_destino = df_full.groupby(['nome_pais', 'commodity']).agg({
        'VL_FOB': 'sum',
        'KG_LIQUIDO': 'sum'
    }).reset_index()
    
    # Ordenar por valor
    matriz_destino = matriz_destino.sort_values(['commodity', 'VL_FOB'], ascending=[True, False])
    
    # 5. Salvar em Gold
    output_path = 'data/03_gold/matriz_destino_exportacao.parquet'
    matriz_destino.to_parquet(output_path, index=False)
    
    print(f"Matriz de Destino salva em: {output_path}")
    
    # Mostrar Top 5 Destinos por Commodity (ex: Soja)
    print("\nTop 5 Compradores de Soja (2023-2024):")
    print(matriz_destino[matriz_destino['commodity'] == 'Soja'].head(5))

if __name__ == "__main__":
    analyze_matriz_paises()
