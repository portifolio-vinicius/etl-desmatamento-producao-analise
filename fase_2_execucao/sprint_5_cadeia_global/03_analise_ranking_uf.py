import pandas as pd
import os

def analyze_ranking_uf():
    """
    Gera o ranking de UFs exportadoras por commodity.
    """
    print("Iniciando análise de ranking por UF...")
    
    # Carregar COMEX Silver
    input_path = 'data/02_silver/comex_por_uf_ano.parquet'
    df_comex = pd.read_parquet(input_path)
    
    # Filtrar apenas Exportação e anos 2023-2024 (completos)
    df_filtered = df_comex[
        (df_comex['tipo_operacao'] == 'Exportação') & 
        (df_comex['ano'].isin([2023, 2024]))
    ].copy()
    
    # Agregar por UF e Commodity
    ranking_uf = df_filtered.groupby(['uf', 'commodity']).agg({
        'vob_fob_usd': 'sum',
        'peso_kg': 'sum',
        'num_operacoes': 'sum'
    }).reset_index()
    
    # Calcular rank por commodity (baseado em valor USD)
    ranking_uf['rank_valor'] = ranking_uf.groupby('commodity')['vob_fob_usd'].rank(ascending=False, method='dense')
    
    # Ordenar
    ranking_uf = ranking_uf.sort_values(['commodity', 'rank_valor'])
    
    # Salvar em Gold
    output_path = 'data/03_gold/ranking_uf_exportadora.parquet'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    ranking_uf.to_parquet(output_path, index=False)
    
    print(f"Ranking UF salvo em: {output_path}")
    print("\nTop 5 UFs Exportadoras de Soja (2023-2024):")
    print(ranking_uf[ranking_uf['commodity'] == 'Soja'].head(5))
    
    print("\nTop 5 UFs Exportadoras de Carne Bovina (2023-2024):")
    print(ranking_uf[ranking_uf['commodity'] == 'Carne Bovina'].head(5))

if __name__ == "__main__":
    analyze_ranking_uf()
