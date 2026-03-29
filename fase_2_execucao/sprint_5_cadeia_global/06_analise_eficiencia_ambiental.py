import pandas as pd
import os

def analyze_eficiencia_ambiental():
    """
    Calcula a eficiência ambiental da exportação (USD exportado por hectare desmatado).
    """
    print("Iniciando análise de Eficiência Ambiental da Exportação...")
    
    # 1. Carregar dados do overlap (que já tem exportação e desmatamento por UF)
    overlap_path = 'data/03_gold/uf_exportacao_vs_desmatamento.parquet'
    df_overlap = pd.read_parquet(overlap_path)
    
    # 2. Agregar Valor Total Exportado por UF (todas commodities de interesse)
    # A área desmatada é a mesma por UF no dataset de overlap
    uf_agg = df_overlap.groupby('uf').agg({
        'vob_fob_usd': 'sum',
        'area_desmatada_ha': 'first'
    }).reset_index()
    
    # 3. Calcular Métrica de Eficiência
    # USD exportado por hectare de desmatamento
    # Se área desmatada for zero, colocamos como valor nulo para evitar divisão por zero
    # mas em nosso caso pode ser interpretado como "infinito" eficiente.
    # Vamos tratar como 0.0001 ha para UFs com desmatamento zero para dar um valor alto.
    uf_agg['area_calculo'] = uf_agg['area_desmatada_ha'].apply(lambda x: x if x > 0 else 0.001)
    
    uf_agg['usd_por_ha_desmatado'] = uf_agg['vob_fob_usd'] / uf_agg['area_calculo']
    
    # 4. Rank de Eficiência
    uf_agg['rank_eficiencia'] = uf_agg['usd_por_ha_desmatado'].rank(ascending=False, method='dense')
    
    # Ordenar
    uf_agg = uf_agg.sort_values('rank_eficiencia')
    
    # 5. Salvar em Gold
    output_path = 'data/03_gold/eficiencia_ambiental_exportacao.parquet'
    uf_agg.to_parquet(output_path, index=False)
    
    print(f"Análise de Eficiência Ambiental salva em: {output_path}")
    
    print("\nTop 5 UFs Mais Eficientes (Maior USD exportado / hectare desmatado):")
    print(uf_agg.head(5)[['uf', 'vob_fob_usd', 'area_desmatada_ha', 'usd_por_ha_desmatado']])
    
    print("\nTop 5 UFs Menos Eficientes (Menor USD exportado / hectare desmatado):")
    print(uf_agg.tail(5)[['uf', 'vob_fob_usd', 'area_desmatada_ha', 'usd_por_ha_desmatado']])

if __name__ == "__main__":
    analyze_eficiencia_ambiental()
