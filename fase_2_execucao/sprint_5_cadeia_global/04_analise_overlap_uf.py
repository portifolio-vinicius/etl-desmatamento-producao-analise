import pandas as pd
import os

def analyze_overlap_uf():
    """
    Cruza o ranking de exportação com o ranking de desmatamento/embargos por UF.
    """
    print("Iniciando análise de overlap Exportação × Desmatamento...")
    
    # 1. Carregar Dados
    ibama_path = 'data/02_silver/embargos_por_municipio_ano.parquet'
    dim_mun_path = 'data/02_silver/dim_municipio.parquet'
    ranking_export_path = 'data/03_gold/ranking_uf_exportadora.parquet'
    
    df_ibama = pd.read_parquet(ibama_path)
    df_dim_mun = pd.read_parquet(dim_mun_path)
    df_ranking_export = pd.read_parquet(ranking_export_path)
    
    # 2. Agregar IBAMA por UF e Ano (2023-2024)
    # Juntar com dim_mun para obter UF
    df_ibama_uf = df_ibama.merge(
        df_dim_mun[['cod_ibge', 'uf']], 
        left_on='cod_munici', 
        right_on='cod_ibge', 
        how='left'
    )
    
    df_ibama_filtered = df_ibama_uf[df_ibama_uf['ano'].isin([2023, 2024])].copy()
    
    ibama_uf_agg = df_ibama_filtered.groupby('uf').agg({
        'num_embargos': 'sum',
        'area_desmatada_ha': 'sum',
        'area_embargada_ha': 'sum'
    }).reset_index()
    
    # Calcular rank de desmatamento
    ibama_uf_agg['rank_desmatamento'] = ibama_uf_agg['area_desmatada_ha'].rank(ascending=False, method='dense')
    
    # 3. Cruzar com Exportações
    # Queremos ver as UFs exportadoras e seus índices de desmatamento
    overlap_df = df_ranking_export.merge(
        ibama_uf_agg,
        on='uf',
        how='left'
    )
    
    # Preencher nulos (UFs sem embargos) com zero
    overlap_df = overlap_df.fillna(0)
    
    # 4. Salvar em Gold
    output_path = 'data/03_gold/uf_exportacao_vs_desmatamento.parquet'
    overlap_df.to_parquet(output_path, index=False)
    
    print(f"Análise de Overlap salva em: {output_path}")
    
    # Mostrar correlação global entre exportação (USD) e área desmatada
    # (Agregando por UF para a correlação geral)
    uf_summary = overlap_df.groupby('uf').agg({
        'vob_fob_usd': 'sum',
        'area_desmatada_ha': 'first' # Área desmatada é a mesma para todas commodities da UF
    })
    
    correlation = uf_summary['vob_fob_usd'].corr(uf_summary['area_desmatada_ha'])
    print(f"\nCorrelação Geral (Exportação USD × Área Desmatada ha): {correlation:.4f}")
    
    print("\nTop 10 UFs por Área Desmatada (2023-2024) e suas exportações totais:")
    print(uf_summary.sort_values('area_desmatada_ha', ascending=False).head(10))

if __name__ == "__main__":
    analyze_overlap_uf()
