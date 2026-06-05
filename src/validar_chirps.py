"""
Script para validar dados CHIRPS gerados.
"""

import pandas as pd
from pathlib import Path

def validar_chirps():
    """Valida qualidade dos dados CHIRPS."""
    
    caminho = Path("data/02_silver/chirps_municipal/chirps_amazonia_2020_2023.parquet")
    
    print("="*60)
    print("VALIDAÇÃO DOS DADOS CHIRPS")
    print("="*60)
    
    if not caminho.exists():
        print(f"❌ Arquivo não encontrado: {caminho}")
        return
    
    df = pd.read_parquet(caminho)
    
    print(f"\nShape: {df.shape}")
    print(f"Tamanho arquivo: {caminho.stat().st_size / 1024:.2f} KB")
    
    print(f"\nColunas: {list(df.columns)}")
    
    print(f"\nTipos de dados:")
    print(df.dtypes)
    
    print(f"\nValores nulos:")
    print(df.isnull().sum())
    
    print(f"\nEstatísticas descritivas:")
    print(df.describe())
    
    print(f"\nDados por ano:")
    print(df.groupby('ano').size())
    
    print(f"\nPrecipitação total por ano:")
    print(df.groupby('ano')['precipitacao_total_mm'].sum())
    
    print("\n" + "="*60)
    print("VALIDAÇÃO CONCLUÍDA")
    print("="*60)

if __name__ == '__main__':
    validar_chirps()
