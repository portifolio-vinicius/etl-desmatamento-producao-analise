"""
Testar se agrobr tem fontes alternativas para dados históricos
"""

import asyncio
from agrobr import datasets

async def testar_datasets_historicos():
    """Testa datasets que podem ter dados históricos"""
    
    print("=== Testando datasets agrobr para dados históricos ===\n")
    
    # Testar dataset preco_diario (tem fallback)
    print("1. Testando datasets.preco_diario (soja)...")
    try:
        df, meta = await datasets.preco_diario('soja', return_meta=True)
        print(f"   Registros: {len(df)}")
        print(f"   Fonte usada: {meta.selected_source}")
        print(f"   Fontes tentadas: {meta.attempted_sources}")
        if len(df) > 0:
            print(f"   Período: {df['data'].min()} a {df['data'].max()}")
            print(f"   Colunas: {df.columns.tolist()}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Testar com dataset específico
    print("\n2. Listando todos os datasets disponíveis...")
    try:
        lista = datasets.list_datasets()
        print(f"   Datasets: {lista}")
    except Exception as e:
        print(f"   Erro: {e}")

if __name__ == "__main__":
    asyncio.run(testar_datasets_historicos())
