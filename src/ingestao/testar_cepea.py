"""
Script simples para testar acesso ao CEPEA via agrobr
"""

import asyncio
from agrobr import cepea

async def testar_cepea():
    """Testa diferentes produtos e períodos"""
    
    print("=== Testando CEPEA via agrobr ===\n")
    
    # Testar lista de produtos disponíveis
    print("1. Listando produtos disponíveis...")
    try:
        produtos = await cepea.produtos()
        print(f"   Produtos: {produtos}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Testar último preço de soja
    print("\n2. Testando último preço de soja...")
    try:
        ultimo = await cepea.ultimo('soja')
        print(f"   Último preço: R$ {ultimo.valor}/sc em {ultimo.data}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Testar indicador com período curto
    print("\n3. Testando indicador soja (últimos 30 dias)...")
    try:
        from datetime import datetime, timedelta
        fim = datetime.now()
        inicio = fim - timedelta(days=30)
        df = await cepea.indicador('soja', inicio=inicio.strftime("%Y-%m-%d"))
        print(f"   Registros: {len(df)}")
        if len(df) > 0:
            print(f"   Colunas: {df.columns.tolist()}")
            print(f"   Primeiras linhas:\n{df.head()}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Testar com nome alternativo
    print("\n4. Testando com produto 'boi_gordo'...")
    try:
        df = await cepea.indicador('boi_gordo', inicio='2024-01-01')
        print(f"   Registros: {len(df)}")
    except Exception as e:
        print(f"   Erro: {e}")

if __name__ == "__main__":
    asyncio.run(testar_cepea())
