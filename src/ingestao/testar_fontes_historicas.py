"""
Testar fontes alternativas no agrobr para dados históricos de preços
"""

import asyncio
from agrobr import b3, conab, ibge

async def testar_b3_futuros():
    """Testa B3 futuros agro - pode ter histórico mais longo"""
    print("=== Testando B3 Futuros Agro ===\n")
    
    # Testar ajustes diários (settlement)
    print("1. Testando b3.ajustes (soja)...")
    try:
        df = await b3.ajustes(data="05/06/2026")
        print(f"   Registros: {len(df)}")
        if len(df) > 0:
            print(f"   Colunas: {df.columns.tolist()}")
            print(f"   Amostra:\n{df.head()}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Testar histórico
    print("\n2. Testando b3.historico (boi)...")
    try:
        df = await b3.historico(contrato="boi", inicio="01/01/2024", fim="05/06/2026")
        print(f"   Registros: {len(df)}")
        if len(df) > 0:
            print(f"   Colunas: {df.columns.tolist()}")
            print(f"   Período: {df['data'].min()} a {df['data'].max()}")
    except Exception as e:
        print(f"   Erro: {e}")

async def testar_conab_serie_historica():
    """Testa CONAB série histórica"""
    print("\n=== Testando CONAB Série Histórica ===\n")
    
    from agrobr import conab
    print("1. Testando conab.serie_historica (soja)...")
    try:
        df = await conab.serie_historica('soja', inicio=2020, fim=2024)
        print(f"   Registros: {len(df)}")
        if len(df) > 0:
            print(f"   Colunas: {df.columns.tolist()}")
            print(f"   Amostra:\n{df.head()}")
    except Exception as e:
        print(f"   Erro: {e}")

async def testar_ibge_pam():
    """Testa IBGE PAM - Produção Agrícola Municipal"""
    print("\n=== Testando IBGE PAM ===\n")
    
    print("1. Testando ibge.pam (soja)...")
    try:
        df = await ibge.pam('soja', ano=2023, nivel='uf')
        print(f"   Registros: {len(df)}")
        if len(df) > 0:
            print(f"   Colunas: {df.columns.tolist()}")
            print(f"   Amostra:\n{df.head()}")
    except Exception as e:
        print(f"   Erro: {e}")

async def main():
    await testar_b3_futuros()
    await testar_conab_serie_historica()
    await testar_ibge_pam()

if __name__ == "__main__":
    asyncio.run(main())
