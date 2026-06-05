"""
Testar fallback para Notícias Agrícolas (pode ter histórico mais longo)
"""

import asyncio
from agrobr import alt

async def testar_noticias_agricolas():
    """Testa Notícias Agrícolas como fallback"""
    print("=== Testando Notícias Agrícolas (fallback CEPEA) ===\n")
    
    # Notícias Agrícolas está em alt namespace
    print("1. Listando fontes alternativas disponíveis...")
    try:
        # agrobr tem um módulo alt para fontes alternativas
        from agrobr import alt
        print(f"   Módulo alt disponível")
        print(f"   Atributos: {[x for x in dir(alt) if not x.startswith('_')]}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Tentar acessar diretamente
    print("\n2. Tentando acessar cotações via alt...")
    try:
        # Verificar se há algo para cotações
        if hasattr(alt, 'noticias_agricolas'):
            print("   noticias_agricolas disponível")
        else:
            print("   noticias_agricolas não encontrado diretamente")
    except Exception as e:
        print(f"   Erro: {e}")

if __name__ == "__main__":
    asyncio.run(testar_noticias_agricolas())
