#!/usr/bin/env python3
"""
Script de teste para verificar funcionamento do pacote climateserv
"""

import climateserv
import sys

def testar_climateserv():
    """Testa conexão básica com API do ClimateSERV"""
    
    print("=== Teste do pacote climateserv ===\n")
    
    # Coordenadas de uma pequena área de teste (região de Manaus)
    # Bounding box pequeno para teste rápido
    GeometryCoords = [[-60.02, -3.12], [-59.98, -3.12], [-59.98, -3.08], [-60.02, -3.08], [-60.02, -3.12]]
    
    # DatasetType 28 = CHIRPS daily precipitation
    DatasetType = 28
    OperationType = 'Average'
    
    # Período curto para teste (1 mês)
    EarliestDate = '01/01/2023'
    LatestDate = '01/31/2023'
    
    SeasonalEnsemble = ''
    SeasonalVariable = ''
    Outfile = 'teste_chirps.csv'
    
    print(f"Configuração do teste:")
    print(f"  - Dataset: CHIRPS (ID: {DatasetType})")
    print(f"  - Operação: {OperationType}")
    print(f"  - Período: {EarliestDate} a {LatestDate}")
    print(f"  - Coordenadas: {GeometryCoords}")
    print(f"  - Arquivo de saída: {Outfile}")
    print()
    
    try:
        print("Tentando conectar à API do ClimateSERV...")
        resultado = climateserv.api.request_data(
            DatasetType, OperationType, EarliestDate, LatestDate,
            GeometryCoords, SeasonalEnsemble, SeasonalVariable, Outfile
        )
        
        print("✓ Requisição enviada com sucesso!")
        print(f"✓ Arquivo '{Outfile}' gerado")
        print()
        
        # Verificar se arquivo foi criado
        import os
        if os.path.exists(Outfile):
            tamanho = os.path.getsize(Outfile)
            print(f"✓ Arquivo existe com {tamanho} bytes")
            
            # Mostrar primeiras linhas
            with open(Outfile, 'r') as f:
                linhas = f.readlines()
                print(f"\nPrimeiras 5 linhas do arquivo:")
                for i, linha in enumerate(linhas[:5]):
                    print(f"  {i+1}: {linha.strip()}")
            
            return True
        else:
            print("✗ Arquivo não foi criado")
            return False
            
    except Exception as e:
        print(f"✗ Erro ao conectar à API: {e}")
        print(f"  Tipo do erro: {type(e).__name__}")
        return False

if __name__ == "__main__":
    sucesso = testar_climateserv()
    
    print("\n" + "="*50)
    if sucesso:
        print("STATUS: climateserv está funcionando corretamente!")
        sys.exit(0)
    else:
        print("STATUS: climateserv apresentou problemas")
        sys.exit(1)
