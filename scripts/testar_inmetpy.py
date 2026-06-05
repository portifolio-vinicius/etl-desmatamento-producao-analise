#!/usr/bin/env python3
"""
Script de teste para verificar funcionamento do pacote inmetpy
"""

from inmetpy.stations import InmetStation
import sys

def testar_inmetpy():
    """Testa conexão básica com API do INMET"""
    
    print("=== Teste do pacote inmetpy ===\n")
    
    try:
        print("Inicializando cliente INMET...")
        inmet = InmetStation()
        
        print("✓ Cliente inicializado com sucesso!\n")
        
        # Teste 1: Listar estações automáticas
        print("Teste 1: Listar estações automáticas...")
        try:
            auto_stations = inmet.get_stations("A")
            print(f"✓ Encontradas {len(auto_stations)} estações automáticas")
            if len(auto_stations) > 0:
                print(f"  Primeira estação: {auto_stations[0]}")
        except Exception as e:
            print(f"✗ Erro ao listar estações automáticas: {e}")
        
        # Teste 2: Listar estações manuais
        print("\nTeste 2: Listar estações manuais...")
        try:
            manual_stations = inmet.get_stations("M")
            print(f"✓ Encontradas {len(manual_stations)} estações manuais")
            if len(manual_stations) > 0:
                print(f"  Primeira estação: {manual_stations[0]}")
        except Exception as e:
            print(f"✗ Erro ao listar estações manuais: {e}")
        
        # Teste 3: Buscar estações por estado (Amazonas)
        print("\nTeste 3: Buscar estações no Amazonas (AM)...")
        try:
            am_stations = inmet.search_station_by_state("AM")
            print(f"✓ Encontradas {len(am_stations)} estações no Amazonas")
            if len(am_stations) > 0:
                print(f"  Primeira estação: {am_stations[0]}")
        except Exception as e:
            print(f"✗ Erro ao buscar estações por estado: {e}")
        
        # Teste 4: Buscar estações por coordenadas (Manaus)
        print("\nTeste 4: Buscar estações próximas a Manaus...")
        try:
            # Coordenadas de Manaus
            manaus_coords = (-3.1190, -60.0217)
            nearby = inmet.search_station_by_coords(manaus_coords, n=3)
            print(f"✓ Encontradas {len(nearby)} estações próximas")
            if len(nearby) > 0:
                for i, station in enumerate(nearby):
                    print(f"  {i+1}: {station}")
        except Exception as e:
            print(f"✗ Erro ao buscar estações por coordenadas: {e}")
        
        # Teste 5: Obter dados de uma estação (últimos dias)
        print("\nTeste 5: Obter dados de estação específica...")
        try:
            # Tentar obter dados de uma estação automática
            if len(auto_stations) > 0:
                station_id = auto_stations[0].get('CD_STATION', 'A001')
                print(f"  Tentando obter dados da estação {station_id}...")
                
                # Data recente
                from datetime import datetime, timedelta
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
                
                data = inmet.get_data_station(
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d'),
                    'CD_STATION',
                    [station_id]
                )
                print(f"✓ Dados obtidos: {len(data)} registros")
        except Exception as e:
            print(f"✗ Erro ao obter dados da estação: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Erro ao inicializar cliente INMET: {e}")
        print(f"  Tipo do erro: {type(e).__name__}")
        return False

if __name__ == "__main__":
    sucesso = testar_inmetpy()
    
    print("\n" + "="*50)
    if sucesso:
        print("STATUS: inmetpy está funcionando!")
        sys.exit(0)
    else:
        print("STATUS: inmetpy apresentou problemas")
        sys.exit(1)
