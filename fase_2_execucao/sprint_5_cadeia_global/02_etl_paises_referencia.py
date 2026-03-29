import pandas as pd
import os

def create_country_mapping():
    """
    Cria uma tabela de referência Código -> País baseada nos dados do COMEX.
    """
    print("Iniciando mapeamento de Países...")
    
    # Dicionário baseado nos top códigos identificados na análise preliminar
    # e códigos comuns do MDIC/Comex Stat
    country_data = {
        'CO_PAIS': [
            160, 586, 249, 63, 845, 580, 476, 158, 434, 169, 493,
            23, 105, 386, 756, 607, 245, 858, 87, 670, 764,
            776, 827, 240, 317, 351, 301, 161, 196, 603, 359, 23, 687
        ],
        'nome_pais': [
            'China', 'China', 'Estados Unidos', 'Argentina', 'Holanda', 'Japão', 
            'Espanha', 'Chile', 'Itália', 'Colômbia', 'México',
            'Alemanha', 'Brasil (Reimportação)', 'Iraque', 'Suíça', 'Portugal',
            'Egito', 'Uruguai', 'Áustria', 'Romênia', 'Tailândia',
            'Vietnã', 'Turquia', 'Equador', 'Coreia do Sul', 'França', 'Gana', 'China', 'Coreia do Norte', 'Índia', 'Indonésia', 'Alemanha', 'Irlanda'
        ]
    }
    
    df_paises = pd.DataFrame(country_data)
    
    # Salvar em Silver
    output_path = 'data/02_silver/pais_reference.parquet'
    df_paises.to_parquet(output_path, index=False)
    
    print(f"Mapeamento de Países salvo em: {output_path}")
    print(f"Total de países mapeados: {len(df_paises)}")
    print(df_paises.head(10))

if __name__ == "__main__":
    create_country_mapping()
