import pandas as pd
import os

def create_ncm_mapping():
    """
    Cria uma tabela de referência NCM -> Commodity baseada nos códigos de interesse.
    """
    print("Iniciando mapeamento de NCMs...")
    
    # Dicionário de mapeamento definido no plano da Sprint 5
    ncm_data = {
        'CO_NCM': [
            # Soja
            '12010000', '12011000', '12019000',
            # Milho
            '10051000', '10059000',
            # Carne Bovina
            '02011000', '02012000', '02013000', '02021000', '02022000', '02023000',
            # Café
            '09011100', '09011200', '09012100', '09012200',
            # Açúcar
            '17011300', '17011400', '17019100', '17019900',
            # Celulose
            '47032100', '47032900',
            # Madeira
            '44011000', '44031000', '44032000', '44071000', '44072100', '44072200', '44072500'
        ],
        'commodity': [
            'Soja', 'Soja', 'Soja',
            'Milho', 'Milho',
            'Carne Bovina', 'Carne Bovina', 'Carne Bovina', 'Carne Bovina', 'Carne Bovina', 'Carne Bovina',
            'Café', 'Café', 'Café', 'Café',
            'Açúcar', 'Açúcar', 'Açúcar', 'Açúcar',
            'Celulose', 'Celulose',
            'Madeira', 'Madeira', 'Madeira', 'Madeira', 'Madeira', 'Madeira', 'Madeira'
        ],
        'descricao_ncm': [
            'Soja em grão', 'Soja para semeadura', 'Outras soja',
            'Milho para semeadura', 'Outros milhos',
            'Carne bovina fresca, carcaça', 'Carne bovina fresca, pedaços', 'Carne bovina fresca, desossada',
            'Carne bovina congelada, carcaça', 'Carne bovina congelada, pedaços', 'Carne bovina congelada, desossada',
            'Café não torrado, não descafeinado', 'Café não torrado, descafeinado', 'Café torrado, não descafeinado', 'Café torrado, descafeinado',
            'Açúcar de cana, bruto', 'Outros açúcares de cana, bruto', 'Açúcar de cana, refinado', 'Outros açúcares de cana',
            'Pasta de celulose, branqueada', 'Pasta de celulose, não branqueada',
            'Lenha', 'Madeira em bruto, tratada', 'Madeira em bruto, não tratada', 'Madeira serrada, conífera', 
            'Madeira serrada, mogno', 'Madeira serrada, virola', 'Madeira serrada, tropical'
        ]
    }
    
    df_ncm = pd.DataFrame(ncm_data)
    
    # Garantir que CO_NCM seja string (como no Bronze)
    df_ncm['CO_NCM'] = df_ncm['CO_NCM'].astype(str)
    
    # Salvar em Silver
    output_path = 'data/02_silver/ncm_commodity_reference.parquet'
    df_ncm.to_parquet(output_path, index=False)
    
    print(f"Mapeamento NCM salvo em: {output_path}")
    print(f"Total de NCMs mapeados: {len(df_ncm)}")
    print(df_ncm.groupby('commodity').size())

if __name__ == "__main__":
    create_ncm_mapping()
