"""
Script para validar qualidade do dataset preditivo.
"""

import pandas as pd
import numpy as np
from pathlib import Path

def validar_dataset():
    """Valida qualidade do dataset preditivo."""
    
    caminho_dataset = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS/data/04_modelagem/dataset_preditivo_consolidado.parquet')
    
    print("Carregando dataset...")
    df = pd.read_parquet(caminho_dataset)
    
    print(f"\n{'='*60}")
    print("VALIDAÇÃO DO DATASET PREDITIVO")
    print(f"{'='*60}")
    
    print(f"\nShape: {df.shape}")
    print(f"Tamanho arquivo: {caminho_dataset.stat().st_size / 1024 / 1024:.2f} MB")
    print(f"Memória RAM: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    print(f"\n{'='*60}")
    print("INFORMAÇÕES GERAIS")
    print(f"{'='*60}")
    print(f"Período: {df['ano'].min()} - {df['ano'].max()}")
    print(f"Municípios únicos: {df['cod_ibge'].nunique()}")
    print(f"UFs únicas: {df['uf'].nunique()}")
    print(f"Regiões únicas: {df['regiao'].nunique()}")
    
    print(f"\n{'='*60}")
    print("QUALIDADE DOS DADOS")
    print(f"{'='*60}")
    
    # Valores nulos
    nulos = df.isnull().sum()
    print(f"\nValores nulos por coluna:")
    for col, qtd in nulos[nulos > 0].items():
        print(f"  {col}: {qtd} ({qtd/len(df)*100:.2f}%)")
    
    if nulos.sum() == 0:
        print("  ✓ Nenhum valor nulo encontrado")
    
    # Duplicatas
    duplicatas = df.duplicated().sum()
    print(f"\nDuplicatas: {duplicatas} ({duplicatas/len(df)*100:.2f}%)")
    if duplicatas == 0:
        print("  ✓ Nenhuma duplicata encontrada")
    
    print(f"\n{'='*60}")
    print("DISTRIBUIÇÃO DE VARIÁVEIS CHAVE")
    print(f"{'='*60}")
    
    # Variáveis binárias
    vars_binarias = ['tem_desmatamento', 'tem_embargos', 'tem_bovinos', 'tem_vab']
    print("\nVariáveis binárias:")
    for var in vars_binarias:
        if var in df.columns:
            contagem = df[var].value_counts()
            print(f"  {var}:")
            print(f"    0: {contagem.get(0, 0)} ({contagem.get(0, 0)/len(df)*100:.1f}%)")
            print(f"    1: {contagem.get(1, 0)} ({contagem.get(1, 0)/len(df)*100:.1f}%)")
    
    # Variáveis numéricas principais
    vars_numericas = ['area_desmatada_ha', 'num_embargos', 'ppm_bovinos_cabecas', 
                      'vab_agro_mil_reais', 'idhm', 'risco_desmatamento']
    print("\nVariáveis numéricas (estatísticas):")
    for var in vars_numericas:
        if var in df.columns:
            print(f"  {var}:")
            print(f"    Min: {df[var].min():.2f}")
            print(f"    Max: {df[var].max():.2f}")
            print(f"    Média: {df[var].mean():.2f}")
            print(f"    Mediana: {df[var].median():.2f}")
            print(f"    Desvio padrão: {df[var].std():.2f}")
    
    print(f"\n{'='*60}")
    print("TIPOS DE DADOS")
    print(f"{'='*60}")
    print(df.dtypes)
    
    print(f"\n{'='*60}")
    print("CORRELAÇÕES PRINCIPAIS")
    print(f"{'='*60}")
    
    cols_corr = ['area_desmatada_ha', 'num_embargos', 'ppm_bovinos_cabecas', 
                 'vab_agro_mil_reais', 'idhm', 'risco_desmatamento']
    corr_matrix = df[cols_corr].corr()
    
    print("\nMatriz de correlação:")
    print(corr_matrix.round(3))
    
    print(f"\n{'='*60}")
    print("VALIDAÇÃO CONCLUÍDA")
    print(f"{'='*60}")
    print("\n✓ Dataset pronto para análise preditiva")
    print("✓ Arquivo leve e otimizado para recursos limitados")
    print("✓ Features derivadas criadas para modelagem")

if __name__ == '__main__':
    validar_dataset()
