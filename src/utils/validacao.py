#!/usr/bin/env python
# coding: utf-8

"""
Módulo de funções de validação compartilhadas.
"""

import pandas as pd
from pathlib import Path


def validar_codigo_ibge(codigo: str) -> bool:
    """
    Valida se um código IBGE tem 7 dígitos.
    
    Args:
        codigo: Código IBGE como string
        
    Returns:
        bool: True se válido, False caso contrário
    """
    if codigo is None:
        return False
    codigo_str = str(codigo).strip()
    return codigo_str.isdigit() and len(codigo_str) == 7


def padronizar_codigo_ibge(codigo: str) -> str:
    """
    Padroniza código IBGE para 7 dígitos com zeros à esquerda.
    
    Args:
        codigo: Código IBGE como string ou int
        
    Returns:
        str: Código IBGE padronizado com 7 dígitos
    """
    codigo_str = str(codigo).strip().zfill(7)
    return codigo_str[-7:]


def validar_dataframe_nao_vazio(df: pd.DataFrame, nome_dataset: str) -> bool:
    """
    Valida se o DataFrame não está vazio.
    
    Args:
        df: DataFrame a validar
        nome_dataset: Nome do dataset para mensagem de erro
        
    Returns:
        bool: True se válido, False caso contrário
        
    Raises:
        ValueError: Se o DataFrame estiver vazio
    """
    if df is None or df.empty:
        raise ValueError(f"Dataset {nome_dataset} está vazio ou é None")
    return True


def validar_colunas_obrigatorias(
    df: pd.DataFrame, 
    colunas_obrigatorias: list, 
    nome_dataset: str
) -> bool:
    """
    Valida se as colunas obrigatórias existem no DataFrame.
    
    Args:
        df: DataFrame a validar
        colunas_obrigatorias: Lista de nomes de colunas obrigatórias
        nome_dataset: Nome do dataset para mensagem de erro
        
    Returns:
        bool: True se válido, False caso contrário
        
    Raises:
        ValueError: Se alguma coluna obrigatória estiver faltando
    """
    colunas_faltando = set(colunas_obrigatorias) - set(df.columns)
    if colunas_faltando:
        raise ValueError(
            f"Dataset {nome_dataset} está faltando colunas obrigatórias: {colunas_faltando}"
        )
    return True
