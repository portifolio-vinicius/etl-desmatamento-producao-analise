#!/usr/bin/env python
# coding: utf-8

"""
Módulo de utilitários para gerenciamento de caminhos do projeto.

Este módulo fornece funções e classes para gerenciar caminhos de dados
do projeto seguindo a arquitetura medallion (Bronze/Silver/Gold).
"""

from pathlib import Path
from typing import List

# Importar tipos customizados
from .types import CaminhoArquivo


def repo_root() -> Path:
    """
    Retorna o diretório raiz do repositório.
    
    Busca recursivamente pelo arquivo requirements.txt para identificar a raiz.
    
    Returns:
        Path: Caminho absoluto do diretório raiz do repositório
        
    Raises:
        RuntimeError: Se requirements.txt não for encontrado
        
    Examples:
        >>> root = repo_root()
        >>> print(f"Raiz do repositório: {root}")
    """
    p = Path(__file__).resolve()
    for d in (p.parent, *p.parents):
        if (d / "requirements.txt").exists():
            return d
    raise RuntimeError(
        "requirements.txt não encontrado. Execute a partir do clone do repositório."
    )


class CaminhosDados:
    """
    Gerencia os caminhos das camadas de dados do projeto.
    
    Esta classe centraliza os caminhos das camadas de dados seguindo
    a arquitetura medallion: Landing → Bronze → Silver → Gold → Reports.
    """
    
    DATA_DIR: Path = Path("data")
    LANDING_DIR: Path = DATA_DIR / "landing"
    BRONZE_DIR: Path = DATA_DIR / "01_bronze"
    SILVER_DIR: Path = DATA_DIR / "02_silver"
    GOLD_DIR: Path = DATA_DIR / "03_gold"
    REPORTS_DIR: Path = DATA_DIR / "04_reports"
    
    @classmethod
    def inicializar_pastas(cls) -> None:
        """
        Cria os diretórios de dados se não existirem.
        
        Este método garante que todas as camadas de dados existam antes
        de executar pipelines ETL, evitando erros de arquivo não encontrado.
        
    Examples:
        >>> CaminhosDados.inicializar_pastas()
        >>> print("Pastas de dados inicializadas")
        """
        pastas: List[Path] = [
            cls.DATA_DIR, 
            cls.LANDING_DIR, 
            cls.BRONZE_DIR, 
            cls.SILVER_DIR, 
            cls.GOLD_DIR, 
            cls.REPORTS_DIR
        ]
        
        for p in pastas:
            p.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def caminho_bronze(cls, nome_arquivo: str) -> CaminhoArquivo:
        """
        Retorna caminho completo para arquivo na camada Bronze.
        
        Args:
            nome_arquivo: Nome do arquivo (com ou sem extensão)
            
        Returns:
            Caminho completo para arquivo na camada Bronze
            
        Examples:
            >>> caminho = CaminhosDados.caminho_bronze("dados.parquet")
            >>> print(caminho)
        """
        return str(cls.BRONZE_DIR / nome_arquivo)
    
    @classmethod
    def caminho_silver(cls, nome_arquivo: str) -> CaminhoArquivo:
        """
        Retorna caminho completo para arquivo na camada Silver.
        
        Args:
            nome_arquivo: Nome do arquivo (com ou sem extensão)
            
        Returns:
            Caminho completo para arquivo na camada Silver
            
        Examples:
            >>> caminho = CaminhosDados.caminho_silver("dados_limpos.parquet")
            >>> print(caminho)
        """
        return str(cls.SILVER_DIR / nome_arquivo)
    
    @classmethod
    def caminho_gold(cls, nome_arquivo: str) -> CaminhoArquivo:
        """
        Retorna caminho completo para arquivo na camada Gold.
        
        Args:
            nome_arquivo: Nome do arquivo (com ou sem extensão)
            
        Returns:
            Caminho completo para arquivo na camada Gold
            
        Examples:
            >>> caminho = CaminhosDados.caminho_gold("analise_final.parquet")
            >>> print(caminho)
        """
        return str(cls.GOLD_DIR / nome_arquivo)
