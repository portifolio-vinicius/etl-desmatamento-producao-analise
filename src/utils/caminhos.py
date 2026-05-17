#!/usr/bin/env python
# coding: utf-8

"""
Módulo de utilitários para gerenciamento de caminhos do projeto.
"""

from pathlib import Path


def repo_root() -> Path:
    """
    Retorna o diretório raiz do repositório.
    
    Busca recursivamente pelo arquivo requirements.txt para identificar a raiz.
    
    Returns:
        Path: Caminho absoluto do diretório raiz do repositório
        
    Raises:
        RuntimeError: Se requirements.txt não for encontrado
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
    """
    
    DATA_DIR = Path("data")
    LANDING_DIR = DATA_DIR / "landing"
    BRONZE_DIR = DATA_DIR / "01_bronze"
    SILVER_DIR = DATA_DIR / "02_silver"
    GOLD_DIR = DATA_DIR / "03_gold"
    REPORTS_DIR = DATA_DIR / "04_reports"
    
    @classmethod
    def inicializar_pastas(cls):
        """
        Cria os diretórios de dados se não existirem.
        """
        for p in [cls.DATA_DIR, cls.LANDING_DIR, cls.BRONZE_DIR, 
                  cls.SILVER_DIR, cls.GOLD_DIR, cls.REPORTS_DIR]:
            p.mkdir(parents=True, exist_ok=True)
