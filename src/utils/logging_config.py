#!/usr/bin/env python
# coding: utf-8

"""
Módulo de configuração de logging compartilhado.
"""

import logging
from pathlib import Path


def configurar_logging(
    nome_arquivo: str = "pipeline_etl.log",
    nivel: int = logging.INFO,
    formato: str = "%(asctime)s - %(levelname)s - %(message)s",
    modo_arquivo: str = "a",
    logs_dir: Path = None
) -> None:
    """
    Configura o logging do projeto.
    
    Args:
        nome_arquivo: Nome do arquivo de log
        nivel: Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        formato: Formato das mensagens de log
        modo_arquivo: Modo de abertura do arquivo ('a' para append, 'w' para overwrite)
        logs_dir: Diretório para salvar os logs (padrão: logs/)
    """
    if logs_dir is None:
        logs_dir = Path("logs")
    
    logs_dir.mkdir(parents=True, exist_ok=True)
    caminho_log = logs_dir / nome_arquivo
    
    logging.basicConfig(
        filename=str(caminho_log),
        level=nivel,
        format=formato,
        filemode=modo_arquivo,
    )
