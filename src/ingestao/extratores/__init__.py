#!/usr/bin/env python
# coding: utf-8

"""
Módulo de extratores para ingestão de dados de diversas fontes.

Este módulo contém classes para extrair dados de APIs públicas brasileiras
(INPE, IBGE, IBAMA, MDIC) seguindo o padrão de arquitetura medallion.
"""

from .extrator_wfs import ExtratorWFS
from .extrator_prodes import ExtratorPRODES
from .extrator_deter import ExtratorDETER

__all__ = [
    "ExtratorWFS",
    "ExtratorPRODES",
    "ExtratorDETER",
]
