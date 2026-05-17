#!/usr/bin/env python
# coding: utf-8

"""
Extrator de dados PRODES (Projeto de Monitoramento do Desmatamento na Amazônia Legal).

Este extrator baixa dados de desmatamento anual do PRODES via API WFS do INPE/TerraBrasilis.
"""

import logging
from typing import List
import pandas as pd

from .extrator_wfs import ExtratorWFS


class ExtratorPRODES(ExtratorWFS):
    """
    Extrator de dados do PRODES via API WFS do INPE/TerraBrasilis.
    
    Baixa dados de desmatamento anual por polígono na Amazônia Legal,
    com campos de ano, área, estado e geometria espacial.
    """
    
    def __init__(
        self,
        anos: List[int],
        chunk_size: int = 1000,
        timeout: int = 30
    ):
        """
        Inicializa o extrator PRODES.
        
        Args:
            anos: Lista de anos para extração (2008-2023 disponíveis)
            chunk_size: Tamanho de cada página (chunk) para paginação
            timeout: Timeout para requisições HTTP em segundos
        """
        super().__init__(anos, chunk_size, timeout)
        self.logger = logging.getLogger("ExtratorPRODES")
    
    @property
    def base_url(self) -> str:
        """URL base da API WFS do PRODES."""
        return "https://terrabrasilis.dpi.inpe.br/geoserver/prodes-legal-amz/wfs"
    
    @property
    def workspace(self) -> str:
        """Nome do workspace no GeoServer."""
        return "prodes-legal-amz"
    
    @property
    def layer(self) -> str:
        """Nome do layer no GeoServer."""
        return "yearly_deforestation"
    
    @property
    def campo_ordenacao(self) -> str:
        """Campo usado para ordenação."""
        return "uid"
    
    def construir_filtro_ano(self, ano: int) -> str:
        """
        Constrói filtro CQL para um ano específico.
        
        Args:
            ano: Ano para filtro
            
        Returns:
            String com filtro CQL
        """
        return f"year = {ano}"
    
    def _features_para_dataframe(self, features: List[dict]) -> pd.DataFrame:
        """
        Converte lista de features GeoJSON para DataFrame pandas.
        
        Args:
            features: Lista de features GeoJSON
            
        Returns:
            DataFrame com os dados (sem geometria)
        """
        if not features:
            return pd.DataFrame()
        
        records = []
        for feature in features:
            props = feature.get("properties", {})
            # Não incluir geometria (dados tabulares apenas)
            records.append(props)
        
        return pd.DataFrame(records)
    
    def extrair_agregado_municipio(self, ano: int) -> pd.DataFrame:
        """
        Extrai dados de um ano e agrega por município (se disponível).
        
        NOTA: O layer yearly_deforestation não tem campo de município direto.
        Este método é mantido para compatibilidade futura com outros layers.
        
        Args:
            ano: Ano para extração
            
        Returns:
            DataFrame com dados agregados por estado
        """
        self.logger.warning(f"Layer {self.layer} não tem campo de município. Retornando dados brutos.")
        
        dfs = list(self.extrair_ano(ano))
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()
