#!/usr/bin/env python
# coding: utf-8

"""
Extrator de dados DETER (Detecção de Desmatamento em Tempo Real).

Este extrator baixa dados de alertas de desmatamento do DETER via API WFS do INPE/TerraBrasilis.
"""

import logging
from typing import List
import pandas as pd

from .extrator_wfs import ExtratorWFS


class ExtratorDETER(ExtratorWFS):
    """
    Extrator de dados do DETER via API WFS do INPE/TerraBrasilis.
    
    Baixa dados de alertas de desmatamento diários com campos de data,
    município, UF, área e geometria espacial.
    """
    
    def __init__(
        self,
        anos: List[int],
        chunk_size: int = 1000,
        timeout: int = 30
    ):
        """
        Inicializa o extrator DETER.
        
        Args:
            anos: Lista de anos para extração (2016-2023 disponíveis)
            chunk_size: Tamanho de cada página (chunk) para paginação
            timeout: Timeout para requisições HTTP em segundos
        """
        super().__init__(anos, chunk_size, timeout)
        self.logger = logging.getLogger("ExtratorDETER")
    
    @property
    def base_url(self) -> str:
        """URL base da API WFS do DETER."""
        return "https://terrabrasilis.dpi.inpe.br/geoserver/deter-amz/wfs"
    
    @property
    def workspace(self) -> str:
        """Nome do workspace no GeoServer."""
        return "deter-amz"
    
    @property
    def layer(self) -> str:
        """Nome do layer no GeoServer."""
        return "deter_amz"
    
    @property
    def campo_ordenacao(self) -> str:
        """Campo usado para ordenação."""
        return "gid"
    
    def construir_filtro_ano(self, ano: int) -> str:
        """
        Constrói filtro CQL para um ano específico.
        
        Args:
            ano: Ano para filtro
            
        Returns:
            String com filtro CQL
        """
        return f"view_date BETWEEN '{ano}-01-01' AND '{ano}-12-31'"
    
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
            
            # Padronizar nomes de campos
            if "areamunkm" in props:
                props["area_km2"] = props.pop("areamunkm")
            if "mun_geocod" in props:
                props["codigo_municipio_ibge"] = props.pop("mun_geocod")
            if "municipality" in props:
                props["municipio"] = props.pop("municipality")
            if "uf" in props:
                props["estado"] = props.pop("uf")
            
            # Não incluir geometria (dados tabulares apenas)
            records.append(props)
        
        return pd.DataFrame(records)
    
    def extrair_agregado_municipio(self, ano: int) -> pd.DataFrame:
        """
        Extrai dados de um ano e agrega por município.
        
        Args:
            ano: Ano para extração
            
        Returns:
            DataFrame com dados agregados por município
        """
        self.logger.info(f"Extraindo e agregando dados do ano {ano} por município")
        
        dfs = list(self.extrair_ano(ano))
        if not dfs:
            return pd.DataFrame()
        
        df_completo = pd.concat(dfs, ignore_index=True)
        
        # Remover geometria para agregação (não é necessária)
        if "geometry" in df_completo.columns:
            df_completo = df_completo.drop(columns=["geometry"])
        
        # Agregar por município e mês
        cols_agregacao = ["codigo_municipio_ibge", "municipio", "estado"]
        cols_disponiveis = [c for c in cols_agregacao if c in df_completo.columns]
        
        if cols_disponiveis:
            # Extrair mês da view_date
            if "view_date" in df_completo.columns:
                df_completo["view_date"] = pd.to_datetime(df_completo["view_date"])
                df_completo["ano_mes"] = df_completo["view_date"].dt.to_period("M").astype(str)
                cols_agrupamento = cols_disponiveis + ["ano_mes"]
            else:
                cols_agrupamento = cols_disponiveis
            
            # Agregar área
            if "area_km2" in df_completo.columns:
                df_agregado = df_completo.groupby(cols_agrupamento).agg({
                    "area_km2": "sum",
                    "gid": "count"  # Contar número de alertas
                }).reset_index()
                df_agregado = df_agregado.rename(columns={"gid": "num_alertas"})
            else:
                df_agregado = df_completo.groupby(cols_agrupamento).size().reset_index(name="num_alertas")
            
            return df_agregado
        
        return df_completo
