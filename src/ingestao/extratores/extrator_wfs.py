#!/usr/bin/env python
# coding: utf-8

"""
Classe base para extratores WFS (Web Feature Service) do GeoServer INPE/TerraBrasilis.

Esta classe fornece funcionalidades comuns para extrair dados de APIs WFS
do INPE (PRODES, DETER, etc.) seguindo o padrão OGC WFS.
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Generator, List
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import time


class ExtratorWFS(ABC):
    """
    Classe base para extratores de dados via WFS (Web Feature Service).
    
    Esta classe abstrata define a interface para extrair dados de APIs WFS
    do INPE/TerraBrasilis, implementando paginação e tratamento de erros.
    """
    
    def __init__(
        self,
        anos: List[int],
        chunk_size: int = 1000,
        timeout: int = 30
    ):
        """
        Inicializa o extrator WFS.
        
        Args:
            anos: Lista de anos para extração
            chunk_size: Tamanho de cada página (chunk) para paginação
            timeout: Timeout para requisições HTTP em segundos
        """
        self.anos = anos
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @property
    @abstractmethod
    def base_url(self) -> str:
        """URL base da API WFS."""
        pass
    
    @property
    @abstractmethod
    def workspace(self) -> str:
        """Nome do workspace no GeoServer."""
        pass
    
    @property
    @abstractmethod
    def layer(self) -> str:
        """Nome do layer no GeoServer."""
        pass
    
    @property
    @abstractmethod
    def campo_ordenacao(self) -> str:
        """Campo usado para ordenação (required pelo GeoServer)."""
        pass
    
    @abstractmethod
    def construir_filtro_ano(self, ano: int) -> str:
        """
        Constrói filtro CQL para um ano específico.
        
        Args:
            ano: Ano para filtro
            
        Returns:
            String com filtro CQL
        """
        pass
    
    def construir_url(
        self,
        cql_filter: Optional[str] = None,
        count: int = None,
        start_index: int = 0
    ) -> str:
        """
        Constrói URL para requisição WFS.
        
        Args:
            cql_filter: Filtro CQL opcional
            count: Número máximo de features (default: self.chunk_size)
            start_index: Índice inicial para paginação
            
        Returns:
            URL completa para requisição
        """
        if count is None:
            count = self.chunk_size
        
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": f"{self.workspace}:{self.layer}",
            "outputFormat": "application/json",
            "count": count,
            "startIndex": start_index,
            "sortBy": self.campo_ordenacao,
        }
        
        if cql_filter:
            params["CQL_FILTER"] = cql_filter
        
        url = self.base_url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
        return url
    
    def baixar_pagina(self, url: str) -> Optional[dict]:
        """
        Baixa uma página de dados da API WFS.
        
        Args:
            url: URL para requisição
            
        Returns:
            Dicionário JSON com dados ou None em caso de erro
        """
        try:
            response = requests.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Erro HTTP {response.status_code}: {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout na requisição: {url[:100]}")
            return None
        except Exception as e:
            self.logger.error(f"Erro na requisição: {str(e)}")
            return None
    
    def extrair_ano(self, ano: int) -> Generator[pd.DataFrame, None, None]:
        """
        Extrai todos os dados de um ano específico usando paginação.
        
        Args:
            ano: Ano para extração
            
        Yields:
            DataFrames com chunks de dados
        """
        self.logger.info(f"Extraindo dados do ano {ano}")
        
        cql_filter = self.construir_filtro_ano(ano)
        start_index = 0
        total_features = 0
        
        with tqdm(desc=f"Ano {ano}", unit=" features") as pbar:
            while True:
                url = self.construir_url(
                    cql_filter=cql_filter,
                    count=self.chunk_size,
                    start_index=start_index
                )
                
                data = self.baixar_pagina(url)
                
                if not data:
                    break
                
                features = data.get("features", [])
                
                if not features:
                    break
                
                # Converter para DataFrame
                df = self._features_para_dataframe(features)
                
                if not df.empty:
                    total_features += len(df)
                    pbar.update(len(df))
                    yield df
                
                # Verificar se chegou ao fim
                if len(features) < self.chunk_size:
                    break
                
                start_index += self.chunk_size
                time.sleep(0.5)  # Rate limiting
        
        self.logger.info(f"Ano {ano}: {total_features} features extraídas")
    
    def extrair(self) -> Generator[pd.DataFrame, None, None]:
        """
        Extrai dados de todos os anos configurados.
        
        Yields:
            DataFrames com chunks de dados
        """
        for ano in self.anos:
            yield from self.extrair_ano(ano)
    
    def _features_para_dataframe(self, features: List[dict]) -> pd.DataFrame:
        """
        Converte lista de features GeoJSON para DataFrame pandas.
        
        Args:
            features: Lista de features GeoJSON
            
        Returns:
            DataFrame com os dados
        """
        if not features:
            return pd.DataFrame()
        
        # Extrair propriedades
        records = []
        for feature in features:
            props = feature.get("properties", {})
            # Adicionar geometria se disponível
            geometry = feature.get("geometry")
            if geometry:
                props["geometry"] = geometry
            records.append(props)
        
        return pd.DataFrame(records)
