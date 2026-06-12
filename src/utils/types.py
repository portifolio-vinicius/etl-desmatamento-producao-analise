"""
Módulo de Tipos Customizados e Interfaces para Análise Preditiva

Este módulo define tipos customizados, protocolos e interfaces para
garantir tipagem forte e segurança de tipo no código de análise preditiva.

Benefícios:
- Type hints avançados para melhor documentação
- Protocolos para interfaces contratuais
- Type aliases para legibilidade
- Validadores de tipo em runtime
"""

from typing import (
    Protocol,
    runtime_checkable,
    TypeVar,
    Generic,
    Union,
    Optional,
    List,
    Dict,
    Tuple,
    Any,
    Callable,
    Literal,
    TypedDict,
    Final,
    ClassVar
)
from enum import Enum
import pandas as pd
import numpy as np
from dataclasses import dataclass


# ============================================================================
# TYPE ALIASES PARA LEGIBILIDADE
# ============================================================================

# Tipos básicos
CodigoIBGE = int  # Código IBGE de 7 dígitos
Ano = int  # Ano (4 dígitos)
AreaHectares = float  # Área em hectares
ValorMonetario = float  # Valores em reais
Probabilidade = float  # Probabilidade entre 0 e 1
Porcentagem = float  # Porcentagem entre 0 e 100

# Tipos de dados
DataFrame = pd.DataFrame
Series = pd.Series
Array = np.ndarray

# Tipos de caminhos
CaminhoArquivo = str
CaminhoDiretorio = str

# Tipos de UF
UF = Literal['AC', 'AM', 'AP', 'MA', 'MT', 'PA', 'RO', 'RR', 'TO']

# Tipos de nível de risco
NivelRisco = Literal['Baixo', 'Moderado', 'Alto', 'Crítico']

# Tipos de categoria de tendência
CategoriaTendencia = Literal['Redução Forte', 'Redução Leve', 'Estável', 'Aumento Leve', 'Aumento Forte']

# Tipo genérico para modelos
ModelType = TypeVar('ModelType')


# ============================================================================
# ENUMS PARA VALORES FIXOS
# ============================================================================

class UFAmazoniaLegal(Enum):
    """UFs da Amazônia Legal brasileira"""
    ACRE = 'AC'
    AMAZONAS = 'AM'
    AMAPA = 'AP'
    MARANHAO = 'MA'
    MATO_GROSSO = 'MT'
    PARA = 'PA'
    RONDONIA = 'RO'
    RORAIMA = 'RR'
    TOCANTINS = 'TO'

    @classmethod
    def todas(cls) -> List[str]:
        """Retorna lista de todas as UFs"""
        return [uf.value for uf in cls]

    @classmethod
    def from_string(cls, uf: str) -> 'UFAmazoniaLegal':
        """Converte string para enum"""
        for uf_enum in cls:
            if uf_enum.value == uf:
                return uf_enum
        raise ValueError(f"UF '{uf}' não pertence à Amazônia Legal")


class NivelRiscoEnum(Enum):
    """Níveis de risco para classificação"""
    BAIXO = 'Baixo'
    MODERADO = 'Moderado'
    ALTO = 'Alto'
    CRITICO = 'Crítico'

    @classmethod
    def from_score(cls, score: float, 
                   threshold_moderado: float = 0.3,
                   threshold_alto: float = 0.5,
                   threshold_critico: float = 0.7) -> 'NivelRiscoEnum':
        """Classifica score em nível de risco"""
        if score >= threshold_critico:
            return cls.CRITICO
        if score >= threshold_alto:
            return cls.ALTO
        if score >= threshold_moderado:
            return cls.MODERADO
        return cls.BAIXO


class CategoriaTendenciaEnum(Enum):
    """Categorias de tendência de desmatamento"""
    REDUCAO_FORTE = 'Redução Forte'
    REDUCAO_LEVE = 'Redução Leve'
    ESTAVEL = 'Estável'
    AUMENTO_LEVE = 'Aumento Leve'
    AUMENTO_FORTE = 'Aumento Forte'


# ============================================================================
# TYPEDDICTS PARA ESTRUTURAS DE DADOS
# ============================================================================

class ConfiguracaoCaminhos(TypedDict):
    """Configuração de caminhos de arquivos"""
    caminho_dados: CaminhoArquivo
    caminho_saida: CaminhoArquivo
    caminho_drive: Optional[CaminhoArquivo]


class ConfiguracaoModelo(TypedDict):
    """Configuração de modelo de machine learning"""
    n_estimators: int
    max_depth: int
    min_samples_split: int
    random_state: int
    class_weight: Optional[Dict[int, float]]


class ThresholdsTendencia(TypedDict):
    """Thresholds para classificação de tendências"""
    reducao_forte: float
    reducao_leve: float
    aumento_leve: float
    aumento_forte: float


class MetricasAvaliacao(TypedDict):
    """Métricas de avaliação de modelo"""
    roc_auc: float
    pr_auc: float
    accuracy: Optional[float]
    precision: Optional[float]
    recall: Optional[float]


class RankingMunicipio(TypedDict):
    """Estrutura de ranking de municípios"""
    cod_ibge: CodigoIBGE
    municipio: str
    uf: UF
    probabilidade: Probabilidade
    area_desmatada: AreaHectares
    vab_agro: Optional[ValorMonetario]


# ============================================================================
# DATACLASSES PARA ESTRUTURAS DE DADOS
# ============================================================================

@dataclass(frozen=True)
class DadosMunicipio:
    """Dados de um município"""
    cod_ibge: CodigoIBGE
    municipio: str
    uf: UF
    area_desmatada_ha: AreaHectares
    vab_agro_mil_reais: Optional[ValorMonetario] = None
    idhm: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário"""
        return {
            'cod_ibge': self.cod_ibge,
            'municipio': self.municipio,
            'uf': self.uf,
            'area_desmatada_ha': self.area_desmatada_ha,
            'vab_agro_mil_reais': self.vab_agro_mil_reais,
            'idhm': self.idhm
        }


@dataclass
class TendenciaDesmatamento:
    """Tendência de desmatamento de um município"""
    cod_ibge: CodigoIBGE
    tendencia_desmatamento: float  # ha/ano
    categoria: CategoriaTendencia
    ano_base: Ano
    ano_projecao: Ano

    def classificar(self, thresholds: ThresholdsTendencia) -> CategoriaTendencia:
        """Classifica tendência baseado em thresholds"""
        if self.tendencia_desmatamento <= thresholds['reducao_forte']:
            return CategoriaTendenciaEnum.REDUCAO_FORTE.value
        if self.tendencia_desmatamento <= thresholds['reducao_leve']:
            return CategoriaTendenciaEnum.REDUCAO_LEVE.value
        if self.tendencia_desmatamento <= thresholds['aumento_leve']:
            return CategoriaTendenciaEnum.ESTAVEL.value
        if self.tendencia_desmatamento <= thresholds['aumento_forte']:
            return CategoriaTendenciaEnum.AUMENTO_LEVE.value
        return CategoriaTendenciaEnum.AUMENTO_FORTE.value


@dataclass
class ResultadoModelo:
    """Resultado de treinamento/avaliação de modelo"""
    modelo: Any
    metricas: MetricasAvaliacao
    feature_importance: Optional[Dict[str, float]] = None
    tempo_treinamento: Optional[float] = None

    def __str__(self) -> str:
        """Representação em string"""
        return f"ResultadoModelo(ROC-AUC: {self.metricas['roc_auc']:.4f}, PR-AUC: {self.metricas['pr_auc']:.4f})"


@dataclass
class DashboardRisco:
    """Dashboard consolidado de risco municipal"""
    cod_ibge: CodigoIBGE
    municipio: str
    uf: UF
    probabilidade_desmatamento: Probabilidade
    probabilidade_embargos: Optional[Probabilidade]
    tendencia_desmatamento: Optional[float]
    categoria_tendencia: Optional[CategoriaTendencia]
    score_risco_combinado: Probabilidade
    nivel_risco: NivelRisco
    recomendacao: str


# ============================================================================
# PROTOCOLOS (INTERFACES)
# ============================================================================

@runtime_checkable
class CarregadorDados(Protocol):
    """Protocolo para carregadores de dados"""
    
    def carregar(self, caminho: CaminhoArquivo, **kwargs) -> DataFrame:
        """Carrega dados de um arquivo"""
        ...


@runtime_checkable
class FiltradorDados(Protocol):
    """Protocolo para filtradores de dados"""
    
    def filtrar_amazonia_legal(self, df: DataFrame, 
                                ufs: Optional[List[UF]] = None) -> DataFrame:
        """Filtra dados para Amazônia Legal"""
        ...


@runtime_checkable
class PreparadorFeatures(Protocol):
    """Protocolo para preparadores de features"""
    
    def preparar_features(self, df: DataFrame, 
                         features: List[str],
                         target: str) -> Tuple[DataFrame, Series]:
        """Prepara features e target"""
        ...


@runtime_checkable
class DivisorDados(Protocol):
    """Protocolo para divisores de dados"""
    
    def dividir_temporalmente(self, features: DataFrame,
                            target: Series,
                            ano_limite: Ano,
                            ano_teste: Ano) -> Tuple[DataFrame, DataFrame, Series, Series]:
        """Divide dados temporalmente"""
        ...


@runtime_checkable
class TreinadorModelo(Protocol):
    """Protocolo para treinadores de modelo"""
    
    def treinar(self, X_train: DataFrame, y_train: Series,
                pesos: Optional[Dict[int, float]] = None,
                **kwargs) -> Any:
        """Treina modelo"""
        ...


@runtime_checkable
class AvaliadorModelo(Protocol):
    """Protocolo para avaliadores de modelo"""
    
    def avaliar(self, modelo: Any, X_test: DataFrame, y_test: Series,
               **kwargs) -> MetricasAvaliacao:
        """Avalia modelo"""
        ...


@runtime_checkable
class CalculadorTendencia(Protocol):
    """Protocolo para calculadores de tendência"""
    
    def calcular_tendencia(self, df: DataFrame) -> DataFrame:
        """Calcula tendência de desmatamento"""
        ...


@runtime_checkable
class ClassificadorRisco(Protocol):
    """Protocolo para classificadores de risco"""
    
    def classificar(self, score: float, 
                   thresholds: Dict[str, float]) -> NivelRisco:
        """Classifica nível de risco"""
        ...


# ============================================================================
# VALIDADORES DE TIPO
# ============================================================================

class ValidadorTipo:
    """Validador de tipos em runtime"""
    
    @staticmethod
    def validar_codigo_ibge(codigo: Any) -> CodigoIBGE:
        """Valida código IBGE"""
        if not isinstance(codigo, int):
            raise TypeError(f"Código IBGE deve ser int, recebido {type(codigo)}")
        if not 1000000 <= codigo <= 9999999:
            raise ValueError(f"Código IBGE deve ter 7 dígitos, recebido {codigo}")
        return codigo
    
    @staticmethod
    def validar_ano(ano: Any) -> Ano:
        """Valida ano"""
        if not isinstance(ano, int):
            raise TypeError(f"Ano deve ser int, recebido {type(ano)}")
        if not 1900 <= ano <= 2100:
            raise ValueError(f"Ano deve estar entre 1900 e 2100, recebido {ano}")
        return ano
    
    @staticmethod
    def validar_uf(uf: Any) -> UF:
        """Valida UF"""
        if not isinstance(uf, str):
            raise TypeError(f"UF deve ser str, recebido {type(uf)}")
        if uf not in UFAmazoniaLegal.todas():
            raise ValueError(f"UF '{uf}' não pertence à Amazônia Legal")
        return uf
    
    @staticmethod
    def validar_probabilidade(prob: Any) -> Probabilidade:
        """Valida probabilidade"""
        if not isinstance(prob, (int, float)):
            raise TypeError(f"Probabilidade deve ser numérica, recebido {type(prob)}")
        if not 0 <= prob <= 1:
            raise ValueError(f"Probabilidade deve estar entre 0 e 1, recebido {prob}")
        return float(prob)
    
    @staticmethod
    def validar_dataframe(df: Any, colunas_obrigatorias: Optional[List[str]] = None) -> DataFrame:
        """Valida DataFrame"""
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Deve ser DataFrame, recebido {type(df)}")
        
        if colunas_obrigatorias:
            colunas_faltantes = set(colunas_obrigatorias) - set(df.columns)
            if colunas_faltantes:
                raise ValueError(f"Colunas obrigatórias faltando: {colunas_faltantes}")
        
        return df


# ============================================================================
# FUNÇÕES UTILITÁRIAS DE TIPO
# ============================================================================

def validar_e_converter_codigo_ibge(codigo: Any) -> CodigoIBGE:
    """Valida e converte código IBGE"""
    return ValidadorTipo.validar_codigo_ibge(int(codigo))


def validar_e_converter_ano(ano: Any) -> Ano:
    """Valida e converte ano"""
    return ValidadorTipo.validar_ano(int(ano))


def validar_e_converter_uf(uf: Any) -> UF:
    """Valida e converte UF"""
    return ValidadorTipo.validar_uf(str(uf).upper())


def validar_e_converter_probabilidade(prob: Any) -> Probabilidade:
    """Valida e converte probabilidade"""
    return ValidadorTipo.validar_probabilidade(float(prob))


# ============================================================================
# CONSTANTES TIPADAS
# ============================================================================

UFS_AMAZONIA_LEGAL: Final[List[UF]] = [
    'AC', 'AM', 'AP', 'MA', 'MT', 'PA', 'RO', 'RR', 'TO'
]

ANO_LIMITE_TREINO: Final[Ano] = 2021
ANO_TESTE: Final[Ano] = 2022
ANO_PREVISAO: Final[Ano] = 2023

THRESHOLDS_RISCO_PADRAO: Final[Dict[str, float]] = {
    'moderado': 0.3,
    'alto': 0.5,
    'critico': 0.7
}

THRESHOLDS_TENDENCIA_PADRAO: Final[ThresholdsTendencia] = {
    'reducao_forte': -50,
    'reducao_leve': -10,
    'aumento_leve': 0,
    'aumento_forte': 10
}


# ============================================================================
# TYPE GUARDS
# ============================================================================

def is_dataframe(obj: Any) -> TypeGuard[DataFrame]:
    """Type guard para DataFrame"""
    return isinstance(obj, pd.DataFrame)


def is_series(obj: Any) -> TypeGuard[Series]:
    """Type guard para Series"""
    return isinstance(obj, pd.Series)


def is_array(obj: Any) -> TypeGuard[Array]:
    """Type guard para Array numpy"""
    return isinstance(obj, np.ndarray)


def is_codigo_ibge_valido(obj: Any) -> TypeGuard[CodigoIBGE]:
    """Type guard para código IBGE válido"""
    try:
        ValidadorTipo.validar_codigo_ibge(obj)
        return True
    except (TypeError, ValueError):
        return False


def is_uf_valida(obj: Any) -> TypeGuard[UF]:
    """Type guard para UF válida"""
    try:
        ValidadorTipo.validar_uf(obj)
        return True
    except (TypeError, ValueError):
        return False
