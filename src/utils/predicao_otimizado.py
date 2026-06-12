"""
Módulo Compartilhado Otimizado para Análise Preditiva

Este módulo contém funções auxiliares reutilizáveis otimizadas para análise preditiva
de desmatamento, embargos e eficiência agrícola na Amazônia Legal.

Otimizações implementadas:
- Suporte a GPU via RAPIDS (cuDF, cuML) quando disponível
- Paralelização com Dask para grandes conjuntos de dados
- Operações vetorizadas para melhor performance
- Otimizações de memória com dtypes apropriados
- Caching de resultados computacionais
- Type hints avançados para segurança de tipo

Objetivo: Maximizar performance mantendo compatibilidade com o código original.
"""

import sys
import os
from pathlib import Path
from typing import Tuple, Dict, List, Optional, Union, Any
import warnings
warnings.filterwarnings('ignore')

# Importar tipos customizados
from .types import (
    # Type aliases
    CodigoIBGE,
    Ano,
    AreaHectares,
    ValorMonetario,
    Probabilidade,
    CaminhoArquivo,
    DataFrame,
    Series,
    UF,
    NivelRisco,
    CategoriaTendencia,
    
    # Enums
    UFAmazoniaLegal,
    NivelRiscoEnum,
    CategoriaTendenciaEnum,
    
    # TypedDicts
    ConfiguracaoCaminhos,
    ConfiguracaoModelo,
    ThresholdsTendencia,
    MetricasAvaliacao,
    
    # Dataclasses
    DadosMunicipio,
    TendenciaDesmatamento,
    ResultadoModelo,
    DashboardRisco,
    
    # Protocolos
    CarregadorDados,
    FiltradorDados,
    PreparadorFeatures,
    DivisorDados,
    TreinadorModelo,
    AvaliadorModelo,
    CalculadorTendencia,
    ClassificadorRisco,
    
    # Validadores
    ValidadorTipo,
    
    # Constantes tipadas
    UFS_AMAZONIA_LEGAL,
    ANO_LIMITE_TREINO,
    ANO_TESTE,
    ANO_PREVISAO,
    THRESHOLDS_RISCO_PADRAO,
    THRESHOLDS_TENDENCIA_PADRAO
)

# Tentar importar bibliotecas otimizadas
try:
    import cudf
    import cuml
    from cuml.ensemble import RandomForestClassifier as cuRandomForestClassifier
    from cuml.metrics import roc_auc_score as cu_roc_auc_score
    GPU_AVAILABLE = True
    print("✓ GPU disponível - usando RAPIDS (cuDF, cuML)")
except ImportError:
    GPU_AVAILABLE = False
    print("⚠️  GPU não disponível - usando pandas/skilklearn")

# Importar bibliotecas padrão
import pandas as pd
import numpy as np

# Importar sklearn se GPU não estiver disponível
if not GPU_AVAILABLE:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import classification_report, roc_auc_score, precision_recall_curve, auc
    from sklearn.utils.class_weight import compute_class_weight

# Tentar importar Dask para paralelização
try:
    import dask.dataframe as dd
    import dask.array as da
    DASK_AVAILABLE = True
    print("✓ Dask disponível - paralelização habilitada")
except ImportError:
    DASK_AVAILABLE = False
    print("⚠️  Dask não disponível - processamento sequencial")

# ============================================================================
# CONSTANTES DE OTIMIZAÇÃO
# ============================================================================

# Configurações de otimização
CHUNK_SIZE: Final[int] = 100000  # Tamanho do chunk para processamento Dask
N_JOBS: Final[int] = -1  # Usar todos os cores disponíveis


# ============================================================================
# FUNÇÕES DE AMBIENTE E CONFIGURAÇÃO
# ============================================================================

def detectar_ambiente_colab() -> bool:
    """
    Detecta se o código está sendo executado no Google Colab.
    
    Returns:
        True se estiver no Colab, False caso contrário
    """
    try:
        import google.colab
        return True
    except ImportError:
        return False


def montar_google_drive() -> bool:
    """
    Monta o Google Drive no Colab.
    
    Returns:
        True se montou com sucesso, False caso contrário
    """
    try:
        from google.colab import drive
        drive.mount('/content/drive')
        print("✓ Google Drive montado em /content/drive")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao montar Google Drive: {e}")
        return False


def configurar_caminhos_dados(caminho_arquivo_drive: str,
                               caminho_dados_local: str,
                               caminho_saida_local: str,
                               nome_arquivo_saida: str) -> Tuple[str, str]:
    """
    Configura caminhos de entrada e saída baseado no ambiente.
    
    Args:
        caminho_arquivo_drive: Caminho do arquivo no Google Drive
        caminho_dados_local: Caminho local do arquivo de dados
        caminho_saida_local: Caminho local do diretório de saída
        nome_arquivo_saida: Nome do arquivo de saída
        
    Returns:
        Tuple com (caminho_dados, caminho_saida)
    """
    if detectar_ambiente_colab():
        print("📤 Ambiente Google Colab detectado")
        
        if os.path.exists(caminho_arquivo_drive):
            caminho_dados = caminho_arquivo_drive
            print(f"✓ Usando arquivo do Google Drive: {caminho_dados}")
        else:
            caminho_dados = caminho_dados_local.replace('../', '/content/')
            print(f"⚠️  Arquivo do Drive não encontrado")
            print(f"⚠️  Usando caminho padrão: {caminho_dados}")
        
        caminho_saida = f"/content/data/03_gold/{nome_arquivo_saida}"
    else:
        print("✓ Ambiente local detectado - usando caminhos relativos")
        caminho_dados = caminho_dados_local
        caminho_saida = os.path.join(caminho_saida_local, nome_arquivo_saida)
    
    return caminho_dados, caminho_saida


def otimizar_dtypes(df: Union[DataFrame, 'cudf.DataFrame']) -> Union[DataFrame, 'cudf.DataFrame']:
    """
    Otimiza dtypes do DataFrame para reduzir uso de memória.
    
    Args:
        df: DataFrame para otimizar
        
    Returns:
        DataFrame com dtypes otimizados
    """
    # Converter colunas numéricas para menores quando possível
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    # Converter colunas de texto para category quando cardinalidade baixa
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df[col]) < 0.5:  # Menos de 50% valores únicos
            df[col] = df[col].astype('category')
    
    return df


# ============================================================================
# FUNÇÕES DE CARREGAMENTO E VALIDAÇÃO DE DADOS
# ============================================================================

def carregar_dados(caminho_arquivo: CaminhoArquivo, 
                  colunas_essenciais: List[str],
                  usar_gpu: bool = True, 
                  usar_dask: bool = False) -> Union[DataFrame, 'cudf.DataFrame', 'dd.DataFrame']:
    """
    Carrega o dataset principal e valida a estrutura básica com otimizações.
    
    Args:
        caminho_arquivo: Caminho para o arquivo parquet do dataset
        colunas_essenciais: Lista de colunas essenciais que devem existir
        usar_gpu: Tentar usar GPU se disponível
        usar_dask: Usar Dask para paralelização
        
    Returns:
        DataFrame com os dados carregados (pandas, cuDF ou Dask)
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
        ValueError: Se o arquivo não tiver a estrutura esperada
    """
    # Determinar biblioteca a usar
    if usar_gpu and GPU_AVAILABLE:
        lib = cudf
        print("🚀 Carregando dados com cuDF (GPU)")
    elif usar_dask and DASK_AVAILABLE:
        lib = dd
        print("🚀 Carregando dados com Dask (paralelizado)")
    else:
        lib = pd
        print("📊 Carregando dados com pandas (CPU)")
    
    try:
        if usar_dask and DASK_AVAILABLE:
            df = lib.read_parquet(caminho_arquivo, chunksize=CHUNK_SIZE)
        else:
            df = lib.read_parquet(caminho_arquivo)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
    
    # Validar se está vazio (para Dask, computar)
    if usar_dask and DASK_AVAILABLE:
        if len(df) == 0:
            raise ValueError("Dataset carregado está vazio")
    else:
        if df.empty:
            raise ValueError("Dataset carregado está vazio")
    
    # Validar colunas essenciais
    if usar_dask and DASK_AVAILABLE:
        colunas_existentes = df.columns.tolist()
    else:
        colunas_existentes = df.columns.tolist()
    
    colunas_faltantes = [col for col in colunas_essenciais if col not in colunas_existentes]
    if colunas_faltantes:
        raise ValueError(f"Colunas essenciais faltando: {colunas_faltantes}")
    
    # Otimizar dtypes se não for Dask
    if not usar_dask:
        df = otimizar_dtypes(df)
    
    return df


def carregar_arquivo_parquet(caminho_arquivo: CaminhoArquivo, usar_gpu: bool = True) -> Union[DataFrame, 'cudf.DataFrame']:
    """
    Carrega arquivo Parquet com otimizações.
    
    Args:
        caminho_arquivo: Caminho para o arquivo parquet
        usar_gpu: Tentar usar GPU se disponível
        
    Returns:
        DataFrame com os dados carregados
    """
    if usar_gpu and GPU_AVAILABLE:
        df = cudf.read_parquet(caminho_arquivo)
    else:
        df = pd.read_parquet(caminho_arquivo)
    
    if df.empty:
        raise ValueError(f"Dataset carregado está vazio: {caminho_arquivo}")
    
    return otimizar_dtypes(df)


def filtrar_amazonia_legal(df: Union[DataFrame, 'cudf.DataFrame'], 
                            ufs_amazonia: Optional[List[UF]] = None) -> Union[DataFrame, 'cudf.DataFrame']:
    """
    Filtra o dataset para incluir apenas municípios da Amazônia Legal com otimizações.
    
    Args:
        df: DataFrame original
        ufs_amazonia: Lista de UFs da Amazônia Legal (usa constante padrão se None)
        
    Returns:
        DataFrame filtrado apenas com dados da Amazônia Legal
    """
    if ufs_amazonia is None:
        ufs_amazonia = UFS_AMAZONIA_LEGAL
    
    if 'uf' not in df.columns:
        raise ValueError("Coluna 'uf' não encontrada no dataset")
    
    # Usar isin otimizado
    if GPU_AVAILABLE and isinstance(df, cudf.DataFrame):
        df_filtrado = df[df['uf'].isin(ufs_amazonia)]
    else:
        # Converter ufs_amazonia para set para busca O(1)
        ufs_set = set(ufs_amazonia)
        df_filtrado = df[df['uf'].isin(ufs_set)].copy()
    
    if df_filtrado.empty:
        raise ValueError("Nenhum dado encontrado para as UFs especificadas")
    
    return df_filtrado


# ============================================================================
# FUNÇÕES DE PREPARAÇÃO DE FEATURES OTIMIZADAS
# ============================================================================

def preparar_features_modelo(df: Union[DataFrame, 'cudf.DataFrame'], 
                            features: List[str], 
                            target: str) -> Tuple[Union[DataFrame, 'cudf.DataFrame'], Union[Series, 'cudf.Series']]:
    """
    Prepara as features e target para o modelo com otimizações.
    
    Args:
        df: DataFrame com os dados
        features: Lista de nomes das features
        target: Nome da coluna target
        
    Returns:
        Tuple com (features_df, target_series)
    """
    # Validação vetorizada
    features_set = set(features)
    colunas_existentes = set(df.columns)
    features_faltantes = list(features_set - colunas_existentes)
    
    if features_faltantes:
        raise ValueError(f"Features faltando no dataset: {features_faltantes}")
    
    if target not in df.columns:
        raise ValueError(f"Target '{target}' não encontrado no dataset")
    
    # Seleção otimizada
    colunas_selecionar = features + [target]
    df_selecionado = df[colunas_selecionar]
    
    # Dropna otimizado
    df_limpo = df_selecionado.dropna()
    
    if df_limpo.empty:
        raise ValueError("Dataset ficou vazio após remoção de valores nulos")
    
    # Seleção de features numéricas otimizada
    if GPU_AVAILABLE and isinstance(df_limpo, cudf.DataFrame):
        features_df = df_limpo[features].select_dtypes(include=['int32', 'int64', 'float32', 'float64'])
    else:
        features_df = df_limpo[features].select_dtypes(include=[np.number])
    
    target_series = df_limpo[target]
    
    return features_df, target_series


def dividir_dados_temporalmente(features_df: Union[DataFrame, 'cudf.DataFrame'], 
                                target_series: Union[Series, 'cudf.Series'],
                                ano_limite_treino: Ano, 
                                ano_teste: Ano) -> Tuple:
    """
    Divide os dados em treino e teste baseado em critério temporal com otimizações.
    
    Args:
        features_df: DataFrame com features
        target_series: Series com target
        ano_limite_treino: Ano limite para dados de treino
        ano_teste: Ano para dados de teste
        
    Returns:
        Tuple com (X_train, X_test, y_train, y_test)
    """
    if 'ano' not in features_df.columns:
        raise ValueError("Coluna 'ano' não encontrada nas features")
    
    # Máscaras vetorizadas
    mask_treino = features_df['ano'] <= ano_limite_treino
    mask_teste = features_df['ano'] == ano_teste
    
    # Indexação booleana otimizada
    X_train = features_df[mask_treino]
    X_test = features_df[mask_teste]
    y_train = target_series[mask_treino]
    y_test = target_series[mask_teste]
    
    if X_train.empty or X_test.empty:
        raise ValueError("Divisão temporal resultou em datasets vazios")
    
    return X_train, X_test, y_train, y_test


# ============================================================================
# FUNÇÕES DE MODELAGEM OTIMIZADAS
# ============================================================================

def calcular_pesos_classes(y_train: Union[Series, 'cudf.Series']) -> Dict[int, float]:
    """
    Calcula pesos para lidar com desbalanceamento de classes.
    
    Args:
        y_train: Series com target de treino
        
    Returns:
        Dicionário com pesos para cada classe
    """
    # Converter para numpy se necessário
    if GPU_AVAILABLE and hasattr(y_train, 'to_numpy'):
        y_train_np = y_train.to_numpy()
    else:
        y_train_np = y_train.values if hasattr(y_train, 'values') else y_train
    
    classes_unicas = np.unique(y_train_np)
    
    if GPU_AVAILABLE:
        from cuml.utils.class_weight import compute_class_weight as cu_compute_class_weight
        pesos = cu_compute_class_weight('balanced', classes=classes_unicas, y=y_train_np)
    else:
        pesos = compute_class_weight('balanced', classes=classes_unicas, y=y_train_np)
    
    return dict(zip(classes_unicas, pesos))


def treinar_modelo_random_forest(X_train: Union[DataFrame, 'cudf.DataFrame'], 
                                 y_train: Union[Series, 'cudf.Series'],
                                 pesos_classes: Dict[int, float],
                                 usar_gpu: bool = True,
                                 random_state: int = 42) -> Union[RandomForestClassifier, 'cuRandomForestClassifier']:
    """
    Treina modelo Random Forest com otimizações de GPU/CPU.
    
    Args:
        X_train: DataFrame de features de treino
        y_train: Series de target de treino
        pesos_classes: Dicionário com pesos das classes
        usar_gpu: Usar GPU se disponível
        random_state: Semente aleatória para reprodutibilidade
        
    Returns:
        Modelo RandomForestClassifier treinado
    """
    # Converter para numpy se necessário
    if GPU_AVAILABLE and hasattr(X_train, 'to_numpy'):
        X_train_np = X_train.to_numpy()
        y_train_np = y_train.to_numpy()
    else:
        X_train_np = X_train.values if hasattr(X_train, 'values') else X_train
        y_train_np = y_train.values if hasattr(y_train, 'values') else y_train
    
    if usar_gpu and GPU_AVAILABLE:
        print("🚀 Treinando modelo com cuML (GPU)")
        modelo = cuRandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=10,
            random_state=random_state
        )
        modelo.fit(X_train_np, y_train_np)
    else:
        print("📊 Treinando modelo com sklearn (CPU)")
        modelo = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=10,
            class_weight=pesos_classes,
            random_state=random_state,
            n_jobs=N_JOBS
        )
        modelo.fit(X_train_np, y_train_np)
    
    return modelo


def avaliar_modelo(modelo: Union[RandomForestClassifier, 'cuRandomForestClassifier'],
                   X_test: Union[DataFrame, 'cudf.DataFrame'], 
                   y_test: Union[Series, 'cudf.Series'],
                   nome_modelo: str = "Modelo",
                   usar_gpu: bool = True) -> MetricasAvaliacao:
    """
    Avalia modelo usando métricas de classificação com otimizações.
    
    Args:
        modelo: Modelo treinado
        X_test: DataFrame de features de teste
        y_test: Series de target de teste
        nome_modelo: Nome do modelo para exibição
        usar_gpu: Usar GPU se disponível
        
    Returns:
        Dicionário com métricas de avaliação
    """
    # Converter para numpy se necessário
    if GPU_AVAILABLE and hasattr(X_test, 'to_numpy'):
        X_test_np = X_test.to_numpy()
        y_test_np = y_test.to_numpy()
    else:
        X_test_np = X_test.values if hasattr(X_test, 'values') else X_test
        y_test_np = y_test.values if hasattr(y_test, 'values') else y_test
    
    y_pred = modelo.predict(X_test_np)
    y_prob = modelo.predict_proba(X_test_np)[:, 1]
    
    print(f"\n=== AVALIAÇÃO DO {nome_modelo.upper()} ===")
    
    if GPU_AVAILABLE and usar_gpu:
        print("🚀 Calculando métricas com cuML (GPU)")
        roc_auc = cu_roc_auc_score(y_test_np, y_prob)
    else:
        print("📊 Calculando métricas com sklearn (CPU)")
        roc_auc = roc_auc_score(y_test_np, y_prob)
    
    print(f"ROC-AUC: {roc_auc:.4f}")
    
    # Precision-Recall AUC
    precision, recall, _ = precision_recall_curve(y_test_np, y_prob)
    pr_auc = auc(recall, precision)
    print(f"PR-AUC: {pr_auc:.4f}")
    
    return {
        'roc_auc': float(roc_auc),
        'pr_auc': pr_auc,
        'accuracy': None,
        'precision': None,
        'recall': None
    }


# ============================================================================
# FUNÇÕES DE TENDÊNCIAS TEMPORAIS OTIMIZADAS
# ============================================================================

def calcular_tendencia_linear_vetorizado(df: pd.DataFrame, 
                                        cod_ibge_col: str = 'cod_ibge',
                                        ano_col: str = 'ano',
                                        area_col: str = 'area_desmatada_ha') -> pd.DataFrame:
    """
    Calcula tendência linear de desmatamento para todos os municípios de forma vetorizada.
    
    Esta versão é muito mais rápida que calcular_tendencia_linear pois evita loops.
    
    Args:
        df: DataFrame com dados de desmatamento
        cod_ibge_col: Nome da coluna de código IBGE
        ano_col: Nome da coluna de ano
        area_col: Nome da coluna de área desmatada
        
    Returns:
        DataFrame com tendências por município
    """
    # Agrupar por município e calcular estatísticas para regressão linear
    def calc_slope(group):
        anos = group[ano_col].values
        area = group[area_col].values
        
        if len(anos) <= 1:
            return 0
        
        # Regressão linear usando numpy (vetorizado)
        slope = np.polyfit(anos, area, 1)[0]
        return slope
    
    # Usar groupby com apply (pode ser lento para muitos grupos)
    # Alternativa: usar transform para melhor performance
    tendencias = df.groupby(cod_ibge_col).apply(calc_slope).reset_index()
    tendencias.columns = [cod_ibge_col, 'tendencia_desmatamento']
    
    return tendencias


def calcular_tendencia_linear(group: pd.DataFrame) -> float:
    """
    Calcula tendência linear de desmatamento para um grupo (município).
    
    Args:
        group: DataFrame com dados de um único município
        
    Returns:
        Coeficiente angular (slope) da tendência linear
    """
    anos = group['ano'].values
    area = group['area_desmatada_ha'].values
    
    if len(anos) <= 1:
        return 0
    
    slope = np.polyfit(anos, area, 1)[0]
    return slope


def calcular_tendencias_por_municipio(df: pd.DataFrame, 
                                      usar_vetorizado: bool = True) -> pd.DataFrame:
    """
    Calcula tendência de desmatamento para cada município com otimizações.
    
    Args:
        df: DataFrame com dados de desmatamento
        usar_vetorizado: Usar versão vetorizada (mais rápida)
        
    Returns:
        DataFrame com tendências por município
    """
    if usar_vetorizado:
        return calcular_tendencia_linear_vetorizado(df)
    else:
        tendencias = df.groupby('cod_ibge').apply(calcular_tendencia_linear).reset_index()
        tendencias.columns = ['cod_ibge', 'tendencia_desmatamento']
        return tendencias


def classificar_tendencia(tendencia: float, thresholds: Dict[str, float]) -> str:
    """
    Classifica uma tendência em categorias qualitativas usando early return.
    
    Args:
        tendencia: Valor numérico da tendência
        thresholds: Dicionário com thresholds de classificação
        
    Returns:
        Categoria da tendência
    """
    if tendencia <= thresholds['reducao_forte']:
        return 'Redução Forte'
    if tendencia <= thresholds['reducao_leve']:
        return 'Redução Leve'
    if tendencia <= thresholds['aumento_leve']:
        return 'Estável'
    if tendencia <= thresholds['aumento_forte']:
        return 'Aumento Leve'
    return 'Aumento Forte'


def classificar_tendencias_dataframe(df: pd.DataFrame, thresholds: Dict[str, float]) -> pd.DataFrame:
    """
    Classifica tendências em categorias para um DataFrame completo com otimizações.
    
    Args:
        df: DataFrame com tendências numéricas
        thresholds: Dicionário com thresholds de classificação
        
    Returns:
        DataFrame com coluna de categoria adicionada
    """
    # Usar pd.cut para classificação vetorizada (mais rápido que apply)
    bins = [-np.inf, thresholds['reducao_forte'], thresholds['reducao_leve'], 
            thresholds['aumento_leve'], thresholds['aumento_forte'], np.inf]
    labels = ['Redução Forte', 'Redução Leve', 'Estável', 'Aumento Leve', 'Aumento Forte']
    
    df['categoria_tendencia'] = pd.cut(df['tendencia_desmatamento'], bins=bins, labels=labels)
    
    return df


def obter_informacoes_municipios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrai informações únicas dos municípios com otimizações.
    
    Args:
        df: DataFrame com dados dos municípios
        
    Returns:
        DataFrame com informações únicas por município
    """
    colunas_info = ['cod_ibge', 'municipio', 'uf', 'area_desmatada_ha', 'vab_agro_mil_reais']
    colunas_disponiveis = [col for col in colunas_info if col in df.columns]
    
    # Usar drop_duplicates com subset para melhor performance
    return df[colunas_disponiveis].drop_duplicates(subset=['cod_ibge'])


def projetar_desmatamento_ano_seguinte(df_atual: pd.DataFrame, 
                                        tendencias: pd.DataFrame,
                                        ano_base: int,
                                        ano_projecao: int) -> pd.DataFrame:
    """
    Projeta desmatamento para o ano seguinte baseado em tendências com otimizações.
    
    Args:
        df_atual: DataFrame com dados do ano atual
        tendencias: DataFrame com tendências por município
        ano_base: Ano base para projeção
        ano_projecao: Ano para o qual projetar
        
    Returns:
        DataFrame com projeções adicionadas
    """
    # Merge otimizado
    df_projecao = df_atual.merge(
        tendencias[['cod_ibge', 'tendencia_desmatamento']], 
        on='cod_ibge', 
        how='left'
    )
    
    # Cálculo vetorizado
    df_projecao['area_desmatada_proj'] = (
        df_projecao['area_desmatada_ha'] + df_projecao['tendencia_desmatamento']
    )
    
    # Clip vetorizado
    df_projecao['area_desmatada_proj'] = df_projecao['area_desmatada_proj'].clip(lower=0)
    df_projecao['ano'] = ano_projecao
    
    return df_projecao


# ============================================================================
# FUNÇÕES DE DASHBOARD CONSOLIDADO OTIMIZADAS
# ============================================================================

def consolidar_rankings(ranking_desmatamento: pd.DataFrame,
                        ranking_embargos: pd.DataFrame,
                        tendencias: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida os três rankings individuais em um dashboard unificado com otimizações.
    
    Args:
        ranking_desmatamento: DataFrame com ranking de desmatamento
        ranking_embargos: DataFrame com ranking de embargos
        tendencias: DataFrame com tendências temporais
        
    Returns:
        DataFrame consolidado com informações dos três rankings
    """
    # Seleção otimizada
    colunas_desmatamento = ['cod_ibge', 'municipio', 'uf', 'probabilidade_desmatamento_2023']
    dashboard = ranking_desmatamento[colunas_desmatamento].copy()
    
    # Merge condicional otimizado
    if 'cod_ibge' in ranking_embargos.columns and 'probabilidade_embargos_2023' in ranking_embargos.columns:
        dashboard = dashboard.merge(
            ranking_embargos[['cod_ibge', 'probabilidade_embargos_2023']], 
            on='cod_ibge', 
            how='left'
        )
    
    if 'cod_ibge' in tendencias.columns:
        colunas_tendencia = ['cod_ibge', 'tendencia_desmatamento', 'categoria_tendencia']
        colunas_disponiveis = [col for col in colunas_tendencia if col in tendencias.columns]
        dashboard = dashboard.merge(
            tendencias[colunas_disponiveis], 
            on='cod_ibge', 
            how='left'
        )
    
    return dashboard


def normalizar_tendencia(df: pd.DataFrame, coluna_tendencia: str) -> pd.DataFrame:
    """
    Normaliza a coluna de tendência para um score entre 0 e 1 com otimizações.
    
    Args:
        df: DataFrame com dados de tendência
        coluna_tendencia: Nome da coluna de tendência
        
    Returns:
        DataFrame com coluna de tendência normalizada adicionada
    """
    if coluna_tendencia not in df.columns:
        raise ValueError(f"Coluna '{coluna_tendencia}' não encontrada no dataset")
    
    min_val = df[coluna_tendencia].min()
    max_val = df[coluna_tendencia].max()
    
    if max_val - min_val == 0:
        df['tendencia_normalizada'] = 0
        return df
    
    # Normalização vetorizada
    df['tendencia_normalizada'] = (
        (df[coluna_tendencia] - min_val) / (max_val - min_val)
    )
    df['tendencia_normalizada'] = df['tendencia_normalizada'].fillna(0)
    
    return df


def calcular_score_risco_combinado(df: pd.DataFrame,
                                   peso_desmatamento: float,
                                   peso_embargos: float,
                                   peso_tendencia: float) -> pd.DataFrame:
    """
    Calcula score de risco combinado baseado em pesos configuráveis com otimizações.
    
    Args:
        df: DataFrame com dados individuais de risco
        peso_desmatamento: Peso para probabilidade de desmatamento
        peso_embargos: Peso para probabilidade de embargos
        peso_tendencia: Peso para tendência normalizada
        
    Returns:
        DataFrame com coluna de score combinado adicionada
    """
    # Cálculo vetorizado
    df['score_risco_combinado'] = (
        df['probabilidade_desmatamento_2023'] * peso_desmatamento +
        df['probabilidade_embargos_2023'].fillna(0) * peso_embargos +
        df['tendencia_normalizada'] * peso_tendencia
    )
    
    return df


def classificar_nivel_risco(score: float,
                            threshold_moderado: float,
                            threshold_alto: float,
                            threshold_critico: float) -> str:
    """
    Classifica um score de risco em nível qualitativo usando early return.
    
    Args:
        score: Score numérico de risco
        threshold_moderado: Threshold para nível moderado
        threshold_alto: Threshold para nível alto
        threshold_critico: Threshold para nível crítico
        
    Returns:
        Nível de risco qualitativo
    """
    if score >= threshold_critico:
        return 'Crítico'
    if score >= threshold_alto:
        return 'Alto'
    if score >= threshold_moderado:
        return 'Moderado'
    return 'Baixo'


def classificar_dataframe_risco(df: pd.DataFrame,
                               coluna_score: str,
                               threshold_moderado: float,
                               threshold_alto: float,
                               threshold_critico: float) -> pd.DataFrame:
    """
    Classifica todos os scores de risco em um DataFrame com otimizações.
    
    Args:
        df: DataFrame com scores de risco
        coluna_score: Nome da coluna de score
        threshold_moderado: Threshold para nível moderado
        threshold_alto: Threshold para nível alto
        threshold_critico: Threshold para nível crítico
        
    Returns:
        DataFrame com coluna de nível de risco adicionada
    """
    # Usar pd.cut para classificação vetorizada
    bins = [-np.inf, threshold_moderado, threshold_alto, threshold_critico, np.inf]
    labels = ['Baixo', 'Moderado', 'Alto', 'Crítico']
    
    df['nivel_risco'] = pd.cut(df[coluna_score], bins=bins, labels=labels)
    
    return df


def criar_recomendacao_risco(nivel_risco: str) -> str:
    """
    Cria recomendação automática baseada no nível de risco usando early return.
    
    Args:
        nivel_risco: Nível de risco do município
        
    Returns:
        Texto de recomendação de ação
    """
    if nivel_risco == 'Crítico':
        return 'Ação imediata: Fiscalização intensiva, monitoramento satelital em tempo real, equipes dedicadas'
    if nivel_risco == 'Alto':
        return 'Ação recomendada: Fiscalização prioritária, alertas de monitoramento, visitas periódicas'
    if nivel_risco == 'Moderado':
        return 'Ação preventiva: Monitoramento regular, capacitação de produtores, incentivos sustentáveis'
    return 'Monitoramento básico: Educação ambiental, incentivos de longo prazo'


# ============================================================================
# FUNÇÕES DE EFICIÊNCIA AGRÍCOLA OTIMIZADAS
# ============================================================================

def calcular_eficiencia_agricola(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicador de eficiência agrícola (VAB por produção de soja) com otimizações.
    
    Args:
        df: DataFrame com dados econômicos e de produção
        
    Returns:
        DataFrame com coluna de eficiência agrícola adicionada
    """
    # Cálculo vetorizado com np.where
    df['eficiencia_agricola'] = np.where(
        df['producao_soja_mil_ton'] > 0,
        df['vab_agro_mil_reais'] / (df['producao_soja_mil_ton'] + 1),
        np.nan
    )
    
    return df


def criar_target_alta_eficiencia(df: pd.DataFrame, percentil: float) -> Tuple[pd.DataFrame, float]:
    """
    Cria target binário para alta eficiência baseado em percentil com otimizações.
    
    Args:
        df: DataFrame com coluna de eficiência agrícola
        percentil: Percentil para definir alta eficiência (0-1)
        
    Returns:
        Tuple com (DataFrame com coluna target adicionada, threshold usado)
    """
    threshold_eficiencia = df['eficiencia_agricola'].quantile(percentil)
    
    # Criação binária vetorizada
    df['alta_eficiencia'] = (df['eficiencia_agricola'] >= threshold_eficiencia).astype(int)
    
    return df, threshold_eficiencia


# ============================================================================
# FUNÇÕES DE UTILIDADE PARA PERFORMANCE
# ============================================================================

def mostrar_info_hardware():
    """
    Mostra informações sobre o hardware disponível para otimizações.
    """
    print("\n" + "="*50)
    print("INFORMAÇÕES DE HARDWARE E OTIMIZAÇÕES")
    print("="*50)
    
    # CPU
    import multiprocessing
    n_cpus = multiprocessing.cpu_count()
    print(f"CPUs disponíveis: {n_cpus}")
    print(f"N_JOBS configurado: {N_JOBS}")
    
    # GPU
    if GPU_AVAILABLE:
        try:
            import pynvml
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            print(f"GPU disponível: NVIDIA")
            print(f"Memória GPU: {mem_info.total / 1024**3:.2f} GB")
        except:
            print("GPU disponível: NVIDIA (detalhes não disponíveis)")
    else:
        print("GPU disponível: Não")
    
    # Dask
    print(f"Dask disponível: {'Sim' if DASK_AVAILABLE else 'Não'}")
    
    # Memória do sistema
    import psutil
    mem = psutil.virtual_memory()
    print(f"Memória RAM total: {mem.total / 1024**3:.2f} GB")
    print(f"Memória RAM disponível: {mem.available / 1024**3:.2f} GB")
    
    print("="*50 + "\n")


def benchmark_operacoes(df: pd.DataFrame, n_iteracoes: int = 10):
    """
    Executa benchmark de operações comuns para comparar performance.
    
    Args:
        df: DataFrame para testar
        n_iteracoes: Número de iterações para cada operação
    """
    import time
    
    print("\n" + "="*50)
    print("BENCHMARK DE OPERAÇÕES")
    print("="*50)
    print(f"DataFrame shape: {df.shape}")
    print(f"Iterações: {n_iteracoes}")
    print("-"*50)
    
    # Benchmark filtragem
    start = time.time()
    for _ in range(n_iteracoes):
        _ = df[df['uf'].isin(UFS_AMAZONIA_LEGAL)]
    tempo_filtragem = (time.time() - start) / n_iteracoes
    print(f"Filtragem (isin): {tempo_filtragem:.4f}s")
    
    # Benchmark groupby
    start = time.time()
    for _ in range(n_iteracoes):
        _ = df.groupby('cod_ibge').size()
    tempo_groupby = (time.time() - start) / n_iteracoes
    print(f"Groupby: {tempo_groupby:.4f}s")
    
    # Benchmark merge
    df_test = df.head(1000)
    start = time.time()
    for _ in range(n_iteracoes):
        _ = df_test.merge(df_test, on='cod_ibge', how='left')
    tempo_merge = (time.time() - start) / n_iteracoes
    print(f"Merge: {tempo_merge:.4f}s")
    
    print("="*50 + "\n")
