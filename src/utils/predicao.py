"""
Módulo Compartilhado para Análise Preditiva

Este módulo contém funções auxiliares reutilizáveis para análise preditiva
de desmatamento, embargos e eficiência agrícola na Amazônia Legal.

Objetivo: Eliminar duplicação de código e melhorar manutenibilidade seguindo
o princípio DRY (Don't Repeat Yourself).

Tipagem: Usa type hints avançados e tipos customizados do módulo types.py
"""

import sys
import os
from pathlib import Path
from typing import Tuple, Dict, List, Optional, Union, Any
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, precision_recall_curve, auc
from sklearn.utils.class_weight import compute_class_weight

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


# ============================================================================
# CONSTANTES COMPARTILHADAS (LEGACY - MANTIDAS PARA COMPATIBILIDADE)
# ============================================================================

# Constantes legadas mantidas para compatibilidade com código existente
# Novo código deve usar as constantes tipadas do módulo types.py


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


def configurar_caminhos_dados(caminho_arquivo_drive: CaminhoArquivo,
                               caminho_dados_local: CaminhoArquivo,
                               caminho_saida_local: CaminhoArquivo,
                               nome_arquivo_saida: str) -> ConfiguracaoCaminhos:
    """
    Configura caminhos de entrada e saída baseado no ambiente.
    
    Args:
        caminho_arquivo_drive: Caminho do arquivo no Google Drive
        caminho_dados_local: Caminho local do arquivo de dados
        caminho_saida_local: Caminho local do diretório de saída
        nome_arquivo_saida: Nome do arquivo de saída
        
    Returns:
        Dict com caminhos configurados
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
    
    return {
        'caminho_dados': caminho_dados,
        'caminho_saida': caminho_saida,
        'caminho_drive': caminho_arquivo_drive if detectar_ambiente_colab() else None
    }


# ============================================================================
# FUNÇÕES DE CARREGAMENTO E VALIDAÇÃO DE DADOS
# ============================================================================

def carregar_dados(caminho_arquivo: CaminhoArquivo, 
                  colunas_essenciais: List[str]) -> DataFrame:
    """
    Carrega o dataset principal e valida a estrutura básica.
    
    Args:
        caminho_arquivo: Caminho para o arquivo parquet do dataset
        colunas_essenciais: Lista de colunas essenciais que devem existir
        
    Returns:
        DataFrame com os dados carregados
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
        ValueError: Se o arquivo não tiver a estrutura esperada
    """
    try:
        df = pd.read_parquet(caminho_arquivo)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
    
    if df.empty:
        raise ValueError("Dataset carregado está vazio")
    
    colunas_faltantes = [col for col in colunas_essenciais if col not in df.columns]
    if colunas_faltantes:
        raise ValueError(f"Colunas essenciais faltando: {colunas_faltantes}")
    
    return df


def carregar_arquivo_parquet(caminho_arquivo: CaminhoArquivo) -> DataFrame:
    """
    Carrega arquivo Parquet e valida a estrutura básica.
    
    Args:
        caminho_arquivo: Caminho para o arquivo parquet
        
    Returns:
        DataFrame com os dados carregados
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
        ValueError: Se o arquivo não tiver a estrutura esperada
    """
    try:
        df = pd.read_parquet(caminho_arquivo)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
    
    if df.empty:
        raise ValueError(f"Dataset carregado está vazio: {caminho_arquivo}")
    
    return df


def filtrar_amazonia_legal(df: DataFrame, 
                            ufs_amazonia: Optional[List[UF]] = None) -> DataFrame:
    """
    Filtra o dataset para incluir apenas municípios da Amazônia Legal.
    
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
    
    df_filtrado = df[df['uf'].isin(ufs_amazonia)].copy()
    
    if df_filtrado.empty:
        raise ValueError("Nenhum dado encontrado para as UFs especificadas")
    
    return df_filtrado


# ============================================================================
# FUNÇÕES DE PREPARAÇÃO DE FEATURES
# ============================================================================

def preparar_features_modelo(df: DataFrame, 
                            features: List[str], 
                            target: str) -> Tuple[DataFrame, Series]:
    """
    Prepara as features e target para o modelo, removendo valores nulos.
    
    Esta função valida a existência de todas as features e do target no DataFrame,
    remove linhas com valores nulos e retorna apenas colunas numéricas para features.
    
    Args:
        df: DataFrame com os dados originais
        features: Lista de nomes das features para o modelo
        target: Nome da coluna target (variável dependente)
        
    Returns:
        Tuple com (features_df, target_series):
            - features_df: DataFrame contendo apenas features numéricas
            - target_series: Series com o target
            
    Raises:
        ValueError: Se features ou target não existirem no DataFrame
        ValueError: Se o dataset ficar vazio após remoção de nulos
        
    Examples:
        >>> X, y = preparar_features_modelo(df, 
        ...                                 ['cod_ibge', 'ano', 'vab_agro_mil_reais'],
        ...                                 'tem_desmatamento')
        >>> print(f"Features: {X.shape[1]}, Observações: {X.shape[0]}")
    """
    features_faltantes = [f for f in features if f not in df.columns]
    if features_faltantes:
        raise ValueError(f"Features faltando no dataset: {features_faltantes}")
    
    if target not in df.columns:
        raise ValueError(f"Target '{target}' não encontrado no dataset")
    
    df_selecionado = df[features + [target]].copy()
    df_limpo = df_selecionado.dropna()
    
    if df_limpo.empty:
        raise ValueError("Dataset ficou vazio após remoção de valores nulos")
    
    features_df = df_limpo[features].select_dtypes(include=[np.number])
    target_series = df_limpo[target]
    
    return features_df, target_series


def dividir_dados_temporalmente(features_df: DataFrame, 
                                target_series: Series,
                                ano_limite_treino: Ano, 
                                ano_teste: Ano) -> Tuple[DataFrame, DataFrame, Series, Series]:
    """
    Divide os dados em treino e teste baseado em critério temporal.
    
    Esta função implementa divisão temporal para evitar data leakage,
    usando dados de anos anteriores para treino e ano específico para teste.
    
    Args:
        features_df: DataFrame com features (deve conter coluna 'ano')
        target_series: Series com target alinhado com features_df
        ano_limite_treino: Ano limite para dados de treino (inclusive)
        ano_teste: Ano específico para dados de teste
        
    Returns:
        Tuple com (X_train, X_test, y_train, y_test):
            - X_train: DataFrame de features de treino
            - X_test: DataFrame de features de teste
            - y_train: Series de target de treino
            - y_test: Series de target de teste
            
    Raises:
        ValueError: Se coluna 'ano' não existir nas features
        ValueError: Se divisão resultar em datasets vazios
        
    Examples:
        >>> X_train, X_test, y_train, y_test = dividir_dados_temporalmente(
        ...     X, y, ano_limite_treino=2022, ano_teste=2023
        ... )
        >>> print(f"Treino: {X_train.shape[0]}, Teste: {X_test.shape[0]}")
    """
    if 'ano' not in features_df.columns:
        raise ValueError("Coluna 'ano' não encontrada nas features")
    
    mask_treino = features_df['ano'] <= ano_limite_treino
    mask_teste = features_df['ano'] == ano_teste
    
    X_train = features_df[mask_treino]
    X_test = features_df[mask_teste]
    y_train = target_series[mask_treino]
    y_test = target_series[mask_teste]
    
    if X_train.empty or X_test.empty:
        raise ValueError("Divisão temporal resultou em datasets vazios")
    
    return X_train, X_test, y_train, y_test


# ============================================================================
# FUNÇÕES DE MODELAGEM
# ============================================================================

def calcular_pesos_classes(y_train: Series) -> Dict[int, float]:
    """
    Calcula pesos para lidar com desbalanceamento de classes.
    
    Esta função usa o método 'balanced' do sklearn para calcular pesos
    inversamente proporcionais à frequência das classes, ajudando o modelo
    a lidar com datasets desbalanceados.
    
    Args:
        y_train: Series com target de treino (variável dependente)
        
    Returns:
        Dicionário com pesos para cada classe (chave: classe, valor: peso)
        
    Examples:
        >>> pesos = calcular_pesos_classes(y_train)
        >>> print(f"Peso classe 0: {pesos[0]:.2f}")
        >>> print(f"Peso classe 1: {pesos[1]:.2f}")
    """
    classes_unicas = np.unique(y_train)
    pesos = compute_class_weight('balanced', classes=classes_unicas, y=y_train)
    return dict(zip(classes_unicas, pesos))


def treinar_modelo_random_forest(X_train: DataFrame, 
                                 y_train: Series,
                                 pesos_classes: Dict[int, float],
                                 random_state: int = 42) -> RandomForestClassifier:
    """
    Treina modelo Random Forest com pesos de classes.
    
    Esta função configura e treina um classificador Random Forest com
    parâmetros otimizados para o problema de desmatamento, incluindo
    pesos de classes para lidar com desbalanceamento.
    
    Args:
        X_train: DataFrame de features de treino (variáveis independentes)
        y_train: Series de target de treino (variável dependente)
        pesos_classes: Dicionário com pesos das classes para balanceamento
        random_state: Semente aleatória para reprodutibilidade (padrão: 42)
        
    Returns:
        Modelo RandomForestClassifier treinado e pronto para uso
        
    Model Configuration:
        - n_estimators: 100 (número de árvores)
        - max_depth: 10 (profundidade máxima para evitar overfitting)
        - min_samples_split: 10 (mínimo de amostras para dividir nó)
        - class_weight: pesos_classes (balanceamento de classes)
        - n_jobs: -1 (usar todos os processadores disponíveis)
        
    Examples:
        >>> modelo = treinar_modelo_random_forest(X_train, y_train, pesos_classes)
        >>> print(f"Modelo treinado: {modelo.__class__.__name__}")
    """
    modelo = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=10,
        class_weight=pesos_classes,
        random_state=random_state,
        n_jobs=-1
    )
    
    modelo.fit(X_train, y_train)
    return modelo


def avaliar_modelo(modelo: RandomForestClassifier, 
                   X_test: DataFrame, 
                   y_test: Series,
                   nome_modelo: str = "Modelo") -> MetricasAvaliacao:
    """
    Avalia modelo usando métricas de classificação.
    
    Esta função calcula e exibe métricas de avaliação do modelo, incluindo
    ROC-AUC, Precision-Recall AUC e relatório de classificação detalhado.
    
    Args:
        modelo: Modelo RandomForestClassifier treinado
        X_test: DataFrame de features de teste (variáveis independentes)
        y_test: Series de target de teste (variável dependente real)
        nome_modelo: Nome do modelo para exibição nos resultados (padrão: "Modelo")
        
    Returns:
        MetricasAvaliacao (TypedDict) com as seguintes chaves:
            - roc_auc: Área sob a curva ROC
            - pr_auc: Área sob a curva Precision-Recall
            - accuracy: Acurácia (None neste momento)
            - precision: Precisão (None neste momento)
            - recall: Recall (None neste momento)
            
    Métricas Calculadas:
        - ROC-AUC: Mede capacidade de discriminação entre classes
        - PR-AUC: Mais adequado para datasets desbalanceados
        - Classification Report: Precision, Recall, F1-score por classe
        
    Examples:
        >>> metricas = avaliar_modelo(modelo, X_test, y_test, "Desmatamento")
        >>> print(f"ROC-AUC: {metricas['roc_auc']:.4f}")
        >>> print(f"PR-AUC: {metricas['pr_auc']:.4f}")
    """
    y_pred = modelo.predict(X_test)
    y_prob = modelo.predict_proba(X_test)[:, 1]
    
    print(f"\n=== AVALIAÇÃO DO {nome_modelo.upper()} ===")
    print(classification_report(y_test, y_pred))
    
    roc_auc = roc_auc_score(y_test, y_prob)
    print(f"ROC-AUC: {roc_auc:.4f}")
    
    precision, recall, _ = precision_recall_curve(y_test, y_prob)
    pr_auc = auc(recall, precision)
    print(f"PR-AUC: {pr_auc:.4f}")
    
    return {
        'roc_auc': roc_auc,
        'pr_auc': pr_auc,
        'accuracy': None,
        'precision': None,
        'recall': None
    }


# ============================================================================
# FUNÇÕES DE ANÁLISE DE TENDÊNCIAS TEMPORAIS
# ============================================================================

def calcular_tendencia_linear(group: DataFrame) -> float:
    """
    Calcula tendência linear de desmatamento para um grupo (município).
    
    Esta função aplica regressão linear simples para calcular a taxa de
    variação do desmatamento ao longo do tempo para um município específico.
    
    Args:
        group: DataFrame com dados de um único município (deve conter colunas 'ano' e 'area_desmatada_ha')
        
    Returns:
        Coeficiente angular (slope) da tendência linear em hectares/ano
        - Valor positivo: tendência de aumento de desmatamento
        - Valor negativo: tendência de redução de desmatamento
        - Valor próximo de 0: tendência estável
        
    Notes:
        - Se houver apenas 1 ano de dados, retorna 0.0 (sem tendência)
        - Usa numpy.polyfit para regressão linear de grau 1
        
    Examples:
        >>> tendencia = calcular_tendencia_linear(df_municipio)
        >>> print(f"Tendência: {tendencia:.2f} ha/ano")
    """
    anos = group['ano'].values
    area = group['area_desmatada_ha'].values
    
    if len(anos) <= 1:
        return 0.0
    
    slope = np.polyfit(anos, area, 1)[0]
    return float(slope)


def calcular_tendencias_por_municipio(df: DataFrame) -> DataFrame:
    """
    Calcula tendência de desmatamento para cada município.
    
    Esta função aplica a análise de tendência linear para todos os municípios
    no DataFrame, retornando um resumo com a tendência de cada um.
    
    Args:
        df: DataFrame com dados de desmatamento (deve conter colunas 'cod_ibge', 'ano', 'area_desmatada_ha')
        
    Returns:
        DataFrame com duas colunas:
            - cod_ibge: Código IBGE do município
            - tendencia_desmatamento: Tendência linear em hectares/ano
            
    Notes:
        - Usa groupby por cod_ibge para calcular tendência por município
        - Aplica função calcular_tendencia_linear para cada grupo
        
    Examples:
        >>> tendencias = calcular_tendencias_por_municipio(df_amazonia)
        >>> print(f"Tendências calculadas para {len(tendencias)} municípios")
        >>> print(tendencias.head())
    """
    tendencias = df.groupby('cod_ibge').apply(calcular_tendencia_linear).reset_index()
    tendencias.columns = ['cod_ibge', 'tendencia_desmatamento']
    return tendencias


def classificar_tendencia(tendencia: float, thresholds: ThresholdsTendencia) -> CategoriaTendencia:
    """
    Classifica uma tendência em categorias qualitativas usando early return.
    
    Esta função implementa early return para melhorar legibilidade e performance,
    classificando tendências de desmatamento em 5 categorias qualitativas.
    
    Args:
        tendencia: Valor numérico da tendência em hectares/ano
        thresholds: TypedDict com thresholds de classificação:
            - reducao_forte: Limite para redução forte
            - reducao_leve: Limite para redução leve
            - aumento_leve: Limite para aumento leve
            - aumento_forte: Limite para aumento forte
            
    Returns:
        CategoriaTendencia (Literal) com uma das seguintes categorias:
            - 'Redução Forte': tendência <= reducao_forte
            - 'Redução Leve': reducao_forte < tendência <= reducao_leve
            - 'Estável': reducao_leve < tendência <= aumento_leve
            - 'Aumento Leve': aumento_leve < tendência <= aumento_forte
            - 'Aumento Forte': tendência > aumento_forte
            
    Examples:
        >>> thresholds = {'reducao_forte': -50, 'reducao_leve': -10, 
        ...              'aumento_leve': 0, 'aumento_forte': 10}
        >>> categoria = classificar_tendencia(15.5, thresholds)
        >>> print(categoria)  # 'Aumento Forte'
    """
    if tendencia <= thresholds['reducao_forte']:
        return CategoriaTendenciaEnum.REDUCAO_FORTE.value
    if tendencia <= thresholds['reducao_leve']:
        return CategoriaTendenciaEnum.REDUCAO_LEVE.value
    if tendencia <= thresholds['aumento_leve']:
        return CategoriaTendenciaEnum.ESTAVEL.value
    if tendencia <= thresholds['aumento_forte']:
        return CategoriaTendenciaEnum.AUMENTO_LEVE.value
    return CategoriaTendenciaEnum.AUMENTO_FORTE.value


def classificar_tendencias_dataframe(df: DataFrame, thresholds: ThresholdsTendencia) -> DataFrame:
    """
    Classifica tendências em categorias para um DataFrame completo.
    
    Esta função aplica a classificação de tendência para todas as linhas do DataFrame,
    adicionando uma coluna com a categoria qualitativa da tendência.
    
    Args:
        df: DataFrame com coluna 'tendencia_desmatamento' contendo valores numéricos
        thresholds: TypedDict com thresholds de classificação
        
    Returns:
        DataFrame com coluna 'categoria_tendencia' adicionada contendo categorias qualitativas
        
    Examples:
        >>> df = classificar_tendencias_dataframe(tendencias, thresholds)
        >>> print(df['categoria_tendencia'].value_counts())
    """
    df['categoria_tendencia'] = df['tendencia_desmatamento'].apply(
        lambda x: classificar_tendencia(x, thresholds)
    )
    return df


def obter_informacoes_municipios(df: DataFrame) -> DataFrame:
    """
    Extrai informações únicas dos municípios.
    
    Esta função seleciona colunas informativas sobre municípios e remove
    duplicatas para garantir uma linha por município único.
    
    Args:
        df: DataFrame com dados dos municípios (deve conter coluna 'cod_ibge')
        
    Returns:
        DataFrame com informações únicas por município contendo:
            - cod_ibge: Código IBGE do município
            - municipio: Nome do município (se disponível)
            - uf: Unidade Federativa (se disponível)
            - area_desmatada_ha: Área desmatada (se disponível)
            - vab_agro_mil_reais: VAB agropecuário (se disponível)
            
    Notes:
        - Remove duplicatas baseado em cod_ibge
        - Apenas colunas disponíveis são incluídas no resultado
        
    Examples:
        >>> info = obter_informacoes_municipios(df_amazonia)
        >>> print(f"Informações de {len(info)} municípios")
    """
    colunas_info = ['cod_ibge', 'municipio', 'uf', 'area_desmatada_ha', 'vab_agro_mil_reais']
    colunas_disponiveis = [col for col in colunas_info if col in df.columns]
    
    return df[colunas_disponiveis].drop_duplicates('cod_ibge')


def projetar_desmatamento_ano_seguinte(df_atual: DataFrame, 
                                        tendencias: DataFrame,
                                        ano_base: Ano,
                                        ano_projecao: Ano) -> DataFrame:
    """
    Projeta desmatamento para o ano seguinte baseado em tendências.
    
    Esta função projeta o desmatamento para o ano seguinte somando a tendência
    calculada ao desmatamento atual, garantindo que a projeção não seja negativa.
    
    Args:
        df_atual: DataFrame com dados do ano base (deve conter 'cod_ibge' e 'area_desmatada_ha')
        tendencias: DataFrame com tendências por município (deve conter 'cod_ibge' e 'tendencia_desmatamento')
        ano_base: Ano base para projeção (ano dos dados atuais)
        ano_projecao: Ano para o qual projetar desmatamento
        
    Returns:
        DataFrame com projeções adicionadas contendo:
            - Colunas originais de df_atual
            - tendencia_desmatamento: Tendência aplicada
            - area_desmatada_proj: Área projetada (clipada em 0)
            - ano: Ano da projeção
            
    Notes:
        - Projeção = area_desmatada_ha + tendencia_desmatamento
        - Projeção é clipada em 0 (não permite valores negativos)
        - Ano é atualizado para ano_projecao
        
    Examples:
        >>> projecao = projetar_desmatamento_ano_seguinte(df_2023, tendencias, 2023, 2024)
        >>> print(f"Projeção para {len(projecao)} municípios")
    """
    df_projecao = df_atual.merge(tendencias[['cod_ibge', 'tendencia_desmatamento']], on='cod_ibge')
    
    df_projecao['area_desmatada_proj'] = (
        df_projecao['area_desmatada_ha'] + df_projecao['tendencia_desmatamento']
    )
    
    df_projecao['area_desmatada_proj'] = df_projecao['area_desmatada_proj'].clip(lower=0)
    df_projecao['ano'] = ano_projecao
    
    return df_projecao


# ============================================================================
# FUNÇÕES DE DASHBOARD CONSOLIDADO
# ============================================================================

def consolidar_rankings(ranking_desmatamento: DataFrame,
                        ranking_embargos: DataFrame,
                        tendencias: DataFrame) -> DataFrame:
    """
    Consolida os três rankings individuais em um dashboard unificado.
    
    Esta função combina informações de desmatamento, embargos e tendências
    em um único DataFrame, usando joins por código IBGE para integrar os dados.
    
    Args:
        ranking_desmatamento: DataFrame com ranking de desmatamento (deve conter 'cod_ibge', 'municipio', 'uf', 'probabilidade_desmatamento_2023')
        ranking_embargos: DataFrame com ranking de embargos (deve conter 'cod_ibge', 'probabilidade_embargos_2023')
        tendencias: DataFrame com tendências temporais (deve conter 'cod_ibge', 'tendencia_desmatamento', 'categoria_tendencia')
        
    Returns:
        DataFrame consolidado com informações dos três rankings contendo:
            - cod_ibge: Código IBGE do município
            - municipio: Nome do município
            - uf: Unidade Federativa
            - probabilidade_desmatamento_2023: Probabilidade de desmatamento
            - probabilidade_embargos_2023: Probabilidade de embargos (se disponível)
            - tendencia_desmatamento: Tendência de desmatamento (se disponível)
            - categoria_tendencia: Categoria da tendência (se disponível)
            
    Notes:
        - Usa left join para preservar todos os municípios do ranking de desmatamento
        - Colunas ausentes são preenchidas com NaN se não disponíveis
        
    Examples:
        >>> dashboard = consolidar_rankings(ranking_desmatamento, ranking_embargos, tendencias)
        >>> print(f"Dashboard consolidado: {len(dashboard)} municípios")
    """
    dashboard = ranking_desmatamento[['cod_ibge', 'municipio', 'uf', 
                                        'probabilidade_desmatamento_2023']].copy()
    
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


def normalizar_tendencia(df: DataFrame, coluna_tendencia: str) -> DataFrame:
    """
    Normaliza a coluna de tendência para um score entre 0 e 1.
    
    Esta função aplica normalização min-max para converter tendências
    em scores normalizados, facilitando comparação e combinação com outras métricas.
    
    Args:
        df: DataFrame com dados de tendência
        coluna_tendencia: Nome da coluna de tendência a ser normalizada
        
    Returns:
        DataFrame com coluna 'tendencia_normalizada' adicionada (valores entre 0 e 1)
        
    Notes:
        - Usa normalização min-max: (x - min) / (max - min)
        - Se max == min, retorna 0 para todos os valores
        - Valores NaN são preenchidos com 0
        
    Examples:
        >>> df = normalizar_tendencia(df, 'tendencia_desmatamento')
        >>> print(df['tendencia_normalizada'].describe())
    """
    if coluna_tendencia not in df.columns:
        raise ValueError(f"Coluna '{coluna_tendencia}' não encontrada no dataset")
    
    min_val = df[coluna_tendencia].min()
    max_val = df[coluna_tendencia].max()
    
    if max_val - min_val == 0:
        df['tendencia_normalizada'] = 0
        return df
    
    df['tendencia_normalizada'] = (
        (df[coluna_tendencia] - min_val) / (max_val - min_val)
    )
    df['tendencia_normalizada'] = df['tendencia_normalizada'].fillna(0)
    
    return df


def calcular_score_risco_combinado(df: DataFrame,
                                   peso_desmatamento: Probabilidade,
                                   peso_embargos: Probabilidade,
                                   peso_tendencia: Probabilidade) -> DataFrame:
    """
    Calcula score de risco combinado baseado em pesos configuráveis.
    
    Esta função combina três componentes de risco (desmatamento, embargos e tendência)
    em um score único usando pesos configuráveis, permitindo priorização flexível.
    
    Args:
        df: DataFrame com dados individuais de risco (deve conter 'probabilidade_desmatamento_2023', 'probabilidade_embargos_2023', 'tendencia_normalizada')
        peso_desmatamento: Peso para probabilidade de desmatamento (entre 0 e 1)
        peso_embargos: Peso para probabilidade de embargos (entre 0 e 1)
        peso_tendencia: Peso para tendência normalizada (entre 0 e 1)
        
    Returns:
        DataFrame com coluna 'score_risco_combinado' adicionada (valores entre 0 e 1)
        
    Notes:
        - Fórmula: score = (prob_desmatamento * peso_desmatamento) + 
                    (prob_embargos * peso_embargos) + 
                    (tendencia_normalizada * peso_tendencia)
        - Valores NaN de probabilidade de embargos são preenchidos com 0
        - Pesos devem somar aproximadamente 1 para interpretação intuitiva
        
    Examples:
        >>> df = calcular_score_risco_combinado(df, 0.4, 0.4, 0.2)
        >>> print(df['score_risco_combinado'].describe())
    """
    df['score_risco_combinado'] = (
        df['probabilidade_desmatamento_2023'] * peso_desmatamento +
        df['probabilidade_embargos_2023'].fillna(0) * peso_embargos +
        df['tendencia_normalizada'] * peso_tendencia
    )
    
    return df


def classificar_nivel_risco(score: Probabilidade,
                            threshold_moderado: Probabilidade,
                            threshold_alto: Probabilidade,
                            threshold_critico: Probabilidade) -> NivelRisco:
    """
    Classifica um score de risco em nível qualitativo usando early return.
    
    Esta função implementa early return para melhorar legibilidade e performance,
    classificando scores de risco em 4 níveis qualitativos.
    
    Args:
        score: Score de risco combinado (entre 0 e 1)
        threshold_moderado: Limite para nível moderado
        threshold_alto: Limite para nível alto
        threshold_critico: Limite para nível crítico
        
    Returns:
        NivelRisco (Literal) com uma das seguintes categorias:
            - 'Baixo': score < threshold_moderado
            - 'Moderado': threshold_moderado <= score < threshold_alto
            - 'Alto': threshold_alto <= score < threshold_critico
            - 'Crítico': score >= threshold_critico
            
    Examples:
        >>> nivel = classificar_nivel_risco(0.75, 0.3, 0.5, 0.7)
        >>> print(nivel)  # 'Crítico'
    """
    if score >= threshold_critico:
        return NivelRiscoEnum.CRITICO.value
    if score >= threshold_alto:
        return NivelRiscoEnum.ALTO.value
    if score >= threshold_moderado:
        return NivelRiscoEnum.MODERADO.value
    return NivelRiscoEnum.BAIXO.value


def classificar_dataframe_risco(df: DataFrame,
                               coluna_score: str,
                               threshold_moderado: Probabilidade,
                               threshold_alto: Probabilidade,
                               threshold_critico: Probabilidade) -> DataFrame:
    """
    Classifica todos os scores de risco em um DataFrame.
    
    Esta função aplica a classificação de nível de risco para todas as linhas
    do DataFrame, adicionando uma coluna com o nível qualitativo de risco.
    
    Args:
        df: DataFrame com coluna de score de risco
        coluna_score: Nome da coluna contendo os scores de risco
        threshold_moderado: Limite para nível moderado
        threshold_alto: Limite para nível alto
        threshold_critico: Limite para nível crítico
        
    Returns:
        DataFrame com coluna 'nivel_risco' adicionada contendo níveis qualitativos
        
    Examples:
        >>> df = classificar_dataframe_risco(df, 'score_risco_combinado', 0.3, 0.5, 0.7)
        >>> print(df['nivel_risco'].value_counts())
    """
    df['nivel_risco'] = df[coluna_score].apply(
        lambda x: classificar_nivel_risco(x, threshold_moderado, threshold_alto, threshold_critico)
    )
    return df


def criar_recomendacao_risco(nivel_risco: NivelRisco) -> str:
    """
    Cria recomendação automática baseada no nível de risco usando early return.
    
    Esta função implementa early return para melhorar legibilidade, gerando
    recomendações de ação específicas para cada nível de risco.
    
    Args:
        nivel_risco: Nível de risco do município ('Baixo', 'Moderado', 'Alto', 'Crítico')
        
    Returns:
        Texto de recomendação de ação específica para o nível de risco
        
    Recomendações:
        - Crítico: Ação imediata com fiscalização intensiva
        - Alto: Ação recomendada com fiscalização prioritária
        - Moderado: Ação preventiva com monitoramento regular
        - Baixo: Monitoramento básico com incentivos de longo prazo
        
    Examples:
        >>> recomendacao = criar_recomendacao_risco('Crítico')
        >>> print(recomendacao)
        'Ação imediata: Fiscalização intensiva, monitoramento satelital em tempo real, equipes dedicadas'
    """
    if nivel_risco == NivelRiscoEnum.CRITICO.value:
        return 'Ação imediata: Fiscalização intensiva, monitoramento satelital em tempo real, equipes dedicadas'
    if nivel_risco == NivelRiscoEnum.ALTO.value:
        return 'Ação recomendada: Fiscalização prioritária, alertas de monitoramento, visitas periódicas'
    if nivel_risco == NivelRiscoEnum.MODERADO.value:
        return 'Ação preventiva: Monitoramento regular, capacitação de produtores, incentivos sustentáveis'
    return 'Monitoramento básico: Educação ambiental, incentivos de longo prazo'


# ============================================================================
# FUNÇÕES DE EFICIÊNCIA AGRÍCOLA
# ============================================================================

def calcular_eficiencia_agricola(df: DataFrame) -> DataFrame:
    """
    Calcula indicador de eficiência agrícola (VAB por produção de soja).
    
    Esta função calcula a eficiência econômica da produção agrícola dividindo
    o VAB agropecuário pela produção de soja, indicando quanto valor econômico
    é gerado por unidade de produção.
    
    Args:
        df: DataFrame com dados econômicos e de produção (deve conter 'vab_agro_mil_reais' e 'producao_soja_mil_ton')
        
    Returns:
        DataFrame com coluna 'eficiencia_agricola' adicionada (VAB/produção)
        
    Notes:
        - Fórmula: eficiencia = vab_agro_mil_reais / producao_soja_mil_ton
        - Valores NaN são gerados quando produção de soja é 0
        - Valores altos indicam maior eficiência econômica por unidade de produção
        
    Examples:
        >>> df = calcular_eficiencia_agricola(df)
        >>> print(df['eficiencia_agricola'].describe())
    """
    df['eficiencia_agricola'] = np.where(
        df['producao_soja_mil_ton'] > 0,
        df['vab_agro_mil_reais'] / (df['producao_soja_mil_ton'] + 1),
        np.nan
    )
    
    return df


def criar_target_alta_eficiencia(df: DataFrame, percentil: float) -> Tuple[DataFrame, float]:
    """
    Cria target binário para alta eficiência baseado em percentil.
    
    Esta função cria uma variável target binária indicando se a eficiência
    agrícola está acima de um determinado percentil, útil para modelos de classificação.
    
    Args:
        df: DataFrame com coluna 'eficiencia_agricola'
        percentil: Percentil para definir alta eficiência (entre 0 e 1, ex: 0.75 para top 25%)
        
    Returns:
        Tuple com (DataFrame com coluna target adicionada, threshold usado):
            - DataFrame: com coluna 'alta_eficiencia' (1 se acima do threshold, 0 caso contrário)
            - threshold: valor de eficiência correspondente ao percentil
            
    Examples:
        >>> df, threshold = criar_target_alta_eficiencia(df, 0.75)
        >>> print(f"Threshold: {threshold:.2f}")
        >>> print(f"Alta eficiência: {df['alta_eficiencia'].sum()} municípios")
    """
    threshold_eficiencia = df['eficiencia_agricola'].quantile(percentil)
    df['alta_eficiencia'] = (df['eficiencia_agricola'] >= threshold_eficiencia).astype(int)
    
    return df, threshold_eficiencia
