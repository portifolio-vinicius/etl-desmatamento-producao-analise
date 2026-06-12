"""
Pacote de utilitários para análise preditiva com tipagem forte.

Este pacote contém:
- Módulo original de predição (CPU-only)
- Módulo otimizado (GPU + CPU)
- Tipos customizados e interfaces para segurança de tipo
"""

# Importar do módulo original
from .predicao import (
    # Constantes
    UFS_AMAZONIA_LEGAL,
    ANO_LIMITE_TREINO,
    ANO_TESTE,
    ANO_PREVISAO,
    
    # Funções de ambiente
    detectar_ambiente_colab,
    montar_google_drive,
    configurar_caminhos_dados,
    
    # Funções de carregamento
    carregar_dados,
    carregar_arquivo_parquet,
    filtrar_amazonia_legal,
    
    # Funções de preparação
    preparar_features_modelo,
    dividir_dados_temporalmente,
    
    # Funções de modelagem
    calcular_pesos_classes,
    treinar_modelo_random_forest,
    avaliar_modelo,
    
    # Funções de tendências
    calcular_tendencia_linear,
    calcular_tendencias_por_municipio,
    classificar_tendencia,
    classificar_tendencias_dataframe,
    obter_informacoes_municipios,
    projetar_desmatamento_ano_seguinte,
    
    # Funções de dashboard
    consolidar_rankings,
    normalizar_tendencia,
    calcular_score_risco_combinado,
    classificar_nivel_risco,
    classificar_dataframe_risco,
    criar_recomendacao_risco,
    
    # Funções de eficiência
    calcular_eficiencia_agricola,
    criar_target_alta_eficiencia,
)

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
    RankingMunicipio,
    
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
    THRESHOLDS_RISCO_PADRAO,
    THRESHOLDS_TENDENCIA_PADRAO,
    
    # Funções utilitárias de tipo
    validar_e_converter_codigo_ibge,
    validar_e_converter_ano,
    validar_e_converter_uf,
    validar_e_converter_probabilidade,
    
    # Type guards
    is_dataframe,
    is_series,
    is_array,
    is_codigo_ibge_valido,
    is_uf_valida
)

__all__ = [
    # Constantes do módulo original
    'UFS_AMAZONIA_LEGAL',
    'ANO_LIMITE_TREINO',
    'ANO_TESTE',
    'ANO_PREVISAO',
    
    # Funções do módulo original
    'detectar_ambiente_colab',
    'montar_google_drive',
    'configurar_caminhos_dados',
    'carregar_dados',
    'carregar_arquivo_parquet',
    'filtrar_amazonia_legal',
    'preparar_features_modelo',
    'dividir_dados_temporalmente',
    'calcular_pesos_classes',
    'treinar_modelo_random_forest',
    'avaliar_modelo',
    'calcular_tendencia_linear',
    'calcular_tendencias_por_municipio',
    'classificar_tendencia',
    'classificar_tendencias_dataframe',
    'obter_informacoes_municipios',
    'projetar_desmatamento_ano_seguinte',
    'consolidar_rankings',
    'normalizar_tendencia',
    'calcular_score_risco_combinado',
    'classificar_nivel_risco',
    'classificar_dataframe_risco',
    'criar_recomendacao_risco',
    'calcular_eficiencia_agricola',
    'criar_target_alta_eficiencia',
    
    # Type aliases
    'CodigoIBGE',
    'Ano',
    'AreaHectares',
    'ValorMonetario',
    'Probabilidade',
    'CaminhoArquivo',
    'DataFrame',
    'Series',
    'UF',
    'NivelRisco',
    'CategoriaTendencia',
    
    # Enums
    'UFAmazoniaLegal',
    'NivelRiscoEnum',
    'CategoriaTendenciaEnum',
    
    # TypedDicts
    'ConfiguracaoCaminhos',
    'ConfiguracaoModelo',
    'ThresholdsTendencia',
    'MetricasAvaliacao',
    'RankingMunicipio',
    
    # Dataclasses
    'DadosMunicipio',
    'TendenciaDesmatamento',
    'ResultadoModelo',
    'DashboardRisco',
    
    # Protocolos
    'CarregadorDados',
    'FiltradorDados',
    'PreparadorFeatures',
    'DivisorDados',
    'TreinadorModelo',
    'AvaliadorModelo',
    'CalculadorTendencia',
    'ClassificadorRisco',
    
    # Validadores
    'ValidadorTipo',
    
    # Constantes tipadas
    'THRESHOLDS_RISCO_PADRAO',
    'THRESHOLDS_TENDENCIA_PADRAO',
    
    # Funções utilitárias de tipo
    'validar_e_converter_codigo_ibge',
    'validar_e_converter_ano',
    'validar_e_converter_uf',
    'validar_e_converter_probabilidade',
    
    # Type guards
    'is_dataframe',
    'is_series',
    'is_array',
    'is_codigo_ibge_valido',
    'is_uf_valida',
]
