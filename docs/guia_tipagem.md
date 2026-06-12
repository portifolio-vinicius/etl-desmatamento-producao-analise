# Guia de Tipagem Forte e Interfaces

## Visão Geral

Este guia explica como usar a tipagem forte e interfaces implementadas no projeto de análise preditiva para melhorar a segurança do tipo e a documentação do código.

## Estrutura de Tipos

### 1. Type Aliases

Type aliases são nomes descritivos para tipos comuns, melhorando a legibilidade do código.

```python
from src.utils.types import (
    CodigoIBGE,      # int - Código IBGE de 7 dígitos
    Ano,             # int - Ano (4 dígitos)
    AreaHectares,    # float - Área em hectares
    ValorMonetario,  # float - Valores em reais
    Probabilidade,   # float - Probabilidade entre 0 e 1
    CaminhoArquivo,  # str - Caminho de arquivo
    DataFrame,       # pd.DataFrame
    Series,          # pd.Series
    UF,              # Literal['AC', 'AM', 'AP', 'MA', 'MT', 'PA', 'RO', 'RR', 'TO']
    NivelRisco,       # Literal['Baixo', 'Moderado', 'Alto', 'Crítico']
    CategoriaTendencia  # Literal['Redução Forte', 'Redução Leve', 'Estável', 'Aumento Leve', 'Aumento Forte']
)
```

#### Exemplo de Uso

```python
from src.utils.types import CodigoIBGE, Ano, Probabilidade

def processar_municipio(codigo: CodigoIBGE, ano: Ano) -> Probabilidade:
    """Processa dados de um município"""
    # código é garantido ser int com 7 dígitos
    # ano é garantido ser int com 4 dígitos
    # retorno é garantido ser float entre 0 e 1
    return 0.75
```

### 2. Enums

Enums fornecem valores fixos e validação automática.

#### UFAmazoniaLegal

```python
from src.utils.types import UFAmazoniaLegal

# Usar enum
uf = UFAmazoniaLegal.ACRE  # UFAmazoniaLegal.ACRE
print(uf.value)  # 'AC'

# Listar todas as UFs
ufs = UFAmazoniaLegal.todas()  # ['AC', 'AM', 'AP', 'MA', 'MT', 'PA', 'RO', 'RR', 'TO']

# Converter string para enum
uf_enum = UFAmazoniaLegal.from_string('AC')  # UFAmazoniaLegal.ACRE
```

#### NivelRiscoEnum

```python
from src.utils.types import NivelRiscoEnum

# Classificar score automaticamente
nivel = NivelRiscoEnum.from_score(0.8)  # NivelRiscoEnum.CRITICO
print(nivel.value)  # 'Crítico'

# Com thresholds customizados
nivel = NivelRiscoEnum.from_score(0.6, 
                                    threshold_moderado=0.2,
                                    threshold_alto=0.4,
                                    threshold_critico=0.6)  # NivelRiscoEnum.CRITICO
```

#### CategoriaTendenciaEnum

```python
from src.utils.types import CategoriaTendenciaEnum

# Acessar categorias
print(CategoriaTendenciaEnum.REDUCAO_FORTE.value)  # 'Redução Forte'
print(CategoriaTendenciaEnum.ESTAVEL.value)  # 'Estável'
```

### 3. TypedDicts

TypedDicts definem estruturas de dicionário com tipos específicos.

#### ConfiguracaoCaminhos

```python
from src.utils.types import ConfiguracaoCaminhos

config: ConfiguracaoCaminhos = {
    'caminho_dados': '/path/to/dados.parquet',
    'caminho_saida': '/path/to/saida.parquet',
    'caminho_drive': '/content/drive/MyDrive/dados.parquet'  # opcional
}
```

#### ConfiguracaoModelo

```python
from src.utils.types import ConfiguracaoModelo

config_modelo: ConfiguracaoModelo = {
    'n_estimators': 100,
    'max_depth': 10,
    'min_samples_split': 10,
    'random_state': 42,
    'class_weight': {0: 1.0, 1: 2.0}  # opcional
}
```

#### MetricasAvaliacao

```python
from src.utils.types import MetricasAvaliacao

metricas: MetricasAvaliacao = {
    'roc_auc': 0.85,
    'pr_auc': 0.72,
    'accuracy': 0.78,
    'precision': 0.81,
    'recall': 0.75
}
```

### 4. Dataclasses

Dataclasses fornecem estruturas de dados imutáveis com validação.

#### DadosMunicipio

```python
from src.utils.types import DadosMunicipio

# Criar dados de município
municipio = DadosMunicipio(
    cod_ibge=1500102,  # CodigoIBGE
    municipio='Alta Floresta D\'Oeste',
    uf='AC',  # UF
    area_desmatada_ha=1250.5,  # AreaHectares
    vab_agro_mil_reais=50000.0,  # ValorMonetario (opcional)
    idhm=0.650  # float (opcional)
)

# Converter para dicionário
dados_dict = municipio.to_dict()
print(dados_dict)
# {'cod_ibge': 1500102, 'municipio': "Alta Floresta D'Oeste", 'uf': 'AC', ...}
```

#### TendenciaDesmatamento

```python
from src.utils.types import TendenciaDesmatamento, ThresholdsTendencia

# Criar tendência
tendencia = TendenciaDesmatamento(
    cod_ibge=1500102,
    tendencia_desmatamento=15.5,  # ha/ano
    categoria='Aumento Forte',  # CategoriaTendencia
    ano_base=2023,
    ano_projecao=2024
)

# Classificar com thresholds
thresholds: ThresholdsTendencia = {
    'reducao_forte': -50,
    'reducao_leve': -10,
    'aumento_leve': 0,
    'aumento_forte': 10
}

categoria = tendencia.classificar(thresholds)
print(categoria)  # 'Aumento Forte'
```

#### ResultadoModelo

```python
from src.utils.types import ResultadoModelo, MetricasAvaliacao

# Criar resultado de modelo
metricas: MetricasAvaliacao = {
    'roc_auc': 0.85,
    'pr_auc': 0.72,
    'accuracy': 0.78,
    'precision': None,
    'recall': None
}

resultado = ResultadoModelo(
    modelo=modelo_rf,
    metricas=metricas,
    feature_importance={'feature1': 0.3, 'feature2': 0.2},  # opcional
    tempo_treinamento=45.2  # opcional (segundos)
)

print(resultado)  # ResultadoModelo(ROC-AUC: 0.8500, PR-AUC: 0.7200)
```

#### DashboardRisco

```python
from src.utils.types import DashboardRisco

# Criar dashboard de risco
dashboard = DashboardRisco(
    cod_ibge=1500102,
    municipio='Alta Floresta D\'Oeste',
    uf='AC',
    probabilidade_desmatamento=0.85,
    probabilidade_embargos=0.30,
    tendencia_desmatamento=15.5,
    categoria_tendencia='Aumento Forte',
    score_risco_combinado=0.70,
    nivel_risco='Crítico',
    recomendacao='Ação imediata: Fiscalização intensiva...'
)
```

### 5. Protocolos (Interfaces)

Protocolos definem interfaces contratuais que classes devem implementar.

#### CarregadorDados

```python
from src.utils.types import CarregadorDados, DataFrame, CaminhoArquivo

class MeuCarregador(CarregadorDados):
    """Implementação de carregador de dados"""
    
    def carregar(self, caminho: CaminhoArquivo, **kwargs) -> DataFrame:
        """Carrega dados de um arquivo"""
        return pd.read_parquet(caminho)

# Usar
carregador = MeuCarregador()
df = carregador.carregar('/path/to/dados.parquet')
```

#### FiltradorDados

```python
from src.utils.types import FiltradorDados, DataFrame, UF

class MeuFiltrador(FiltradorDados):
    """Implementação de filtrador de dados"""
    
    def filtrar_amazonia_legal(self, df: DataFrame, 
                                ufs: Optional[List[UF]] = None) -> DataFrame:
        """Filtra dados para Amazônia Legal"""
        if ufs is None:
            ufs = ['AC', 'AM', 'AP', 'MA', 'MT', 'PA', 'RO', 'RR', 'TO']
        return df[df['uf'].isin(ufs)].copy()
```

#### PreparadorFeatures

```python
from src.utils.types import PreparadorFeatures, DataFrame, Series

class MeuPreparador(PreparadorFeatures):
    """Implementação de preparador de features"""
    
    def preparar_features(self, df: DataFrame, 
                         features: List[str],
                         target: str) -> Tuple[DataFrame, Series]:
        """Prepara features e target"""
        df_selecionado = df[features + [target]].copy()
        df_limpo = df_selecionado.dropna()
        return df_limpo[features], df_limpo[target]
```

### 6. Validadores de Tipo

Validadores fornecem validação em runtime com mensagens de erro claras.

```python
from src.utils.types import ValidadorTipo

# Validar código IBGE
codigo = ValidadorTipo.validar_codigo_ibge(1500102)  # OK
# ValidadorTipo.validar_codigo_ibge(123)  # ValueError: Código IBGE deve ter 7 dígitos

# Validar ano
ano = ValidadorTipo.validar_ano(2023)  # OK
# ValidadorTipo.validar_ano(1800)  # ValueError: Ano deve estar entre 1900 e 2100

# Validar UF
uf = ValidadorTipo.validar_uf('AC')  # OK
# ValidadorTipo.validar_uf('SP')  # ValueError: UF 'SP' não pertence à Amazônia Legal

# Validar probabilidade
prob = ValidadorTipo.validar_probabilidade(0.75)  # OK
# ValidadorTipo.validar_probabilidade(1.5)  # ValueError: Probabilidade deve estar entre 0 e 1

# Validar DataFrame
df = ValidadorTipo.validar_dataframe(df, colunas_obrigatorias=['cod_ibge', 'ano'])
```

### 7. Funções Utilitárias de Tipo

Funções para validar e converter tipos automaticamente.

```python
from src.utils.types import (
    validar_e_converter_codigo_ibge,
    validar_e_converter_ano,
    validar_e_converter_uf,
    validar_e_converter_probabilidade
)

# Validar e converter
codigo = validar_e_converter_codigo_ibge('1500102')  # int: 1500102
ano = validar_e_converter_ano('2023')  # int: 2023
uf = validar_e_converter_uf('ac')  # str: 'AC' (converte para maiúscula)
prob = validar_e_converter_probabilidade('0.75')  # float: 0.75
```

### 8. Type Guards

Type guards permitem verificar tipos em runtime com suporte do type checker.

```python
from src.utils.types import (
    is_dataframe,
    is_series,
    is_array,
    is_codigo_ibge_valido,
    is_uf_valida
)

# Verificar se é DataFrame
if is_dataframe(obj):
    print("É um DataFrame")

# Verificar se é Series
if is_series(obj):
    print("É uma Series")

# Verificar se é array numpy
if is_array(obj):
    print("É um array numpy")

# Verificar se código IBGE é válido
if is_codigo_ibge_valido(1500102):
    print("Código IBGE válido")

# Verificar se UF é válida
if is_uf_valida('AC'):
    print("UF válida")
```

## Integração com Módulos de Predição

### Usar Tipos no Módulo Original

```python
from src.utils import (
    carregar_dados,
    filtrar_amazonia_legal,
    CodigoIBGE,
    Ano,
    DataFrame,
    ValidadorTipo
)

# Carregar dados com validação de tipo
df: DataFrame = carregar_dados(
    '/path/to/dados.parquet',
    colunas_essenciais=['cod_ibge', 'ano', 'area_desmatada_ha']
)

# Filtrar com tipo
df_filtrado: DataFrame = filtrar_amazonia_legal(df)

# Validar código específico
codigo: CodigoIBGE = ValidadorTipo.validar_codigo_ibge(1500102)
```

### Usar Tipos no Módulo Otimizado

```python
from src.utils.predicao_otimizado import (
    carregar_dados,
    treinar_modelo_random_forest,
    GPU_AVAILABLE,
    CodigoIBGE,
    Ano,
    Probabilidade,
    DataFrame,
    MetricasAvaliacao
)

# Carregar com GPU se disponível
df: DataFrame = carregar_dados(
    '/path/to/dados.parquet',
    colunas_essenciais=['cod_ibge', 'ano', 'area_desmatada_ha'],
    usar_gpu=GPU_AVAILABLE
)

# Treinar modelo
modelo = treinar_modelo_random_forest(
    X_train, y_train, pesos_classes,
    usar_gpu=GPU_AVAILABLE
)

# Avaliar com tipo explícito
metricas: MetricasAvaliacao = avaliar_modelo(
    modelo, X_test, y_test,
    usar_gpu=GPU_AVAILABLE
)
```

## Exemplos Práticos

### Exemplo 1: Função com Tipagem Forte

```python
from src.utils.types import (
    CodigoIBGE,
    Ano,
    AreaHectares,
    Probabilidade,
    DataFrame,
    ValidadorTipo
)

def analisar_desmatamento_municipio(
    codigo: CodigoIBGE,
    ano_inicio: Ano,
    ano_fim: Ano,
    df: DataFrame
) -> Probabilidade:
    """
    Analisa desmatamento de um município em um período.
    
    Args:
        codigo: Código IBGE do município (7 dígitos)
        ano_inicio: Ano inicial da análise
        ano_fim: Ano final da análise
        df: DataFrame com dados de desmatamento
        
    Returns:
        Probabilidade de desmatamento futuro
    """
    # Validar tipos
    codigo = ValidadorTipo.validar_codigo_ibge(codigo)
    ano_inicio = ValidadorTipo.validar_ano(ano_inicio)
    ano_fim = ValidadorTipo.validar_ano(ano_fim)
    
    # Filtrar dados do município
    df_municipio = df[df['cod_ibge'] == codigo].copy()
    df_periodo = df_municipio[
        (df_municipio['ano'] >= ano_inicio) & 
        (df_municipio['ano'] <= ano_fim)
    ]
    
    # Calcular probabilidade (exemplo simples)
    area_total = df_periodo['area_desmatada_ha'].sum()
    probabilidade = min(area_total / 10000, 1.0)
    
    return ValidadorTipo.validar_probabilidade(probabilidade)
```

### Exemplo 2: Classe com Protocolos

```python
from src.utils.types import (
    CarregadorDados,
    FiltradorDados,
    PreparadorFeatures,
    DataFrame,
    CaminhoArquivo,
    UF
)

class PipelineAnalise:
    """Pipeline de análise com interfaces tipadas"""
    
    def __init__(self, carregador: CarregadorDados,
                 filtrador: FiltradorDados,
                 preparador: PreparadorFeatures):
        self.carregador = carregador
        self.filtrador = filtrador
        self.preparador = preparador
    
    def executar(self, caminho: CaminhoArquivo,
                  features: list,
                  target: str,
                  ufs: list[UF]) -> tuple:
        """Executa pipeline completo"""
        # Carregar
        df: DataFrame = self.carregador.carregar(caminho)
        
        # Filtrar
        df_filtrado: DataFrame = self.filtrador.filtrar_amazonia_legal(df, ufs)
        
        # Preparar
        X, y = self.preparador.preparar_features(df_filtrado, features, target)
        
        return X, y
```

### Exemplo 3: Dataclass com Validação

```python
from src.utils.types import (
    DadosMunicipio,
    ValidadorTipo,
    CodigoIBGE,
    UF
)

def criar_municipio(
    codigo: int,
    nome: str,
    uf: str,
    area: float,
    vab: float = None,
    idhm: float = None
) -> DadosMunicipio:
    """Cria objeto DadosMunicipio com validação"""
    # Validar tipos
    codigo_valido: CodigoIBGE = ValidadorTipo.validar_codigo_ibge(codigo)
    uf_valida: UF = ValidadorTipo.validar_uf(uf)
    
    # Criar dataclass
    return DadosMunicipio(
        cod_ibge=codigo_valido,
        municipio=nome,
        uf=uf_valida,
        area_desmatada_ha=area,
        vab_agro_mil_reais=vab,
        idhm=idhm
    )
```

## Benefícios da Tipagem Forte

### 1. Segurança de Tipo

```python
# Sem tipagem - erro em runtime
def processar(codigo, ano):
    return codigo + ano  # Erro se codigo for string

# Com tipagem - erro em desenvolvimento
def processar(codigo: CodigoIBGE, ano: Ano) -> int:
    return codigo + ano  # Type checker detecta erro
```

### 2. Autocompletar Melhor

```python
from src.utils.types import CodigoIBGE, Ano, Probabilidade

def analisar(codigo: CodigoIBGE, ano: Ano) -> Probabilidade:
    # IDE sabe que codigo é int e ano é int
    # Autocompletar funciona melhor
    return 0.75
```

### 3. Documentação Automática

```python
from src.utils.types import DashboardRisco

def criar_dashboard(dados: dict) -> DashboardRisco:
    """
    Cria dashboard de risco.
    
    Args:
        dados: Dicionário com dados do município
        
    Returns:
        DashboardRisco com todas as informações
    """
    return DashboardRisco(**dados)
```

### 4. Refatoração Segura

```python
# Mudar tipo de retorno é seguro
def processar() -> Probabilidade:
    return 0.75

# Type checker avisa se mudar para tipo incompatível
def processar() -> str:  # Erro de type checker
    return "0.75"
```

## Melhores Práticas

### 1. Usar Type Aliases Sempre que Possível

```python
# ❌ Ruim
def processar(codigo: int, ano: int) -> float:
    ...

# ✅ Bom
def processar(codigo: CodigoIBGE, ano: Ano) -> Probabilidade:
    ...
```

### 2. Usar Enums para Valores Fixos

```python
# ❌ Ruim
def classificar_uf(uf: str) -> str:
    if uf == 'AC':
        return 'Acre'
    # ...

# ✅ Bom
def classificar_uf(uf: UF) -> str:
    return UFAmazoniaLegal.from_string(uf).name
```

### 3. Usar Dataclasses para Estruturas de Dados

```python
# ❌ Ruim
def criar_municipio(codigo, nome, uf, area):
    return {'codigo': codigo, 'nome': nome, 'uf': uf, 'area': area}

# ✅ Bom
def criar_municipio(codigo: CodigoIBGE, nome: str, 
                   uf: UF, area: AreaHectares) -> DadosMunicipio:
    return DadosMunicipio(cod_ibge=codigo, municipio=nome, 
                         uf=uf, area_desmatada_ha=area)
```

### 4. Usar Protocolos para Interfaces

```python
# ❌ Ruim
class MeuCarregador:
    def carregar(self, caminho):
        return pd.read_parquet(caminho)

# ✅ Bom
class MeuCarregador(CarregadorDados):
    def carregar(self, caminho: CaminhoArquivo, **kwargs) -> DataFrame:
        return pd.read_parquet(caminho)
```

### 5. Validar Inputs em Runtime

```python
# ❌ Ruim
def processar(codigo: int):
    # Assume que código tem 7 dígitos
    return codigo

# ✅ Bom
def processar(codigo: int) -> CodigoIBGE:
    return ValidadorTipo.validar_codigo_ibge(codigo)
```

## Integração com Notebooks

### Exemplo em Notebook

```python
# notebook_analise_preditiva/01_previsao_desmatamento.ipynb

import sys
sys.path.append('..')

from src.utils import (
    carregar_dados,
    filtrar_amazonia_legal,
    treinar_modelo_random_forest,
    CodigoIBGE,
    Ano,
    Probabilidade,
    DataFrame,
    ValidadorTipo,
    MetricasAvaliacao
)

# Carregar dados com tipagem
df: DataFrame = carregar_dados(
    CAMINHO_DADOS,
    colunas_essenciais=['cod_ibge', 'ano', 'area_desmatada_ha']
)

# Filtrar com validação
df_amazonia: DataFrame = filtrar_amazonia_legal(df)

# Validar código específico
codigo_teste: CodigoIBGE = ValidadorTipo.validar_codigo_ibge(1500102)

# Treinar modelo
modelo = treinar_modelo_random_forest(X_train, y_train, pesos_classes)

# Avaliar com tipo explícito
metricas: MetricasAvaliacao = avaliar_modelo(modelo, X_test, y_test)
```

## Verificação de Tipos

### Usar mypy

```bash
# Instalar mypy
pip install mypy

# Verificar tipos no projeto
mypy src/utils/predicao.py
mypy src/utils/predicao_otimizado.py
mypy src/utils/types.py
```

### Usar pyright (VS Code)

```bash
# Instalar pyright
pip install pyright

# Verificar tipos
pyright src/utils/
```

## Troubleshooting

### Erro: "NameError: name 'CodigoIBGE' is not defined"

**Solução**: Importar do módulo correto
```python
from src.utils.types import CodigoIBGE
# ou
from src.utils import CodigoIBGE
```

### Erro: "TypeError: Argument 1 has unexpected type"

**Solução**: Validar e converter tipo
```python
from src.utils.types import validar_e_converter_codigo_ibge

codigo = validar_e_converter_codigo_ibge('1500102')  # Converte para int
```

### Erro: "ValueError: UF 'SP' não pertence à Amazônia Legal"

**Solução**: Usar UF válida
```python
from src.utils.types import UFAmazoniaLegal

# Usar enum
uf = UFAmazoniaLegal.ACRE.value  # 'AC'

# Ou validar
from src.utils.types import ValidadorTipo
uf = ValidadorTipo.validar_uf('AC')
```

## Conclusão

A tipagem forte e interfaces implementadas fornecem:

1. **Segurança de tipo**: Erros detectados em desenvolvimento
2. **Documentação automática**: Tipos documentam o código
3. **Autocompletar melhor**: IDEs fornecem sugestões mais precisas
4. **Refatoração segura**: Mudanças que quebram contratos são detectadas
5. **Validação em runtime**: Erros de dados são detectados cedo

Use os tipos customizados sempre que possível para maximizar esses benefícios.
