# Dataset para Análise Preditiva

## Descrição

Dataset consolidado e otimizado para análise preditiva de desmatamento e impacto socioambiental na Amazônia Legal brasileira.

## Características

- **Arquivo**: `dataset_preditivo_consolidado.parquet`
- **Formato**: Parquet (otimizado para leitura rápida)
- **Tamanho**: 0.81 MB
- **Linhas**: 22.280
- **Colunas**: 28
- **Período**: 2020-2023
- **Municípios**: 5.570
- **UFs**: 27

## Uso de Memória

- **Memória em disco**: 0.81 MB
- **Memória RAM (carregado)**: ~2.5 MB

## Colunas do Dataset

### Identificadores

- `cod_ibge`: Código IBGE do município (int32)
- `municipio`: Nome do município (category)
- `uf`: Unidade Federativa (category)
- `regiao`: Região geográfica (category)

### Temporais

- `ano`: Ano de referência (int16)
- `ano_inicio_analise`: Ano inicial de observação do município (int16)
- `anos_obs`: Número de anos de observação (int8)

### Variáveis Econômicas

- `vab_agro_mil_reais`: Valor Adicionado Bruto da agropecuária em mil reais (float32)
- `tem_vab`: Indicador se tem VAB agropecuário (int8)
- `log_vab`: Logaritmo do VAB (float32)
- `pressao_economica`: Índice de pressão econômica (float32)

### Variáveis de Pecuária

- `ppm_bovinos_cabecas`: Efetivo de bovinos em cabeças (float32)
- `tem_bovinos`: Indicador se tem bovinos (int8)
- `log_bovinos`: Logaritmo do efetivo de bovinos (float32)

### Variáveis de Desmatamento

- `area_desmatada_ha`: Área desmatada em hectares (float32)
- `tem_desmatamento`: Indicador se tem desmatamento (int8)
- `log_area_desmatada`: Logaritmo da área desmatada (float32)
- `area_desmatada_historica_ha`: Área desmatada histórica total (float64)

### Variáveis de Fiscalização

- `num_embargos`: Número de embargos no ano (float32)
- `area_embargada_ha`: Área embargada em hectares (float64)
- `tem_embargos`: Indicador se tem embargos (int8)
- `log_num_embargos`: Logaritmo do número de embargos (float32)
- `log_area_embargada`: Logaritmo da área embargada (float32)
- `embargos_historicos_total`: Total histórico de embargos (float32)
- `area_embargada_historica_ha`: Área embargada histórica total (float64)

### Variáveis Sociais

- `idhm`: Índice de Desenvolvimento Humano Municipal (float32)
- `idhm_categoria`: Categoria do IDHM (category: Muito Baixo, Baixo, Médio, Alto)

### Features Derivadas

- `risco_desmatamento`: Índice composto de risco de desmatamento (float32)
  - Combina: desmatamento (40%), embargos (30%), bovinos (20%), IDHM baixo (10%)
  - Escala: 0 a 1

## Carregamento do Dataset

### Python (pandas)

```python
import pandas as pd

df = pd.read_parquet('data/04_modelagem/dataset_preditivo_consolidado.parquet')
print(df.shape)  # (22280, 28)
```

### Python (DuckDB - para consultas analíticas)

```python
import duckdb

con = duckdb.connect()
df = con.execute("SELECT * FROM 'data/04_modelagem/dataset_preditivo_consolidado.parquet'").df()
```

### R

```r
library(arrow)

df <- read_parquet("data/04_modelagem/dataset_preditivo_consolidado.parquet")
```

## Sugestões de Análises Preditivas

### 1. Previsão de Desmatamento

**Target**: `tem_desmatamento` (classificação) ou `area_desmatada_ha` (regressão)

**Features principais**:
- `ppm_bovinos_cabecas`
- `vab_agro_mil_reais`
- `idhm`
- `embargos_historicos_total`
- `pressao_economica`
- `uf` (one-hot encoding)
- `ano` (para capturar tendências temporais)

### 2. Previsão de Embargos

**Target**: `tem_embargos` (classificação) ou `num_embargos` (regressão)

**Features principais**:
- `area_desmatada_ha`
- `ppm_bovinos_cabecas`
- `vab_agro_mil_reais`
- `idhm`
- `risco_desmatamento`

### 3. Análise de Impacto Socioeconômico

**Target**: `idhm` (regressão)

**Features principais**:
- `vab_agro_mil_reais`
- `area_desmatada_ha`
- `num_embargos`
- `ppm_bovinos_cabecas`

## Considerações para Modelagem

### Tratamento de Variáveis Categóricas

- `uf`, `regiao`, `municipio`, `idhm_categoria` são categóricas
- Use one-hot encoding ou target encoding
- Para `municipio`, considere usar embeddings ou agrupar por características

### Variáveis Temporais

- O dataset tem estrutura painel (municípios ao longo do tempo)
- Considere modelos que capturam dependência temporal:
  - Random Forest com features de lag
  - Gradient Boosting com features temporais
  - LSTM/GRU para séries temporais
  - Modelos mistos (mixed effects)

### Balanceamento de Classes

- `tem_desmatamento` e `tem_embargos` são classes desbalanceadas
- Considere técnicas:
  - SMOTE para oversampling
  - Class weights
  - Focal loss

### Valores Nulos

- O dataset já teve valores nulos tratados (preenchidos com 0 ou mediana)
- Verifique se essa estratégia é adequada para seu modelo

## Metadados

Arquivo `metadados_dataset.json` contém informações detalhadas sobre:
- Shape do dataset
- Lista de colunas
- Tipos de dados
- Uso de memória
- Período de análise
- Número de municípios e UFs

## Script de Geração

O dataset foi gerado pelo script `src/consolidar_dataset_preditivo.py`

Para regerar o dataset:

```bash
python3 src/consolidar_dataset_preditivo.py
```

## Próximos Passos Sugeridos

1. **Análise Exploratória**: Entender distribuições e correlações
2. **Feature Engineering**: Criar features adicionais se necessário
3. **Seleção de Features**: Identificar as mais relevantes para cada target
4. **Validação Cruzada**: Usar validação temporal (time-series cross-validation)
5. **Modelagem**: Testar diferentes algoritmos (Random Forest, XGBoost, LightGBM)
6. **Interpretação**: Usar SHAP values para entender importância das features
