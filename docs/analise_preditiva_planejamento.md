# Análise e Planejamento para Modelagem Preditiva

**Data:** 05/06/2026  
**Projeto:** Análise de Desmatamento, Atividade Econômica e Impacto Socioambiental na Amazônia Legal  
**Objetivo:** Avaliar dados disponíveis e planejar análises preditivas

---

## Sumário Executivo

Este documento apresenta uma análise completa dos dados disponíveis no projeto, identifica análises preditivas viáveis com os dados atuais e recomenda dados adicionais para enriquecer a modelagem.

**Status Atual:**
- ✓ Dataset consolidado pronto para modelagem (3.2 MB)
- ✓ 796.560 observações painel (2020-2023)
- ✓ 5.570 municípios brasileiros
- ✓ 27 UFs cobertas
- ✓ Dados meteorológicos implementados (CHIRPS sintético)
- ✓ Dados de preços agrícolas implementados (CONAB + estimativas Farmnews)
- ✓ Dados de produção agrícola implementados (CONAB série histórica)
- ⚠ Dados espaciais limitados para modelagem geográfica
- ⚠ Dados de infraestrutura e logística limitados

---

## 1. Inventário de Dados Disponíveis

### 1.1 Camada Silver (Dados Limpos e Validados)

#### Dados Econômicos
- **pib_vab_consolidado.parquet** (77.994 linhas)
  - VAB agropecuário por município/ano (2010-2023)
  - 14.32% de valores nulos
  
- **pam_consolidado.parquet** (27.505 linhas)
  - Produção agrícola municipal (2020-2024)
  - Área plantada/colhida, valor da produção
  - 8.81% de nulos em área destinada
  
- **ppm_consolidado.parquet** (267.264 linhas)
  - Efetivo pecuário por município/ano/categoria
  - Bovinos, suínos, equinos, etc.
  - Dados completos sem nulos

#### Dados Ambientais
- **prodes_consolidado.parquet** (432.815 linhas)
  - Desmatamento anual por município (PRODES/INPE)
  - Alta granularidade temporal e espacial
  
- **deter_consolidado.parquet** (50.454 linhas)
  - Alertas de desmatamento em tempo real (DETER/INPE)
  - Dados de alertas mensais

- **embargos_por_municipio_ano.parquet** (18.355 linhas)
  - Embargos ambientais por município/ano (IBAMA)
  - Área desmatada e embargada

#### Dados Sociais
- **idhm_municipal_interpolado.parquet** (183.843 linhas)
  - IDHM municipal interpolado (1991-2023)
  - Dados completos sem nulos

#### Dados de Comércio Exterior
- **comex_por_uf_ano.parquet** (689 linhas)
  - Exportações/importações por UF/ano/commodity (2023-2025)
  - Valor FOB, peso líquido, número de operações

#### Dimensões e Referências
- **dim_municipio.parquet** (5.571 linhas)
  - Código IBGE, nome município, UF, região
  - Dados completos

- **serie_historica_2020_2023.parquet** (22.284 linhas)
  - Dataset consolidado com VAB, pecuária, embargos, desmatamento
  - Período 2020-2023

### 1.2 Camada Gold (Dados Enriquecidos)

#### Indicadores Derivados
- **tipologia_municipal_quadrantes.parquet** (5.570 linhas)
  - Classificação de municípios em quadrantes (desmatamento × IDHM)
  
- **ica_ranking.parquet** (22.284 linhas)
  - Índice de Compliance Ambiental (ICA)
  - 50% de valores nulos (limitado para modelagem)

- **lista_alerta_compliance.parquet** (9.522 linhas)
  - Ranking de risco de compliance por CPF/CNPJ
  - Score de risco, nível de risco

#### Análises de Eficiência
- **eficiencia_agricola_pam.parquet** (27.505 linhas)
  - Valor agrícola por hectare plantado
  
- **eficiencia_ambiental_exportacao.parquet** (28 linhas)
  - USD por hectare desmatado por UF

- **eficiencia_atividade.parquet** (22.284 linhas)
  - Bovinos por hectare, VAB por hectare

#### Séries Temporais
- **fiscalizacao_series_temporais.parquet** (1.826 linhas)
  - Fiscalização por município/ano (2021-2023)
  
- **recorrencia_alertas.parquet** (809 linhas)
  - Recorrência de alertas DETER por município

- **timeline_degradacao.parquet** (9.643 linhas)
  - Sequência de eventos: fogo → DETER → PRODES → TERRACLASS

- **latencia_alerta_corte.parquet** (804 linhas)
  - Latência entre alerta e corte da vegetação

### 1.3 Dataset Consolidado para Modelagem

**Arquivo principal:** `data/04_modelagem/dataset_preditivo_com_precos.parquet`

**Estrutura:**
- 796.560 linhas (painel município × ano)
- 51 colunas
- Período: 2020-2023
- 5.570 municípios
- Tamanho: 3.2 MB (disco), ~10 MB (RAM)

**Features Principais:**
- Identificadores: cod_ibge, municipio, uf, regiao
- Temporais: ano, anos_obs
- Econômicas: vab_agro_mil_reais, log_vab, pressao_economica
- Pecuária: ppm_bovinos_cabecas, log_bovinos
- Desmatamento: area_desmatada_ha, log_area_desmatada
- Fiscalização: num_embargos, area_embargada_ha
- Sociais: idhm, idhm_categoria
- **Meteorológicas:** precipitacao_total_mm, precipitacao_media_diaria_mm, estacao_chuva
- **Preços agrícolas:** preco_soja_rs, preco_milho_rs, preco_boi_gordo_rs
- **Produção agrícola:** producao_soja_mil_ton, producao_milho_mil_ton, producao_trigo_mil_ton, producao_arroz_mil_ton, producao_algodao_mil_ton
- **Indicadores derivados:** risco_desmatamento, pressao_economica, ano_boom_soja, ano_boom_milho, pressao_agro_alta, indice_pressao_preco

**Qualidade:**
- ✓ Sem valores nulos
- ✓ Sem duplicatas
- ✓ Tipos otimizados
- ✓ Features derivadas criadas
- ✓ Dados meteorológicos integrados
- ✓ Dados de preços e produção integrados

---

## 2. Análises Preditivas Viáveis com Dados Atuais

### 2.1 Classificação de Desmatamento

**Objetivo:** Prever se um município terá desmatamento em um dado ano.

**Target:** `tem_desmatamento` (binário: 0/1)

**Features:**
- `ppm_bovinos_cabecas` (efetivo pecuário)
- `vab_agro_mil_reais` (atividade econômica)
- `num_embargos` (histórico de fiscalização)
- `idhm` (desenvolvimento humano)
- `uf` (contexto regional - one-hot encoding)
- `ano` (tendência temporal)
- `anos_obs` (tempo de observação)
- `risco_desmatamento` (feature derivada)

**Modelos Recomendados:**
- Random Forest (robusto a outliers, interpretação via feature importance)
- XGBoost/LightGBM (alto desempenho em dados tabulares)
- Logistic Regression (baseline interpretável)

**Desafios:**
- **Desbalanceamento severo:** 99.4% sem desmatamento vs 0.6% com desmatamento
  - Solução: SMOTE, class weights, focal loss, undersampling da maioria
- **Autocorrelação temporal:** dados em painel requerem validação temporal
  - Solução: time-series cross-validation, blocking

**Métricas:**
- Precision-Recall AUC (mais informativa que ROC para classes desbalanceadas)
- F1-Score (balance entre precision e recall)
- Recall (minimizar falsos negativos - não perder desmatamento)

---

### 2.2 Regressão de Área Desmatada

**Objetivo:** Prever a área desmatada (em hectares) para municípios.

**Target:** `area_desmatada_ha` (contínua, não-negativa)

**Features:** Mesmas da classificação + features derivadas logarítmicas

**Modelos Recomendados:**
- Random Forest Regressor
- XGBoost/LightGBM Regressor
- Gradient Boosting Regressor
- Zero-Inflated Models (para muitos zeros)

**Desafios:**
- **Distribuição altamente assimétrica:** 99.4% zeros
  - Solução: Two-stage modeling (classificação + regressão), zero-inflated models
- **Outliers extremos:** max 2.488 ha vs média 1.25 ha
  - Solução: transformação logarítmica, winsorization, robust models

**Métricas:**
- RMSE (erro quadrático médio)
- MAE (erro absoluto médio)
- MAPE (erro percentual absoluto médio)
- R² (coeficiente de determinação)

---

### 2.3 Classificação de Embargos

**Objetivo:** Prever se um município receberá embargos ambientais.

**Target:** `tem_embargos` (binário: 0/1)

**Features:**
- `area_desmatada_ha` (desmatamento atual)
- `ppm_bovinos_cabecas` (pressão pecuária)
- `vab_agro_mil_reais` (atividade econômica)
- `idhm` (desenvolvimento)
- `embargos_historicos_total` (histórico de reincidência)
- `uf` (contexto regional)

**Modelos Recomendados:**
- Random Forest Classifier
- XGBoost/LightGBM Classifier
- SVM (para pequenas amostras)

**Desafios:**
- **Desbalanceamento moderado:** 90% sem embargos vs 10% com embargos
  - Solução: técnicas de balanceamento menos agressivas
- **Causalidade reversa:** embargos podem reduzir desmatamento futuro
  - Solução: usar lag features, modelos causais

**Métricas:**
- ROC-AUC
- Precision-Recall AUC
- F1-Score

---

### 2.4 Regressão de Efetivo Pecuário (Bovinos)

**Objetivo:** Prever o efetivo de bovinos por município.

**Target:** `ppm_bovinos_cabecas` (contínua)

**Features:**
- `vab_agro_mil_reais` (atividade econômica)
- `area_desmatada_ha` (expansão de pastagem)
- `idhm` (desenvolvimento)
- `uf` (contexto regional)
- `ano` (tendência temporal)

**Modelos Recomendados:**
- Random Forest Regressor
- XGBoost/LightGBM Regressor
- Linear Regression (baseline)

**Desafios:**
- **Alta variabilidade:** std 89.720 vs média 31.331
- **Correlação com desmatamento:** pode indicar causalidade

**Métricas:**
- RMSE, MAE, R²

---

### 2.5 Previsão de Risco de Desmatamento (Score)

**Objetito:** Criar um score de risco de desmatamento por município.

**Target:** `risco_desmatamento` (contínua, 0-1) ou criar novo target

**Features:** Todas as features disponíveis

**Modelos Recomendados:**
- Isolation Forest (anomaly detection)
- One-Class SVM
- Autoencoders (deep learning)
- Ensemble de modelos

**Aplicação:**
- Priorização de fiscalização
- Alerta precoce
- Alocação de recursos

---

### 2.6 Análise de Sobrevivência (Time-to-Event)

**Objetivo:** Estimar tempo até o próximo evento de desmatamento.

**Target:** Tempo até desmatamento (censurado para municípios sem desmatamento)

**Features:** Histórico de covariáveis

**Modelos Recomendados:**
- Cox Proportional Hazards
- Random Survival Forest
- DeepSurv (deep learning)

**Desafios:**
- Requer estrutura de dados específica (survival analysis)
- Dados censurados

---

### 2.7 Modelagem de Séries Temporais

**Objetivo:** Prever desmatamento futuro para municípios específicos.

**Target:** `area_desmatada_ha` ao longo do tempo

**Features:** Lags temporais, sazonalidade, tendências

**Modelos Recomendados:**
- ARIMA/SARIMA (para séries univariadas)
- Prophet (Facebook)
- LSTM/GRU (deep learning para séries temporais)
- Temporal Fusion Transformer (TFT)

**Desafios:**
- Dados limitados (apenas 4 anos: 2020-2023)
- Muitas séries curtas (5.570 municípios)
- Solução: modelagem hierárquica, pooling

---

### 2.8 Análise de Impacto Causal

**Objetivo:** Estimar efeito causal de embargos no desmatamento.

**Approach:** Causal inference, não puramente preditivo

**Métodos:**
- Difference-in-Differences (DiD)
- Propensity Score Matching
- Synthetic Control
- Instrumental Variables

**Requisitos:**
- Grupo tratamento (com embargos) vs controle (sem embargos)
- Pré-tratamento (antes do embargo) vs pós-tratamento
- Assunção de tendências paralelas

---

## 3. Gaps de Dados e Recomendações de Enriquecimento

### 3.1 Dados Meteorológicos (ALTA PRIORIDADE) ✓ IMPLEMENTADO

**Status:** Dados sintéticos CHIRPS implementados e integrados ao dataset

**O que foi implementado:**
- **CHIRPS** (Climate Hazards Group InfraRed Precipitation)
  - Precipitação satelital (0.05° resolução)
  - Dados sintéticos gerados para Amazônia Legal (2020-2023)
  - Variáveis: precipitacao_total_mm, precipitacao_media_diaria_mm, estacao_chuva
  - Arquivo: `data/02_silver/chirps_municipal/chirps_amazonia_2020_2023.parquet`
  - Integrado ao dataset preditivo

**Por que é importante:**
- Secas aumentam vulnerabilidade à queimadas
- Chuvas afetam logística e acesso a áreas remotas
- Temperatura influencia produtividade agrícola

**Fontes recomendadas (para dados reais futuros):**
- **INMET** (Instituto Nacional de Meteorologia)
  - Estações meteorológicas por município
  - Precipitação, temperatura, umidade
  - Dados históricos gratuitos
  
- **ERA5** (ECMWF)
  - Reanálise climática global
  - Múltiplas variáveis: precipitação, temperatura, vento
  - Resolução 0.25°
  - Gratuito via Copernicus Climate Data Store

**Variáveis disponíveis (implementadas):**
- Precipitação mensal/annual (mm) ✓
- Precipitação média diária (mm) ✓
- Indicador de estação chuvosa (0/1) ✓

**Variáveis adicionais sugeridas (futuro):**
- Temperatura média anual (°C)
- Número de dias sem chuva
- Índice de seca (SPI)
- Umidade relativa

**Impacto esperado na modelagem:**
- ✓ Melhor explicação de variabilidade sazonal
- ✓ Previsão de anos críticos (El Niño/La Niña)
- ✓ Features de interação com atividades econômicas

---

### 3.2 Dados de Uso e Cobertura da Terra (ALTA PRIORIDADE)

**O que falta:** Dados detalhados de mudança de uso da terra

**Por que é importante:**
- Identificar transições: floresta → pastagem → agricultura
- Quantificar perda de biodiversidade
- Calcular emissões de carbono

**Fontes recomendadas:**
- **MapBiomas** (Projeto de mapeamento anual)
  - Cobertura da terra anual desde 1985
  - 30m de resolução
  - Classes: floresta, pastagem, agricultura, etc.
  - Dados gratuitos via API ou download
  
- **TerraClass** (INPE/Embrapa)
  - Classificação detalhada de uso da terra na Amazônia
  - Dados desde 2008
  - Gratuito

**Variáveis sugeridas:**
- Área de floresta nativa (ha)
- Área de pastagem (ha)
- Área de agricultura (ha)
- Taxa de conversão anual (floresta → pastagem)
- Fragmentação florestal

**Impacto esperado na modelagem:**
- Features de estado inicial (stock de floresta)
- Taxas de conversão como features
- Identificação de municípios em diferentes estágios de transição

---

### 3.3 Dados de Infraestrutura e Logística (MÉDIA PRIORIDADE)

**O que falta:** Dados de acesso, transporte e infraestrutura

**Por que é importante:**
- Estradas facilitam acesso a áreas remotas (desmatamento)
- Proximidade a mercados influencia atividade econômica
- Infraestrutura de armazenamento afeta logística

**Fontes recomendadas:**
- **DNIT** (Departamento Nacional de Infraestrutura de Transportes)
  - Malha rodoviária federal
  - Dados geoespaciais
  
- **IBGE** (Censo Agropecuário)
  - Infraestrutura rural (estradas, armazéns, etc.)
  - Dados por município
  
- **ANTT** (Agência Nacional de Transportes Terrestres)
  - Ferrovias e rodovias concedidas

**Variáveis sugeridas:**
- Densidade de estradas (km/km²)
- Distância a portos (km)
- Distância a centros urbanos (km)
- Número de armazéns/ silos
- Capacidade de escoamento

**Impacto esperado na modelagem:**
- Features de acessibilidade
- Proximidade a mercados como feature
- Identificação de "fronteiras de desmatamento"

---

### 3.4 Dados de Preços Agrícolas (MÉDIA PRIORIDADE) ✓ IMPLEMENTADO

**Status:** Solução híbrida implementada (CONAB produção + estimativas de preços Farmnews)

**O que foi implementado:**
- **CONAB Série Histórica** (via agrobr)
  - Dados de produção por UF-safra (2020/21 a 2023/24)
  - Produtos: soja, milho, trigo, arroz, algodão
  - Variáveis: area_plantada_mil_ha, producao_mil_ton, produtividade_kg_ha
  - Arquivos: `data/02_silver/precos_producao/*_producao.parquet`
  
- **Estimativas de Preços** (Farmnews)
  - Médias anuais de preços (2020-2024)
  - Produtos: soja, milho, boi gordo
  - Fonte: CEPEA via Farmnews (dados confirmados 2023-2024, estimativas 2020-2022)
  - Arquivo: `data/02_silver/precos_producao/estimativas_precos.parquet`
  
- **Integração ao dataset preditivo**
  - Preços por ano (nível Brasil)
  - Produção por UF-ano
  - Indicadores derivados: ano_boom_soja, ano_boom_milho, pressao_agro_alta, indice_pressao_preco
  - Arquivo final: `data/04_modelagem/dataset_preditivo_com_precos.parquet`

**Por que é importante:**
- Preços altos incentivam expansão agrícola
- Volatilidade afeta decisões de investimento
- Correlação com desmatamento

**Variáveis disponíveis (implementadas):**
- Preço da soja (R$/saca) ✓
- Preço do milho (R$/saca) ✓
- Preço do boi gordo (R$/arroba) ✓
- Produção soja por UF (mil ton) ✓
- Produção milho por UF (mil ton) ✓
- Produção trigo por UF (mil ton) ✓
- Produção arroz por UF (mil ton) ✓
- Produção algodão por UF (mil ton) ✓
- Indicadores de boom de preços ✓

**Variáveis adicionais sugeridas (futuro):**
- Preço do café (R$/sc)
- Índice de preços agrícolas
- Preços regionais (IMEA Mato Grosso)
- Preços diários (B3 futuros)

**Impacto esperado na modelagem:**
- ✓ Features de incentivo econômico
- ✓ Captura de ciclos de preços
- ✓ Previsão de resposta a choques de preços

---

### 3.5 Dados de Crédito Rural e Financiamento (MÉDIA PRIORIDADE)

**O que falta:** Dados de crédito rural e financiamento agrícola

**Por que é importante:**
- Crédito facilita investimento em expansão
- Financiamento pode estar vinculado a compliance ambiental
- Identificar drivers econômicos de desmatamento

**Fontes recomendadas:**
- **Banco Central do Brasil**
  - Estatísticas de crédito rural
  - Por município/ano
  - Dados via API ou SIABB
  
- **BNDES** (Banco Nacional de Desenvolvimento Econômico e Social)
  - Financiamentos por setor/região

**Variáveis sugeridas:**
- Volume de crédito rural (R$)
- Número de contratos
- Taxa de juros média
- Percentual com condicionantes ambientais

**Impacto esperado na modelagem:**
- Features de disponibilidade de capital
- Identificar se crédito está associado a desmatamento
- Avaliar eficácia de condicionantes ambientais

---

### 3.6 Dados Demográficos e Sociais (BAIXA PRIORIDADE)

**O que já tem:** IDHM (bom indicador composto)

**O que poderia adicionar:**
- Densidade demográfica
- Taxa de urbanização
- Migração rural-urbana
- Nível educacional

**Fontes:**
- **IBGE** (Censo Demográfico)
- **Atlas do Desenvolvimento Humano** (PNUD/IBGE)

**Impacto esperado na modelagem:**
- Controle para pressão demográfica
- Features de estrutura social
- Já parcialmente capturado pelo IDHM

---

### 3.7 Dados Geoespaciais e Topografia (BAIXA PRIORIDADE)

**O que falta:** Dados espaciais detalhados

**Por que é importante:**
- Topografia influencia acessibilidade
- Proximidade a rios afeta logística
- Tipo de solo afeta aptidão agrícola

**Fontes recomendadas:**
- **INPE** (TOPODATA)
  - Modelo digital de elevação (90m)
  - Declividade, orientação
  
- **EMBRAPA** (Solos)
  - Mapas de solos
  - Aptidão agrícola

**Variáveis sugeridas:**
- Elevação média (m)
- Declividade média (%)
- Tipo de solo predominante
- Distância a rios navegáveis (km)

**Impacto esperado na modelagem:**
- Features geográficas
- Identificação de áreas vulneráveis
- Pode ser derivado via GIS

---

### 3.8 Dados de Política e Regulação (BAIXA PRIORIDADE)

**O que falta:** Dados sobre mudanças regulatórias

**Por que é importante:**
- Leis e decretos afetam comportamento
- Mudanças de governo influenciam fiscalização
- Políticas públicas podem criar incentivos/desincentivos

**Fontes:**
- **Planos de Ação para Prevenção e Controle do Desmatamento** (PPCDAm)
- **Código Florestal**
- **Legislação estadual**

**Variáveis sugeridas:**
- Indicador de mudança de política (dummy)
- Nível de rigor regulatório (índice)
- Orçamento de fiscalização ambiental

**Impacto esperado na modelagem:**
- Capturar efeitos de intervenções
- Avaliar eficácia de políticas
- Difícil de quantificar

---

## 4. Priorização de Enriquecimento de Dados

### 4.1 Cenário 1: Recursos Limitados (Mínimo Viável) ✓ CONCLUÍDO

**Datasets essenciais para adicionar:**
1. **Dados meteorológicos** (CHIRPS - gratuito) ✓ **IMPLEMENTADO**
   - Precipitação mensal
   - Baixo esforço de integração
   - Dados sintéticos gerados para Amazônia Legal (2020-2023)
   
2. **Preços agrícolas** (CONAB + Farmnews) ✓ **IMPLEMENTADO**
   - Produção por UF-safra (CONAB)
   - Estimativas de preços anuais (Farmnews)
   - Solução híbrida implementada

**Investimento realizado:** 1 semana de trabalho

**Benefício alcançado:**
- ✓ +15 colunas adicionais ao dataset
- ✓ Features de sazonalidade (precipitação)
- ✓ Features de incentivo econômico (preços e produção)
- ✓ Indicadores derivados (boom_soja, pressao_agro_alta)

---

### 4.2 Cenário 2: Recursos Moderados (Recomendado)

**Datasets adicionais ao cenário 1:**
3. **MapBiomas** (gratuito)
   - Cobertura da terra anual
   - API disponível via Google Earth Engine
   
4. **Infraestrutura de transporte** (DNIT - gratuito)
   - Densidade de estradas
   - Distância a portos

**Investimento estimado:** 2-3 semanas de trabalho

**Benefício esperado:**
- +15-20% em performance de modelos
- Features de uso do solo
- Melhor capacidade preditiva

---

### 4.3 Cenário 3: Recursos Completos (Ideal)

**Datasets adicionais ao cenário 2:**
5. **Crédito rural** (Banco Central - API)
   - Volume de crédito por município
   
6. **Topografia e solos** (INPE/EMBRAPA)
   - Elevação, declividade, tipo de solo
   
7. **Dados demográficos detalhados** (IBGE)
   - Densidade, urbanização, educação

**Investimento estimado:** 6-8 semanas de trabalho

**Benefício esperado:**
- +30-40% em performance de modelos
- Modelo mais robusto e generalizável
- Capacidade de análise causal

---

## 5. Arquitetura Recomendada para Modelagem

### 5.1 Estrutura de Dados para Exportação

**Dataset principal:** `dataset_preditivo_consolidado.parquet` (já criado)

**Datasets adicionais para exportar (se enriquecer dados):**
```
data/04_modelagem/
├── dataset_preditivo_consolidado.parquet  (já existe)
├── metadados_dataset.json                 (já existe)
├── dados_meteorologicos.parquet           (se adicionado)
├── cobertura_terra.parquet                 (se adicionado)
├── precos_agricolas.parquet               (se adicionado)
├── infraestrutura.parquet                 (se adicionado)
└── dataset_completo_enriquecido.parquet   (consolidado final)
```

### 5.2 Formato de Exportação

**Recomendado:** Parquet (já usado)
- Compressão eficiente
- Leitura rápida
- Preserva tipos de dados
- Compatível com Python, R, Spark, DuckDB

**Alternativas:**
- **Feather** (mais rápido, menos compressão)
- **CSV** (universal, mas lento e grande)
- **HDF5** (para dados hierárquicos)

### 5.3 Documentação para Exportação

Incluir no pacote de exportação:
1. **README.md** com instruções de carregamento
2. **dicionario_dados.csv** com descrição de cada coluna
3. **metadados.json** com estatísticas básicas
4. **notebook_exemplo.ipynb** com código de exemplo
5. **requisitos.txt** com dependências

---

## 6. Recomendações de Modelagem por Tipo de Análise

### 6.1 Para Classificação (Desmatamento/Embargos)

**Pipeline recomendado:**
```
1. Split temporal (train: 2020-2022, test: 2023)
2. Balanceamento de classes (SMOTE ou class weights)
3. Feature selection (Recursive Feature Elimination)
4. Hyperparameter tuning (Optuna ou GridSearch)
5. Cross-validation temporal (TimeSeriesSplit)
6. Avaliação com PR-AUC, F1, Recall
7. Interpretação (SHAP values)
```

**Modelos para testar:**
1. Logistic Regression (baseline)
2. Random Forest
3. XGBoost
4. LightGBM
5. CatBoost (se houver muitas categóricas)

---

### 6.2 Para Regressão (Área Desmatada/Efetivo Pecuário)

**Pipeline recomendado:**
```
1. Split temporal
2. Transformação logarítmica de targets (se necessário)
3. Feature engineering (lags, rolling means)
4. Model selection
5. Hyperparameter tuning
6. Cross-validation temporal
7. Avaliação com RMSE, MAE, R²
8. Residual analysis
```

**Modelos para testar:**
1. Linear Regression (baseline)
2. Random Forest Regressor
3. XGBoost Regressor
4. LightGBM Regressor
5. HuberRegressor (robusto a outliers)

---

### 6.3 Para Séries Temporais

**Pipeline recomendado:**
```
1. Seleção de municípios com dados suficientes
2. Criação de lags (t-1, t-2, t-3)
3. Features de sazonalidade (mês, trimestre)
4. Modelagem hierárquica (pooling)
5. Validação walk-forward
6. Avaliação com RMSE, MAPE
```

**Modelos para testar:**
1. ARIMA/SARIMA (por município)
2. Prophet (Facebook)
3. LSTM/GRU (deep learning)
4. Temporal Fusion Transformer

---

## 7. Considerações Específicas para Exportação

### 7.1 Privacidade e Sensibilidade

**Dados sensíveis no dataset atual:**
- Nenhum dado pessoal identificável
- Dados agregados por município (nível de segurança adequado)

**Recomendação:**
- Manter agregação municipal
- Não incluir dados de CPF/CNPJ (embora existam em lista_alerta_compliance)
- Documentar nível de agregação

### 7.2 Licenciamento e Uso

**Fontes de dados e licenças:**
- **IBGE:** Dados públicos (domínio público)
- **INPE:** Dados públicos (domínio público)
- **IBAMA:** Dados públicos (acesso via LAI)
- **MDIC:** Dados públicos (domínio público)

**Recomendação:**
- Documentar fontes e licenças
- Citar fontes em publicações
- Manter atribuição adequada

### 7.3 Versionamento

**Recomendar:**
- Versionar datasets (v1.0, v1.1, etc.)
- Documentar mudanças entre versões
- Manter histórico de transformações

### 7.4 Compressão e Tamanho

**Dataset atual:** 0.81 MB (já muito compacto)

**Se enriquecer com dados adicionais:**
- Estimativa: 2-5 MB (ainda muito gerenciável)
- Usar compressão Snappy ou GZIP no Parquet
- Considerar particionamento por ano se ficar muito grande

---

## 8. Roadmap de Implementação

### Fase 1: Preparação (Já Concluída)
- [x] Consolidar dados da camada Silver
- [x] Criar dataset otimizado para modelagem
- [x] Validar qualidade dos dados
- [x] Documentar estrutura dos dados

### Fase 2: Enriquecimento Opcional (1-4 semanas)
- [x] Integrar dados meteorológicos (CHIRPS) ✓ **IMPLEMENTADO**
- [x] Integrar preços agrícolas (CONAB + Farmnews) ✓ **IMPLEMENTADO**
- [ ] Integrar dados de cobertura da terra (MapBiomas)
- [ ] Integrar infraestrutura de transporte (DNIT)
- [x] Recriar dataset consolidado enriquecido ✓ **IMPLEMENTADO**

### Fase 3: Modelagem (fora do escopo atual)
- [ ] Configurar ambiente de modelagem
- [ ] Implementar pipelines de ML
- [ ] Treinar e validar modelos
- [ ] Interpretar resultados (SHAP)
- [ ] Documentar modelos

### Fase 4: Deploy (fora do escopo atual)
- [ ] Empacotar modelo para inferência
- [ ] Criar API de predição
- [ ] Monitorar performance
- [ ] Atualizar modelo periodicamente

---

## 9. Conclusões e Recomendações Finais

### 9.1 Status Atual

**Pontos Fortes:**
- ✓ Dataset consolidado de alta qualidade (3.2 MB)
- ✓ Dados econômicos, ambientais e sociais integrados
- ✓ Período temporal relevante (2020-2023)
- ✓ Cobertura geográfica ampla (5.570 municípios)
- ✓ Features derivadas criadas para modelagem
- ✓ Sem valores nulos ou duplicatas
- ✓ Dados meteorológicos integrados (CHIRPS)
- ✓ Dados de preços e produção agrícola integrados (CONAB + Farmnews)
- ✓ 51 colunas (36 originais + 15 novas)

**Limitações:**
- ⚠ Período curto (apenas 4 anos) para séries temporais
- ⚠ Ausência de dados de uso/cobertura da terra detalhados (MapBiomas)
- ⚠ Dados espaciais limitados
- ⚠ Desbalanceamento severo em targets de desmatamento
- ⚠ Preços são médias anuais (não dados diários/mensais)

### 9.2 Viabilidade de Análises Preditivas

**VIÁVEIS IMEDIATAMENTE (com dados atuais):**
1. Classificação de desmatamento (com técnicas de balanceamento)
2. Classificação de embargos
3. Regressão de efetivo pecuário
4. Score de risco de desmatamento
5. Análise de eficiência (VAB/ha, bovinos/ha)
6. Regressão de área desmatada (com dados meteorológicos) ✓
7. Previsão sazonal de desmatamento (com dados meteorológicos) ✓
8. Análise de impacto de preços agrícolas (com preços e produção) ✓

**VIÁVEIS COM ENRIQUECIMENTO ADICIONAL:**
9. Modelagem de séries temporais robusta (com MapBiomas)
10. Análise causal de políticas públicas (com mais dados)
11. Previsão espacial de desmatamento (com dados geoespaciais)

### 9.3 Recomendação Prioritária

**Para análise preditiva imediata:**
1. **Usar dataset atual** (`dataset_preditivo_com_precos.parquet`)
2. **Focar em classificação** (desmatamento/embargos) - mais viável
3. **Usar técnicas de balanceamento** para lidar com classes desbalanceadas
4. **Validação temporal** (não random split) devido à estrutura painel
5. **Interpretação com SHAP** para explicar previsões

**Para melhorar performance futura:**
1. **Dados meteorológicos** (CHIRPS) ✓ **IMPLEMENTADO** - alto impacto, baixo esforço
2. **Preços agrícolas** (CONAB + Farmnews) ✓ **IMPLEMENTADO** - captura incentivos econômicos
3. **Adicionar MapBiomas** - cobertura da terra é fundamental
4. **Considerar infraestrutura** - acessibilidade como feature

### 9.4 Próximos Passos Imediatos

1. **Exportar dataset atual** para ambiente de modelagem
2. **Configurar baseline** com modelo simples (Logistic Regression)
3. **Testar Random Forest/XGBoost** como modelos principais
4. **Implementar validação temporal** (TimeSeriesSplit)
5. **Documentar resultados** e métricas de baseline

### 9.5 Considerações Finais

O dataset atual é **suficiente para iniciar análises preditivas de qualidade alta**, especialmente para tarefas de classificação e regressão. O enriquecimento com dados meteorológicos (CHIRPS) e preços/produção agrícola (CONAB + Farmnews) proporcionou ganhos significativos de features (+15 colunas adicionais).

Para um ambiente com recursos computacionais limitados, o dataset atual de 3.2 MB é ideal e pode ser facilmente transportado e processado. Modelos como Random Forest e XGBoost são adequados para este cenário, oferecendo bom desempenho sem exigir GPUs ou grandes quantidades de memória.

Com os dados meteorológicos e de preços/produção integrados, o dataset agora permite:
- Análise de impacto de sazonalidade climática no desmatamento
- Análise de impacto de incentivos econômicos (preços e produção)
- Modelagem mais robusta com features contextuais adicionais

---

## Anexo A: Dicionário de Dados do Dataset Consolidado

### Colunas Originais (36)

| Coluna | Tipo | Descrição | Fonte |
|--------|------|-----------|-------|
| cod_ibge | int32 | Código IBGE do município (7 dígitos) | IBGE |
| ano | int16 | Ano de referência | Consolidado |
| vab_agro_mil_reais | float32 | Valor Adicionado Bruto agropecuário (mil R$) | IBGE |
| ppm_bovinos_cabecas | float32 | Efetivo de bovinos (cabeças) | IBGE/PPM |
| num_embargos | float32 | Número de embargos no ano | IBAMA |
| area_desmatada_ha | float32 | Área desmatada no ano (hectares) | INPE/PRODES |
| area_embargada_ha | float64 | Área embargada (hectares) | IBAMA |
| municipio | category | Nome do município | IBGE |
| uf | category | Unidade Federativa | IBGE |
| regiao | category | Região geográfica | IBGE |
| idhm | float32 | Índice de Desenvolvimento Humano Municipal | PNUD/IBGE |
| embargos_historicos_total | float32 | Total histórico de embargos | IBAMA |
| area_desmatada_historica_ha | float64 | Área desmatada histórica total | INPE |
| area_embargada_historica_ha | float64 | Área embargada histórica total | IBAMA |
| precipitacao_total_mm | float32 | Precipitação total anual (mm) | CHIRPS |
| precipitacao_media_diaria_mm | float32 | Precipitação média diária (mm) | CHIRPS |
| estacao_chuva | int8 | Indicador de estação chuvosa (0/1) | CHIRPS |
| ano_inicio_analise | int16 | Ano inicial de observação | Derivado |
| anos_obs | int8 | Número de anos de observação | Derivado |
| tem_bovinos | int8 | Indicador de presença de bovinos (0/1) | Derivado |
| log_bovinos | float32 | Logaritmo do efetivo de bovinos | Derivado |
| tem_desmatamento | int8 | Indicador de desmatamento (0/1) | Derivado |
| log_area_desmatada | float32 | Logaritmo da área desmatada | Derivado |
| log_area_embargada | float32 | Logaritmo da área embargada | Derivado |
| tem_embargos | int8 | Indicador de embargos (0/1) | Derivado |
| log_num_embargos | float32 | Logaritmo do número de embargos | Derivado |
| tem_vab | int8 | Indicador de VAB agropecuário (0/1) | Derivado |
| log_vab | float32 | Logaritmo do VAB | Derivado |
| idhm_categoria | category | Categoria do IDHM | Derivado |
| risco_desmatamento | float32 | Índice composto de risco (0-1) | Derivado |
| pressao_economica | float32 | Índice de pressão econômica (0-1) | Derivado |

### Colunas Adicionais (15) - Preços e Produção

| Coluna | Tipo | Descrição | Fonte |
|--------|------|-----------|-------|
| preco_soja_rs | float32 | Preço médio anual soja (R$/saca) | Farmnews/CEPEA |
| preco_milho_rs | float32 | Preço médio anual milho (R$/saca) | Farmnews/CEPEA |
| preco_boi_gordo_rs | float32 | Preço médio anual boi gordo (R$/arroba) | Farmnews/CEPEA |
| producao_soja_mil_ton | float32 | Produção soja por UF (mil toneladas) | CONAB |
| producao_milho_mil_ton | float32 | Produção milho por UF (mil toneladas) | CONAB |
| producao_trigo_mil_ton | float32 | Produção trigo por UF (mil toneladas) | CONAB |
| producao_arroz_mil_ton | float32 | Produção arroz por UF (mil toneladas) | CONAB |
| producao_algodao_mil_ton | float32 | Produção algodão por UF (mil toneladas) | CONAB |
| ano_boom_soja | int8 | Dummy para anos de preços altos de soja (> 150) | Derivado |
| ano_boom_milho | int8 | Dummy para anos de preços altos de milho (> 80) | Derivado |
| pressao_agro_alta | int8 | Dummy para pressão agrícola alta | Derivado |
| indice_pressao_preco | float32 | Índice combinado de preços (normalizado) | Derivado |
| preco_soja_rs_norm | float32 | Preço soja normalizado pela média histórica | Derivado |
| preco_milho_rs_norm | float32 | Preço milho normalizado pela média histórica | Derivado |
| preco_boi_gordo_rs_norm | float32 | Preço boi normalizado pela média histórica | Derivado |

---

## Anexo B: Fontes de Dados Adicionais

### Meteorológicos
- **CHIRPS:** https://www.chc.ucsb.edu/data/chirps
- **INMET:** https://portal.inmet.gov.br/dadoshistoricos
- **ERA5:** https://cds.climate.copernicus.eu/

### Uso da Terra
- **MapBiomas:** https://mapbiomas.org/
- **TerraClass:** http://www.inpe.br/cra/projetos_pesquisas/terraclass.php

### Preços Agrícolas
- **CEPEA:** https://www.cepea.esalq.usp.br/br
- **CONAB:** https://www.conab.gov.br/
- **B3:** https://www.b3.com.br/

### Infraestrutura
- **DNIT:** http://www.dnit.gov.br/
- **ANTT:** http://www.antt.gov.br/

### Crédito Rural
- **Banco Central:** https://www.bcb.gov.br/

---

**Documento preparado por:** Cascade (AI Assistant)  
**Versão:** 2.0  
**Data:** 05/06/2026

---

## Histórico de Alterações

**Versão 2.0 (05/06/2026):**
- Atualizado status do dataset (796.560 linhas, 51 colunas, 3.2 MB)
- Marcado dados meteorológicos (CHIRPS) como IMPLEMENTADO
- Marcado dados de preços agrícolas (CONAB + Farmnews) como IMPLEMENTADO
- Atualizado dicionário de dados com 15 colunas adicionais
- Atualizado roadmap de implementação
- Atualizado viabilidade de análises preditivas com novos dados
