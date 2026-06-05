# Análise da Melhor Solução para Preços Agrícolas no Dataset Preditivo

## Contexto do Dataset Atual

**Arquivo:** `data/04_modelagem/dataset_preditivo_com_mapbiomas.parquet`

**Estrutura:**
- 267.360 linhas (município-ano)
- 31 colunas
- Período: 2020-2023
- 5.570 municípios, 27 UFs

**Colunas principais:**
- `cod_ibge`, `ano`, `municipio`, `uf`, `regiao`
- `vab_agro_mil_reais` - VAB agropecuário (dado econômico)
- `ppm_bovinos_cabecas` - Dados pecuários
- `area_desmatada_ha`, `num_embargos` - Dados ambientais
- `precipitacao_total_mm` - Dados climáticos
- `idhm` - Dados socioeconômicos
- `risco_desmatamento`, `pressao_economica` - Variáveis derivadas

## Problema: Dados de Preços Agrícolas

**Limitação identificada:**
- CEPEA via agrobr retorna apenas últimos 15-30 dias (não histórico 2020-2023)
- Dados diários/mensais históricos não disponíveis via API gratuita

## Soluções Avaliadas

### 1. Médias Anuais do Farmnews (RECOMENDADA)

**Vantagens:**
- Dados confirmados para 2023-2024
- Estimativas razoáveis para 2020-2022 baseadas em variações percentuais
- Cobertura completa do período 2020-2023
- Fonte confiável (CEPEA via Farmnews)

**Implementação:**
```python
# Criar tabela de preços médios anuais
precos_anuais = pd.DataFrame({
    'ano': [2020, 2021, 2022, 2023],
    'preco_soja_media': [140.0, 160.0, 175.0, 145.0],  # R$/saca
    'preco_milho_media': [75.0, 88.0, 88.1, 66.0],     # R$/saca
    'preco_boi_media': [220.0, 280.0, 300.0, 255.1],   # R$/arroba
})

# Merge com dataset principal
dataset = dataset.merge(precos_anuais, on='ano', how='left')
```

**Limitações:**
- Dados agregados por ano (não por município)
- Valores estimados para 2020-2022
- Não captura variação sazonal dentro do ano

### 2. CONAB Série Histórica

**Vantagens:**
- Dados oficiais confirmados
- Disponível via agrobr
- Período 2020-2024 coberto

**Limitações:**
- Dados de PRODUÇÃO (área, produtividade), não PREÇOS
- Não serve diretamente para análise de preços de mercado

**Uso recomendado:** Complementar, não substituto de preços

### 3. B3 Futuros Agro

**Vantagens:**
- Preços de mercado reais
- Dados históricos disponíveis
- Alta frequência (diária)

**Limitações:**
- Zona cinza em termos de licença (agrobr avisa)
- Requer tratamento complexo (contratos futuros ≠ preços físicos)
- Pode não refletir preços físicos regionais

**Uso recomendado:** Apenas se necessário para análise avançada

### 4. IMEA (Mato Grosso)

**Vantagens:**
- Dados regionais detalhados
- Preços físicos reais

**Limitações:**
- Apenas Mato Grosso (não Brasil todo)
- Pode não estar disponível via agrobr
- Não cobre Amazônia Legal completa

## Recomendação: Solução Híbrida

### Implementação Sugerida

**Passo 1: Adicionar médias anuais de preços (Farmnews)**
```python
# Tabela de preços médios anuais (nível Brasil)
precos_anuais_brasil = pd.DataFrame({
    'ano': [2020, 2021, 2022, 2023],
    'preco_soja_brasil_rs_saca': [140.0, 160.0, 175.0, 145.0],
    'preco_milho_brasil_rs_saca': [75.0, 88.0, 88.1, 66.0],
    'preco_boi_brasil_rs_arroba': [220.0, 280.0, 300.0, 255.1],
    'ano_boom_soja': [0, 1, 1, 0],  # Dummy para anos de recorde
    'ano_queda_milho': [0, 0, 0, 1],  # Dummy para anos de queda forte
})

dataset = dataset.merge(precos_anuais_brasil, on='ano', how='left')
```

**Passo 2: Adicionar dados de produção CONAB (complementar)**
```python
# Dados de produção por UF-ano (via agrobr)
producao_conab = await conab.serie_historica('soja', inicio=2020, fim=2023)
# Merge por UF e ano
dataset = dataset.merge(producao_conab, on=['uf', 'ano'], how='left')
```

**Passo 3: Criar variáveis derivadas**
```python
# Relação preço x produção
dataset['pressao_preco_soja'] = (
    dataset['preco_soja_brasil_rs_saca'] * 
    dataset['producao_soja_uf_ton'] / 
    dataset['area_plantada_ha']
)

# Dummy para pressão econômica
dataset['pressao_agro_alta'] = (
    (dataset['preco_soja_brasil_rs_saca'] > 150) & 
    (dataset['ano'].isin([2021, 2022]))
).astype(int)
```

### Justificativa

**Por que médias anuais?**
- Dataset é agregado por município-ano (não mensal)
- Análise preditiva foca em tendências anuais de desmatamento
- Preços anuais capturam ciclos de mercado relevantes
- Simplicidade e transparência do modelo

**Por que não dados diários?**
- Desmatamento é processo lento (não responde a preços diários)
- Overfitting com dados de alta frequência
- Complexidade desnecessária para o objetivo

**Por que solução híbrida?**
- Preços (Farmnews) → sinal de mercado
- Produção (CONAB) → capacidade produtiva regional
- Combinação → pressão econômica mais robusta

## Impacto no Dataset Preditivo

**Novas colunas sugeridas:**
1. `preco_soja_brasil_rs_saca` - Preço médio anual soja
2. `preco_milho_brasil_rs_saca` - Preço médio anual milho
3. `preco_boi_brasil_rs_arroba` - Preço médio anual boi
4. `producao_soja_uf_ton` - Produção soja por UF (CONAB)
5. `area_plantada_soja_uf_ha` - Área plantada soja por UF (CONAB)
6. `pressao_preco_soja` - Índice combinado preço x produção
7. `ano_boom_commodities` - Dummy para anos de preços altos

**Benefícios esperados:**
- Melhor correlação com desmatamento em fronteiras agrícolas
- Captura incentivos econômicos para conversão de floresta
- Variáveis explicativas mais robustas para modelo preditivo
- Transparência na interpretação do modelo

## Próximos Passos

1. Criar script para extrair/preparar dados de preços anuais
2. Baixar dados CONAB série histórica via agrobr
3. Implementar merge com dataset preditivo
4. Validar correlações com variáveis de desmatamento
5. Testar impacto no modelo preditivo
