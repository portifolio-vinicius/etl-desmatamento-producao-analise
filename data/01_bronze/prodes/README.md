# Documentação: prodes

**Pasta:** `prodes`

**Descrição:** Dados brutos da camada Bronze para prodes

---

## Resumo Geral

- **Total de arquivos:** 1
- **Total de linhas:** 2,797
- **Tamanho total:** 0.06 MB

---

## Detalhes por Arquivo

### 1. prodes_desmatamento_anual.parquet

**Tipo:** parquet
**Tamanho:** 0.06 MB
**Linhas:** 2,797
**Colunas:** 9

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 804 | 1100015.00 | 5108956.00 | 2240307.74 | 1715754.00 |
| municipio | object | 0 | 0.0% | 799 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 9 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 6 | 2018.00 | 2023.00 | 2020.49 | 2020.00 |
| area_desmatada_km2 | float64 | 0 | 0.0% | 2,429 | 1.03 | 352.09 | 50.77 | 35.77 |
| area_desmatada_ha | float64 | 0 | 0.0% | 2,793 | 103.32 | 35208.84 | 5076.83 | 3576.75 |
| bioma | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| fase | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| fonte | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, AP, TO, MA, MT

**bioma:** Amazônia

**fase:** Corte Raso

**fonte:** PRODES

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| cod_ibge | 1.591 | 0.910 | 0.582 | 1502905.00 | 2109239.00 | 606334.00 |
| ano | 0.000 | -1.266 | 0.001 | 2019.00 | 2022.00 | 3.00 |
| area_desmatada_km2 | 1.918 | 5.019 | 0.965 | 15.89 | 70.10 | 54.21 |
| area_desmatada_ha | 1.918 | 5.019 | 0.965 | 1588.52 | 7009.64 | 5421.12 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Correlações Fortes (|r| > 0.7)

| Variável 1 | Variável 2 | Correlação |
|-----------|-----------|------------|
| area_desmatada_km2 | area_desmatada_ha | 1.000 |

**Atenção:** Correlações fortes podem indicar multicolinearidade ou relações causais que merecem investigação.

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| cod_ibge | 1502905.00 | 2109239.00 | 606334.00 | 593404.00 | 3018740.00 | 456 | 16.30% |
| ano | 2019.00 | 2022.00 | 3.00 | 2014.50 | 2026.50 | 0 | 0.00% |
| area_desmatada_km2 | 15.89 | 70.10 | 54.21 | -65.42 | 151.41 | 131 | 4.68% |
| area_desmatada_ha | 1588.52 | 7009.64 | 5421.12 | -6543.16 | 15141.32 | 131 | 4.68% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| cod_ibge | 804 | 0.2875 (Média) |
| municipio | 799 | 0.2857 (Média) |
| uf | 9 | 0.0032 (Muito Baixa - categórica) |
| ano | 6 | 0.0021 (Muito Baixa - categórica) |
| area_desmatada_km2 | 2,429 | 0.8684 (Média-Alta) |
| area_desmatada_ha | 2,793 | 0.9986 (Alta - quase única) |
| bioma | 1 | 0.0004 (Muito Baixa - categórica) |
| fase | 1 | 0.0004 (Muito Baixa - categórica) |
| fonte | 1 | 0.0004 (Muito Baixa - categórica) |

