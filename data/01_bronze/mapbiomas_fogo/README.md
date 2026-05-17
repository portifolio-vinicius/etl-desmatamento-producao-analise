# Documentação: mapbiomas_fogo

**Pasta:** `mapbiomas_fogo`

**Descrição:** Dados brutos da camada Bronze para mapbiomas_fogo

---

## Resumo Geral

- **Total de arquivos:** 1
- **Total de linhas:** 6,444
- **Tamanho total:** 0.12 MB

---

## Detalhes por Arquivo

### 1. mapbiomas_fogo_ocorrencias.parquet

**Tipo:** parquet
**Tamanho:** 0.12 MB
**Linhas:** 6,444
**Colunas:** 7

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 3,766 | 1100015.00 | 5300108.00 | 3182459.78 | 3103405.00 |
| municipio | object | 0 | 0.0% | 3,637 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 6 | 2018.00 | 2023.00 | 2020.48 | 2020.00 |
| area_queimada_ha | float64 | 0 | 0.0% | 5,970 | 20.01 | 1754.17 | 223.87 | 162.06 |
| num_focos | int64 | 0 | 0.0% | 25 | 1.00 | 30.00 | 2.29 | 1.00 |
| fonte | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, ...

**fonte:** MapBiomas Fogo

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| cod_ibge | 0.263 | -0.787 | 0.358 | 2304496.50 | 4122614.25 | 1818117.75 |
| ano | 0.012 | -1.240 | 0.001 | 2019.00 | 2022.00 | 3.00 |
| area_queimada_ha | 2.065 | 6.189 | 0.917 | 79.41 | 301.83 | 222.42 |
| num_focos | 3.517 | 19.468 | 1.034 | 1.00 | 3.00 | 2.00 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Correlações Fortes (|r| > 0.7)

| Variável 1 | Variável 2 | Correlação |
|-----------|-----------|------------|
| area_queimada_ha | num_focos | 0.871 |

**Atenção:** Correlações fortes podem indicar multicolinearidade ou relações causais que merecem investigação.

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| cod_ibge | 2304496.50 | 4122614.25 | 1818117.75 | -422680.12 | 6849790.88 | 0 | 0.00% |
| ano | 2019.00 | 2022.00 | 3.00 | 2014.50 | 2026.50 | 0 | 0.00% |
| area_queimada_ha | 79.41 | 301.83 | 222.42 | -254.22 | 635.46 | 313 | 4.86% |
| num_focos | 1.00 | 3.00 | 2.00 | -2.00 | 6.00 | 351 | 5.45% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| cod_ibge | 3,766 | 0.5844 (Média-Alta) |
| municipio | 3,637 | 0.5644 (Média-Alta) |
| uf | 27 | 0.0042 (Muito Baixa - categórica) |
| ano | 6 | 0.0009 (Muito Baixa - categórica) |
| area_queimada_ha | 5,970 | 0.9264 (Alta - quase única) |
| num_focos | 25 | 0.0039 (Muito Baixa - categórica) |
| fonte | 1 | 0.0002 (Muito Baixa - categórica) |

