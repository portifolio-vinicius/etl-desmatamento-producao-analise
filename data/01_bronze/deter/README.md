# Documentação: deter

**Pasta:** `deter`

**Descrição:** Dados brutos da camada Bronze para deter

---

## Resumo Geral

- **Total de arquivos:** 1
- **Total de linhas:** 22,096
- **Tamanho total:** 0.19 MB

---

## Detalhes por Arquivo

### 1. deter_alertas_diarios.parquet

**Tipo:** parquet
**Tamanho:** 0.19 MB
**Linhas:** 22,096
**Colunas:** 9

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 809 | 1100015.00 | 5108956.00 | 2280090.05 | 1715754.00 |
| municipio | object | 0 | 0.0% | 804 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 9 | N/A | N/A | N/A | N/A |
| data_alerta | datetime64[us] | 0 | 0.0% | 2,016 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 6 | 2018.00 | 2023.00 | 2020.50 | 2021.00 |
| mes | int64 | 0 | 0.0% | 12 | 1.00 | 12.00 | 7.43 | 8.00 |
| area_ha | float64 | 0 | 0.0% | 14,214 | 10.01 | 1000.00 | 111.01 | 79.73 |
| tipo | object | 0 | 0.0% | 4 | N/A | N/A | N/A | N/A |
| fonte | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, AP, TO, MA, MT

**tipo:** Desmatamento, Degradação, Corte Raso, Mineração

**fonte:** DETER

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| cod_ibge | 1.510 | 0.603 | 0.586 | 1503309.00 | 2109502.00 | 606193.00 |
| ano | -0.002 | -1.264 | 0.001 | 2019.00 | 2022.00 | 3.00 |
| mes | -0.461 | -0.720 | 0.414 | 5.00 | 10.00 | 5.00 |
| area_ha | 1.961 | 5.638 | 0.909 | 38.52 | 151.48 | 112.96 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| cod_ibge | 1503309.00 | 2109502.00 | 606193.00 | 594019.50 | 3018791.50 | 3861 | 17.47% |
| ano | 2019.00 | 2022.00 | 3.00 | 2014.50 | 2026.50 | 0 | 0.00% |
| mes | 5.00 | 10.00 | 5.00 | -2.50 | 17.50 | 0 | 0.00% |
| area_ha | 38.52 | 151.48 | 112.96 | -130.93 | 320.93 | 990 | 4.48% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| cod_ibge | 809 | 0.0366 (Baixa) |
| municipio | 804 | 0.0364 (Baixa) |
| uf | 9 | 0.0004 (Muito Baixa - categórica) |
| data_alerta | 2,016 | 0.0912 (Baixa) |
| ano | 6 | 0.0003 (Muito Baixa - categórica) |
| mes | 12 | 0.0005 (Muito Baixa - categórica) |
| area_ha | 14,214 | 0.6433 (Média-Alta) |
| tipo | 4 | 0.0002 (Muito Baixa - categórica) |
| fonte | 1 | 0.0000 (Muito Baixa - categórica) |

