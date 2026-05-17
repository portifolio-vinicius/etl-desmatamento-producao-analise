# Documentação: terra_class

**Pasta:** `terra_class`

**Descrição:** Dados brutos da camada Bronze para terra_class

---

## Resumo Geral

- **Total de arquivos:** 1
- **Total de linhas:** 11,031
- **Tamanho total:** 0.09 MB

---

## Detalhes por Arquivo

### 1. terra_class_uso.parquet

**Tipo:** parquet
**Tamanho:** 0.09 MB
**Linhas:** 11,031
**Colunas:** 7

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 809 | 1100015.00 | 5108956.00 | 2298618.74 | 1716604.00 |
| municipio | object | 0 | 0.0% | 804 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 9 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 3 | 2018.00 | 2022.00 | 2020.03 | 2020.00 |
| classe_uso | object | 0 | 0.0% | 6 | N/A | N/A | N/A | N/A |
| area_ha | float64 | 0 | 0.0% | 8,505 | 10.00 | 500.00 | 108.81 | 79.04 |
| fonte | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, AP, TO, MA, MT

**classe_uso:** Vegetação Secundária, Pastagem, Mineração, Agricultura, Solo Exposto, Área Urbana

**fonte:** TerraClass

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| cod_ibge | 1.469 | 0.462 | 0.588 | 1503309.00 | 2109759.00 | 606450.00 |
| ano | -0.030 | -1.508 | 0.001 | 2018.00 | 2022.00 | 4.00 |
| area_ha | 1.678 | 3.062 | 0.884 | 39.25 | 146.85 | 107.60 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| cod_ibge | 1503309.00 | 2109759.00 | 606450.00 | 593634.00 | 3019434.00 | 1990 | 18.04% |
| ano | 2018.00 | 2022.00 | 4.00 | 2012.00 | 2028.00 | 0 | 0.00% |
| area_ha | 39.25 | 146.85 | 107.60 | -122.16 | 308.26 | 570 | 5.17% |

### Qualidade de Dados

**Duplicatas:** 2 (0.02%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| cod_ibge | 809 | 0.0733 (Baixa) |
| municipio | 804 | 0.0729 (Baixa) |
| uf | 9 | 0.0008 (Muito Baixa - categórica) |
| ano | 3 | 0.0003 (Muito Baixa - categórica) |
| classe_uso | 6 | 0.0005 (Muito Baixa - categórica) |
| area_ha | 8,505 | 0.7710 (Média-Alta) |
| fonte | 1 | 0.0001 (Muito Baixa - categórica) |

