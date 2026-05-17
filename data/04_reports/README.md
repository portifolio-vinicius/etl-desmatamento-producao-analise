# Documentação: Camada Reports (data/04_reports)

**Descrição:** Relatórios consolidados e resumos estatísticos para apresentação.

---

## Resumo Geral

- **Total de arquivos:** 8
- **Total de linhas:** 397
- **Tamanho total:** 0.05 MB

---

## Detalhes por Arquivo

### 1. resumo_detalhado_ibama.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 77
**Colunas:** 15

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| file | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| total_rows | int64 | 0 | 0.0% | 1 | 88586.00 | 88586.00 | 88586.00 | 88586.00 |
| mem_mb | float64 | 0 | 0.0% | 2 | 170.20 | 170.88 | 170.54 | 170.88 |
| column | object | 0 | 0.0% | 39 | N/A | N/A | N/A | N/A |
| dtype | object | 0 | 0.0% | 4 | N/A | N/A | N/A | N/A |
| non_null_count | int64 | 0 | 0.0% | 25 | 9465.00 | 88586.00 | 70314.81 | 76742.00 |
| null_count | int64 | 0 | 0.0% | 25 | 0.00 | 79121.00 | 18271.19 | 11844.00 |
| null_percent | float64 | 0 | 0.0% | 24 | 0.00 | 89.32 | 20.63 | 13.37 |
| unique_values | int64 | 0 | 0.0% | 37 | 2.00 | 88586.00 | 35857.08 | 34155.00 |
| min | float64 | 59 | 76.62% | 5 | -135.50 | 1100015.00 | 122198.22 | 0.00 |
| max | float64 | 59 | 76.62% | 9 | 0.00 | 9999999.00 | 1545626.68 | 68744.00 |
| mean | float64 | 59 | 76.62% | 9 | -51.56 | 2485295.27 | 418005.01 | 121.82 |
| std | float64 | 59 | 76.62% | 9 | 0.00 | 1391934.51 | 226814.59 | 745.20 |
| median | float64 | 59 | 76.62% | 9 | -53.74 | 1716208.00 | 361302.84 | 20.59 |
| sample_values | object | 0 | 0.0% | 38 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**file:** embargos_ibama_tabular.parquet, embargos_ibama_full.geoparquet

**column:** objectid, seq_tad, num_tad, serie_tad, operacao, ...

**dtype:** int64, object, float64, geometry

**sample_values:** [25298, 25299, 25300], [1639007, 1472789, 1610984], ['756611', '602347', '729878'], ['E', 'C', 'A'], ['ONDA VERDE P11', 'ONDA VERDE', 'CONTROLE REMOTO P1'], ...

---

### 2. resumo_top_culturas_pam.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 0
**Colunas:** 2

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| Produto | object | 0 | nan% | 0 | N/A | N/A | N/A | N/A |
| Contagem | int64 | 0 | nan% | 0 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**Produto:** 

---

### 3. resumo_consolidado_pib.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 12
**Colunas:** 7

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| Ano | Int64 | 0 | 0.0% | 12 | 2010.00 | 2021.00 | 2015.50 | 2015.50 |
| Variavel | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| Qtd_Municipios_Validos | int64 | 0 | 0.0% | 2 | 5565.00 | 5570.00 | 5568.75 | 5570.00 |
| Soma_VAB | float64 | 0 | 0.0% | 12 | 159931982.00 | 591085024.00 | 296295009.00 | 280969010.50 |
| Media_VAB | float64 | 0 | 0.0% | 12 | 28738.90 | 106119.39 | 53202.20 | 50443.27 |
| Min_VAB | float64 | 0 | 0.0% | 2 | -2299.00 | 0.00 | -191.58 | 0.00 |
| Max_VAB | float64 | 0 | 0.0% | 12 | 613895.00 | 5004239.00 | 1957817.25 | 1598220.50 |

#### Exemplos de Valores (Colunas Categóricas)

**Variavel:** Valor adicionado bruto a preços correntes da agropecuária

---

### 4. resumo_detalhado_pam.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 13
**Colunas:** 8

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| column | object | 0 | 0.0% | 13 | N/A | N/A | N/A | N/A |
| dtype | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| total_rows | int64 | 0 | 0.0% | 1 | 888340.00 | 888340.00 | 888340.00 | 888340.00 |
| null_count | int64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| null_percent | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| unique_values_count | int64 | 0 | 0.0% | 6 | 2.00 | 11092.00 | 1285.69 | 6.00 |
| min | object | 13 | 100.0% | 0 | N/A | N/A | N/A | N/A |
| max | object | 13 | 100.0% | 0 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**column:** NC, NN, MC, MN, V, ...

**dtype:** str

**min:** 

**max:** 

---

### 5. resumo_consolidado_ppm.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 48
**Colunas:** 8

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| Ano | Int64 | 0 | 0.0% | 4 | 2021.00 | 2024.00 | 2022.50 | 2022.50 |
| Variavel | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| Subtipo_Produto | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| Qtd_Municipios | int64 | 0 | 0.0% | 4 | 0.00 | 5538.00 | 461.42 | 0.00 |
| Total_Efetivo | float64 | 0 | 0.0% | 5 | 0.00 | 238620910.00 | 19505316.56 | 0.00 |
| Media_por_Mun | float64 | 44 | 91.67% | 4 | 40563.84 | 43103.49 | 42272.71 | 42711.75 |
| Maior_Rebanho | float64 | 44 | 91.67% | 4 | 2452095.00 | 2522608.00 | 2490844.50 | 2494337.50 |
| Categoria | object | 0 | 0.0% | 12 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**Variavel:** Efetivo dos rebanhos

**Subtipo_Produto:** , Bovino

**Categoria:** CAPRINOS, CODORNAS, GALINHAS, BOVINOS, GALINACEOS_TOTAL, ...

---

### 6. resumo_stats_por_variavel_pam.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 10
**Colunas:** 9

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| D2N | object | 0 | 0.0% | 10 | N/A | N/A | N/A | N/A |
| count | float64 | 0 | 0.0% | 5 | 0.00 | 104994.00 | 63004.40 | 54882.00 |
| mean | float64 | 2 | 20.0% | 5 | 100.00 | 69381.23 | 11881.76 | 599.25 |
| std | float64 | 2 | 20.0% | 5 | 0.00 | 293489.85 | 48262.17 | 1619.10 |
| min | float64 | 2 | 20.0% | 2 | 1.00 | 100.00 | 50.50 | 50.50 |
| 25% | float64 | 2 | 20.0% | 5 | 26.00 | 833.00 | 241.12 | 100.00 |
| 50% | float64 | 2 | 20.0% | 5 | 100.00 | 4941.00 | 1064.62 | 104.50 |
| 75% | float64 | 2 | 20.0% | 5 | 100.00 | 38500.25 | 6805.53 | 363.50 |
| max | float64 | 2 | 20.0% | 5 | 100.00 | 11478917.00 | 1748103.62 | 30965.00 |

#### Exemplos de Valores (Colunas Categóricas)

**D2N:** Quantidade produzida, Rendimento médio da produção, Valor da produção, Valor da produção - percentual do total geral, Área colhida, Área colhida - percentual do total geral, Área destinada à colheita, Área destinada à colheita - percentual do total geral, Área plantada, Área plantada - percentual do total geral

---

### 7. resumo_estatistico_producao_pam.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 8
**Colunas:** 2

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| V | float64 | 0 | 0.0% | 7 | 1.00 | 8313733.00 | 1073899.81 | 8686.87 |
---

### 8. resumo_detalhado_bronze.parquet

**Tipo:** parquet
**Tamanho:** 0.02 MB
**Linhas:** 229
**Colunas:** 15

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| dataset | object | 0 | 0.0% | 16 | N/A | N/A | N/A | N/A |
| total_rows | int64 | 0 | 0.0% | 5 | 5568.00 | 1603796.00 | 100841.72 | 5568.00 |
| mem_mb | float64 | 0 | 0.0% | 6 | 3.69 | 180.48 | 43.56 | 4.02 |
| column | object | 0 | 0.0% | 63 | N/A | N/A | N/A | N/A |
| dtype | object | 0 | 0.0% | 5 | N/A | N/A | N/A | N/A |
| non_null_count | int64 | 0 | 0.0% | 28 | 5568.00 | 1603796.00 | 97800.30 | 5568.00 |
| null_count | int64 | 0 | 0.0% | 24 | 0.00 | 79121.00 | 3041.42 | 0.00 |
| null_percent | float64 | 0 | 0.0% | 24 | 0.00 | 89.32 | 3.43 | 0.00 |
| unique_values | int64 | 0 | 0.0% | 54 | 1.00 | 214720.00 | 8577.90 | 2.00 |
| min | float64 | 210 | 91.7% | 10 | -135.50 | 1100015.00 | 117449.05 | 0.00 |
| max | float64 | 210 | 91.7% | 17 | 0.00 | 14528422000.00 | 1604332096.06 | 68744.00 |
| mean | float64 | 210 | 91.7% | 19 | -51.56 | 49977130.14 | 2926596.30 | 503.40 |
| std | float64 | 210 | 91.7% | 19 | 0.00 | 38598722.16 | 6091030.34 | 745.20 |
| median | float64 | 210 | 91.7% | 19 | -53.74 | 48101399.00 | 2746011.19 | 32.00 |
| sample_values | object | 0 | 0.0% | 91 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**dataset:** comex_stat, ibama_embargos, pam, pib_vab_agro, ppm_asininos, ...

**column:** CO_ANO, CO_MES, CO_NCM, CO_UNID, CO_PAIS, ...

**dtype:** int16, int8, int64, object, float64

**sample_values:** [2024], [4, 11, 3, 8, 7], [94036000, 87088000, 2071400, 90303390, 40129090], [11, 10, 13, 15, 16], [245, 493, 173, 158, 301], ...

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| total_rows | nan | nan | 0.000 | 88586.00 | 88586.00 | 0.00 |
| mem_mb | -0.026 | -1.999 | 0.002 | 170.20 | 170.88 | 0.68 |
| non_null_count | -1.389 | 0.956 | 0.320 | 56659.00 | 88586.00 | 31927.00 |
| null_count | 1.389 | 0.956 | 1.230 | 0.00 | 31927.00 | 31927.00 |
| null_percent | 1.389 | 0.956 | 1.230 | 0.00 | 36.04 | 36.04 |
| unique_values | 0.225 | -1.596 | 0.942 | 133.00 | 65653.00 | 65520.00 |
| min | 2.475 | 4.125 | 2.911 | 0.00 | 0.00 | 0.00 |
| max | 2.211 | 3.321 | 2.053 | 14.73 | 1876484.75 | 1876470.02 |
| mean | 1.759 | 1.537 | 2.031 | 0.00 | 44293.50 | 44293.50 |
| std | 1.874 | 1.993 | 2.052 | 6.85 | 25572.72 | 25565.87 |
| median | 1.358 | -0.119 | 1.898 | 0.00 | 44293.50 | 44293.50 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| total_rows | 88586.00 | 88586.00 | 0.00 | 88586.00 | 88586.00 | 0 | 0.00% |
| mem_mb | 170.20 | 170.88 | 0.68 | 169.18 | 171.90 | 0 | 0.00% |
| non_null_count | 56659.00 | 88586.00 | 31927.00 | 8768.50 | 136476.50 | 0 | 0.00% |
| null_count | 0.00 | 31927.00 | 31927.00 | -47890.50 | 79817.50 | 0 | 0.00% |
| null_percent | 0.00 | 36.04 | 36.04 | -54.06 | 90.10 | 0 | 0.00% |
| unique_values | 133.00 | 65653.00 | 65520.00 | -98147.00 | 163933.00 | 0 | 0.00% |
| min | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 8 | 44.44% |
| max | 14.73 | 1876484.75 | 1876470.02 | -2814690.31 | 4691189.79 | 2 | 11.11% |
| mean | 0.00 | 44293.50 | 44293.50 | -66440.25 | 110733.75 | 4 | 22.22% |
| std | 6.85 | 25572.72 | 25565.87 | -38341.95 | 63921.52 | 4 | 22.22% |
| median | 0.00 | 44293.50 | 44293.50 | -66440.25 | 110733.75 | 4 | 22.22% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| file | 2 | 0.0260 (Baixa) |
| total_rows | 1 | 0.0130 (Baixa) |
| mem_mb | 2 | 0.0260 (Baixa) |
| column | 39 | 0.5065 (Média-Alta) |
| dtype | 4 | 0.0519 (Baixa) |
| non_null_count | 25 | 0.3247 (Média) |
| null_count | 25 | 0.3247 (Média) |
| null_percent | 24 | 0.3117 (Média) |
| unique_values | 37 | 0.4805 (Média) |
| min | 5 | 0.0649 (Baixa) |
| max | 9 | 0.1169 (Média) |
| mean | 9 | 0.1169 (Média) |
| std | 9 | 0.1169 (Média) |
| median | 9 | 0.1169 (Média) |
| sample_values | 38 | 0.4935 (Média) |

