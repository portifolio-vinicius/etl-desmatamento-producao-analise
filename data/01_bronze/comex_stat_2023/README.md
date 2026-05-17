# Documentação: comex_stat_2023

**Pasta:** `comex_stat_2023`

**Descrição:** Dados brutos da camada Bronze para comex_stat_2023

---

## Resumo Geral

- **Total de arquivos:** 1
- **Total de linhas:** 1,563,659
- **Tamanho total:** 19.72 MB

---

## Detalhes por Arquivo

### 1. chunk_91f20307.parquet

**Tipo:** parquet
**Tamanho:** 19.72 MB
**Linhas:** 1,563,659
**Colunas:** 11

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| CO_ANO | int64 | 0 | 0.0% | 1 | 2023.00 | 2023.00 | 2023.00 | 2023.00 |
| CO_MES | int64 | 0 | 0.0% | 12 | 1.00 | 12.00 | 6.57 | 7.00 |
| CO_NCM | int64 | 0 | 0.0% | 8,014 | 1012100.00 | 97069000.00 | 50558940.26 | 48236900.00 |
| CO_UNID | int64 | 0 | 0.0% | 13 | 10.00 | 22.00 | 10.68 | 10.00 |
| CO_PAIS | int64 | 0 | 0.0% | 243 | 13.00 | 890.00 | 389.26 | 369.00 |
| SG_UF_NCM | object | 0 | 0.0% | 28 | N/A | N/A | N/A | N/A |
| CO_VIA | int64 | 0 | 0.0% | 12 | 0.00 | 15.00 | 3.02 | 1.00 |
| CO_URF | int64 | 0 | 0.0% | 78 | 117600.00 | 9999999.00 | 797352.56 | 817800.00 |
| QT_ESTAT | int64 | 0 | 0.0% | 97,698 | 0.00 | 14646306000.00 | 368676.60 | 31.00 |
| KG_LIQUIDO | int64 | 0 | 0.0% | 118,571 | 0.00 | 14646306000.00 | 512884.34 | 38.00 |
| VL_FOB | int64 | 0 | 0.0% | 209,582 | 0.00 | 1309004663.00 | 217244.15 | 739.00 |

#### Exemplos de Valores (Colunas Categóricas)

**SG_UF_NCM:** RJ, SP, RS, MG, SC, ...

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| CO_ANO | nan | nan | 0.000 | 2023.00 | 2023.00 | 0.00 |
| CO_MES | -0.017 | -1.188 | 0.520 | 4.00 | 10.00 | 6.00 |
| CO_NCM | -0.136 | -1.544 | 0.626 | 20081900.00 | 84291190.00 | 64209290.00 |
| CO_UNID | 4.206 | 19.077 | 0.162 | 10.00 | 11.00 | 1.00 |
| CO_PAIS | 0.258 | -1.021 | 0.609 | 169.00 | 586.00 | 417.00 |
| CO_VIA | 1.498 | 3.293 | 0.875 | 1.00 | 4.00 | 3.00 |
| CO_URF | 16.531 | 426.588 | 0.455 | 817600.00 | 917800.00 | 100200.00 |
| QT_ESTAT | 253.116 | 76258.106 | 97.857 | 5.00 | 495.00 | 490.00 |
| KG_LIQUIDO | 239.545 | 70463.037 | 71.757 | 5.00 | 796.00 | 791.00 |
| VL_FOB | 108.567 | 16547.034 | 24.958 | 87.00 | 11073.00 | 10986.00 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Correlações Fortes (|r| > 0.7)

| Variável 1 | Variável 2 | Correlação |
|-----------|-----------|------------|
| QT_ESTAT | KG_LIQUIDO | 0.969 |

**Atenção:** Correlações fortes podem indicar multicolinearidade ou relações causais que merecem investigação.

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| CO_ANO | 2023.00 | 2023.00 | 0.00 | 2023.00 | 2023.00 | 0 | 0.00% |
| CO_MES | 4.00 | 10.00 | 6.00 | -5.00 | 19.00 | 0 | 0.00% |
| CO_NCM | 20081900.00 | 84291190.00 | 64209290.00 | -76232035.00 | 180605125.00 | 0 | 0.00% |
| CO_UNID | 10.00 | 11.00 | 1.00 | 8.50 | 12.50 | 98201 | 6.28% |
| CO_PAIS | 169.00 | 586.00 | 417.00 | -456.50 | 1211.50 | 0 | 0.00% |
| CO_VIA | 1.00 | 4.00 | 3.00 | -3.50 | 8.50 | 21294 | 1.36% |
| CO_URF | 817600.00 | 917800.00 | 100200.00 | 667300.00 | 1068100.00 | 199587 | 12.76% |
| QT_ESTAT | 5.00 | 495.00 | 490.00 | -730.00 | 1230.00 | 310707 | 19.87% |
| KG_LIQUIDO | 5.00 | 796.00 | 791.00 | -1181.50 | 1982.50 | 320939 | 20.52% |
| VL_FOB | 87.00 | 11073.00 | 10986.00 | -16392.00 | 27552.00 | 281485 | 18.00% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| CO_ANO | 1 | 0.0000 (Muito Baixa - categórica) |
| CO_MES | 12 | 0.0000 (Muito Baixa - categórica) |
| CO_NCM | 8,014 | 0.0051 (Muito Baixa - categórica) |
| CO_UNID | 13 | 0.0000 (Muito Baixa - categórica) |
| CO_PAIS | 243 | 0.0002 (Muito Baixa - categórica) |
| SG_UF_NCM | 28 | 0.0000 (Muito Baixa - categórica) |
| CO_VIA | 12 | 0.0000 (Muito Baixa - categórica) |
| CO_URF | 78 | 0.0000 (Muito Baixa - categórica) |
| QT_ESTAT | 97,698 | 0.0625 (Baixa) |
| KG_LIQUIDO | 118,571 | 0.0758 (Baixa) |
| VL_FOB | 209,582 | 0.1340 (Média) |

