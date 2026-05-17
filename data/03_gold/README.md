# Documentação: Camada Gold (data/03_gold)

**Descrição:** Conjuntos de dados prontos para análise com lógica de negócio aplicada.

---

## Resumo Geral

- **Total de arquivos:** 28
- **Total de linhas:** 123,671
- **Tamanho total:** 2.79 MB

---

## Detalhes por Arquivo

### 1. fiscalizacao_series_temporais.parquet

**Tipo:** parquet
**Tamanho:** 0.03 MB
**Linhas:** 1,826
**Colunas:** 9
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 1,249 | 1100015.00 | 5300108.00 | 2914565.20 | 2802956.50 |
| ano | int64 | 0 | 0.0% | 3 | 2021.00 | 2023.00 | 2022.16 | 2022.00 |
| num_embargos | int64 | 0 | 0.0% | 85 | 1.00 | 352.00 | 7.14 | 2.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 98 | 0.00 | 2488.87 | 12.88 | 0.00 |
| area_embargada_ha | float64 | 0 | 0.0% | 1,053 | 0.00 | 80874.90 | 751.98 | 3.28 |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| amazonia_legal | bool | 0 | 0.0% | 2 | 0.00 | 1.00 | 0.34 | 0.00 |
| regiao | object | 0 | 0.0% | 5 | N/A | N/A | N/A | N/A |
| outlier_fiscalizacao | bool | 0 | 0.0% | 2 | 0.00 | 1.00 | 0.05 | 0.00 |

#### Exemplos de Valores (Colunas Categóricas)

**uf:** RO, AC, AM, RR, PA, ...

**regiao:** Norte, Nordeste, Sudeste, Sul, Centro-Oeste

---

### 2. tipologia_municipal_quadrantes.parquet

**Tipo:** parquet
**Tamanho:** 0.19 MB
**Linhas:** 5,570
**Colunas:** 22
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,570 | 1100015.00 | 5300108.00 | 3253590.77 | 3146280.00 |
| ano | int64 | 0 | 0.0% | 1 | 2023.00 | 2023.00 | 2023.00 | 2023.00 |
| vab_agro_mil_reais | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_asininos_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_bovinos_cabecas | float64 | 0 | 0.0% | 4,858 | 0.00 | 2452095.00 | 42840.38 | 13832.50 |
| ppm_bubalinos_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_caprinos_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_codornas_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_equinos_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_galinaceos_total_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_galinhas_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_muar_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_ovinos_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_suinos_matrizes_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_suinos_total_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| num_embargos | float64 | 0 | 0.0% | 55 | 0.00 | 352.00 | 1.01 | 0.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 47 | 0.00 | 1230.44 | 1.50 | 0.00 |
| area_embargada_ha | float64 | 0 | 0.0% | 426 | 0.00 | 56599.80 | 105.51 | 0.00 |
| idhm | float64 | 0 | 0.0% | 5,570 | 0.64 | 0.86 | 0.75 | 0.75 |
| municipio | object | 0 | 0.0% | 5,297 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| quadrante | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, ...

**quadrante:** Alto Desmatamento / Baixo IDHM (Paradoxo), Alto Desmatamento / Alto IDHM

---

### 3. ica_ranking.parquet

**Tipo:** parquet
**Tamanho:** 0.15 MB
**Linhas:** 22,284
**Colunas:** 6
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,571 | -1.00 | 5300108.00 | 3253006.75 | 3146255.00 |
| ano | int64 | 0 | 0.0% | 4 | 2020.00 | 2023.00 | 2021.50 | 2021.50 |
| vab_agro_mil_reais | float64 | 0 | 0.0% | 10,613 | 0.00 | 5004239.00 | 46028.81 | 0.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 123 | 0.00 | 2488.87 | 1.25 | 0.00 |
| area_embargada_ha | float64 | 0 | 0.0% | 1,291 | 0.00 | 80874.90 | 70.30 | 0.00 |
| ica | float64 | 11,148 | 50.03% | 44 | 0.00 | 0.01 | 0.00 | 0.00 |
---

### 4. impacto_embargo_producao.parquet

**Tipo:** parquet
**Tamanho:** 0.03 MB
**Linhas:** 893
**Colunas:** 10
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 893 | 1100015.00 | 5300108.00 | 2997286.96 | 2911808.00 |
| ano_embargo | int64 | 0 | 0.0% | 2 | 2021.00 | 2022.00 | 2021.51 | 2022.00 |
| vab_antes | float64 | 0 | 0.0% | 888 | 361.00 | 3694787.00 | 137315.88 | 59008.00 |
| vab_depois | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| bovinos_antes | float64 | 0 | 0.0% | 450 | 0.00 | 612172.00 | 24895.34 | 233.00 |
| bovinos_depois | float64 | 0 | 0.0% | 869 | 0.00 | 2522608.00 | 98279.46 | 23361.00 |
| delta_vab_pct | float64 | 0 | 0.0% | 1 | -100.00 | -100.00 | -100.00 | -100.00 |
| delta_bovinos_pct | float64 | 0 | 0.0% | 448 | -48.57 | 327.35 | 3.71 | 0.00 |
| sucesso_embargo | int64 | 0 | 0.0% | 2 | 0.00 | 1.00 | 0.16 | 0.00 |
| aumento_pos_embargo | int64 | 0 | 0.0% | 2 | 0.00 | 1.00 | 0.29 | 0.00 |
---

### 5. matriz_destino_exportacao.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 333
**Colunas:** 4
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| nome_pais | object | 0 | 0.0% | 171 | N/A | N/A | N/A | N/A |
| commodity | object | 0 | 0.0% | 5 | N/A | N/A | N/A | N/A |
| VL_FOB | int64 | 0 | 0.0% | 331 | 1.00 | 71765917812.00 | 443792659.48 | 7495937.00 |
| KG_LIQUIDO | int64 | 0 | 0.0% | 318 | 0.00 | 149828480547.00 | 923169980.34 | 10568550.00 |

#### Exemplos de Valores (Colunas Categóricas)

**nome_pais:** China, Outro (361), Outro (365), Outro (59), Outro (474), ...

**commodity:** Açúcar, Celulose, Madeira, Milho, Soja

---

### 6. status_regularizacao_embargos.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 2
**Colunas:** 4
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| situacao | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| contagem | int64 | 0 | 0.0% | 2 | 32850.00 | 55736.00 | 44293.00 | 44293.00 |
| descricao | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| pct | float64 | 0 | 0.0% | 2 | 37.08 | 62.92 | 50.00 | 50.00 |

#### Exemplos de Valores (Colunas Categóricas)

**situacao:** D, N

**descricao:** Desmatamento / Degradação, Não Desmatamento / Outros

---

### 7. ranking_concentracao.parquet

**Tipo:** parquet
**Tamanho:** 0.14 MB
**Linhas:** 5,571
**Colunas:** 6
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,571 | -1.00 | 5300108.00 | 3253006.75 | 3146255.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 91 | 0.00 | 5075.75 | 5.00 | 0.00 |
| area_embargada_ha | float64 | 0 | 0.0% | 795 | 0.00 | 157370.99 | 281.21 | 0.00 |
| rank_desmat | int64 | 0 | 0.0% | 5,571 | 1.00 | 5571.00 | 2786.00 | 2786.00 |
| vab_agro_mil_reais | float64 | 0 | 0.0% | 5,512 | 0.00 | 7724069.00 | 184115.25 | 78189.00 |
| rank_vab | int64 | 0 | 0.0% | 5,571 | 1.00 | 5571.00 | 2786.00 | 2786.00 |
---

### 8. reincidentes_embargos.parquet

**Tipo:** parquet
**Tamanho:** 0.27 MB
**Linhas:** 9,522
**Colunas:** 8
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cpf_cnpj_e | object | 0 | 0.0% | 9,522 | N/A | N/A | N/A | N/A |
| num_embargos | int64 | 0 | 0.0% | 22 | 2.00 | 191.00 | 2.48 | 2.00 |
| anos_ativos | int64 | 0 | 0.0% | 8 | 1.00 | 8.00 | 1.68 | 2.00 |
| area_total_ha | float64 | 0 | 0.0% | 7,073 | 0.00 | 278455.25 | 299.71 | 35.56 |
| uf_principal | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| municipio_principal | object | 0 | 0.0% | 1,598 | N/A | N/A | N/A | N/A |
| recurrence_rate | float64 | 0 | 0.0% | 47 | 1.00 | 23.88 | 1.65 | 1.50 |

#### Exemplos de Valores (Colunas Categóricas)

**cpf_cnpj_e:** 05440892273, 00915017253, 04680054000104, 34030786120, 63574896468, ...

**uf_principal:** PA, AM, AL, RO, CE, ...

**municipio_principal:** Trairão, Boca do Acre, Humaitá, Novo Progresso, Traipu, ...

---

### 9. eficiencia_ambiental_exportacao.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 28
**Colunas:** 6
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| uf | object | 0 | 0.0% | 28 | N/A | N/A | N/A | N/A |
| vob_fob_usd | float64 | 0 | 0.0% | 28 | 133114172.00 | 142896724480.00 | 24169354561.36 | 5855914547.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 25 | 0.00 | 17163.38 | 2647.14 | 252.15 |
| area_calculo | float64 | 0 | 0.0% | 25 | 0.00 | 17163.38 | 2647.14 | 252.15 |
| usd_por_ha_desmatado | float64 | 0 | 0.0% | 28 | 11461.80 | 92512326359000.00 | 5295988522885.84 | 30173035.72 |
| rank_eficiencia | float64 | 0 | 0.0% | 28 | 1.00 | 28.00 | 14.50 | 14.50 |

#### Exemplos de Valores (Colunas Categóricas)

**uf:** RJ, GO, SC, ND, SP, ...

---

### 10. ranking_top100_vab.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 100
**Colunas:** 4
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 100 | 1100205.00 | 5300108.00 | 4221826.89 | 5003479.00 |
| vab_agro_mil_reais | float64 | 0 | 0.0% | 100 | 1233420.00 | 7724069.00 | 2396126.11 | 1822089.00 |
| rank_desmat | int64 | 0 | 0.0% | 100 | 11.00 | 5524.00 | 2202.35 | 1966.50 |
| rank_vab | int64 | 0 | 0.0% | 100 | 1.00 | 100.00 | 50.50 | 50.50 |
---

### 11. ranking_uf_exportadora.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 150
**Colunas:** 6
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| uf | object | 0 | 0.0% | 28 | N/A | N/A | N/A | N/A |
| commodity | object | 0 | 0.0% | 8 | N/A | N/A | N/A | N/A |
| vob_fob_usd | float64 | 0 | 0.0% | 150 | 37.00 | 109449713660.00 | 4511612851.45 | 67829203.00 |
| peso_kg | float64 | 0 | 0.0% | 150 | 12.00 | 359763209664.00 | 10745405394.49 | 54816090.50 |
| num_operacoes | int64 | 0 | 0.0% | 133 | 1.00 | 1200817.00 | 21116.37 | 265.00 |
| rank_valor | float64 | 0 | 0.0% | 28 | 1.00 | 28.00 | 11.76 | 11.00 |

#### Exemplos de Valores (Colunas Categóricas)

**uf:** SP, MG, PR, MS, GO, ...

**commodity:** Açúcar, Café, Carne Bovina, Celulose, Madeira, Milho, Outros, Soja

---

### 12. eficiencia_atividade.parquet

**Tipo:** parquet
**Tamanho:** 0.24 MB
**Linhas:** 22,284
**Colunas:** 7
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,571 | -1.00 | 5300108.00 | 3253006.75 | 3146255.00 |
| ano | int64 | 0 | 0.0% | 4 | 2020.00 | 2023.00 | 2021.50 | 2021.50 |
| vab_agro_mil_reais | float64 | 0 | 0.0% | 10,613 | 0.00 | 5004239.00 | 46028.81 | 0.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 123 | 0.00 | 2488.87 | 1.25 | 0.00 |
| ppm_bovinos_cabecas | float64 | 0 | 0.0% | 12,669 | 0.00 | 2522608.00 | 31326.26 | 7666.50 |
| bovinos_por_ha | float64 | 22,161 | 99.45% | 99 | 0.00 | 15940333.33 | 156391.60 | 1280.36 |
| vab_por_ha | float64 | 22,161 | 99.45% | 44 | 0.00 | 1852173.33 | 31010.46 | 0.00 |
---

### 13. uf_exportacao_vs_desmatamento.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 150
**Colunas:** 10
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| uf | object | 0 | 0.0% | 28 | N/A | N/A | N/A | N/A |
| commodity | object | 0 | 0.0% | 8 | N/A | N/A | N/A | N/A |
| vob_fob_usd | float64 | 0 | 0.0% | 150 | 37.00 | 109449713660.00 | 4511612851.45 | 67829203.00 |
| peso_kg | float64 | 0 | 0.0% | 150 | 12.00 | 359763209664.00 | 10745405394.49 | 54816090.50 |
| num_operacoes | int64 | 0 | 0.0% | 133 | 1.00 | 1200817.00 | 21116.37 | 265.00 |
| rank_valor | float64 | 0 | 0.0% | 28 | 1.00 | 28.00 | 11.76 | 11.00 |
| num_embargos | float64 | 0 | 0.0% | 27 | 0.00 | 1884.00 | 305.65 | 165.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 25 | 0.00 | 17163.38 | 2532.27 | 123.02 |
| area_embargada_ha | float64 | 0 | 0.0% | 28 | 0.00 | 281039.04 | 33103.46 | 4327.23 |
| rank_desmatamento | float64 | 0 | 0.0% | 26 | 0.00 | 25.00 | 13.55 | 14.50 |

#### Exemplos de Valores (Colunas Categóricas)

**uf:** SP, MG, PR, MS, GO, ...

**commodity:** Açúcar, Café, Carne Bovina, Celulose, Madeira, Milho, Outros, Soja

---

### 14. ranking_top100_desmatamento.parquet

**Tipo:** parquet
**Tamanho:** 0.01 MB
**Linhas:** 100
**Colunas:** 4
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 100 | 1100106.00 | 5106802.00 | 2346771.50 | 1658754.50 |
| area_desmatada_ha | float64 | 0 | 0.0% | 91 | 0.00 | 5075.75 | 278.40 | 29.74 |
| rank_desmat | int64 | 0 | 0.0% | 100 | 1.00 | 100.00 | 50.50 | 50.50 |
| rank_vab | int64 | 0 | 0.0% | 100 | 31.00 | 5267.00 | 1601.01 | 1260.50 |
---

### 15. densidade_fiscalizacao_municipal.parquet

**Tipo:** parquet
**Tamanho:** 0.21 MB
**Linhas:** 5,571
**Colunas:** 8
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,571 | 1100015.00 | 5300108.00 | 3253922.53 | 3146305.00 |
| municipio | object | 0 | 0.0% | 5,298 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| regiao | object | 0 | 0.0% | 5 | N/A | N/A | N/A | N/A |
| chave_municipio | object | 0 | 0.0% | 5,571 | N/A | N/A | N/A | N/A |
| area_embargada_total_ha | float64 | 0 | 0.0% | 1,378 | 0.00 | 157371.04 | 281.21 | 0.00 |
| total_embargos | float64 | 0 | 0.0% | 87 | 0.00 | 759.00 | 2.74 | 0.00 |
| vab_agro_mil_reais | float64 | 0 | 0.0% | 5,512 | 0.00 | 3862034.50 | 92057.62 | 39094.50 |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, ...

**regiao:** Norte, Nordeste, Sudeste, Sul, Centro-Oeste

**chave_municipio:** ALTA FLORESTA D'OESTE - RO, ARIQUEMES - RO, CABIXI - RO, CACOAL - RO, CEREJEIRAS - RO, ...

---

### 16. correlacao_idhm_desmatamento.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 1
**Colunas:** 3
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| correlacao_spearman_desmat_idhm | float64 | 0 | 0.0% | 1 | 0.01 | 0.01 | 0.01 | 0.01 |
| correlacao_spearman_vab_idhm | float64 | 0 | 0.0% | 1 | -0.06 | -0.06 | -0.06 | -0.06 |
| interpretacao | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**interpretacao:** Correlação próxima de zero sugere que o desmatamento não impulsiona o desenvolvimento humano local.

---

### 17. lista_alerta_compliance.parquet

**Tipo:** parquet
**Tamanho:** 0.39 MB
**Linhas:** 9,522
**Colunas:** 13
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cpf_cnpj_e | object | 0 | 0.0% | 9,522 | N/A | N/A | N/A | N/A |
| num_embargos | int64 | 0 | 0.0% | 22 | 2.00 | 191.00 | 2.48 | 2.00 |
| anos_ativos | int64 | 0 | 0.0% | 8 | 1.00 | 8.00 | 1.68 | 2.00 |
| area_total_ha | float64 | 0 | 0.0% | 7,073 | 0.00 | 278455.25 | 299.71 | 35.56 |
| uf_principal | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| municipio_principal | object | 0 | 0.0% | 1,598 | N/A | N/A | N/A | N/A |
| recurrence_rate | float64 | 0 | 0.0% | 47 | 1.00 | 23.88 | 1.65 | 1.50 |
| score_volume | float64 | 0 | 0.0% | 22 | 8.36 | 40.00 | 9.20 | 8.36 |
| score_frequencia | float64 | 0 | 0.0% | 47 | 6.47 | 30.00 | 8.79 | 8.55 |
| area_capped | float64 | 0 | 0.0% | 7,057 | 0.00 | 10000.00 | 255.83 | 35.56 |
| score_severidade | float64 | 0 | 0.0% | 7,056 | 0.00 | 30.00 | 10.67 | 11.72 |
| compliance_risk_score | float64 | 0 | 0.0% | 2,620 | 14.83 | 100.00 | 28.67 | 28.89 |
| nivel_risco | object | 0 | 0.0% | 4 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**cpf_cnpj_e:** 05440892273, 02155400853, 00915017253, 04680054000104, 00375972001647, ...

**uf_principal:** PA, RO, AM, MT, AL, ...

**municipio_principal:** Trairão, Seringueiras, Boca do Acre, Humaitá, Confresa, ...

**nivel_risco:** Crítico (Bloqueio Imediato), Alto (Auditoria Requerida), Médio (Monitoramento), Baixo

---

### 18. eficiencia_agricola_pam.parquet

**Tipo:** parquet
**Tamanho:** 0.69 MB
**Linhas:** 27,505
**Colunas:** 7
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| municipio | object | 0 | 0.0% | 5,243 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 5 | 2020.00 | 2024.00 | 2022.00 | 2022.00 |
| area_colhida_ha | float64 | 0 | 0.0% | 12,537 | 0.00 | 1218591.00 | 15425.36 | 2303.00 |
| area_plantada_ha | float64 | 0 | 0.0% | 12,609 | 0.00 | 1225091.00 | 15533.93 | 2427.00 |
| valor_producao_mil_reais | float64 | 0 | 0.0% | 20,150 | 0.00 | 11478917.00 | 111112.29 | 13191.00 |
| valor_agri_por_ha_plantada | float64 | 64 | 0.23% | 26,909 | 0.00 | 428.71 | 8.24 | 6.35 |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Abadia de Goiás, Abadia dos Dourados, Abadiânia, Abaetetuba, Abaeté, ...

**uf:** GO, MG, PA, CE, BA, ...

---

### 19. correlacao_delta.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 2
**Colunas:** 4
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| metrica | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| correlacao | float64 | 0 | 0.0% | 2 | -0.02 | -0.01 | -0.02 | -0.02 |
| p_valor | float64 | 0 | 0.0% | 2 | 0.00 | 0.20 | 0.10 | 0.10 |
| n_observacoes | int64 | 0 | 0.0% | 1 | 16713.00 | 16713.00 | 16713.00 | 16713.00 |

#### Exemplos de Valores (Colunas Categóricas)

**metrica:** pearson, spearman

---

### 20. regressao_resultados.csv

**Tipo:** csv
**Tamanho:** 0.0 MB
**Linhas:** 1
**Colunas:** 6
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| modelo | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| r_squared | float64 | 0 | 0.0% | 1 | 0.06 | 0.06 | 0.06 | 0.06 |
| r_squared_adj | float64 | 0 | 0.0% | 1 | 0.06 | 0.06 | 0.06 | 0.06 |
| coef_desmatamento | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| p_valor_desmatamento | float64 | 1 | 100.0% | 0 | N/A | N/A | N/A | N/A |
| n_observacoes | float64 | 0 | 0.0% | 1 | 22161.00 | 22161.00 | 22161.00 | 22161.00 |

#### Exemplos de Valores (Colunas Categóricas)

**modelo:** simples

---

### 21. lista_alerta_top1000.csv

**Tipo:** csv
**Tamanho:** 0.14 MB
**Linhas:** 1,000
**Colunas:** 13
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cpf_cnpj_e | int64 | 0 | 0.0% | 1,000 | 10296140.00 | 83848309000163.00 | 2179580138354.07 | 37630886534.00 |
| num_embargos | int64 | 0 | 0.0% | 22 | 2.00 | 191.00 | 4.22 | 3.00 |
| anos_ativos | int64 | 0 | 0.0% | 8 | 1.00 | 8.00 | 2.18 | 2.00 |
| area_total_ha | float64 | 0 | 0.0% | 994 | 9.00 | 278455.25 | 2017.45 | 986.14 |
| uf_principal | object | 0 | 0.0% | 24 | N/A | N/A | N/A | N/A |
| municipio_principal | object | 0 | 0.0% | 270 | N/A | N/A | N/A | N/A |
| recurrence_rate | float64 | 0 | 0.0% | 47 | 1.00 | 23.88 | 2.19 | 2.00 |
| score_volume | float64 | 0 | 0.0% | 22 | 8.36 | 40.00 | 11.69 | 10.55 |
| score_frequencia | float64 | 0 | 0.0% | 47 | 6.47 | 30.00 | 10.29 | 10.26 |
| area_capped | float64 | 0 | 0.0% | 978 | 9.00 | 10000.00 | 1599.58 | 986.14 |
| score_severidade | float64 | 0 | 0.0% | 978 | 7.50 | 30.00 | 22.28 | 22.46 |
| compliance_risk_score | float64 | 0 | 0.0% | 634 | 39.55 | 100.00 | 44.27 | 42.89 |
| nivel_risco | object | 0 | 0.0% | 3 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**uf_principal:** PA, RO, AM, MT, AL, ...

**municipio_principal:** Trairão, Seringueiras, Boca do Acre, Humaitá, Confresa, ...

**nivel_risco:** Crítico (Bloqueio Imediato), Alto (Auditoria Requerida), Médio (Monitoramento)

---

### 22. resumo_status_embargos.json

**Tipo:** json
**Tamanho:** 0.0 MB
**Conteúdo:** {'distribuicao_geral': [{'situacao': 'D', 'contagem': 55736, 'descricao': 'Desmatamento / Degradação', 'pct': 62.91739100986612}, {'situacao': 'N', 'contagem': 32850, 'descricao': 'Não Desmatamento / Outros', 'pct': 37.08260899013388}], 'por_ano_2020_2023': {'D': {'2020': 1841, '2021': 2581, '2022': 3614, '2023': 5139}, 'N': {'2020': 410, '2021': 380, '2022': 858, '2023': 459}}}

---

### 23. resumo_sprint6.json

**Tipo:** json
**Tamanho:** 0.0 MB
**Conteúdo:** {'periodo_analise': '2021-2023', 'municipios_com_embargos_periodo': 1249, 'total_embargos_periodo': 13031, 'area_total_embargada_ha': 1373123.89539102, 'impacto_producao': {'municipios_analisados': 893, 'delta_bovinos_medio_pct': 3.7098784162521223, 'delta_vab_medio_pct': -100.0, 'conclusao_impacto': 'Estabilidade/Aumento'}, 'reincidencia': {'total_infratores_reincidentes': 9522, 'top_10_reincidentes_avg_embargos': 36.3, 'max_embargos_unico_cpf': 191}, 'status_desmatamento': {'pct_direto_desmata...

---

### 24. resumo_executivo_sprint5.json

**Tipo:** json
**Tamanho:** 0.0 MB
**Conteúdo:** {'total_exportado_usd': 676741927718.0, 'top_5_ufs_valor': {'SP': 142896724480.0, 'RJ': 92512326359.0, 'MG': 82286139084.0, 'MT': 59803954270.0, 'PR': 48627449535.0}, 'correlacao_export_desmat': -0.0011967512799375998, 'china_share_soja': 0.7450348026736631, 'uf_mais_eficiente': 'RJ', 'uf_menos_eficiente': 'AC'}

---

### 25. resumo_executivo.json

**Tipo:** json
**Tamanho:** 0.0 MB
**Conteúdo:** {'periodo_analise': '2020-2023', 'municipios_analisados': 5571, 'municipios_com_desmatamento': 123, 'ica_medio': 0.0011800756058564026, 'ica_mediana': 0.0005721272041596412, 'correlacao_pearson': -0.009876206712135106, 'p_valor_correlacao': 0.2017013019784289, 'interpretacao_correlacao': 'Fraca/nula - desmatamento NÃO está associado a crescimento do VAB', 'overlap_top100_pct': 7.000000000000001, 'municipios_alto_desmat_baixo_vab': 34, 'r_squared_regressao': 0.06156081340200592, 'coef_desmatament...

---

### 26. recorrencia_alertas.parquet

**Tipo:** parquet
**Tamanho:** 0.03 MB
**Linhas:** 809
**Colunas:** 12
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 809 | 1100015.00 | 5108956.00 | 2283785.81 | 1716505.00 |
| municipio | object | 0 | 0.0% | 804 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 9 | N/A | N/A | N/A | N/A |
| num_alertas_total | int64 | 0 | 0.0% | 32 | 13.00 | 45.00 | 27.31 | 27.00 |
| area_total_alertas_ha | float64 | 0 | 0.0% | 808 | 1061.03 | 5859.24 | 3032.10 | 3004.67 |
| ano_primeiro_alerta | int64 | 0 | 0.0% | 2 | 2018.00 | 2019.00 | 2018.02 | 2018.00 |
| ano_ultimo_alerta | int64 | 0 | 0.0% | 2 | 2022.00 | 2023.00 | 2022.99 | 2023.00 |
| anos_alerta | int64 | 0 | 0.0% | 2 | 5.00 | 6.00 | 5.98 | 6.00 |
| alertas_por_ano | float64 | 0 | 0.0% | 41 | 2.33 | 7.50 | 4.57 | 4.50 |
| recorrencia_alta | bool | 0 | 0.0% | 2 | 0.00 | 1.00 | 0.03 | 0.00 |
| recorrencia_media | bool | 0 | 0.0% | 2 | 0.00 | 1.00 | 0.45 | 0.00 |
| recorrencia_baixa | bool | 0 | 0.0% | 2 | 0.00 | 1.00 | 0.52 | 1.00 |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, AP, TO, MA, MT

---

### 27. timeline_degradacao.parquet

**Tipo:** parquet
**Tamanho:** 0.2 MB
**Linhas:** 9,643
**Colunas:** 11
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 3,842 | 1100015.00 | 5300108.00 | 2881906.77 | 2701209.00 |
| municipio | object | 0 | 0.0% | 3,705 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 6 | 2018.00 | 2023.00 | 2020.49 | 2020.00 |
| sequencia_eventos | object | 0 | 0.0% | 12 | N/A | N/A | N/A | N/A |
| num_eventos | int64 | 0 | 0.0% | 4 | 1.00 | 4.00 | 1.71 | 1.00 |
| fogo_ha | float64 | 3,199 | 33.17% | 5,970 | 20.01 | 1754.17 | 223.87 | 162.06 |
| deter_ha | float64 | 4,834 | 50.13% | 4,726 | 10.72 | 2372.95 | 510.08 | 470.57 |
| prodes_ha | float64 | 6,846 | 70.99% | 2,793 | 103.32 | 35208.84 | 5076.83 | 3576.75 |
| classe_uso | object | 7,247 | 75.15% | 6 | N/A | N/A | N/A | N/A |
| terra_class_ha | float64 | 7,247 | 75.15% | 2,377 | 10.49 | 1899.74 | 500.97 | 448.88 |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Umburatiba, Antônio Prado, Itaipava do Grajaú, Giruá, Campo Largo, ...

**uf:** MG, RS, MA, PR, SC, ...

**sequencia_eventos:** FOGO, DETER → PRODES → TERRACLASS, DETER → FOGO → PRODES, DETER → FOGO → PRODES → TERRACLASS, DETER → TERRACLASS, ...

**classe_uso:** Pastagem, Vegetação Secundária, Agricultura, Solo Exposto, Área Urbana, Mineração

---

### 28. latencia_alerta_corte.parquet

**Tipo:** parquet
**Tamanho:** 0.02 MB
**Linhas:** 804
**Colunas:** 8
#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 804 | 1100015.00 | 5108956.00 | 2274196.41 | 1716257.50 |
| municipio | object | 0 | 0.0% | 799 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 9 | N/A | N/A | N/A | N/A |
| ano_primeiro_alerta | int64 | 0 | 0.0% | 2 | 2018.00 | 2019.00 | 2018.02 | 2018.00 |
| ano_primeiro_corte | int64 | 0 | 0.0% | 6 | 2018.00 | 2023.00 | 2018.72 | 2018.00 |
| latencia_anos | int64 | 0 | 0.0% | 7 | -1.00 | 5.00 | 0.71 | 0.00 |
| latencia_dias | int64 | 0 | 0.0% | 7 | -365.00 | 1825.00 | 258.31 | 0.00 |
| latencia_categoria | object | 0 | 0.0% | 5 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, AP, TO, MA, MT

**latencia_categoria:** Mesmo ano, 1 ano, 2-3 anos, Corte antes do alerta, > 3 anos

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| cod_ibge | 0.430 | -0.690 | 0.413 | 2104800.00 | 3520703.50 | 1415903.50 |
| ano | -0.280 | -1.314 | 0.000 | 2022.00 | 2023.00 | 1.00 |
| num_embargos | 8.704 | 97.638 | 3.148 | 1.00 | 4.00 | 3.00 |
| area_desmatada_ha | 14.860 | 252.649 | 9.344 | 0.00 | 0.00 | 0.00 |
| area_embargada_ha | 11.326 | 157.506 | 5.777 | 0.00 | 84.34 | 84.34 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Correlações Fortes (|r| > 0.7)

| Variável 1 | Variável 2 | Correlação |
|-----------|-----------|------------|
| num_embargos | area_embargada_ha | 0.798 |

**Atenção:** Correlações fortes podem indicar multicolinearidade ou relações causais que merecem investigação.

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| cod_ibge | 2104800.00 | 3520703.50 | 1415903.50 | -19055.25 | 5644558.75 | 0 | 0.00% |
| ano | 2022.00 | 2023.00 | 1.00 | 2020.50 | 2024.50 | 0 | 0.00% |
| num_embargos | 1.00 | 4.00 | 3.00 | -3.50 | 8.50 | 265 | 14.51% |
| area_desmatada_ha | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 98 | 5.37% |
| area_embargada_ha | 0.00 | 84.34 | 84.34 | -126.51 | 210.85 | 318 | 17.42% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| cod_ibge | 1,249 | 0.6840 (Média-Alta) |
| ano | 3 | 0.0016 (Muito Baixa - categórica) |
| num_embargos | 85 | 0.0465 (Baixa) |
| area_desmatada_ha | 98 | 0.0537 (Baixa) |
| area_embargada_ha | 1,053 | 0.5767 (Média-Alta) |
| uf | 27 | 0.0148 (Baixa) |
| amazonia_legal | 2 | 0.0011 (Muito Baixa - categórica) |
| regiao | 5 | 0.0027 (Muito Baixa - categórica) |
| outlier_fiscalizacao | 2 | 0.0011 (Muito Baixa - categórica) |

