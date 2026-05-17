# Documentação: Camada Silver (data/02_silver)

**Descrição:** Dados limpos e validados com schema consistente.

---

## Resumo Geral

- **Total de arquivos:** 11
- **Total de linhas:** 678,242
- **Tamanho total:** 58.84 MB

---

## Detalhes por Arquivo

### 1. pib_vab_consolidado.parquet

**Tipo:** parquet
**Tamanho:** 0.38 MB
**Linhas:** 77,994
**Colunas:** 3

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,571 | -1.00 | 5300108.00 | 3253006.75 | 3146255.00 |
| ano | int64 | 0 | 0.0% | 14 | 2010.00 | 2023.00 | 2016.50 | 2016.50 |
| vab_agro_mil_reais | float64 | 11,169 | 14.32% | 45,853 | -2299.00 | 5004239.00 | 53206.74 | 23761.00 |
---

### 2. pam_consolidado.parquet

**Tipo:** parquet
**Tamanho:** 0.58 MB
**Linhas:** 27,505
**Colunas:** 14

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| chave_municipio | object | 0 | 0.0% | 5,510 | N/A | N/A | N/A | N/A |
| municipio | object | 0 | 0.0% | 5,243 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 5 | 2020.00 | 2024.00 | 2022.00 | 2022.00 |
| tipo_lavoura | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| produto | object | 0 | 0.0% | 1 | N/A | N/A | N/A | N/A |
| area_colhida_ha | float64 | 3 | 0.01% | 12,536 | 1.00 | 1218591.00 | 15427.04 | 2303.50 |
| area_colhida_pct | float64 | 3 | 0.01% | 1 | 100.00 | 100.00 | 100.00 | 100.00 |
| area_destinada_colheita_ha | float64 | 2,424 | 8.81% | 4,102 | 1.00 | 61830.00 | 1098.50 | 109.00 |
| area_destinada_colheita_pct | float64 | 2,424 | 8.81% | 1 | 100.00 | 100.00 | 100.00 | 100.00 |
| area_plantada_ha | float64 | 64 | 0.23% | 12,608 | 1.00 | 1225091.00 | 15570.16 | 2445.00 |
| area_plantada_pct | float64 | 64 | 0.23% | 1 | 100.00 | 100.00 | 100.00 | 100.00 |
| valor_producao_mil_reais | float64 | 3 | 0.01% | 20,149 | 1.00 | 11478917.00 | 111124.41 | 13199.50 |
| valor_producao_pct | float64 | 3 | 0.01% | 1 | 100.00 | 100.00 | 100.00 | 100.00 |

#### Exemplos de Valores (Colunas Categóricas)

**chave_municipio:** Abadia de Goiás_GO, Abadia dos Dourados_MG, Abadiânia_GO, Abaetetuba_PA, Abaeté_MG, ...

**municipio:** Abadia de Goiás, Abadia dos Dourados, Abadiânia, Abaetetuba, Abaeté, ...

**uf:** GO, MG, PA, CE, BA, ...

**tipo_lavoura:** Temporária

**produto:** Total

---

### 3. ppm_consolidado.parquet

**Tipo:** parquet
**Tamanho:** 0.24 MB
**Linhas:** 267,264
**Colunas:** 4

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,568 | -1.00 | 5300108.00 | 3252900.67 | 3146156.50 |
| ano | int64 | 0 | 0.0% | 5 | -1.00 | 2024.00 | 2022.14 | 2022.00 |
| categoria | object | 0 | 0.0% | 12 | N/A | N/A | N/A | N/A |
| efetivo_cabecas | int64 | 0 | 0.0% | 16,050 | 0.00 | 2522608.00 | 3503.11 | 0.00 |

#### Exemplos de Valores (Colunas Categóricas)

**categoria:** ppm_asininos, ppm_bovinos, ppm_bubalinos, ppm_caprinos, ppm_codornas, ...

---

### 4. serie_historica_2020_2023.parquet

**Tipo:** parquet
**Tamanho:** 0.26 MB
**Linhas:** 22,284
**Colunas:** 18

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,571 | -1.00 | 5300108.00 | 3253006.75 | 3146255.00 |
| ano | int64 | 0 | 0.0% | 4 | 2020.00 | 2023.00 | 2021.50 | 2021.50 |
| vab_agro_mil_reais | float64 | 0 | 0.0% | 10,613 | 0.00 | 5004239.00 | 46028.81 | 0.00 |
| ppm_asininos_cabecas | float64 | 0 | 0.0% | 1 | 0.00 | 0.00 | 0.00 | 0.00 |
| ppm_bovinos_cabecas | float64 | 0 | 0.0% | 12,669 | 0.00 | 2522608.00 | 31326.26 | 7666.50 |
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
| num_embargos | float64 | 0 | 0.0% | 91 | 0.00 | 352.00 | 0.69 | 0.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 123 | 0.00 | 2488.87 | 1.25 | 0.00 |
| area_embargada_ha | float64 | 0 | 0.0% | 1,291 | 0.00 | 80874.90 | 70.30 | 0.00 |
---

### 5. idhm_municipal_interpolado.parquet

**Tipo:** parquet
**Tamanho:** 1.71 MB
**Linhas:** 183,843
**Colunas:** 3

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| ano | int64 | 0 | 0.0% | 33 | 1991.00 | 2023.00 | 2007.00 | 2007.00 |
| cod_ibge | int64 | 0 | 0.0% | 5,571 | 1100015.00 | 5300108.00 | 3253922.53 | 3146305.00 |
| idhm | float64 | 0 | 0.0% | 183,843 | 0.30 | 0.86 | 0.64 | 0.65 |
---

### 6. comex_por_uf_ano.parquet

**Tipo:** parquet
**Tamanho:** 0.02 MB
**Linhas:** 689
**Colunas:** 7

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| uf | object | 0 | 0.0% | 29 | N/A | N/A | N/A | N/A |
| ano | int64 | 0 | 0.0% | 3 | 2023.00 | 2025.00 | 2024.00 | 2024.00 |
| tipo_operacao | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| commodity | object | 0 | 0.0% | 8 | N/A | N/A | N/A | N/A |
| vob_fob_usd | float64 | 0 | 0.0% | 689 | 1.00 | 86398826326.00 | 2625386457.82 | 19144759.00 |
| peso_kg | float64 | 0 | 0.0% | 680 | 0.00 | 194345284820.00 | 4372990017.12 | 5738937.00 |
| num_operacoes | int64 | 0 | 0.0% | 384 | 1.00 | 824560.00 | 16888.05 | 98.00 |

#### Exemplos de Valores (Colunas Categóricas)

**uf:** AC, AL, AM, AP, BA, ...

**tipo_operacao:** Exportação, Importação

**commodity:** Açúcar, Carne Bovina, Outros, Soja, Café, Celulose, Milho, Madeira

---

### 7. embargos_por_municipio_ano.parquet

**Tipo:** parquet
**Tamanho:** 0.2 MB
**Linhas:** 18,355
**Colunas:** 5

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_munici | int64 | 0 | 0.0% | 3,769 | 1100015.00 | 9999999.00 | 3010831.24 | 2918001.00 |
| ano | int64 | 0 | 0.0% | 39 | 1987.00 | 2026.00 | 2012.33 | 2011.00 |
| num_embargos | int64 | 0 | 0.0% | 161 | 1.00 | 583.00 | 4.83 | 2.00 |
| area_desmatada_ha | float64 | 0 | 0.0% | 5,068 | 0.00 | 1876487.13 | 748.77 | 0.00 |
| area_embargada_ha | float64 | 0 | 0.0% | 8,965 | 0.00 | 165935.25 | 373.53 | 1.19 |
---

### 8. dim_municipio.parquet

**Tipo:** parquet
**Tamanho:** 0.16 MB
**Linhas:** 5,571
**Colunas:** 5

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| cod_ibge | int64 | 0 | 0.0% | 5,571 | 1100015.00 | 5300108.00 | 3253922.53 | 3146305.00 |
| municipio | object | 0 | 0.0% | 5,298 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| regiao | object | 0 | 0.0% | 5 | N/A | N/A | N/A | N/A |
| chave_municipio | object | 0 | 0.0% | 5,571 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**municipio:** Alta Floresta D'Oeste, Ariquemes, Cabixi, Cacoal, Cerejeiras, ...

**uf:** RO, AC, AM, RR, PA, ...

**regiao:** Norte, Nordeste, Sudeste, Sul, Centro-Oeste

**chave_municipio:** ALTA FLORESTA D'OESTE - RO, ARIQUEMES - RO, CABIXI - RO, CACOAL - RO, CEREJEIRAS - RO, ...

---

### 9. ncm_commodity_reference.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 28
**Colunas:** 3

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| CO_NCM | object | 0 | 0.0% | 28 | N/A | N/A | N/A | N/A |
| commodity | object | 0 | 0.0% | 7 | N/A | N/A | N/A | N/A |
| descricao_ncm | object | 0 | 0.0% | 28 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**CO_NCM:** 12010000, 12011000, 12019000, 10051000, 10059000, ...

**commodity:** Soja, Milho, Carne Bovina, Café, Açúcar, Celulose, Madeira

**descricao_ncm:** Soja em grão, Soja para semeadura, Outras soja, Milho para semeadura, Outros milhos, ...

---

### 10. pais_reference.parquet

**Tipo:** parquet
**Tamanho:** 0.0 MB
**Linhas:** 33
**Colunas:** 2

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| CO_PAIS | int64 | 0 | 0.0% | 32 | 23.00 | 858.00 | 410.76 | 359.00 |
| nome_pais | object | 0 | 0.0% | 30 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**nome_pais:** China, Estados Unidos, Argentina, Holanda, Japão, ...

---

### 11. embargos_com_geometria.parquet

**Tipo:** parquet
**Tamanho:** 55.29 MB
**Linhas:** 74,676
**Colunas:** 10

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| num_tad | object | 0 | 0.0% | 73,906 | N/A | N/A | N/A | N/A |
| seq_tad | int64 | 0 | 0.0% | 73,626 | 0.00 | 1876749.00 | 1328816.50 | 1534905.50 |
| cod_munici | int64 | 0 | 0.0% | 3,200 | 1100015.00 | 9999999.00 | 2366737.56 | 1507300.00 |
| municipio | object | 0 | 0.0% | 3,088 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| ano_embargo | int64 | 0 | 0.0% | 33 | 1993.00 | 2026.00 | 2015.83 | 2016.00 |
| dat_embarg | object | 0 | 0.0% | 72,124 | N/A | N/A | N/A | N/A |
| sit_desmat | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| area_final_ha | float64 | 0 | 0.0% | 64,620 | 0.00 | 68744.00 | 90.64 | 10.19 |
| geometry | object | 0 | 0.0% | 71,719 | N/A | N/A | N/A | N/A |

#### Exemplos de Valores (Colunas Categóricas)

**num_tad:** 756611, 602347, 729878, 746952, 728729, ...

**municipio:** Lábrea, Santa Cruz Cabrália, Altamira, Manga, Manicoré, ...

**uf:** AM, BA, PA, MG, MT, ...

**dat_embarg:** 11/07/17 14:48:00, 11/07/17 16:49:00, 12/07/17 08:40:00, 12/07/17 10:58:00, 12/07/17 15:44:00, ...

**sit_desmat:** D, N

**geometry:** b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x18\x00\x00\x00DH\xdd\xce\xbe\xa5P\xc0@\x1a\x86\x8f\x88\xb9"\xc0\xe8\xb7\xaf\x03\xe7\xa5P\xc0`\xd9\xaf;\xdd\xb9"\xc0\xb8\xef\x18\x1e\xfb\xa5P\xc0\xe0s_\xcel\xb7"\xc0\xb8W\xadL\xf8\xa5P\xc0\xa0wF[\x95\xb4"\xc0\x98\r2\xc9\xc8\xa5P\xc0\x80b\x9c\xbf\t\xb5"\xc0\xf0\xcd67\xa6\xa5P\xc0@\xca\xf9b\xef\xb5"\xc0\x80 @\x86\x8e\xa5P\xc0`\xf8k\xb2F\xb5"\xc0,>\x05\xc0x\xa5P\xc0 \xe6u\xc4!\xb3"\xc0h\xefSUh\xa5P\xc0\x80\xb6\xf1\'*\xb3"\xc0\xe0g#\xd7M\xa5P\xc0\x80\x8c*\xc3\xb8\xb3"\xc0PB\xb0\xaa^\xa5P\xc0`\x92\xe6\x8fi\xb5"\xc0LN\xb4\xab\x90\xa5P\xc0`\xd2\xfb\xc6\xd7\xb6"\xc0(n\xdcb~\xa5P\xc0\xa05[y\xc9\xb7"\xc0\xc4\xb6\x0c8K\xa5P\xc0\xe0\x86\xc2g\xeb\xb8"\xc0\x94\xbb\x96\x90\x0f\xa5P\xc0\xe0X\x16L\xfc\xb9"\xc0\\\xffun\xda\xa4P\xc0\xa0\xd1\xab\x01J\xbb"\xc0\xf8v\x12\x11\xfe\xa4P\xc0@\xd8\xd3\x0e\x7f\xbd"\xc0\xe8\x1b\x98\xdc(\xa5P\xc0`\xc3(\x08\x1e\xbf"\xc0\xec\xb2_w\xba\xa5P\xc0`\xc3(\x08\x1e\xbf"\xc0\x98\x1b\xd3\x13\x96\xa5P\xc0\xc0\x92\x005\xb5\xbc"\xc0H\xb1\xa3q\xa8\xa5P\xc0`\x8cd\x8fP\xbb"\xc0\x88\xb3"j\xa2\xa5P\xc0@\x83i\x18>\xba"\xc0\xb8\xedBs\x9d\xa5P\xc0`\x03w\xa0N\xb9"\xc0DH\xdd\xce\xbe\xa5P\xc0@\x1a\x86\x8f\x88\xb9"\xc0', b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x10\x00\x00\x000\xf0j\xb93\x97C\xc0\xd0\xfa\xb0\xde\xa8-0\xc0\xd8\xc3^(`\x97C\xc0\x00/O\xe7\x8a.0\xc0\xa8?\xc20`\x97C\xc0\xf0\xd2\xdb\x9f\x8b.0\xc08\xa2Bus\x97C\xc0\xb0\x8d\x94-\x92.0\xc0\x98\xb8\xe3M~\x97C\xc0\xe0mlv\xa4.0\xc0\x00\xa5/\x84\x9c\x97C\xc0\xc0\xbaF\xcb\x81.0\xc0pa\xa4\x17\xb5\x97C\xc0P\xa9.\xe0e.0\xc0p\xb9\xc1P\x87\x97C\xc0\x80\xe9{\r\xc1-0\xc0\xa8\x9a\xe7\x88|\x97C\xc0 \xd4($\x99-0\xc0\x10\xdfP\xf8l\x97C\xc0 t\x97\xc4Y-0\xc0`P4\x0f`\x97C\xc0@\xbe\x12H\x89-0\xc0\xe82\xc3FY\x97C\xc0\x901\x05k\x9c-0\xc0\xd8BW"P\x97C\xc0\x80\xd4{*\xa7-0\xc0\x80c]\xdcF\x97C\xc0\xa0B\xcaO\xaa-0\xc0X\xb3\\6:\x97C\xc0\xd0\xb7\xe8d\xa9-0\xc00\xf0j\xb93\x97C\xc0\xd0\xfa\xb0\xde\xa8-0\xc0', b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x11\x00\x00\x00\xa0\xd6\xfb\x8dvnK\xc0\xc0_\x91_?\x04\x1a\xc0\x102\x90g\x97oK\xc0\x00\xe3\x89 \xce\xe3\x19\xc00\xb8\xad-<oK\xc0@Qf\x83L\xe2\x19\xc0\xb0d\x8e\xe5]oK\xc0\x80\xa2w*\xe0\xde\x19\xc0\x00v5y\xcanK\xc0@a\x88\x9c\xbe\xde\x19\xc0\x80\x00\x19:vnK\xc0@\xa5\x82\x8a\xaa\xdf\x19\xc0\xf8]\x9f9\xebmK\xc0\x80\xb2`\xe2\x8f\xe2\x19\xc0\xf8\xc1\xc0s\xefmK\xc0\x00\x18?\x8d{\xe3\x19\xc0\xd0>\x1d\x8f\x19nK\xc0@\x99D\xbd\xe0\xe3\x19\xc0hA\xef\x8d!nK\xc0\x80\xe69"\xdf\xe5\x19\xc0\x90\xa7\xac\xa6\xebmK\xc0\xc0\xa3\xfd\x0f\xb0\xe6\x19\xc0\x88zO\xe5\xb4mK\xc0\x00Ks+\x84\xe5\x19\xc0\xf0g\x06\xf1\x81mK\xc0\xc0\x823\xf8\xfb\xe5\x19\xc0x\xbb%9`mK\xc0\xc0\x1e.9\xee\xe4\x19\xc0\xa0>\xc9\x1d6mK\xc0\x00\xa03iS\xe5\x19\xc0\xd0\xd9\x90\x7ffnK\xc0\x00fh<\x11\x04\x1a\xc0\xa0\xd6\xfb\x8dvnK\xc0\xc0_\x91_?\x04\x1a\xc0', b'\x01\x03\x00\x00\x00\x01\x00\x00\x00!\x00\x00\x00h\xc3\xef\xa6[\x08F\xc0@\x89\xb2\xb7\x94\xbb-\xc0X8\x10\x92\x05\x08F\xc0\xa0\xbb\x95%:\xbb-\xc0\xe8\xb1\xf4\xa1\x0b\x08F\xc0\x80\x19\x8b\xa6\xb3\xbb-\xc08\xa4\xdf\xbe\x0e\x08F\xc0\xa0!T\xa9\xd9\xbb-\xc0\xb8]/M\x11\x08F\xc0\x00\xc6O\xe3\xde\xbc-\xc0\x00~\x8d$A\x08F\xc0@\xae\xd3HK\xbd-\xc00\x02*\x1cA\x08F\xc0@\xce\xde\x19m\xbd-\xc0\xd8,%\xcbI\x08F\xc0\x80\xbdl;m\xbd-\xc0\x08\xb1\xc1\xc2I\x08F\xc0`\xddw\x0c\x8f\xbd-\xc0\xb8\xdb\xbcqR\x08F\xc0\xa0\xcc\x05.\x8f\xbd-\xc0\x98\x8aT\x18[\x08F\xc0\x00\xbc\x93O\x8f\xbd-\xc0\xc8\x0e\xf1\x0f[\x08F\xc0\xe0\xdb\x9e \xb1\xbd-\xc0p9\xec\xbec\x08F\xc0 \xcb,B\xb1\xbd-\xc0\xa0\xbd\x88\xb6c\x08F\xc0\xc0\xfb\xa9\xf1\xd2\xbd-\xc0P\xe8\x83el\x08F\xc0\x00\xeb7\x13\xd3\xbd-\xc003\xfa\xd1p\x08F\xc0`\xda\xc54\xd3\xbd-\xc0x:\xe5\xd1\x8d\x08F\xc0\xa0s\x9a\x05\xda\xbd-\xc0@\x12\x84+\xa0\x08F\xc0 \x95Ea\x17\xbd-\xc0\xb0\x1f)"\xc3\x08F\xc0\x00\n\x11p\x08\xbd-\xc0\xe8\x1d5&\xc4\x08F\xc0\x80\x0b\xea[\xe6\xbc-\xc0\xf0L\xf6\xcf\xd3\x08F\xc0@\tkc\xec\xbc-\xc00K\x02\xd4\xd4\x08F\xc0\xa0\xff\xc8t\xe8\xbc-\xc0\xc8\x1d\xfd/\xd7\x08F\xc0\xa0\xff\xc8t\xe8\xbc-\xc0\xd8\xd5\xab\xc8\xe8\x08F\xc0\xa0\xcf\xf1\xd1\xe2\xbc-\xc0 \xd0\'\xf2$\tF\xc0\xe08\xef\xff\xe3\xbc-\xc00g}\xca1\tF\xc0\x00\x06\xf3W\xc8\xbc-\xc0\xe0a\xa1\xd64\tF\xc0\x80\x9fT\xfbt\xbc-\xc0P\x03\x94\x86\x1a\tF\xc0\x00\n\xd8\x0eF\xbc-\xc0\xf8,\x96"\xf9\x08F\xc0 !t\xd0%\xbc-\xc0\xc8\x81\x1ej\xdb\x08F\xc0\xc0\x9c\xbc\xc8\x04\xbc-\xc0Ps\x80`\x8e\x08F\xc0\x00G\x90J\xb1\xbb-\xc0\xf8\x9c\x82\xfcl\x08F\xc0\xa0\x87\xd9\xcb\xb6\xbb-\xc0h\xc3\xef\xa6[\x08F\xc0@\x89\xb2\xb7\x94\xbb-\xc0', b'\x01\x03\x00\x00\x00\x01\x00\x00\x00 \x00\x00\x00\x00_\xd1\xad\xd7\xb4J\xc0\x00\xde\x01\x9e\xb4P\x18\xc0`\x82\x1a\xbe\x85\xb5J\xc0@9A\x9b\x1c^\x18\xc0\xa8\xa5\x80\xb4\xff\xb5J\xc0\xc0\\\x89@\xf5_\x18\xc0\x809A\x9b\x1c\xb6J\xc0@Hj\xa1db\x18\xc0\xc8C\xa6|\x08\xb6J\xc0\xc0\x94F\xcc\xecc\x18\xc00,F]k\xb5J\xc0\x80\x80\xb4\xff\x01f\x18\xc0\x18[\x96\xaf\xcb\xb4J\xc0@HM\xbb\x98f\x18\xc0x\x19\x8c\x11\x89\xb4J\xc0\x00\xfa|\x94\x11g\x18\xc0\xe8\x8f0\x0cX\xb4J\xc0\x00\xf8\xfb\xc5li\x18\xc0\x08C\xab\x933\xb4J\xc0\x80\xa9+\x9f\xe5i\x18\xc0\x10?\x8d{\xf3\xb3J\xc0\xc0\xfbS\xaaDi\x18\xc0XI\xf2\\\xdf\xb3J\xc0\x00\xf8\xfb\xc5li\x18\xc0Xw\xba\xf3\xc4\xb3J\xc0\x80*\x17*\xffj\x18\xc0\xd8\x8c\x9a\xaf\x92\xb3J\xc0\xc0F<\xd9\xcdl\x18\xc0\x08\xca\xfb8\x9a\xb3J\xc0\x80\xab\xe5\xceLp\x18\xc0\x80\xfcl\xe4\xba\xb3J\xc0\xc0\xc3\xb2\x99Cr\x18\xc08\xc4?l\xe9\xb3J\xc0@\xf9\xf3m\xc1r\x18\xc0\xf0\xb9\xda\x8a\xfd\xb3J\xc0\xc0\xc4\xe4\r0s\x18\xc0P4\x9d\x9d\x0c\xb4J\xc0\xc0r\xbc\x02\xd1s\x18\xc0\xe8\xd2Mb\x10\xb4J\xc0\xc0\xb7u7Ou\x18\xc0@\x95a\xdc\r\xb4J\xc0\x00\x0cuX\xe1v\x18\xc0\xa8\xf6\xb0\x17\n\xb4J\xc0@oa\xddxw\x18\xc0\xa0i\x17\xd3L\xb5J\xc0@tx\x08\xe3w\x18\xc0\x00\x94\x86\x1a\x85\xb6J\xc0@\xf7\xab\x00\xdf}\x18\xc0x\x0f\x97\x1cw\xb6J\xc0\xc0F<\xd9\xcdl\x18\xc0\xe8\xfa\xccY\x9f\xb6J\xc0@\xbb\x97\xfb\xe4h\x18\xc0(b\xd8aL\xb6J\xc0@\x0f{\xa1\x80]\x18\xc0p\xccy\xc6\xbe\xb6J\xc0\xc0\xb1\xf4\xa1\x0bZ\x18\xc0\x98\xf4\x85\x90\xf3\xb6J\xc0\x80\xfc\x16\x9d,U\x18\xc0\x18\xd8\xf1_ \xb6J\xc0\xc0\xcaJ\x93RP\x18\xc0X\xcd\x01\x829\xb6J\xc0\x80\x92\x005\xb5L\x18\xc0\x00_\xd1\xad\xd7\xb4J\xc0\x00\xde\x01\x9e\xb4P\x18\xc0', ...

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| cod_ibge | 0.116 | -0.514 | 0.303 | 2512077.00 | 4119202.00 | 1607125.00 |
| ano | 0.000 | -1.212 | 0.002 | 2013.00 | 2020.00 | 7.00 |
| vab_agro_mil_reais | 11.753 | 273.651 | 2.135 | 9510.00 | 55289.00 | 45779.00 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| cod_ibge | 2512077.00 | 4119202.00 | 1607125.00 | 101389.50 | 6529889.50 | 14 | 0.02% |
| ano | 2013.00 | 2020.00 | 7.00 | 2002.50 | 2030.50 | 0 | 0.00% |
| vab_agro_mil_reais | 9510.00 | 55289.00 | 45779.00 | -59158.50 | 123957.50 | 6109 | 9.14% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| cod_ibge | 5,571 | 0.0714 (Baixa) |
| ano | 14 | 0.0002 (Muito Baixa - categórica) |
| vab_agro_mil_reais | 45,853 | 0.5879 (Média-Alta) |

