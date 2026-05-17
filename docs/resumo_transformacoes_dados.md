# 📊 Resumo das Transformações, Limpeza e Relacionamentos de Dados

## Visão Geral do Pipeline

```
┌───────────────────────────────────────────────────────────────────────┐
│                         PIPELINE DE DADOS                              │
│                                                                        │
│  BRONZE (Raw) → SILVER (Tratado) → GOLD (Análises) → REPORTS         │
│     9 fontes      11 arquivos       19 parquets       8 parquets      │
│   ~12M registros   ~420k registros   + CSV/JSON/PNG   + resumos       │
│                                                                        │
│  FASE 2 (ETL - 32 scripts)  →  FASE 3 (Análise - 4 EPICs)            │
└───────────────────────────────────────────────────────────────────────┘
```

**Lógica do pipeline:**
- **Bronze → Silver:** dados brutos de 9 fontes passam por limpeza, padronização e estruturação
- **Silver → Gold:** dados padronizados são integrados e transformados em análises temáticas
- **Gold → Reports:** resultados analíticos condensados em relatórios e resumos executivos
- Cada camada tem responsabilidade clara — quanto mais à direita, mais prontos para responder às perguntas do projeto

---

## 1. 📁 CAMADA BRONZE - DADOS RAW

**Papel da camada Bronze:**
- **Função:** ponto de entrada — dados em estado bruto, sem nenhuma transformação aplicada
- **Origem:** órgãos governamentais e sistemas oficiais (INPE, IBAMA, IBGE, MDIC)
- **Princípio:** preservar rastreabilidade e permitir reprocessamento futuro
- **Valor:** cada fonte cobre uma dimensão diferente do nexo desmatamento-agropecuária; a combinação das 9 viabiliza as análises multidimensionais das camadas seguintes

### Fontes e Volumes

| Fonte | Arquivo | Registros | Descrição |
|-------|---------|-----------|-----------|
| **PRODES** | `prodes_desmatamento_anual.parquet` | 2.793 | Desmatamento anual oficial (INPE) por município |
| **DETER** | `deter_alertas_diarios.parquet` | 22.072 | Alertas diários de desmatamento em tempo quase real |
| **IBAMA** | `embargos_ibama_tabular.parquet` | 88.586 | Termos de embargo ambiental (autuações) |
| **TerraClass** | `terra_class_uso.parquet` | 11.016 | Uso do solo após desmatamento |
| **MapBiomas Fogo** | `mapbiomas_fogo_ocorrencias.parquet` | 6.441 | Ocorrências de incêndios/queimadas |
| **PAM** | `pam/D1C=Município/*.parquet` | 888.340 | Produção agrícola municipal (10 chunks) |
| **PPM** | `ppm/*/ano=YYYY/*.parquet` | 267.264 | Pecuária municipal (12 categorias × 4 anos) |
| **PIB** | `pib/pib_vab_agro/ano=YYYY/dados.parquet` | 77.994 | VAB da agropecuária (2010-2023) |
| **COMEX** | `comex/comex_stat/{EXP,IMP}_YYYY.parquet` | 11.635.864 | Exportações e importações por NCM/UF (2023-2025) |

### Schema dos Principais Arquivos Bronze

**PRODES:**
```python
cod_ibge, municipio, uf, ano, area_desmatada_km2, area_desmatada_ha, bioma, fase, fonte
```

**IBAMA Embargos:**
```python
objectid, seq_tad, num_tad, serie_tad, operacao, cod_uf, uf, cod_munici, municipio,
nome_imove, cpf_cnpj_e, sit_desmat, dat_embarg, num_longit, num_latitu,
qtd_area_d (área desmatada), qtd_area_e (área embargada)
```

**PAM (raw):**
```python
NC, NN, MC, MN, V, D1N (municipio_uf), D2C, D2N (variável), D3C (ano), D4N (produto), tipo_lavoura
```

**PPM (raw):**
```python
NC, NN, MC, MN, V (efetivo), D1C (cod_ibge), D1N, D2C, D2N, D3C (ano), D4N (categoria)
```

---

## 2. 🧹 TRANSFORMAÇÕES E LIMPEZA (CAMADA SILVER)

**Papel da camada Silver:**
- **Função:** limpeza, padronização e estruturação dos dados brutos de cada fonte
- **Desafio:** formatos de origem heterogêneos — chunks particionados, campos codificados, datas inconsistentes, nulos como strings
- **Solução:** script ETL dedicado por fonte, cada um tratando as particularidades da origem
- **Resultado:** arquivos padronizados com chaves comuns (`cod_ibge`, `uf`, `ano`), prontos para integração

### Scripts - Sprint 0: Ingestão

| Script | Descrição |
|--------|-----------|
| `sprint_0_ingestao/download-dados-em-parquet-v2.py` | Download e conversão de todas as fontes para parquet |

---

### Scripts - Sprint 1: Silver

| Script | Descrição |
|--------|-----------|
| `sprint_1_silver/pam/01_etl_pam_consolidado.py` | Consolidação PAM → pam_consolidado.parquet |
| `sprint_1_silver/ppm/01_etl_ppm_consolidado.py` | Consolidação PPM → ppm_consolidado.parquet |
| `sprint_1_silver/pib/01_etl_pib_vab_consolidado.py` | Consolidação PIB → pib_vab_consolidado.parquet |
| `sprint_1_silver/ibama/01_etl_embargos_municipio_ano.py` | Agregação IBAMA → embargos_por_municipio_ano.parquet |
| `sprint_1_silver/comex/01_etl_comex_por_uf_ano.py` | Agregação COMEX → comex_por_uf_ano.parquet + ncm_commodity_reference.parquet |
| `sprint_1_silver/dimensao/01_etl_dim_municipio.py` | Dimensão município via API IBGE → dim_municipio.parquet |
| `sprint_1_silver/02_etl_serie_historica_comum.py` | Integração final → serie_historica_2020_2023.parquet |

---

### 2.1 PAM - Produção Agrícola Municipal

**Contexto e desafios do PAM:**
- **Formato de origem:** 10 chunks particionados por município, campos codificados (D1N, D2N, D3C), valores como strings
- **Transformação central:** pivotamento long → wide — variáveis viram colunas nomeadas por produto agrícola
- **Chave criada:** `chave_municipio` (texto "Município - UF") — o PAM não possui `cod_ibge` nativo
- **Resultado:** facilita o join com demais fontes e a análise por produto agrícola

**Script:** `fase_2_execucao/sprint_1_silver/pam/01_etl_pam_consolidado.py`

**Transformações:**
1. **Limpeza de cabeçalhos:** Remover linhas com valores codificados ("Ano (Código)", "Nível")
2. **Extração de campos:**
   - `ano`: via regex de `D3C`
   - `municipio_uf`: split de `D1N` (formato "Município - UF")
   - `variavel`: extraído de `D2N`
3. **Conversão de tipos:** `valor` string → float
4. **Pivotamento:** Long → Wide (variáveis → colunas)
5. **Criação de chave:** `chave_municipio = municipio + "_" + uf`

**Schema Silver:**
```python
chave_municipio: string      # "Alta Floresta_D'Oeste - RO"
municipio: string
uf: string
ano: int64
tipo_lavoura: string         # Temporária/Permanente
produto: string              # Soja, Milho, Algodão, etc.
area_colhida_ha: float64
area_plantada_ha: float64
valor_producao_mil_reais: float64
area_colhida_pct: float64
area_destinada_colheita_ha: float64
valor_producao_pct: float64
```

**Volume:** 888.340 → 27.505 registros (consolidação)

---

### 2.2 PPM - Pecuária Municipal

**Contexto e desafios do PPM:**
- **Formato de origem:** 12 categorias de animais em arquivos separados por ano (estrutura particionada)
- **Similaridade com PAM:** mesma lógica de campos codificados, porém com `cod_ibge` disponível
- **Transformação central:** consolidação de todos os arquivos em um único, preservando hierarquia `cod_ibge + ano + categoria`
- **Uso na série histórica:** as 12 categorias viram 12 colunas de efetivo animal por município

**Script:** `fase_2_execucao/sprint_1_silver/ppm/01_etl_ppm_consolidado.py`

**Transformações:**
1. **Extração de cod_ibge:** via regex `(\d{7})` de `D1C`
2. **Conversão de valores:** `".."` → null para `efetivo`
3. **Consolidação:** Todas 12 categorias em único arquivo
4. **Preservação de hierarquia:** `cod_ibge + ano + categoria`

**Categorias:**
```
asininos, bovinos, bubalinos, caprinos, codornas, equinos,
galinaceos_total, galinhas, muar, ovinos, suinos_matrizes, suinos_total
```

**Schema Silver:**
```python
cod_ibge: int64
ano: int64
categoria: string            # Bovino, Suíno, etc.
efetivo_cabecas: int64
```

**Volume:** 267.264 registros (mantido)

---

### 2.3 PIB VAB Agropecuária

**Papel do PIB no projeto:**
- **Função:** âncora econômica — fornece o VAB da agropecuária como principal variável de retorno
- **Cobertura:** todos os 5.571 municípios, com `cod_ibge` consistente em todos os anos
- **Papel estrutural:** usado como base (spine) do join na série histórica — os demais datasets são mesclados a partir dele
- **Período:** 2010–2023, permitindo análise de longa duração

**Script:** `fase_2_execucao/sprint_1_silver/pib/01_etl_pib_vab_consolidado.py`

**Transformações:**
1. **Consolidação de anos:** 2010-2023 em único arquivo
2. **Filtro:** Apenas VAB da agropecuária
3. **Manutenção de cod_ibge:** Chave primária

**Schema Silver:**
```python
cod_ibge: int64
ano: int64
vab_agro_mil_reais: float64
```

**Volume:** 77.994 registros

---

### 2.4 IBAMA - Embargos por Município/Ano

**Contexto e desafios do IBAMA:**
- **Granularidade de origem:** nível de auto de infração individual — 88.586 registros com datas inconsistentes e coordenadas geográficas
- **Transformação central:** agregação por município e ano → reduz a 18.355 registros
- **Métricas criadas:** `num_embargos`, `area_desmatada_ha` e `area_embargada_ha` acumuladas por município/ano
- **Uso nas análises Gold:** mede a efetividade da fiscalização ambiental (Sprint 6)

**Script:** `fase_2_execucao/sprint_1_silver/ibama/01_etl_embargos_municipio_ano.py`

**Transformações:**
1. **Conversão de data:** `dat_embarg` string → datetime (`%d/%m/%y %H:%M:%S`)
2. **Extração de ano:** da data do embargo
3. **Agregação:** groupby(`cod_munici`, `ano`)
   - `num_embargos`: count
   - `area_desmatada_ha`: sum(`qtd_area_d`)
   - `area_embargada_ha`: sum(`qtd_area_e`)

**Schema Silver:**
```python
cod_munici: int64            # Código IBGE do município
ano: int64
num_embargos: int64
area_desmatada_ha: float64
area_embargada_ha: float64
```

**Volume:** 88.586 → 18.355 registros (agregação)

---

### 2.5 COMEX - Exportações/Importações por UF

**Contexto e desafios do COMEX:**
- **Volume bruto:** ~11,6 milhões de registros — maior fonte do projeto em registros
- **Granularidade de origem:** produto (código NCM de 8 dígitos) × UF × ano
- **Passo 1:** mapeamento NCM → 8 commodities estratégicas (soja, carnes, milho, algodão, etc.)
- **Passo 2:** agregação para nível UF + ano → reduz a 689 registros
- **Limitação estrutural:** COMEX não tem dado de município — relaciona-se via `uf`, não `cod_ibge`

**Script:** `fase_2_execucao/sprint_1_silver/comex/01_etl_comex_por_uf_ano.py`

**Transformações:**
1. **Mapeamento NCM → Commodity:** via tabela de referência (`ncm_commodity_reference.parquet`)
2. **Tabela de países:** gerada como `pais_reference.parquet`
3. **Agregação:** por UF + ano + tipo + commodity
   - `vob_fob_usd`: sum
   - `peso_kg`: sum
   - `num_operacoes`: count

**Commodities:**
```
Soja, Carne Bovina, Carne Suína, Carne de Frango,
Milho, Algodão, Café, Arroz
```

**Schema Silver (`comex_por_uf_ano`):**
```python
uf: string
ano: int64
tipo_operacao: string        # EXP / IMP
commodity: string
vob_fob_usd: float64
peso_kg: float64
num_operacoes: int64
```

**Volume:** 11.635.864 → 689 registros (agregação extrema)

---

### 2.6 IDHM - Interpolação

**Contexto e solução para o IDHM:**
- **Problema:** IDHM disponível apenas nos anos censitários (1991, 2000, 2010) — sem dados anuais
- **Solução:** interpolação linear entre censos + extrapolação para 2011–2023
- **Resultado:** série contínua de 1991 a 2023 para todos os municípios (183.810 registros)
- **Uso analítico:** essencial para o "paradoxo do desmatamento" — municípios que mais desmatam melhoram em qualidade de vida?

**Script:** `fase_2_execucao/sprint_7_idhm/etl_7_1_idhm_interpolacao.py`

**Transformações:**
1. **Interpolação linear:** entre anos censitários (1991, 2000, 2010)
2. **Extrapolação:** para anos 2011-2023
3. **Preenchimento:** todos municípios para todos anos

**Schema Silver:**
```python
ano: int64
cod_ibge: int64
idhm: float64
```

**Volume:** 183.810 registros

---

### 2.7 Dimensão Município

**Papel da Dimensão Município:**
- **Função:** tabela de referência central — equivalente a uma tabela de dimensão em modelagem dimensional
- **Origem:** API oficial do IBGE (5.570 municípios)
- **Atributos registrados:** `cod_ibge`, `uf`, `região`, `amazonia_legal` (atributos fixos do município)
- **Papel de elo:** `cod_ibge` canônico conecta fontes que identificam o município de formas diferentes (código, nome+UF, sigla do estado)

**Script:** `fase_2_execucao/sprint_1_silver/dimensao/01_etl_dim_municipio.py`

**Fonte:** API IBGE

**Schema Silver:**
```python
cod_ibge: int64
uf: string
amazonia_legal: bool
regiao: string
municipio: string
```

**Volume:** 5.570 municípios

---

### 2.8 Geometrias Espaciais (IBAMA)

**O que esta etapa adiciona:**
- **Enriquecimento:** dados tabulares de embargos recebem geometrias geoespaciais dos municípios
- **Análise habilitada:** efeito spillover — o impacto do embargo transborda para municípios vizinhos?
- **Integração com outros sprints:**
  - Complementa a análise de fiscalização do **Sprint 6** (efetividade dos embargos)
  - Complementa a análise de dinâmica territorial do **Sprint 3**

**Scripts:**
- `fase_2_execucao/sprint_3_inteligencia_espacial/etl_3_1_geometrias_ibama.py`
- `fase_2_execucao/sprint_3_inteligencia_espacial/analise_3_3_buffer_spillover.py`

**Arquivo gerado:** `data/02_silver/espacial/embargos_com_geometria.parquet`

**Transformações:**
1. **Enriquecimento espacial:** join dos embargos com geometrias dos municípios
2. **Análise de buffer/spillover:** impacto espacial dos embargos em áreas adjacentes

---

## 3. 🔗 INTEGRAÇÃO - SÉRIE HISTÓRICA COMUM (2020-2023)

**Por que e como a integração foi feita:**
- **Objetivo:** unificar todas as fontes Silver em uma única tabela analítica — `serie_historica_2020_2023`
- **Escolha do período 2020–2023:** único intervalo em que PAM, PPM, PIB, IBAMA e IDHM têm dados completos simultaneamente
- **Produto cartesiano:** 5.571 municípios × 4 anos = 22.284 linhas — todos os municípios em todos os anos, mesmo sem eventos registrados
- **Por que preencher com zero:** evita viés de seleção em regressões e comparações temporais — ausência de dado ≠ ausência de fenômeno

**Script:** `fase_2_execucao/sprint_1_silver/02_etl_serie_historica_comum.py`

### 3.1 Período Comum
- **Anos:** 2020, 2021, 2022, 2023
- **Municípios base:** 5.571 (do PIB)

### 3.2 Pipeline de Integração

```
┌──────────────────────────────────────────────────────────────────┐
│                   ETL SÉRIE HISTÓRICA                             │
│                                                                   │
│  1. FILTRAR PERÍODO COMUM (2020-2023)                            │
│     - pam_consolidado, pib_vab, ppm, embargos                    │
│                                                                   │
│  2. AGREGAR PAM                                                   │
│     - groupby(chave_municipio, ano)                              │
│     - somar: area_plantada_ha, area_colhida_ha, valor_producao   │
│                                                                   │
│  3. PIVOTAR PPM                                                   │
│     - pivot_table(index=[cod_ibge, ano], columns='categoria')    │
│     - 12 categorias → 12 colunas                                 │
│                                                                   │
│  4. CRIAR BASE COMUM                                              │
│     - MultiIndex: municípios × anos (produto cartesiano)         │
│     - Base: PIB (tem cod_ibge)                                   │
│                                                                   │
│  5. JOINS                                                         │
│     - PIB: merge on [cod_ibge, ano]                              │
│     - PPM: merge on [cod_ibge, ano]                              │
│     - IBAMA: merge on [cod_ibge, ano] (renomear cod_munici)      │
│     - PAM: separado (usa chave_municipio, não tem cod_ibge)      │
│     - COMEX: separado (nível UF)                                 │
│                                                                   │
│  6. PREENCHER NULOS                                               │
│     - colunas numéricas → 0                                      │
│                                                                   │
│  7. EXPORTAR                                                      │
│     - serie_historica_2020_2023.parquet (22.284 registros)       │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### 3.3 Schema da Série Histórica

```python
cod_ibge: int64
ano: int64
vab_agro_mil_reais: float64
ppm_asininos_cabecas: float64
ppm_bovinos_cabecas: float64
ppm_bubalinos_cabecas: float64
ppm_caprinos_cabecas: float64
ppm_codornas_cabecas: float64
ppm_equinos_cabecas: float64
ppm_galinaceos_total_cabecas: float64
ppm_galinhas_cabecas: float64
ppm_muar_cabecas: float64
ppm_ovinos_cabecas: float64
ppm_suinos_matrizes_cabecas: float64
ppm_suinos_total_cabecas: float64
num_embargos: float64
area_desmatada_ha: float64
area_embargada_ha: float64
```

**Volume:** 5.571 municípios × 4 anos = 22.284 registros

---

## 4. 🔑 CHAVES DE RELACIONAMENTO

**Por que as chaves importam:**
- **Não há chave única universal:** cada fonte usa uma forma diferente de identificar o município
  - **PIB, PPM, IDHM:** `cod_ibge` — chave padrão, viabiliza joins diretos
  - **IBAMA:** `cod_munici` — equivale ao `cod_ibge`, mas exige renomeação no join
  - **PAM:** `chave_municipio` (texto "Município - UF") — sem `cod_ibge`, join separado via dim_municipio
  - **COMEX:** `uf` — granularidade de estado, não de município; join possível apenas no nível UF
- **Impacto na série histórica:** as diferenças de granularidade determinaram como cada fonte foi incorporada e quais análises cruzadas são possíveis

### Tabela de Chaves

| Tabela | Chave Primária | Chave Estrangeira | Relaciona Com |
|--------|----------------|-------------------|---------------|
| `dim_municipio` | `cod_ibge` | - | Todas tabelas com cod_ibge |
| `pam_consolidado` | `chave_municipio` | - | Isolada (sem cod_ibge) |
| `ppm_consolidado` | `cod_ibge + ano + categoria` | `cod_ibge` → dim_municipio | dim_municipio, serie_historica |
| `pib_vab_consolidado` | `cod_ibge + ano` | `cod_ibge` → dim_municipio | dim_municipio, serie_historica |
| `serie_historica_2020_2023` | `cod_ibge + ano` | `cod_ibge` → dim_municipio | Todas (base integrada) |
| `embargos_por_municipio_ano` | `cod_munici + ano` | `cod_munici` → dim_municipio.cod_ibge | dim_municipio, serie_historica |
| `comex_por_uf_ano` | `uf + ano + tipo + commodity` | `uf` → dim_municipio.uf | dim_municipio (nível UF) |
| `idhm_municipal_interpolado` | `cod_ibge + ano` | `cod_ibge` → dim_municipio | dim_municipio, serie_historica |
| `ncm_commodity_reference` | `ncm` | - | comex_por_uf_ano |
| `pais_reference` | `co_pais` | - | comex_por_uf_ano |
| `embargos_com_geometria` | `objectid` | `cod_munici` → dim_municipio | Análises espaciais |

### Diagrama de Relacionamentos

```
                    ┌─────────────────┐
                    │ dim_municipio   │
                    │ cod_ibge (PK)   │
                    │ uf              │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ ppm_consolidado │ │ pib_consolidado │ │ embargos_por_   │
│ cod_ibge (FK)   │ │ cod_ibge (FK)   │ │ municipio_ano   │
│ ano             │ │ ano             │ │ cod_munici (FK) │
│ categoria       │ │                 │ │ ano             │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             ▼
                  ┌─────────────────────────┐
                  │ serie_historica_2020_   │
                  │ 2023                    │
                  │ cod_ibge (FK)           │
                  │ ano                     │
                  │ (todas métricas)        │
                  └───────────┬─────────────┘
                              │
                ┌─────────────┼──────────────┐
                │             │              │
                ▼             ▼              ▼
       ┌─────────────┐ ┌─────────────┐ ┌──────────────┐
       │ idhm        │ │ tipologia   │ │ eficiencia   │
       │ cod_ibge    │ │ quadrantes  │ │ atividade    │
       └─────────────┘ └─────────────┘ └──────────────┘
```

---

## 5. 📈 CAMADA GOLD - ANÁLISES PRONTAS

**Papel da camada Gold:**
- **Função:** transformar a série histórica integrada em respostas analíticas prontas para consumo
- **Cada arquivo Gold responde a uma pergunta específica:**
  - *"O desmatamento gera retorno econômico?"* → `eficiencia_atividade`, `ica_ranking`
  - *"Os embargos são efetivos?"* → `impacto_embargo_producao`, `status_regularizacao_embargos`
  - *"Quais municípios têm maior risco de compliance?"* → `lista_alerta_compliance`
- **Artefatos gerados:** 19 Parquets + CSVs + JSONs + visualizações PNG
- **Consumidores:** notebooks analíticos dos 4 EPICs e o dashboard final

### Inventário Completo de Arquivos Gold

| Arquivo | Tamanho | Sprint de Origem |
|---------|---------|-----------------|
| `tipologia_municipal_quadrantes.parquet` | 140K | Sprint 7 |
| `eficiencia_atividade.parquet` | 242K | Sprint 2 |
| `ica_ranking.parquet` | 158K | Sprint 2 |
| `correlacao_idhm_desmatamento.parquet` | 3,2K | Sprint 7 |
| `correlacao_delta.parquet` | 2,9K | Sprint 2 |
| `impacto_embargo_producao.parquet` | 32K | Sprint 6 |
| `reincidentes_embargos.parquet` | 280K | Sprint 6 |
| `status_regularizacao_embargos.parquet` | 3,3K | Sprint 6 |
| `densidade_fiscalizacao_municipal.parquet` | 93K | Sprint 6 |
| `fiscalizacao_series_temporais.parquet` | 29K | Sprint 6 |
| `lista_alerta_compliance.parquet` | 395K | Sprint 6 |
| `eficiencia_agricola_pam.parquet` | 710K | Sprint 5 |
| `eficiencia_ambiental_exportacao.parquet` | 5,3K | Sprint 5 |
| `matriz_destino_exportacao.parquet` | 8,7K | Sprint 5 |
| `ranking_uf_exportadora.parquet` | 7,5K | Sprint 5 |
| `uf_exportacao_vs_desmatamento.parquet` | 11K | Sprint 5 |
| `ranking_concentracao.parquet` | 145K | Sprint 2 |
| `ranking_top100_desmatamento.parquet` | 5,4K | Sprint 2 |
| `ranking_top100_vab.parquet` | 5,2K | Sprint 2 |

**Artefatos não-parquet:**
| Arquivo | Tipo | Descrição |
|---------|------|-----------|
| `lista_alerta_top1000.csv` | CSV | Top 1000 municípios por risco de compliance |
| `regressao_resultados.csv` | CSV | Resultados de regressão desmatamento × VAB |
| `resumo_executivo.json` | JSON | Resumo executivo geral do projeto |
| `resumo_executivo_sprint5.json` | JSON | Resumo executivo da cadeia global |
| `resumo_sprint6.json` | JSON | Resumo executivo de fiscalização |
| `resumo_status_embargos.json` | JSON | Status de regularização dos embargos |

**Visualizações (`data/03_gold/visualizacoes/`):**
```
concentracao_territorial.png       delta_bovinos_histogram.png
distribuicao_ica.png               eficacia_embargo_pizza.png
eficiencia_pecuaria_agricultura.png impacto_producao_boxplot.png
quadrantes_desmatamento_idhm.png   resumo_visual.png
scatter_delta_vab_desmat.png       scatter_desmatamento_idhm.png
serie_temporal_embargos.png        top20_ica_municipios.png
top20_reincidentes.png
```

---

### 5.1 Tipologia Municipal (Sprint 7)

**O que esta análise produz e por quê:**
- **Objetivo:** classificar cada município no cruzamento IDHM × desmatamento (4 quadrantes)
- **Quadrante central do projeto:** "Alto Desmatamento / Baixo IDHM (Paradoxo)" — desmata sem retorno social
- **Método:** comparação com medianas nacionais de desmatamento e IDHM no ano de 2023
- **Alimenta:** EPIC 3 — Dinâmica Espacial e Paradoxo Social

**Script:** `fase_2_execucao/sprint_7_idhm/analise_7_4_tipologia_quadrantes.py`

**Transformações:**
1. **Join:** serie_historica + idhm + dim_municipio
2. **Filtro:** ano = 2023 (último ano disponível)
3. **Cálculo de medianas:**
   - `med_desmat` = mediana(area_desmatada_ha)
   - `med_idhm` = mediana(idhm)
4. **Classificação em 4 quadrantes:**
   - "Alto Desmatamento / Alto IDHM"
   - "Alto Desmatamento / Baixo IDHM (Paradoxo)"
   - "Baixo Desmatamento / Alto IDHM"
   - "Baixo Desmatamento / Baixo IDHM"

**Schema:**
```python
cod_ibge, ano, vab_agro_mil_reais, ppm_*_cabecas,
num_embargos, area_desmatada_ha, area_embargada_ha,
idhm, municipio, uf, quadrante
```

**Volume:** 5.570 registros

---

### 5.2 Correlação IDHM vs Desmatamento (Sprint 7)

**O que esta análise produz e por quê:**
- **Objetivo:** quantificar numericamente a força da relação entre desmatamento e IDHM
- **Complementa:** a tipologia de quadrantes (5.1) com um coeficiente objetivo
- **Por que Spearman e não Pearson:** desmatamento tem distribuição altamente assimétrica com outliers extremos — Spearman é mais robusto nesse cenário
- **Uso dos resultados:** coeficientes citados diretamente nos insights do EPIC 3

**Script:** `fase_2_execucao/sprint_7_idhm/analise_7_3_correlacao_idhm.py`

**Transformações:**
1. **Join:** serie_historica + idhm
2. **Cálculo de correlação:** Spearman (robusto a outliers)
   - `corr_desmat` = correlação(area_desmatada_ha, idhm)
   - `corr_vab` = correlação(vab_agro_mil_reais, idhm)

**Schema:**
```python
correlacao_spearman_desmat_idhm: float64
correlacao_spearman_vab_idhm: float64
interpretacao: string
```

---

### 5.3 Eficiência de Atividade e ICA (Sprint 2)

**O que este sprint produz e por quê:**
- **Pergunta central:** quanto de retorno econômico cada hectare desmatado gera?
- **Indicador criado — ICA (Índice de Custo Ambiental):** `área desmatada / VAB agropecuário`
  - ICA alto = mais área destruída por unidade de riqueza gerada → menor eficiência ambiental
- **Rankings produzidos:** top 100 municípios por desmatamento e por VAB — revelam baixo overlap entre os dois grupos
- **Papel no projeto:** primeiro MVP analítico; os rankings são referência comparativa em todos os EPICs seguintes

**Scripts:**
- `fase_2_execucao/sprint_2_gold_mvp/sprint2_mvp_economico.py`
- `fase_2_execucao/sprint_2_gold_mvp/sprint2_visualizacoes.py`

**Transformações:**
1. **Eficiência pecuária:**
   - `bovinos_por_ha` = ppm_bovinos_cabecas / area_desmatada_ha
   - `vab_por_ha` = vab_agro_mil_reais / area_desmatada_ha
2. **ICA (Índice de Custo Ambiental):**
   - `ica` = area_desmatada_ha / vab_agro_mil_reais
3. **Rankings:** top 100 municípios por desmatamento e por VAB
4. **Correlação delta:** diff() ano a ano (Pearson e Spearman)
5. **Concentração territorial:** ranking_concentracao

**Arquivos gerados:**
- `eficiencia_atividade.parquet`
- `ica_ranking.parquet`
- `correlacao_delta.parquet`
- `ranking_top100_desmatamento.parquet`
- `ranking_top100_vab.parquet`
- `ranking_concentracao.parquet`

---

### 5.4 Fiscalização e Compliance (Sprint 6)

**Pergunta central e eixos de análise:**
- **Questão prática:** o embargo ambiental muda o comportamento do produtor ou é apenas uma sanção sem efeito real?
- **Cinco eixos analisados:**
  1. **Evolução temporal:** série histórica de embargos por município ao longo dos anos
  2. **Impacto na produção:** comparação antes vs. depois do embargo (VAB e efetivo bovino)
  3. **Reincidência:** infratores com mais de 1 embargo — taxa de recorrência por CPF/CNPJ
  4. **Status de regularização:** proporção de embargos ainda ativos vs. regularizados
  5. **Compliance risk score:** score composto por município para priorização da fiscalização

**Scripts:**
- `sprint_6_fiscalizacao/etl_6_1_fiscalizacao_series.py` → `fiscalizacao_series_temporais.parquet`
- `sprint_6_fiscalizacao/etl_6_2_impacto_producao.py` → `impacto_embargo_producao.parquet`
- `sprint_6_fiscalizacao/etl_6_3_reincidentes.py` → `reincidentes_embargos.parquet`
- `sprint_6_fiscalizacao/etl_6_4_status_regularizacao.py` → `status_regularizacao_embargos.parquet`
- `sprint_6_fiscalizacao/etl_6_5_compliance_risk_score.py` → `lista_alerta_compliance.parquet`
- `sprint_6_fiscalizacao/sprint6_analise.py` → `densidade_fiscalizacao_municipal.parquet`
- `sprint_6_fiscalizacao/sprint6_visualizacoes.py` → visualizações PNG

**Impacto de Embargo na Produção:**
1. **Identificar primeiro embargo:** por município (2021 ou 2022)
2. **Comparar Antes vs Depois:** ano_embargo ± 1
3. **Calcular deltas:**
   - `delta_vab_pct` = ((vab_depois - vab_antes) / vab_antes) × 100
   - `delta_bovinos_pct` = idem para bovinos
4. **Classificar:** `sucesso_embargo` e `aumento_pos_embargo`

**Reincidentes:**
1. **Agrupamento por CPF/CNPJ**
2. **Taxa de recorrência:** num_embargos / anos_ativos
3. **Filtro:** num_embargos > 1

**Compliance Risk Score:**
1. **Score composto:** por município, baseado em embargos, reincidência, área embargada
2. **Lista de alerta:** top 1000 municípios por risco

---

### 5.5 Cadeia Global de Exportação (Sprint 5)

**Pergunta central e o que foi mapeado:**
- **Questão:** UFs que mais exportam commodities são as que mais desmatam?
- **Cruzamento realizado:** COMEX (nível UF) × desmatamento (PRODES/IBAMA)
- **Rota mapeada:** da floresta ao mercado global — qual commodity está por trás do desmatamento?
- **Indicador inédito:** eficiência ambiental exportada = `USD exportado / hectare desmatado`
  - Revela quais estados "pagam" ambientalmente mais caro por cada dólar exportado

**Scripts:**
- `sprint_5_cadeia_global/01_etl_mapeamento_ncm.py`
- `sprint_5_cadeia_global/02_etl_paises_referencia.py`
- `sprint_5_cadeia_global/03_analise_ranking_uf.py` → `ranking_uf_exportadora.parquet`
- `sprint_5_cadeia_global/04_analise_overlap_uf.py` → `uf_exportacao_vs_desmatamento.parquet`
- `sprint_5_cadeia_global/05_analise_matriz_paises.py` → `matriz_destino_exportacao.parquet`
- `sprint_5_cadeia_global/06_analise_eficiencia_ambiental.py` → `eficiencia_ambiental_exportacao.parquet`

**Transformações:**
1. **Mapeamento NCM → Commodity** (8 commodities principais)
2. **Tabela de referência de países** exportadores/importadores
3. **Ranking de UFs** por volume exportado
4. **Overlap UF:** correlação desmatamento × exportação por UF
5. **Matriz de destinos:** volume exportado por país × commodity
6. **Eficiência ambiental:** USD exportado por hectare desmatado

---

### 5.6 Rota Temporal (Sprint 4)

**O que o Sprint 4 acrescenta:**
- **Dimensão introduzida:** temporal + geoespacial combinadas — evolução espacial do desmatamento ao longo dos anos
- **Produto gerado:** timeline de degradação por área geográfica
- **Perguntas habilitadas:** áreas embargadas anteriormente reincidiam? O desmatamento migra para municípios vizinhos?
- **Complementa:** análise de spillover do Sprint 3 com perspectiva temporal

**Scripts:**
- `sprint_4_rota_temporal/etl_4.1_ingestao_dados_espaciais.py`
- `sprint_4_rota_temporal/etl_4.2_timeline_degradacao.py`
- `sprint_4_rota_temporal/sprint4_validacao_dados.py`

**Foco:** Timeline de degradação ambiental e evolução espacial dos embargos ao longo do tempo.

---

## 6. 📊 CAMADA REPORTS (04_reports)

**Papel da camada Reports:**
- **Função:** auditoria e controle de qualidade do pipeline — não é uma camada analítica
- **Origem:** gerados como subproduto das transformações Silver
- **Conteúdo:** distribuições, totais, contagens de nulos e estatísticas descritivas por fonte
- **Analogia:** são os "recibos" do ETL — confirmam que os dados chegaram corretamente às camadas seguintes

Resumos estatísticos intermediários para auditoria e validação do pipeline.

| Arquivo | Descrição |
|---------|-----------|
| `resumo_detalhado_bronze.parquet` | Estatísticas gerais da camada bronze |
| `resumo_detalhado_pam.parquet` | Detalhamento estatístico do PAM |
| `resumo_detalhado_ibama.parquet` | Detalhamento estatístico do IBAMA |
| `resumo_consolidado_pib.parquet` | Consolidação de métricas do PIB |
| `resumo_consolidado_ppm.parquet` | Consolidação de métricas do PPM |
| `resumo_estatistico_producao_pam.parquet` | Estatísticas de produção agrícola |
| `resumo_stats_por_variavel_pam.parquet` | Stats por variável do PAM |
| `resumo_top_culturas_pam.parquet` | Ranking das principais culturas |

---

## 7. 🔬 FASE 3 - EXECUÇÃO ANALÍTICA (EPICs)

**Localização:** `fase_3_execucao_analitica/`

**O que diferencia a Fase 3 da Fase 2:**
- **Fase 2 (ETL):** preparou, limpou e estruturou os dados — produz arquivos prontos
- **Fase 3 (Analítica):** consome os arquivos Gold e produz respostas às perguntas do projeto
- **4 EPICs cobrem dimensões complementares:**
  - EPIC 1 → eficiência econômica do desmatamento
  - EPIC 2 → cadeia global de exportação e efetividade da fiscalização
  - EPIC 3 → dinâmica espacial e paradoxo social (IDHM × desmatamento)
  - EPIC 4 → integração narrativa e storytelling para apresentação

Esta fase transforma os dados Gold em narrativas analíticas respondendo às perguntas centrais do projeto sobre o nexo desmatamento-agropecuária.

### Estrutura

```
fase_3_execucao_analitica/
├── notebooks/
│   ├── 01_epic_eficiencia_economica.ipynb    (+ .py)
│   ├── 02_epic_cadeia_global_fiscalizacao.ipynb  (+ .py)
│   ├── 03_epic_dinamica_espacial_paradoxo_social.ipynb  (+ .py)
│   └── 04_epic_produtizacao_storytelling.ipynb  (+ .py)
└── outputs/txt/
    ├── conclusoes_epic_1.txt
    ├── conclusoes_epic_2.txt
    ├── conclusoes_epic_3.txt
    └── conclusoes_finais.txt
```

### EPICs

| EPIC | Notebook | Foco Analítico |
|------|----------|----------------|
| **1 - Eficiência Econômica** | `01_epic_eficiencia_economica` | ICA, VAB por ha, correlação desmatamento × crescimento |
| **2 - Cadeia Global e Fiscalização** | `02_epic_cadeia_global_fiscalizacao` | Exportações, compliance, eficácia dos embargos |
| **3 - Dinâmica Espacial e Paradoxo Social** | `03_epic_dinamica_espacial_paradoxo_social` | Quadrantes IDHM × desmatamento, análise espacial |
| **4 - Produtização e Storytelling** | `04_epic_produtizacao_storytelling` | Dashboard narrativo, conclusões integradas |

---

## 8. 🛠️ INVENTÁRIO COMPLETO DE SCRIPTS (32 scripts)

**Ordem de execução e dependências:**
- **Sprint 0:** ingestão — pré-requisito de todos os demais
- **Sprint 1:** Silver — depende do Sprint 0; pré-requisito dos Sprints 2–7
- **Sprints 2–7:** Gold — cada um pode rodar independentemente após o Sprint 1
- **Fase 3 (EPICs):** depende de todos os scripts Gold estarem concluídos
- A sequência numerada garante que nenhuma análise seja executada antes dos dados necessários estarem disponíveis

### Sprint 0 - Ingestão
- `download-dados-em-parquet-v2.py`

### Sprint 1 - Silver (7 scripts)
- `pam/01_etl_pam_consolidado.py`
- `ppm/01_etl_ppm_consolidado.py`
- `pib/01_etl_pib_vab_consolidado.py`
- `ibama/01_etl_embargos_municipio_ano.py`
- `comex/01_etl_comex_por_uf_ano.py`
- `dimensao/01_etl_dim_municipio.py`
- `02_etl_serie_historica_comum.py`

### Sprint 2 - Gold MVP (2 scripts)
- `sprint2_mvp_economico.py`
- `sprint2_visualizacoes.py`

### Sprint 3 - Inteligência Espacial (2 scripts)
- `etl_3_1_geometrias_ibama.py`
- `analise_3_3_buffer_spillover.py`

### Sprint 4 - Rota Temporal (3 scripts)
- `etl_4.1_ingestao_dados_espaciais.py`
- `etl_4.2_timeline_degradacao.py`
- `sprint4_validacao_dados.py`

### Sprint 5 - Cadeia Global (6 scripts)
- `01_etl_mapeamento_ncm.py`
- `02_etl_paises_referencia.py`
- `03_analise_ranking_uf.py`
- `04_analise_overlap_uf.py`
- `05_analise_matriz_paises.py`
- `06_analise_eficiencia_ambiental.py`

### Sprint 6 - Fiscalização (7 scripts)
- `etl_6_1_fiscalizacao_series.py`
- `etl_6_2_impacto_producao.py`
- `etl_6_3_reincidentes.py`
- `etl_6_4_status_regularizacao.py`
- `etl_6_5_compliance_risk_score.py`
- `sprint6_analise.py`
- `sprint6_visualizacoes.py`

### Sprint 7 - IDHM (3 scripts)
- `etl_7_1_idhm_interpolacao.py`
- `analise_7_3_correlacao_idhm.py`
- `analise_7_4_tipologia_quadrantes.py`

### Sprint 8 - Produtização (1 script)
- `app_dashboard.py`

---

## 9. 📊 RESUMO ESTATÍSTICO DOS DADOS

**Leitura dos números:**
- **Bronze:** ~12 milhões de registros brutos — ponto de partida
- **Silver (agregação):** de 12M para ~420 mil registros — limpeza e consolidação por fonte
- **Série histórica:** 22.284 registros — produto cartesiano 5.571 municípios × 4 anos; tabela analítica central do projeto
- O efeito de compressão de cada camada é intencional: reduz volume sem perder informação analítica relevante

### 9.1 Períodos e Abrangência

| Base | Período | Municípios/UFs | Total Registros |
|------|---------|----------------|-----------------|
| **Série Histórica** | 2020-2023 | 5.571 municípios | 22.284 |
| **PAM** | 2020-2024 | 5.510 municípios | 27.505 |
| **PPM** | 2021-2024 | 5.568 municípios | 267.264 |
| **PIB VAB** | 2010-2023 | 5.571 municípios | 77.994 |
| **Embargos** | 1987-2026 | 3.769 municípios | 18.355 |
| **COMEX** | 2023-2025 | 29 UFs | 689 |
| **IDHM** | 1991-2023 | 5.570 municípios | 183.810 |
| **Tipologia Quadrantes** | 2023 | 5.570 municípios | 5.570 |
| **Reincidentes** | 1987-2026 | 9.522 infratores | 9.522 |

---

### 9.2 Métricas Principais

**Embargos (Silver):**
- Total de embargos: **88.586**
- Área desmatada total: **13.743.735 ha**
- Área embargada total: **6.856.144 ha**

**COMEX (Silver):**
- Total FOB USD: **US$ 1.808.891.269.441**
- Commodities: **8** (Soja, Carne, etc.)
- UFs: **29** (inclui EX, ND)

**IDHM (Silver):**
- IDHM médio: **0,6370**
- IDHM mínimo: **0,3028**
- IDHM máximo: **0,8629**

**Série Histórica 2020-2023:**
- Municípios com desmatamento > 0: **123** (~2% dos municípios)

---

### 9.3 Distribuição por Quadrantes (Tipologia 2023)

| Quadrante | Municípios | % Total |
|-----------|------------|---------|
| Alto Desmatamento / Alto IDHM | ~1.400 | ~25% |
| Alto Desmatamento / Baixo IDHM (Paradoxo) | ~1.400 | ~25% |
| Baixo Desmatamento / Alto IDHM | ~1.400 | ~25% |
| Baixo Desmatamento / Baixo IDHM | ~1.400 | ~25% |

---

## 10. 🎯 PRINCIPAIS INSIGHTS DOS DADOS

**Origem e natureza dos insights:**
- Emergem da combinação das análises Gold com os EPICs da Fase 3
- Não são hipóteses — são conclusões sustentadas pelos dados
- Base: 9 fontes integradas, 4 anos de série histórica, 5.571 municípios brasileiros

1. **Baixa correlação entre desmatamento e VAB:** Correlação de Pearson ~0,01 indica que desmatar não gera crescimento econômico local imediato.

2. **Paradoxo do Desmatamento:** ~25% dos municípios estão no quadrante "Alto Desmatamento / Baixo IDHM", sugerindo degradação ambiental sem retorno social.

3. **Concentração territorial:** Top 100 municípios por desmatamento têm baixo overlap com Top 100 por VAB.

4. **Reincidentes de embargo:** 9.522 infratores com mais de 1 embargo, indicando necessidade de fiscalização mais efetiva.

5. **Evolução temporal:** Pico de desmatamento em 2022 (>10.800 ha), com leve redução em 2023 (~8.300 ha), mas ainda 2× maior que 2020.

6. **Cadeia global:** UFs com maior exportação de commodities concentram também os maiores índices de desmatamento, evidenciando nexo direto entre pressão de mercado e desmatamento.

7. **Eficácia dos embargos:** Análise antes/depois mostra resultado variável — parte dos municípios reduziu efetivo bovino após embargo, mas outra parte apresentou aumento, indicando baixa efetividade.

---

## 11. 💻 CÓDIGO PARA INCLUIR NO NOTEBOOK UNIFICADO

**Padrão de uso do código:**
- **Carregamento centralizado:** todos os arquivos carregados no início do notebook — facilita manutenção dos paths
- **Evita recarregamentos:** uma única célula de ingestão para toda a sessão de análise
- **Convenção de nomenclatura:** `df_<nome>` — identifica imediatamente a origem de cada variável (ex: `df_embargos`, `df_serie`, `df_quadrantes`)

### 11.1 Carregamento de Dados

```python
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

# Configurar paths
BASE_DIR = Path('/caminho/para/projeto')
SILVER_DIR = BASE_DIR / 'data/02_silver'
GOLD_DIR = BASE_DIR / 'data/03_gold'

def load_parquet(path):
    """Carregar arquivo parquet"""
    return pd.read_parquet(path)

# Dados Silver principais
df_serie = load_parquet(SILVER_DIR / 'serie_historica_2020_2023.parquet')
df_pam = load_parquet(SILVER_DIR / 'pam_consolidado.parquet')
df_ppm = load_parquet(SILVER_DIR / 'ppm_consolidado.parquet')
df_pib = load_parquet(SILVER_DIR / 'pib_vab_consolidado.parquet')
df_embargos = load_parquet(SILVER_DIR / 'embargos_por_municipio_ano.parquet')
df_comex = load_parquet(SILVER_DIR / 'comex_por_uf_ano.parquet')
df_idhm = load_parquet(SILVER_DIR / 'idhm_municipal_interpolado.parquet')
df_dim = load_parquet(SILVER_DIR / 'dim_municipio.parquet')
df_ncm = load_parquet(SILVER_DIR / 'ncm_commodity_reference.parquet')
df_paises = load_parquet(SILVER_DIR / 'pais_reference.parquet')

# Dados Gold - Econômico / Ambiental
df_quadrantes = load_parquet(GOLD_DIR / 'tipologia_municipal_quadrantes.parquet')
df_eficiencia = load_parquet(GOLD_DIR / 'eficiencia_atividade.parquet')
df_ica = load_parquet(GOLD_DIR / 'ica_ranking.parquet')
df_correlacao = load_parquet(GOLD_DIR / 'correlacao_idhm_desmatamento.parquet')
df_correlacao_delta = load_parquet(GOLD_DIR / 'correlacao_delta.parquet')
df_ranking_desmat = load_parquet(GOLD_DIR / 'ranking_top100_desmatamento.parquet')
df_ranking_vab = load_parquet(GOLD_DIR / 'ranking_top100_vab.parquet')
df_concentracao = load_parquet(GOLD_DIR / 'ranking_concentracao.parquet')

# Dados Gold - Fiscalização
df_impacto = load_parquet(GOLD_DIR / 'impacto_embargo_producao.parquet')
df_reincidentes = load_parquet(GOLD_DIR / 'reincidentes_embargos.parquet')
df_status_reg = load_parquet(GOLD_DIR / 'status_regularizacao_embargos.parquet')
df_densidade = load_parquet(GOLD_DIR / 'densidade_fiscalizacao_municipal.parquet')
df_series_fisc = load_parquet(GOLD_DIR / 'fiscalizacao_series_temporais.parquet')
df_compliance = load_parquet(GOLD_DIR / 'lista_alerta_compliance.parquet')

# Dados Gold - Cadeia Global
df_ranking_uf = load_parquet(GOLD_DIR / 'ranking_uf_exportadora.parquet')
df_overlap_uf = load_parquet(GOLD_DIR / 'uf_exportacao_vs_desmatamento.parquet')
df_matriz_paises = load_parquet(GOLD_DIR / 'matriz_destino_exportacao.parquet')
df_efi_ambiental = load_parquet(GOLD_DIR / 'eficiencia_ambiental_exportacao.parquet')
df_efi_agricola = load_parquet(GOLD_DIR / 'eficiencia_agricola_pam.parquet')
```

### 11.2 Resumo dos Dados Extraídos

```python
def gerar_resumo_dados():
    """Gerar resumo estatístico dos dados"""

    resumo = {
        'periodo_analise': '2020-2023',
        'municipios_analisados': df_serie['cod_ibge'].nunique(),
        'municipios_com_desmatamento': (df_serie['area_desmatada_ha'] > 0).sum(),
        'total_embargos': df_embargos['num_embargos'].sum(),
        'area_desmatada_total_ha': df_embargos['area_desmatada_ha'].sum(),
        'area_embargada_total_ha': df_embargos['area_embargada_ha'].sum(),
        'idhm_medio': df_idhm['idhm'].mean(),
        'vab_agro_total_mil_reais': df_serie['vab_agro_mil_reais'].sum(),
        'reincidentes_embargos': len(df_reincidentes),
        'municipios_paradoxo': len(df_quadrantes[
            df_quadrantes['quadrante'] == 'Alto Desmatamento / Baixo IDHM (Paradoxo)'
        ])
    }

    return pd.DataFrame([resumo])

# Exibir resumo
resumo_df = gerar_resumo_dados().T
resumo_df.columns = ['valor']
print(resumo_df)
```

### 11.3 Transformações para Análise

```python
# Unir Série Histórica com IDHM e dimensão
df_analise = df_serie.merge(df_idhm, on=['cod_ibge', 'ano'], how='inner')
df_analise = df_analise.merge(
    df_dim[['cod_ibge', 'municipio', 'uf', 'amazonia_legal']],
    on='cod_ibge', how='left'
)

# Filtrar Amazônia Legal (se necessário)
df_amazonia = df_analise[df_analise['amazonia_legal'] == True]

# Calcular métricas derivadas
df_analise['ica'] = df_analise['area_desmatada_ha'] / df_analise['vab_agro_mil_reais'].replace(0, pd.NA)
df_analise['desmatamento_acumulado'] = df_analise.groupby('cod_ibge')['area_desmatada_ha'].cumsum()

# Agrupamentos para análise
por_ano = df_analise.groupby('ano').agg({
    'area_desmatada_ha': 'sum',
    'vab_agro_mil_reais': 'sum',
    'num_embargos': 'sum'
}).reset_index()

por_uf = df_analise.groupby('uf').agg({
    'area_desmatada_ha': 'sum',
    'vab_agro_mil_reais': 'sum',
    'cod_ibge': 'nunique'
}).reset_index()
```

---

*Documento atualizado em 2026-04-09 para refletir o estado completo do projeto: 32 scripts, 11 arquivos Silver, 19 arquivos Gold, 8 reports e 4 EPICs analíticos na Fase 3.*
