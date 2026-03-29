# Plano de Implementação: Análise de Desmatamento e Impacto Socioambiental

## 📊 Status Atual do Projeto (Atualizado: 28/03/2026)

| Sprint | Status | Conclusão |
|--------|--------|-----------|
| Sprint 0: Ingestão Parquet | ✅ Concluído | 100% |
| Sprint 0.5: Data Quality | ✅ Concluído | 100% |
| Sprint 1: Limpeza e Join | 🔄 Próximo | 0% |
| Sprint 2-8: Analíticos | ⏳ Pendente | 0% |

**Próxima Entrega:** Sprint 1 - Fundação de Dados Tabulares (Limpeza e Join)

---

Para transformar este documento em realidade, é necessário um Plano de Implementação estruturado em etapas lógicas de Engenharia e Análise de Dados (pipeline de ETL e Modelagem). Abaixo, apresento o roteiro de execução para responder a todas as perguntas levantadas.

---

## Fase 1: Arquitetura e Stack Tecnológico

Antes de baixar os dados, defina o ambiente de trabalho para suportar o volume (especialmente os dados espaciais e do MapBiomas, que são pesados).

*   **Linguagem:** Python (ideal para automação e análise espacial).
*   **Bibliotecas Tabulares:** `pandas`, `numpy`, `basedosdados` (para SQL) e `sidrapy` (API do IBGE).
*   **Bibliotecas Espaciais:** `geopandas` (shapefiles), `shapely` (buffers), `rasterio` (imagens MapBiomas).
*   **Visualização:** `matplotlib`, `seaborn` (estatística), `folium` ou `kepler.gl` (mapas interativos).
*   **Armazenamento:** Arquivos **Parquet** locais para o MVP; **PostgreSQL** com extensão **PostGIS** para escala.

---

## Fase 2: Roteiro de Execução (9 Sprints)

Para reduzir o risco de gargalos, especialmente com bases espaciais e séries temporais complexas, o escopo foi dividido em sprints modulares. Cada ciclo entrega um resultado analítico funcional.

### Sprint 0: Ingestão e Padronização Parquet (Data Lake Prep) ✅ CONCLUÍDO
*   **Objetivo:** Download automatizado e conversão de fontes brutas para Parquet de forma modular.
*   **Ação:** Implementação de notebooks individuais para teste e validação de cada fonte de dados, garantindo resiliência contra falhas de API ou mudanças de formato.
*   **Notebooks de Extração:**
    *   ✅ `download-dados-em-parquet-v2.ipynb`: Pipeline unificado de download (SIDRA/IBAMA/COMEX).
    *   ✅ `test-download-pam.ipynb`: Produção Agrícola Municipal (SIDRA) - 888.340 registros.
    *   ✅ `test-download-ppm.ipynb`: Pecuária Municipal - 12 categorias de rebanhos (SIDRA).
    *   ✅ `test-download-pib.ipynb`: PIB VAB Agropecuária (SIDRA) - 14 anos (2010-2023).
    *   ✅ `test-download-ibama.ipynb`: Embargos Ambientais (Shapefile/Geopandas) - 88.586 registros.
    *   ✅ `test-download-comex.ipynb`: Exportações (COMEX STAT) - 1.6M registros (2023-2025).
*   **Processamento:** Conversão imediata de CSV/SHP para **Parquet** (compressão `snappy`) na camada Bronze.
*   **Artefatos Gerados:**
    | Fonte | Diretório Bronze | Registros | Arquivos Parquet |
    |-------|------------------|-----------|------------------|
    | COMEX | `comex/data/bronze/comex_stat/` | 1.603.796 | 6 arquivos (EXP/IMP 2023-2025) |
    | IBAMA | `ibama/data/bronze/ibama_embargos/` | 88.586 | 2 arquivos (tabular + geoparquet) |
    | PAM | `pam/data/bronze/pam/` | 888.340 | 10 chunks |
    | PIB | `pib/data/bronze/pib_vab_agro/` | ~77.980 | 14 arquivos (2010-2023) |
    | PPM | `ppm/data/bronze/ppm_*/` | Variável | 12 categorias (bovinos, suinos, etc.) |
*   **Status:** ✅ **CONCLUÍDO** - Todas as fontes ingeridas e validadas em Parquet.

### Sprint 0.5: Análise Exploratória e Resumo de Dados (Data Quality) ✅ CONCLUÍDO
*   **Objetivo:** Gerar metadados e estatísticas descritivas para todas as fontes da camada Bronze.
*   **Notebooks de Resumo:**
    | Notebook | Saída Parquet | Colunas Analisadas | Status |
    |----------|---------------|-------------------|--------|
    | `resumo_dados_bronze.ipynb` | `resumo_detalhado_bronze.parquet` (17KB) | 229 colunas | ✅ Concluído |
    | `resumo_dados_ibama.ipynb` | `resumo_detalhado_ibama.parquet` (14KB) | 77 colunas | ✅ Concluído |
    | `resumo_dados_pam.ipynb` | `resumo_detalhado_pam.parquet` + stats | 13 colunas | ✅ Concluído |
    | `resumo_dados_pib.ipynb` | `resumo_consolidado_pib.parquet` (5,2KB) | Estatísticas por ano | ✅ Concluído |
    | `resumo_dados_ppm.ipynb` | `resumo_consolidado_ppm.parquet` (5,5KB) | 12 categorias | ✅ Concluído |
*   **Esquema Unificado de Metadados:**
    ```
    dataset/file | total_rows | mem_mb | column | dtype | non_null_count | 
    null_count | null_percent | unique_values | min | max | mean | std | 
    median | sample_values
    ```
*   **Insights da Qualidade:**
    - **PAM:** 0% de nulos, 6 anos (2020-2024), 5.564 municípios, 11 tipos de variáveis (área plantada, colhida, valor).
    - **PIB:** 0% de nulos (2013-2021), dados 2022-2023 incompletos, VAB total 2021: R$ 591 bi.
    - **PPM:** Bovinos = 238M cabeças (2024), outras categorias sem dados no último ano.
    - **IBAMA:** 88.586 embargos, geometria disponível em GeoParquet.
    - **COMEX:** 1,6M registros, 11 colunas (NCM, UF, país, valor, peso).
*   **Status:** ✅ **CONCLUÍDO** - Metadados gerados e validados para todas as fontes.

### Sprint 1: Fundação de Dados Tabulares (Limpeza e Join) 🔄 PRÓXIMO
*   **Objetivo:** Engenharia de dados socioeconômicos e ambientais a partir dos arquivos Parquet.
*   **ETL 1.0:** Leitura dos arquivos Parquet de PRODES, PAM, PPM e PIB Municipal.
*   **Tratamento:** Padronização de tipos de dados, correção de Código IBGE e alinhamento temporal.
*   **Ações Necessárias:**
    - [ ] Unificar schema de municípios (nome + código IBGE)
    - [ ] Criar tabela dimensão de município único
    - [ ] Padronizar anos (série histórica comum)
    - [ ] Tratar valores nulos identificados no Sprint 0.5
*   **Artefato:** Silver Layer (Data Lake local limpo e tipado em Parquet).

### Sprint 2: O MVP Analítico Econômico (Insights Rápidos)
*   **Objetivo:** Responder ao custo-benefício do desmatamento via Parquet queries.
*   **Análise:** Cálculo do $\Delta$ Desmatamento, $\Delta$ PIB Agro e aplicação do $ICA$ usando vetores de alto desempenho.
*   **Correlação:** Isolar impacto Pecuária (PPM) vs Agricultura (PAM).
*   **Artefato:** "Ranking do Desmatamento Ineficiente" e relatório de insights.

### Sprint 3: Inteligência Espacial e Vazamento (Spillover)
*   **Objetivo:** Integrar geometrias (GeoParquet) e tratar influência territorial.
*   **ETL 2.0:** Ingestão de shapefiles (UCs e PRODES vetorial) e conversão para **GeoParquet**.
*   **Análise:** Criação de buffers de 10 km e aplicação de `sjoin` em memória otimizada.
*   **Artefato:** Mapas de densidade e tabelas relacionais georreferenciadas.

### Sprint 4: Rota Temporal e Transição do Uso do Solo
*   **Objetivo:** Cronologia da degradação via séries temporais Parquet.
*   **ETL 3.0:** Processamento de bases temporais (DETER, MapBiomas Fogo, TerraClass) vindas do Sprint 0.
*   **Análise:** Merge de arquivos Parquet temporais e sobreposição: Alerta $\rightarrow$ Fogo $\rightarrow$ Corte Raso $\rightarrow$ Pastagem.
*   **Artefato:** Matriz de transição eficiente para grandes volumes.

### Sprint 5: Rastreabilidade da Cadeia Global
*   **Objetivo:** Cruzar dano ambiental com exportações (Comex Stat Parquet).
*   **ETL 4.0:** Ingestão de exportações por NCM (Soja, Milho, Carne).
*   **Mercado:** Cruzar com o ranking do Sprint 2 via Join de arquivos Parquet.
*   **Artefato:** Matriz de Rastreabilidade (Origem $\rightarrow$ Rota $\rightarrow$ Destino).

### Sprint 6: O Peso da Fiscalização
*   **Objetivo:** Medir eficácia via base de Embargos (IBAMA Parquet).
*   **ETL 5.0:** Cruzamento temporal de multas e embargos com queda de produção.
*   **Análise:** Filtros em Parquet para isolar quebras estruturais.
*   **Artefato:** Relatório estatístico sobre eficácia punitiva.

### Sprint 7: O Paradoxo Socioeconômico
*   **Objetivo:** Integrar desenvolvimento humano (IDHM Parquet) à tese central.
*   **ETL 6.0:** Leitura da Gold Layer consolidada em Parquet e cruzamento Final.
*   **Análise Social:** Gráficos de dispersão unindo PIB, IDHM e Área Desmatada.
*   **Artefato:** Tese final e Gold Layer pronta para consumo visual.

### Sprint 8: Produtização e Data Storytelling
*   **Objetivo:** Empacotamento em produto interativo alimentado por Parquet.
*   **Desenvolvimento:** Dashboard unificado (Streamlit, Power BI ou Metabase).
*   **Refinamento:** Tooltips, filtros rápidos por município (aproveitando indexação Parquet).
*   **Artefato:** Painel analítico final implantado e documentado.

---

## Fase 3: Detalhamento Metodológico (Resumo)

> **Nota:** A metodologia abaixo será implementada a partir do Sprint 2. Os dados necessários já estão disponíveis na camada Bronze (Sprint 0 ✅).

### A. Eficiência Econômica e o "Custo Ambiental"
*   **Cálculo do ICA:**
    $$ICA_{i} = \frac{\Delta Desmatamento_{i} (ha)}{\Delta VAB\_Agro_{i} (R\$)}$$
*   **Pecuária vs Agricultura:** Correlação de Pearson para medir R$ gerado por hectare desmatado em cada grupo.
*   **Dados Disponíveis:** ✅ PIB (pib_vab_agro), ✅ PAM (área plantada/colhida), ✅ PPM (efetivo rebanhos)

### B. Dinâmica Espacial e Efeito Vazamento (Spillover)
*   **Análise UCs:** Uso de `buffer()` e `sjoin` para comparar densidades de desmatamento.
*   **Rota Temporal:** Query de sobreposição histórica (DETER -> Fogo -> PRODES -> TerraClass).
*   **Dados Disponíveis:** ✅ IBAMA (geoparquet), ⏳ PRODES/DETER (pendente ingestão)

### C. Cadeia de Suprimentos e Mercado Global
*   **Rastreio:** Filtro NCM no Comex Stat cruzado com os 100 municípios líderes de desmatamento.
*   **Fiscalização:** Avaliação de quebras estruturais na produção após picos de embargos do IBAMA.
*   **Dados Disponíveis:** ✅ COMEX (exportações NCM), ✅ IBAMA (embargos)

### D. O Paradoxo do Desenvolvimento Social
*   **Correlação:** Join triplo para evidenciar se o PIB Agro reflete no IDHM local através de gráficos de bolhas.
*   **Dados Disponíveis:** ✅ PIB/PAM/PPM (municipal), ⏳ IDHM (pendente ingestão)

---

## Inventário Técnico de Artefatos (Atualizado)

### Camada Bronze (Dados Brutos em Parquet)
| Fonte | Localização | Tamanho | Registros | Colunas |
|-------|-----------|---------|-----------|---------|
| COMEX | `comex/data/bronze/comex_stat/` | ~171 MB | 1.603.796 | 11 |
| IBAMA | `ibama/data/bronze/ibama_embargos/` | ~94 MB | 88.586 | 38+ |
| PAM | `pam/data/bronze/pam/` | ~10 chunks | 888.340 | 13 |
| PIB | `pib/data/bronze/pib_vab_agro/` | 14 arquivos | ~77.980 | 9 |
| PPM | `ppm/data/bronze/ppm_*/` | 12 categorias | Variável | 9 |

### Camada Resume (Metadados e Estatísticas)
| Arquivo | Tamanho | Descrição |
|---------|---------|-----------|
| `resumo_detalhado_bronze.parquet` | 17 KB | Schema completo de todas as fontes (229 colunas) |
| `resumo_detalhado_ibama.parquet` | 14 KB | Metadados IBAMA por arquivo (77 colunas) |
| `resumo_detalhado_pam.parquet` | 4,9 KB | Estatísticas PAM por variável |
| `resumo_consolidado_pib.parquet` | 5,2 KB | VAB por ano e município |
| `resumo_consolidado_ppm.parquet` | 5,5 KB | Efetivo de rebanhos por categoria |

### Notebooks de ETL e Resumo
| Diretório | Notebooks | Status |
|-----------|-----------|--------|
| `comex/data/resume/` | `resumo_dados_bronze.ipynb` | ✅ Executado |
| `ibama/data/resume/` | `resumo_dados_ibama.ipynb` | ✅ Executado |
| `pam/data/resume/` | `resumo_dados_pam.ipynb` | ✅ Executado |
| `pib/data/resume/` | `resumo_dados_pib.ipynb` | ✅ Executado |
| `ppm/data/resume/` | `resumo_dados_ppm.ipynb` | ✅ Executado |

