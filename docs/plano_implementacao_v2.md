# Plano de Implementação: Análise de Desmatamento e Impacto Socioambiental

## 📊 Status Atual do Projeto (Atualizado: 28/03/2026 - 23:45)

| Sprint | Status | Conclusão | Próxima Entrega |
|--------|--------|-----------|-----------------|
| Sprint 0: Ingestão Parquet | ✅ Concluído | 100% | - |
| Sprint 0.5: Data Quality | ✅ Concluído | 100% | - |
| Sprint 1: Limpeza e Join | 🔄 Em Progresso | 0% | ETL 1.1-1.7 (Silver Layer) |
| Sprint 2: MVP Econômico | ⏳ Pendente | 0% | Após Sprint 1 |
| Sprint 3-8: Analíticos | ⏳ Pendente | 0% | - |

**Próxima Entrega:** Sprint 1 - ETL de padronização para camada Silver (5 ETLs municipais + 1 COMEX-UF)

**Descobertas Críticas da Análise (28/03/2026):**
- 📌 **PAM/PIB/PPM:** Dados IBGE em formato "long" requerem pivotamento (colunas codificadas D1C, D2C, D3C...)
- 📌 **COMEX:** Sem município, apenas UF → join apenas no nível estadual
- 📌 **IBAMA:** 3.769 municípios, 88.586 embargos (1987-2025), áreas com 60-89% nulls
- 📌 **Série comum:** 2020-2023 (4 anos) para análise municipal integrada
- 📌 **PPM Bovinos:** 238M cabeças (2024), outras categorias sem dados recentes

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

### Sprint 1: Fundação de Dados Tabulares (Limpeza e Join) 🔄 EM PROGRESSO
*   **Objetivo:** Engenharia de dados socioeconômicos e ambientais a partir dos arquivos Parquet da camada Bronze.
*   **Contexto dos Dados (Validado em 28/03/2026):**
    
    | Fonte | Schema | Chave Município | Chave Ano | Período | Nulls | Observações |
    |-------|--------|-----------------|-----------|---------|-------|-------------|
    | **PAM** | Long (13 cols: NC,NN,MC,MN,V,D1C,D1N,D2C,D2N,D3C,D3N,D4C,D4N) | D1C (código 7 dígitos) | D3C | 2020-2024 | 0% | 11 variáveis (área plantada, colhida, produção, valor) |
    | **PIB** | Long (11 cols: NC,NN,MC,MN,V,D1C,D1N,D2C,D2N,D3C,D3N) | D1C (código 7 dígitos) | D3C | 2010-2023 | 0% (2013-2021) | VAB Agropecuária (R$ mil), 2022-2023 incompletos |
    | **PPM** | Long (13 cols: NC,NN,MC,MN,V,D1C,D1N,D2C,D2N,D3C,D3N,D4C,D4N) | D1C (código 7 dígitos) | D3C | 2021-2024 | Variável | 12 categorias, bovinos=238M (2024), outras sem dados 2024 |
    | **IBAMA** | Wide (38 cols) | cod_munici (inteiro) | dat_embarg (extrair ano) | 1987-2025 | 6-89% | 3.769 municípios, geometria em geoparquet separado |
    | **COMEX** | Wide (11 cols) | **NÃO TEM** (apenas UF: SG_UF_NCM) | CO_ANO | 2023-2025 | 0% | 1,6M regs, agregável apenas por UF |

    **Descobertas Críticas:**
    - **PAM/PIB/PPM (IBGE/SIDRA):** Dados em formato "long" com colunas codificadas
      - `V` (Valor): string, contém ".." para dados ausentes → converter para float
      - `D1C`: código IBGE com sufixo textual → extrair 7 dígitos
      - `D3C/D3N`: ano como string → converter para inteiro
      - Necessário pivotar: long → wide para análise
    - **IBAMA:** 
      - `cod_munici`: já inteiro, pronto para join
      - `dat_embarg`: formato "dd/mm/yy HH:MM:SS" → extrair ano
      - Campos com alto null_percent: `operacao` (89%), `nome_imove` (87%), `tipo_area` (60%)
    - **COMEX:** 
      - **NÃO possui município**, apenas UF → join apenas no nível estadual
      - `SG_UF_NCM`: 28 UFs + "ND" (não determinado)
      - `VL_FOB`: valor em dólares (inteeiro)
      - `KG_LIQUIDO`: peso em kg
      - `CO_NCM`: código NCM (8 dígitos) para rastreio de commodities

*   **ETL 1.1: Padronização PAM (Produção Agrícola Municipal)**
    - [ ] Ler todos os chunks Parquet de `pam/data/bronze/pam/`
    - [ ] Filtrar linhas onde `D1C` tem 7 dígitos (código IBGE válido)
    - [ ] Extrair: `cod_ibge = D1C.str.extract('(\d{7})').astype(int)`
    - [ ] Extrair: `ano = D3C.str.extract('(\d{4})').astype(int)`
    - [ ] Converter: `valor = pd.to_numeric(V, errors='coerce')`
    - [ ] Pivotar: index=[cod_ibge, ano, municipio], columns=[D2N], values=valor
    - [ ] Resultado: uma linha por município-ano com colunas para cada variável (área plantada, colhida, produção, valor)
    - [ ] Salvar: `pam/data/silver/pam_consolidado.parquet`

*   **ETL 1.2: Padronização PIB (PIB VAB Agropecuária)**
    - [ ] Ler todos os arquivos de `pib/data/bronze/pib_vab_agro/`
    - [ ] Filtrar linhas onde `D1C` tem 7 dígitos
    - [ ] Extrair: `cod_ibge = D1C.str.extract('(\d{7})').astype(int)`
    - [ ] Extrair: `ano = D3C.str.extract('(\d{4})').astype(int)`
    - [ ] Converter: `vab_agro = pd.to_numeric(V, errors='coerce')` (em R$ mil)
    - [ ] Manter apenas: cod_ibge, municipio (D1N), ano, vab_agro
    - [ ] Salvar: `pib/data/silver/pib_vab_agro_consolidado.parquet`

*   **ETL 1.3: Padronização PPM (Pecuária Municipal)**
    - [ ] Ler todas as 12 categorias de `ppm/data/bronze/ppm_*/`
    - [ ] Filtrar linhas onde `D1C` tem 7 dígitos
    - [ ] Extrair: `cod_ibge = D1C.str.extract('(\d{7})').astype(int)`
    - [ ] Extrair: `ano = D3C.str.extract('(\d{4})').astype(int)`
    - [ ] Converter: `efetivo = pd.to_numeric(V, errors='coerce')` (cabeças)
    - [ ] Extrair categoria de `D4N` (Bovino, Suíno, Caprino, etc.)
    - [ ] Pivotar: index=[cod_ibge, ano, municipio], columns=[D4N], values=efetivo
    - [ ] Salvar: `ppm/data/silver/ppm_consolidado.parquet`
    - [ ] **Atenção:** Apenas "Bovino" tem dados completos 2021-2024

*   **ETL 1.4: Padronização IBAMA (Embargos Ambientais)**
    - [ ] Ler `ibama/data/bronze/ibama_embargos/embargos_ibama_tabular.parquet`
    - [ ] Extrair: `ano_embarg = pd.to_datetime(dat_embarg, format='%d/%m/%y %H:%M:%S').dt.year`
    - [ ] Manter colunas relevantes: cod_munici, municipio, uf, ano_embarg, num_tad, sit_desmat, qtd_area_d, qtd_area_e
    - [ ] Agrupar por município-ano: soma de áreas embargadas, contagem de embargos
    - [ ] Salvar: `ibama/data/silver/embargos_por_municipio_ano.parquet`
    - [ ] **Separado:** Manter geoparquet para análise espacial (Sprint 3)

*   **ETL 1.5: Padronização COMEX (Exportações/Importações)**
    - [ ] Ler todos os arquivos de `comex/data/bronze/comex_stat/`
    - [ ] Extrair: `uf = SG_UF_NCM`, `ano = CO_ANO`
    - [ ] Mapear NCM para commodities:
      - Soja: 12010000, 12011000, 12019000
      - Milho: 10059000, 10051000
      - Carne Bovina: 02011000, 02012000, 02013000, 02021000, 02022000, 02023000
    - [ ] Agrupar por UF-ano: soma VL_FOB (USD), soma KG_LIQUIDO (kg)
    - [ ] Salvar: `comex/data/silver/comex_por_uf_ano.parquet`
    - [ ] **Nota:** NÃO entra no join municipal (apenas análise por UF)

*   **ETL 1.6: Dimensão de Municípios (Tabela de Referência)**
    - [ ] Criar tabela única de municípios a partir de PAM (mais completa: 5.564 municípios)
    - [ ] Colunas: `cod_ibge` (int), `municipio` (str), `uf` (str, extrair de D1N)
    - [ ] Validar: todos os códigos têm 7 dígitos e UF válida
    - [ ] Salvar: `dim_municipio.parquet` (camada Silver)

*   **ETL 1.7: Série Histórica Comum (Anos)**
    - [ ] Identificar interseção temporal:
      - PAM: 2020-2024
      - PIB: 2010-2023 (recomendar 2013-2021 para dados completos)
      - PPM: 2021-2024 (apenas bovinos)
      - IBAMA: 1987-2025 (recomendar 2010-2024)
      - COMEX: 2023-2025
    - [ ] **Série comum para análise municipal:** 2020-2023 (4 anos)
    - [ ] **Série estendida (com lacunas):** 2013-2024

*   **Artefatos da Sprint 1 (Camada Silver):**
    | Arquivo | Chaves | Colunas Principais | Período |
    |---------|--------|-------------------|---------|
    | `pam_consolidado.parquet` | cod_ibge + ano | area_plantada, area_colhida, producao, valor | 2020-2024 |
    | `pib_vab_consolidado.parquet` | cod_ibge + ano | vab_agro (R$ mil) | 2010-2023 |
    | `ppm_consolidado.parquet` | cod_ibge + ano | bovinos, suinos, ovinos, etc. | 2021-2024 |
    | `embargos_por_municipio_ano.parquet` | cod_munici + ano | num_embargos, area_embargada_ha | 1987-2025 |
    | `comex_por_uf_ano.parquet` | uf + ano | vob_fob_usd, peso_kg, ncm | 2023-2025 |
    | `dim_municipio.parquet` | cod_ibge | municipio, uf | - |

*   **Critérios de Aceite:**
    - [ ] Todos os arquivos Silver validados com schema consistente
    - [ ] Chaves de join padronizadas: `cod_ibge` (int7), `ano` (int)
    - [ ] 0% de strings nos campos de valor (todos numéricos)
    - [ ] Documentação de cada campo no dicionário de dados
    - [ ] Notebooks de ETL reprodutíveis e testados

### Sprint 2: O MVP Analítico Econômico (Custo-Benefício do Desmatamento)
*   **Objetivo:** Responder à pergunta central: "O desmatamento gera eficiência econômica?"
*   **Pré-requisito:** Sprint 1 concluída (Silver Layer disponível)

*   **Análise 2.1: Cálculo do ICA (Índice de Custo Ambiental)**
    - [ ] Fórmula: `ICA_municipio_ano = area_desmatada_ha / vab_agro_R$`
    - [ ] Interpretação: hectares desmatados por R$ 1.000 de VAB agropecuário
    - [ ] Dados: IBAMA (área embargada como proxy de desmatamento) + PIB (VAB agro)
    - [ ] Agregação: ranking de municípios por ICA (maior = mais ineficiente)
    - [ ] Output: `ica_ranking_municipio.parquet`

*   **Análise 2.2: Delta Desmatamento vs Delta PIB Agro**
    - [ ] Calcular variação anual: `ΔDesmatamento = area_embargada(t) - area_embargada(t-1)`
    - [ ] Calcular variação anual: `ΔVAB = vab_agro(t) - vab_agro(t-1)`
    - [ ] Correlação de Pearson entre ΔDesmatamento e ΔVAB
    - [ ] Scatter plot: ΔVAB (x) vs ΔDesmatamento (y) por município
    - [ ] Hipótese: correlação negativa ou nula (desmatamento não gera crescimento)
    - [ ] Output: `correlacao_delta_desmatamento_pib.parquet`

*   **Análise 2.3: Pecuária vs Agricultura (PPM vs PAM)**
    - [ ] Calcular eficiência por hectare:
      - Pecuária: `cabeças_bovinas / area_embargada`
      - Agricultura: `producao_toneladas / area_embargada`
    - [ ] Calcular valor por hectare:
      - Pecuária: `valor_bovinos_R$ / area_embargada`
      - Agricultura: `valor_lavoura_R$ / area_embargada`
    - [ ] Ranking: quais atividades geram mais R$/ha desmatado?
    - [ ] Output: `eficiencia_pecuaria_vs_agricultura.parquet`

*   **Análise 2.4: Concentração Territorial**
    - [ ] Top 100 municípios em área embargada (2020-2023)
    - [ ] Top 100 municípios em VAB Agro (2020-2023)
    - [ ] Overlap: quantos estão em ambas as listas?
    - [ ] Perfil: municípios que desmatam muito mas têm baixo VAB
    - [ ] Output: `ranking_concentracao_desmatamento.parquet`

*   **Análise 2.5: Validação Estatística**
    - [ ] Teste de hipótese: municípios com alto desmatamento têm menor IDHM? (dados IDHM pendentes)
    - [ ] Regressão linear: VAB Agro = f(área embargada, ano, UF)
    - [ ] Controle por UF: efeito fixo para controlar diferenças estaduais
    - [ ] Output: `regressao_desmatamento_pib.csv` (coeficientes, p-valores)

*   **Artefatos da Sprint 2 (Camada Gold - Analítica):**
    | Arquivo | Descrição | Métricas |
    |---------|-----------|----------|
    | `ica_ranking.parquet` | Ranking municipal do ICA | ICA, area_desmatada, vab_agro |
    | `correlacao_delta.parquet` | Correlação ΔDesmatamento × ΔVAB | Pearson, p-valor, N |
    | `eficiencia_atividade.parquet` | Pecuária vs Agricultura | R$/ha, cabeças/ha, ton/ha |
    | `ranking_top100.parquet` | Top 100 desmatamento vs Top 100 VAB | overlap, perfil |
    | `regressao_resultados.csv` | Modelo de regressão | coeficientes, R², p-valores |

*   **Visualizações Esperadas:**
    - [ ] Mapa de calor: ICA por município (coroplético)
    - [ ] Scatter: ΔVAB vs ΔDesmatamento (com linha de tendência)
    - [ ] Boxplot: R$/ha desmatado (Pecuária vs Agricultura)
    - [ ] Barras: Top 20 municípios em ICA (ineficientes)
    - [ ] Tabela: Ranking dos "100 Piores" (alto desmatamento, baixo VAB)

*   **Critérios de Aceite:**
    - [ ] ICA calculado para ≥80% dos municípios (2020-2023)
    - [ ] Correlação estatisticamente significativa (p < 0.05)
    - [ ] Visualizações claras e publicáveis
    - [ ] Relatório executivo: "Desmatamento gera crescimento econômico?"
    - [ ] Código reprodutível e documentado

### Sprint 3: Inteligência Espacial e Vazamento (Spillover)
*   **Objetivo:** Integrar geometrias (GeoParquet) e analisar influência territorial de embargos.
*   **Contexto dos Dados:**
    - IBAMA geoparquet disponível com 88.586 embargos georreferenciados
    - 3.769 municípios únicos com geometria
    - Campos espaciais: `num_longit`, `num_latitu`, `st_area(sh)`, `st_perimet`

*   **ETL 3.1: Processamento de Geometrias IBAMA**
    - [ ] Ler `ibama/data/bronze/ibama_embargos/embargos_ibama_geoparquet.parquet` com geopandas
    - [ ] Extrair centroides: `geometry.centroid` para agregação municipal
    - [ ] Calcular área dos polígonos: `geometry.area` (em hectares)
    - [ ] Unir com dados tabulares via `seq_tad` ou coordenadas
    - [ ] Salvar: `ibama/data/silver/embargos_com_geometria.parquet`

*   **ETL 3.2: Shapefiles Externos (Pendentes de Ingestão)**
    - [ ] PRODES vetorial (INPE): limites anuais de desmatamento
    - [ ] Unidades de Conservação (UCs): SNUC/MMA
    - [ ] Terras Indígenas: FUNAI
    - [ ] Hidrografia: ANA
    - [ ] Converter todos para GeoParquet: `dados_espaciais/bronze/`

*   **Análise 3.3: Buffer e Spillover**
    - [ ] Criar buffers de 10km ao redor de embargos
    - [ ] Identificar municípios adjacentes a embargos (efeito vazamento)
    - [ ] Calcular densidade de embargos por microrregião
    - [ ] Output: `spillover_municipios_adjacentes.parquet`

*   **Artefatos da Sprint 3:**
    | Arquivo | Tipo | Descrição |
    |---------|------|-----------|
    | `embargos_com_geometria.parquet` | GeoParquet | Embargos com geometria válida |
    | `densidade_embargos_municipio.parquet` | Tabular | ha embargados / km² municipal |
    | `spillover_adjacencia.parquet` | Tabular | municípios com buffer sobreposto |

*   **Critérios de Aceite:**
    - [ ] 100% dos embargos com geometria processada
    - [ ] Buffer de 10km aplicado e validado
    - [ ] Mapa de densidade gerado (QGIS/Kepler.gl)

### Sprint 4: Rota Temporal e Transição do Uso do Solo
*   **Objetivo:** Reconstruir cronologia da degradação ambiental via séries temporais.
*   **Dados Pendentes de Ingestão:**
    - ⏳ PRODES (INPE): corte raso anual (desde 1988)
    - ⏳ DETER (INPE): alertas diários (desde 2004)
    - ⏳ MapBiomas Fogo: ocorrências de incêndio
    - ⏳ TerraClass: uso pós-desmatamento (pastagem, agricultura)

*   **ETL 4.1: Pipeline Temporal**
    - [ ] Ingerir PRODES: `prod_desmatamento_anual.parquet` (município, ano, ha)
    - [ ] Ingerir DETER: `deter_alertas_diarios.parquet` (município, data, ha, tipo)
    - [ ] Ingerir MapBiomas Fogo: `fogo_ocorrencias.parquet`
    - [ ] Ingerir TerraClass: `terra_classificacao.parquet`

*   **Análise 4.2: Sequência de Degradação**
    - [ ] Ordenar temporalmente: DETER (alerta) → Fogo → PRODES (corte) → TerraClass (uso)
    - [ ] Calcular latência média entre alerta e corte raso
    - [ ] Identificar municípios com alta recorrência de alertas
    - [ ] Output: `sequencia_degradacao_timeline.parquet`

*   **Análise 4.3: Matriz de Transição**
    - [ ] Calcular transições: Floresta → Alerta → Corte → Pastagem/Agricultura
    - [ ] Percentual de transição por município
    - [ ] Output: `matriz_transicao_uso.parquet`

*   **Artefatos da Sprint 4:**
    | Arquivo | Descrição |
    |---------|-----------|
    | `timeline_degradacao.parquet` | Sequência temporal por município |
    | `latencia_alerta_corte.parquet` | Dias entre alerta e corte raso |
    | `matriz_transicao.parquet` | % transição floresta → uso |

*   **Critérios de Aceite:**
    - [ ] Timeline reconstruída para ≥5 anos (2018-2023)
    - [ ] Matriz de transição calculada para Amazônia Legal
    - [ ] Latência média documentada (alerta → ação)

### Sprint 5: Rastreabilidade da Cadeia Global (Commodities)
*   **Objetivo:** Cruzar exportações de commodities com municípios desmatadores.
*   **Contexto COMEX (Validado 28/03/2026):**
    - 1,6M registros (2023-2025)
    - **Sem município**, apenas UF (`SG_UF_NCM`)
    - NCM disponível para filtrar commodities
    - `VL_FOB` em USD, `KG_LIQUIDO` em kg

*   **ETL 5.1: Mapeamento NCM-Commodity**
    - [ ] Filtrar NCMs relevantes:
      ```
      Soja: 1201.00.00, 1201.10.00, 1201.90.00
      Milho: 1005.10.00, 1005.90.00
      Carne Bovina: 0201.10.00, 0201.20.00, 0201.30.00, 0202.10.00, 0202.20.00, 0202.30.00
      Madeira: 4401-4409 (capítulos)
      ```
    - [ ] Agrupar por UF-ano-commodity: soma VL_FOB, soma KG_LIQUIDO
    - [ ] Output: `comex_commodities_por_uf.parquet`

*   **Análise 5.2: Rastreio Indireto (UF-Match)**
    - [ ] Rank de UFs exportadoras de soja (2023-2024)
    - [ ] Rank de UFs com maior desmatamento (IBAMA/PRODES)
    - [ ] Overlap: UFs que lideram ambos os rankings
    - [ ] Calcular: USD exportado / ha desmatado (eficácia ambiental da exportação)
    - [ ] Output: `rank_uf_exportadora_vs_desmatamento.parquet`

*   **Análise 5.3: Matriz de Destino**
    - [ ] Identificar países compradores por NCM (`CO_PAIS`)
    - [ ] Top 10 destinos para soja, milho, carne
    - [ ] Cruzar com acordos comerciais (UE, China, EUA)
    - [ ] Output: `matriz_destino_exportacao.parquet`

*   **Artefatos da Sprint 5:**
    | Arquivo | Descrição |
    |---------|-----------|
    | `comex_commodities_uf.parquet` | Exportações por UF e commodity |
    | `uf_rank_exportacao_desmatamento.parquet` | UF: exportação vs desmatamento |
    | `paises_destino_commodities.parquet` | Top compradores por commodity |

*   **Limitação Identificada:**
    - ⚠️ COMEX não tem município → rastreio apenas no nível estadual
    - ⚠️ Para rastreio municipal, seria necessário cruzar com:
      - Nota Fiscal Eletrônica (SEFAZ)
      - Guia de Trânsito Animal (GTA)
      - Cadastro Ambiental Rural (CAR)

*   **Critérios de Aceite:**
    - [ ] 100% das commodities mapeadas por NCM
    - [ ] Ranking de UFs exportadoras documentado
    - [ ] Matriz de destinos (países) visualizada

### Sprint 6: O Peso da Fiscalização (Efetividade dos Embargos)
*   **Objetivo:** Medir se embargos do IBAMA reduzem desmatamento e produção.
*   **Contexto IBAMA (Validado 28/03/2026):**
    - 88.586 embargos (1987-2025)
    - 3.769 municípios afetados
    - Campos: `num_tad`, `dat_embarg`, `qtd_area_d`, `qtd_area_e`, `sit_desmat`

*   **ETL 6.1: Séries Temporais de Fiscalização**
    - [ ] Agrupar embargos por município-ano: count, sum(area)
    - [ ] Calcular: embargos_per_capita, area_embargada_per_municipio
    - [ ] Identificar picos de fiscalização (outliers)
    - [ ] Output: `fiscalizacao_series_temporais.parquet`

*   **Análise 6.2: Impacto na Produção (PAM/PPM)**
    - [ ] Antes vs Depois do embargo:
      - Δ Área plantada (PAM)
      - Δ Efetivo de rebanho (PPM)
      - Δ VAB Agro (PIB)
    - [ ] Janela temporal: -2 anos a +2 anos do embargo
    - [ ] Teste t pareado: produção_antes vs produção_depois
    - [ ] Output: `impacto_embargo_producao.parquet`

*   **Análise 6.3: Recorrência de Embargados**
    - [ ] Identificar CPF/CNPJ reincidentes (`cpf_cnpj_e`)
    - [ ] Calcular: embargo_recurrence_rate
    - [ ] Top 100 embargados reincidentes
    - [ ] Output: `reincidentes_embargos.parquet`

*   **Análise 6.4: Situação do Desmatamento**
    - [ ] Analisar campo `sit_desmat` (2 valores únicos)
    - [ ] Percentual por situação: "Embargado" vs "Regularizado"?
    - [ ] Tempo médio de regularização
    - [ ] Output: `status_regularizacao_embargos.parquet`

*   **Artefatos da Sprint 6:**
    | Arquivo | Descrição |
    |---------|-----------|
    | `fiscalizacao_series.parquet` | Embargos/ano por município |
    | `impacto_producao_antes_depois.parquet` | Δ Produção pré/pós embargo |
    | `reincidentes_cpf_cnpj.parquet` | Embargados reincidentes |
    | `status_regularizacao.parquet` | Situação e tempo de regularização |

*   **Critérios de Aceite:**
    - [ ] Impacto na produção quantificado (p-valor < 0.05)
    - [ ] Taxa de reincidência calculada
    - [ ] Relatório: "Embargos reduzem desmatamento?"

### Sprint 7: O Paradoxo Socioeconômico (IDHM e Desenvolvimento)
*   **Objetivo:** Verificar se desmatamento correlaciona com desenvolvimento humano.
*   **Dados Pendentes:**
    - ⏳ IDHM (IPEA/IBGE): municipal, quinquenal (1991, 2000, 2010, 2020)
    - ⏳ PIB per capita municipal
    - ⏳ Taxa de pobreza municipal

*   **ETL 7.1: Ingestão IDHM**
    - [ ] Baixar IDHM-M de 1991-2020
    - [ ] Padronizar código IBGE (7 dígitos)
    - [ ] Calcular interpolação para anos intermediários
    - [ ] Output: `idhm_municipal_interpolado.parquet`

*   **Análise 7.2: Correlação Desmatamento × IDHM**
    - [ ] Correlação de Spearman: area_desmatada vs IDHM
    - [ ] Correlação: VAB_Agro per capita vs IDHM
    - [ ] Scatter plot com linha de tendência
    - [ ] Output: `correlacao_idhm_desmatamento.parquet`

*   **Análise 7.3: Tipologia Municipal**
    - [ ] Classificar municípios em 4 quadrantes:
      1. Alto Desmatamento / Alto IDHM
      2. Alto Desmatamento / Baixo IDHM (paradoxo)
      3. Baixo Desmatamento / Alto IDHM
      4. Baixo Desmatamento / Baixo IDHM
    - [ ] Contar municípios por quadrante
    - [ ] Output: `tipologia_municipal_quadrantes.parquet`

*   **Artefatos da Sprint 7:**
    | Arquivo | Descrição |
    |---------|-----------|
    | `idhm_interpolado.parquet` | IDHM anual (1991-2024) |
    | `correlacao_idhm_desmatamento.parquet` | Spearman, p-valor |
    | `tipologia_municipal.parquet` | 4 quadrantes de classificação |

*   **Critérios de Aceite:**
    - [ ] IDHM interpolado para 2020-2023
    - [ ] Correlação estatisticamente testada
    - [ ] Mapa de quadrantes municipais

### Sprint 8: Produtização e Data Storytelling
*   **Objetivo:** Empacotar análises em dashboard interativo.
*   **Camadas Disponíveis:**
    - ✅ Bronze: dados brutos
    - 🔄 Silver: dados padronizados (Sprint 1)
    - ⏳ Gold: análises consolidadas (Sprints 2-7)

*   **Desenvolvimento 8.1: Dashboard Streamlit**
    - [ ] Página 1: Visão Geral (KPIs: total desmatado, VAB, embargos)
    - [ ] Página 2: ICA Ranking (municípios mais ineficientes)
    - [ ] Página 3: Série Temporal (evolução anual)
    - [ ] Página 4: Mapa (coroplético de desmatamento/ICA)
    - [ ] Página 5: Commodities (exportação por UF)
    - [ ] Página 6: Fiscalização (embargos e impacto)
    - [ ] Deploy: Streamlit Cloud ou servidor local

*   **Desenvolvimento 8.2: Relatório Executivo**
    - [ ] Resumo: "Desmatamento gera crescimento econômico?"
    - [ ] Metodologia: fontes, fórmulas, limitações
    - [ ] Resultados: ICA, correlações, rankings
    - [ ] Conclusão: evidências e recomendações
    - [ ] Formato: PDF + Markdown (GitHub)

*   **Desenvolvimento 8.3: Repositório Público**
    - [ ] Organizar estrutura: `data/bronze`, `data/silver`, `data/gold`
    - [ ] README.md com instruções de reprodução
    - [ ] requirements.txt com dependências
    - [ ] Notebooks numerados: `01_etl_pam.ipynb`, `02_etl_pib.ipynb`...
    - [ ] Licença: MIT ou CC-BY

*   **Artefatos da Sprint 8:**
    | Artefato | Descrição |
    |----------|-----------|
    | `dashboard_streamlit/` | App interativo (6 páginas) |
    | `relatorio_executivo.pdf` | 20-30 páginas com resultados |
    | `repositorio_github/` | Código e dados documentados |

*   **Critérios de Aceite:**
    - [ ] Dashboard funcional e responsivo
    - [ ] Relatório revisado e publicado
    - [ ] Repositório reproduzível (qualquer pessoa pode rodar)
    - [ ] Apresentação final (15-20 min)

---

## Fase 3: Detalhamento Metodológico (Atualizado 28/03/2026)

> **Nota:** A metodologia abaixo será implementada a partir do Sprint 2. Os dados necessários já estão disponíveis na camada Bronze (Sprint 0 ✅) e em processamento na Silver (Sprint 1 🔄).

### A. Eficiência Econômica e o "Custo Ambiental"
*   **Cálculo do ICA:**
    $$ICA_{i} = \frac{Area\_Embargada_{i} (ha)}{VAB\_Agro_{i} (R\$ mil)}$$
*   **Interpretação:** hectares embargados por R$ 1.000 de VAB agropecuário
*   **Pecuária vs Agricultura:**
    - Pecuária: `cabeças_bovinas / area_embargada_ha`
    - Agricultura: `producao_toneladas / area_embargada_ha`
*   **Dados Disponíveis:** 
    - ✅ PIB (pib_vab_agro) - 2010-2023, R$ mil
    - ✅ PAM (área plantada/colhida/produção) - 2020-2024, 0% nulls
    - ✅ PPM (efetivo rebanhos) - 2021-2024, bovinos=238M (2024)
    - ✅ IBAMA (área embargada) - 88.586 registros, 3.769 municípios

### B. Dinâmica Espacial e Efeito Vazamento (Spillover)
*   **Análise UCs:** Uso de `buffer()` e `sjoin` para comparar densidades de desmatamento.
*   **Rota Temporal:** Sequência: DETER (alerta) → Fogo → PRODES (corte) → TerraClass (uso)
*   **Dados Disponíveis:** 
    - ✅ IBAMA (geoparquet com 88.586 embargos)
    - ⏳ PRODES/DETER (pendente ingestão - INPE)
    - ⏳ MapBiomas Fogo (pendente)
    - ⏳ TerraClass (pendente)

### C. Cadeia de Suprimentos e Mercado Global
*   **Rastreio:** Filtro NCM no Comex Stat cruzado com rankings de desmatamento.
*   **Fiscalização:** Avaliação de quebras estruturais na produção após picos de embargos.
*   **Dados Disponíveis:** 
    - ✅ COMEX (1,6M exportações NCM, 2023-2025)
    - ✅ IBAMA (embargos 1987-2025)
*   **⚠️ Limitação Crítica:** COMEX não possui código municipal, apenas UF (`SG_UF_NCM`)
    - Rastreio possível apenas no nível **estadual**
    - Para rastreio municipal, seriam necessários:
      - Nota Fiscal Eletrônica (SEFAZ)
      - Guia de Trânsito Animal (GTA)
      - Cadastro Ambiental Rural (CAR)

### D. O Paradoxo do Desenvolvimento Social
*   **Correlação:** Join triplo para evidenciar se o PIB Agro reflete no IDHM local.
*   **Dados Disponíveis:** 
    - ✅ PIB/PAM/PPM (municipal, 2020-2024)
    - ⏳ IDHM (pendente ingestão - IPEA/IBGE, quinquenal)
    - ⏳ PIB per capita municipal (pendente)

---

## Inventário Técnico de Artefatos (Atualizado 28/03/2026 - 23:45)

### Camada Bronze (Dados Brutos em Parquet)
| Fonte | Localização | Tamanho | Registros | Colunas | Schema | Chaves |
|-------|-----------|---------|-----------|---------|--------|--------|
| COMEX | `comex/data/bronze/comex_stat/` | ~171 MB | 1.603.796 | 11 | Wide | UF + Ano |
| IBAMA | `ibama/data/bronze/ibama_embargos/` | ~94 MB | 88.586 | 38+ | Wide | cod_munici + dat_embarg |
| PAM | `pam/data/bronze/pam/` | ~10 chunks | 888.340 | 13 | **Long** | D1C + D3C |
| PIB | `pib/data/bronze/pib_vab_agro/` | 14 arquivos | ~77.980 | 11 | **Long** | D1C + D3C |
| PPM | `ppm/data/bronze/ppm_*/` | 12 categorias | Variável | 13 | **Long** | D1C + D3C + D4C |

**Notas sobre Schema Long:**
- PAM/PIB/PPM possuem colunas codificadas: `NC, NN, MC, MN, V, D1C, D1N, D2C, D2N, D3C, D3N, D4C, D4N`
- Requerem pivotamento: `V` (Valor) é string com ".." para ausentes
- `D1C` = código IBGE município, `D3C` = ano, `D4C` = produto/categoria

### Camada Silver (Dados Padronizados - EM PRODUÇÃO)
| Arquivo | Status | Chaves | Colunas Principais | Período |
|---------|--------|--------|-------------------|---------|
| `pam_consolidado.parquet` | ⏳ Pendente | cod_ibge + ano | area_plantada, area_colhida, producao, valor | 2020-2024 |
| `pib_vab_consolidado.parquet` | ⏳ Pendente | cod_ibge + ano | vab_agro (R$ mil) | 2010-2023 |
| `ppm_consolidado.parquet` | ⏳ Pendente | cod_ibge + ano | bovinos, suinos, ovinos, etc. | 2021-2024 |
| `embargos_por_municipio_ano.parquet` | ⏳ Pendente | cod_munici + ano | num_embargos, area_embargada_ha | 1987-2025 |
| `comex_por_uf_ano.parquet` | ⏳ Pendente | uf + ano | vob_fob_usd, peso_kg, ncm | 2023-2025 |
| `dim_municipio.parquet` | ⏳ Pendente | cod_ibge | municipio, uf | - |

### Camada Resume (Metadados e Estatísticas)
| Arquivo | Tamanho | Descrição | Status |
|---------|---------|-----------|--------|
| `resumo_detalhado_bronze.parquet` | 17 KB | Schema completo de todas as fontes (229 colunas) | ✅ Concluído |
| `resumo_detalhado_ibama.parquet` | 14 KB | Metadados IBAMA por arquivo (77 colunas) | ✅ Concluído |
| `resumo_detalhado_pam.parquet` | 4,9 KB | Estatísticas PAM por variável (13 colunas, 0% nulls) | ✅ Concluído |
| `resumo_consolidado_pib.parquet` | 5,2 KB | VAB por ano e município (14 anos) | ✅ Concluído |
| `resumo_consolidado_ppm.parquet` | 5,5 KB | Efetivo de rebanhos por categoria (12 categorias) | ✅ Concluído |

### Notebooks de ETL e Resumo
| Diretório | Notebooks | Status | Output |
|-----------|-----------|--------|--------|
| `comex/data/resume/` | `resumo_dados_bronze.ipynb` | ✅ Executado | resumo_detalhado_bronze.parquet |
| `ibama/data/resume/` | `resumo_dados_ibama.ipynb` | ✅ Executado | resumo_detalhado_ibama.parquet |
| `pam/data/resume/` | `resumo_dados_pam.ipynb` | ✅ Executado | resumo_detalhado_pam.parquet |
| `pib/data/resume/` | `resumo_dados_pib.ipynb` | ✅ Executado | resumo_consolidado_pib.parquet |
| `ppm/data/resume/` | `resumo_dados_ppm.ipynb` | ✅ Executado | resumo_consolidado_ppm.parquet |

### Dicionário de Dados Resumido

**PAM (Produção Agrícola Municipal):**
- `D2N`: Variável (Área plantada, Área colhida, Produção, Valor)
- `D4N`: Produto (Soja, Milho, Arroz, Feijão, etc. - 11 tipos)
- `tipo_lavoura`: temporaria vs permanente

**PIB (PIB VAB Agropecuária):**
- `V`: Valor adicionado bruto (R$ mil)
- Única variável: "Valor adicionado bruto a preços correntes da agropecuária"

**PPM (Pecuária Municipal):**
- `D4N`: Tipo de rebanho (Bovino, Suíno, Caprino, Ovino, Equino, Asinino, Muares, Galinhas, Galináceos, Codornas, Coelhos)
- `V`: Efetivo (cabeças)
- **Atenção:** Apenas "Bovino" tem dados completos 2021-2024

**IBAMA (Embargos Ambientais):**
- `cod_munici`: Código IBGE (inteiro, 3.769 únicos)
- `dat_embarg`: Data do embargo (dd/mm/yy HH:MM:SS)
- `qtd_area_d`: Área desmatada (ha)
- `qtd_area_e`: Área embargada (ha)
- `sit_desmat`: Situação (2 valores únicos)

**COMEX (Exportações/Importações):**
- `CO_ANO`: Ano
- `SG_UF_NCM`: UF (28 estados + "ND")
- `CO_NCM`: Código NCM (8 dígitos)
- `VL_FOB`: Valor FOB (USD)
- `KG_LIQUIDO`: Peso líquido (kg)
- `CO_PAIS`: País comprador (243 únicos)

