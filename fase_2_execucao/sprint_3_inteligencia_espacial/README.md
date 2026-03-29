# Sprint 3: Inteligência Espacial e Vazamento (Spillover)

**Status:** 🔄 EM EXECUÇÃO (Data: 29/03/2026)  
**Pré-requisitos:** ✅ Sprint 1 (Silver) | ✅ Sprint 2 (Gold MVP)

---

## 1. Visão Geral

### 1.1 Objetivo da Sprint 3

Integrar geometrias (GeoParquet) e analisar a influência territorial dos embargos do IBAMA. O foco é entender como a fiscalização se distribui no espaço e se existe um efeito de "vazamento" (spillover) de desmatamento para áreas adjacentes aos embargos.

### 1.2 Perguntas de Análise

1. **Densidade de Fiscalização:** Qual é a relação entre ha embargados e a área total do município?
2. **Buffer e Adjacência:** Áreas adjacentes (buffer de 10km) a embargos apresentam maior ou menor atividade produtiva/desmatamento?
3. **Distribuição Geográfica:** Onde estão os clusters de embargos e como eles se sobrepõem às áreas de alta produtividade (VAB)?

---

## 2. Dados Utilizados

| Base | Fonte | Formato | Caminho |
|------|-------|---------|---------|
| **Embargos (Geometria)** | IBAMA | GeoParquet | `data/01_bronze/ibama/ibama_embargos/embargos_ibama_full.geoparquet` |
| **Municípios (Referência)** | IBGE | Parquet | `data/02_silver/dim_municipio.parquet` |
| **Série Silver (Tabular)** | Consolidado | Parquet | `data/02_silver/serie_historica_2020_2023.parquet` |

---

## 3. ETLs e Análises

### 3.1 ETL 3.1: Processamento de Geometrias IBAMA
**Script:** `etl_3_1_geometrias_ibama.py`
- Carregar GeoParquet do IBAMA.
- Validar e corrigir geometrias.
- Extrair centroides e calcular áreas reais em hectares.
- Unir com dados tabulares via `cod_munici`.

### 3.2 ETL 3.2: Ingestão de Dados Espaciais Externos
**Script:** `etl_3_2_ingestao_espacial.py`
- Ingerir ou simular dados de PRODES/DETER para cruzamento espacial.
- Converter shapefiles para GeoParquet na camada Bronze/Silver.

### 3.3 Análise 3.3: Buffer e Spillover
**Script:** `analise_3_3_buffer_spillover.py`
- Criar buffers de 10km ao redor dos polígonos de embargo.
- Identificar municípios e áreas vizinhas impactadas.
- Analisar a dinâmica de desmatamento/VAB nessas zonas de amortecimento.

---

## 4. Artefatos Esperados

- `data/02_silver/espacial/embargos_com_geometria.parquet` (GeoParquet Silver)
- `data/03_gold/espacial/spillover_adjacencia.parquet` (Tabela de Impacto)
- `fase_2_execucao/sprint_3_inteligencia_espacial/visualizacoes/mapa_densidade_embargos.png`

---

## 5. Critérios de Aceite

- [ ] 100% dos embargos com geometria válida processados.
- [ ] Buffer de 10km aplicado e analisado.
- [ ] Mapa de densidade municipal gerado.
- [ ] Relatório técnico de spillover concluído.
