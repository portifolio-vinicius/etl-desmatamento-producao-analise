# Validação de Dados para Sprint 4: Rota Temporal e Transição do Uso do Solo

**Data da Validação:** 29/03/2026  
**Status:** 🟡 PRONTO PARA INICIAR (com dependências pendentes)

---

## 1. Resumo Executivo

### 1.1 Contexto da Sprint 4

A **Sprint 4** tem como objetivo reconstruir a cronologia da degradação ambiental através de séries temporais, respondendo à pergunta:

> *"Qual é a rota temporal da conversão do solo? É possível provar a cronologia do crime ambiental mostrando que, na mesma coordenada, houve um alerta de degradação (DETER no ano 1), seguido de incêndio (MapBiomas Fogo no ano 2), consolidado como desmatamento (PRODES no ano 3) e transformado em pastagem (TerraClass no ano 4)?"*

### 1.2 Status Geral

| Componente | Status | Prontidão | Observações |
|------------|--------|-----------|-------------|
| Camada Silver (Sprint 1) | ✅ Concluído | 100% | Dados consolidados disponíveis |
| Camada Gold (Sprint 2) | ✅ Concluído | 100% | MVP Econômico validado |
| Dados PRODES | ❌ Pendente | 0% | **Necessário ingestão** |
| Dados DETER | ❌ Pendente | 0% | **Necessário ingestão** |
| MapBiomas Fogo | ❌ Pendente | 0% | **Necessário ingestão** |
| TerraClass | ❌ Pendente | 0% | **Necessário ingestão** |
| Dados IBAMA (Silver) | ✅ Disponível | 100% | Pode ser usado como proxy |

---

## 2. Validação da Camada Silver (Existente)

### 2.1 Dados Disponíveis

| Arquivo | Linhas | Colunas | Período | Municípios | Status |
|---------|--------|---------|---------|------------|--------|
| `pam_consolidado.parquet` | 27.505 | 14 | 2020-2024 | 5.510 | ✅ Válido |
| `pib_vab_consolidado.parquet` | 77.994 | 3 | 2010-2023 | 5.571 | ✅ Válido |
| `ppm_consolidado.parquet` | 267.264 | 4 | 2021-2024 | 5.568 | ✅ Válido |
| `embargos_por_municipio_ano.parquet` | 18.355 | 5 | 1987-2026 | 3.769 | ✅ Válido |
| `comex_por_uf_ano.parquet` | 689 | 7 | 2023-2025 | - (UF) | ✅ Válido |
| `dim_municipio.parquet` | 5.510 | 6 | - | 5.510 | ✅ Válido |
| `serie_historica_2020_2023.parquet` | 22.284 | 18 | 2020-2023 | 5.571 | ✅ Válido |

### 2.2 Schema Validado

#### `embargos_por_municipio_ano.parquet` (Proxy de Desmatamento)
```python
{
    'cod_munici': int64,        # Chave município
    'ano': int64,               # Ano do embargo
    'num_embargos': int64,      # Contagem de embargos
    'area_desmatada_ha': float64,  # Área desmatada (hectares)
    'area_embargada_ha': float64   # Área embargada (hectares)
}
```
**Período válido para análise:** 2020-2023 (4 anos)  
**Municípios com embargos:** 3.769 (67,8% do total)

#### `pib_vab_consolidado.parquet` (VAB Agropecuária)
```python
{
    'cod_ibge': int64,           # Chave município
    'ano': int64,                # Ano de referência
    'vab_agro_mil_reais': float64  # VAB em R$ mil
}
```
**Período válido:** 2020-2023  
**Cobertura:** 5.571 municípios (100%)

#### `pam_consolidado.parquet` (Produção Agrícola)
```python
{
    'chave_municipio': string,     # Chave município
    'municipio': string,           # Nome do município
    'uf': string,                  # UF
    'ano': int64,                  # Ano
    'tipo_lavoura': string,        # Temporária/Permanente
    'produto': string,             # Cultura (soja, milho, etc.)
    'area_colhida_ha': float64,    # Área colhida
    'area_plantada_ha': float64,   # Área plantada
    'valor_producao_mil_reais': float64  # Valor da produção
}
```
**Período válido:** 2020-2024  
**Produtos principais:** Soja, Milho, Arroz, Feijão, Trigo, Cana-de-açúcar

#### `ppm_consolidado.parquet` (Pecuária)
```python
{
    'cod_ibge': int64,          # Chave município
    'ano': int64,               # Ano
    'categoria': string,        # Tipo de rebanho
    'efetivo_cabecas': int64    # Quantidade de cabeças
}
```
**Categorias disponíveis:** Bovinos, Suínos, Ovinos, Caprinos, Equinos, etc.  
**Período válido:** 2021-2024 (bovinos com dados completos)

### 2.3 Métricas de Qualidade

| Métrica | Valor | Status |
|---------|-------|--------|
| Nulls em `embargos` | 0% | ✅ |
| Nulls em `vab_agro` | 0% (2020-2023) | ✅ |
| Chaves inconsistentes | 0% | ✅ |
| Período sobreposto | 2020-2023 | ✅ |
| Municípios comuns | 3.769 | ✅ |

---

## 3. Dados Pendentes de Ingestão (Pré-requisitos Sprint 4)

### 3.1 PRODES (INPE) - Corte Raso Anual

**Descrição:** Taxa oficial de desmatamento anual por corte raso (km²)  
**Período:** 1988-2024  
**Formato:** Shapefile/GeoJSON  
**Chave:** Município + Ano

**Schema Esperado:**
```python
{
    'cod_ibge': int64,           # Código IBGE 7 dígitos
    'ano': int64,                # Ano do desmatamento
    'area_desmatada_km2': float64,  # Área em km²
    'bioma': string,             # Amazônia/Cerrado/Mata Atlântica
    'municipio': string,         # Nome do município
    'uf': string                 # UF
}
```

**Fonte:** [TerraBrasilis Downloads](https://terrabrasilis.dpi.inpe.br/downloads/)  
**Status:** ❌ **NÃO INGERIDO** - Requer ETL 4.1

---

### 3.2 DETER (INPE) - Alertas em Tempo Real

**Descrição:** Alertas de desmatamento em tempo quase real  
**Período:** 2004-2025  
**Formato:** Shapefile/CSV  
**Chave:** Município + Data

**Schema Esperado:**
```python
{
    'cod_ibge': int64,           # Código IBGE 7 dígitos
    'data_alerta': date,         # Data do alerta
    'ano': int64,                # Ano extraído
    'mes': int64,                # Mês extraído
    'area_ha': float64,          # Área do alerta (hectares)
    'tipo': string,              # Desmatamento/Degradação
    'municipio': string,         # Nome do município
    'uf': string                 # UF
}
```

**Fonte:** [TerraBrasilis Downloads](https://terrabrasilis.dpi.inpe.br/downloads/)  
**Status:** ❌ **NÃO INGERIDO** - Requer ETL 4.1

---

### 3.3 MapBiomas Fogo - Ocorrências de Incêndio

**Descrição:** Cicatrizes de incêndios, indicando a "limpeza" da área  
**Período:** 1985-2024  
**Formato:** Raster/Shapefile  
**Chave:** Município + Ano

**Schema Esperado:**
```python
{
    'cod_ibge': int64,           # Código IBGE 7 dígitos
    'ano': int64,                # Ano da ocorrência
    'area_queimada_ha': float64,  # Área queimada (hectares)
    'num_ocorrencias': int64,    # Número de focos
    'municipio': string,         # Nome do município
    'uf': string                 # UF
}
```

**Fonte:** [MapBiomas Fogo](https://plataforma.brasil.mapbiomas.org/)  
**Status:** ❌ **NÃO INGERIDO** - Requer ETL 4.1

---

### 3.4 TerraClass - Uso Pós-Desmatamento

**Descrição:** Mapeamento do que ocorreu após o desmatamento  
**Período:** 2008-2022 (bienal)  
**Formato:** Shapefile/Raster  
**Chave:** Município + Ano

**Schema Esperado:**
```python
{
    'cod_ibge': int64,           # Código IBGE 7 dígitos
    'ano': int64,                # Ano de referência
    'classe_uso': string,        # Pastagem/Agricultura/Solo Exposto
    'area_ha': float64,          # Área da classe (hectares)
    'municipio': string,         # Nome do município
    'uf': string                 # UF
}
```

**Fonte:** [TerraClass Download](https://www.terraclass.gov.br/download-de-dados)  
**Status:** ❌ **NÃO INGERIDO** - Requer ETL 4.1

---

## 4. Estratégia Alternativa (Dados Disponíveis)

### 4.1 Proxy com Dados IBAMA

Como os dados PRODES/DETER não estão ingeridos, **podemos usar os embargos do IBAMA como proxy** para análise temporal:

| Vantagem | Limitação |
|----------|-----------|
| ✅ Já disponível na camada Silver | ⚠️ Não cobre todo desmatamento |
| ✅ Série temporal longa (1987-2026) | ⚠️ Viés de fiscalização |
| ✅ 3.769 municípios cobertos | ⚠️ Área embargada ≠ área desmatada |

### 4.2 Análise Possível com Dados Atuais

Com os dados existentes, podemos realizar:

1. **Série Temporal de Embargos (2020-2023)**
   - Evolução de `area_embargada_ha` por ano
   - Picos de fiscalização por município

2. **Correlação com VAB Agropecuária**
   - `area_embargada_ha` vs `vab_agro_mil_reais` ao longo do tempo
   - Latência: embargo → impacto na produção

3. **Transição Agrícola (PAM)**
   - `area_plantada_ha` antes/depois de embargos
   - Mudança de cultura: soja → milho → pastagem

4. **Expansão Pecuária (PPM)**
   - `bovinos_cabecas` vs `area_embargada_ha`
   - Intensificação vs expansão territorial

---

## 5. ETLs Necessárias para Sprint 4

### 5.1 ETL 4.1: Ingestão de Dados Espaciais Temporais

**Objetivo:** Ingerir PRODES, DETER, MapBiomas Fogo e TerraClass

```python
# Pipeline de ingestão
etl_4_1_ingestao_dados_espaciais.py
├── ingest_prodes()      # Shapefile → Parquet
├── ingest_deter()       # Shapefile → Parquet
├── ingest_mapbiomas()   # Raster → Parquet
└── ingest_terra_class() # Shapefile → Parquet
```

**Esforço estimado:** 4-6 horas  
**Dependências:** `geopandas`, `rasterio`, `shapely`

---

### 5.2 ETL 4.2: Construção da Timeline de Degradação

**Objetivo:** Ordenar eventos temporalmente por município

```python
# Pipeline de construção da timeline
etl_4_2_timeline_degradacao.py
├── load_silver_data()       # Carregar dados Silver
├── load_espacial_data()     # Carregar dados ETL 4.1
├── build_timeline()         # Ordenar: DETER → Fogo → PRODES → TerraClass
├── calculate_latency()      # Dias entre eventos
└── save_gold()              # Salvar camada Gold
```

**Esforço estimado:** 3-4 horas  
**Output:** `timeline_degradacao.parquet`

---

### 5.3 ETL 4.3: Matriz de Transição de Uso do Solo

**Objetivo:** Calcular transições entre classes de uso

```python
# Pipeline de matriz de transição
etl_4_3_matriz_transicao.py
├── load_timeline()          # Carregar timeline
├── calculate_transitions()  # Floresta → Alerta → Corte → Uso
├── aggregate_by_municipio() # Agrupar por município
└── save_gold()              # Salvar camada Gold
```

**Esforço estimado:** 2-3 horas  
**Output:** `matriz_transicao_uso.parquet`

---

## 6. Artefatos Esperados da Sprint 4

### 6.1 Camada Gold (Analítica)

| Arquivo | Descrição | Colunas Principais |
|---------|-----------|-------------------|
| `timeline_degradacao.parquet` | Sequência temporal por município | cod_ibge, ano, evento, area_ha, tipo |
| `latencia_alerta_corte.parquet` | Dias entre alerta e corte raso | cod_ibge, data_alerta, data_corte, latencia_dias |
| `matriz_transicao.parquet` | % transição floresta → uso | origem, destino, area_ha, pct_transicao |
| `recorrencia_alertas.parquet` | Municípios com alta recorrência | cod_ibge, num_alertas, area_total_ha |

### 6.2 Visualizações

| Arquivo | Descrição |
|---------|-----------|
| `timeline_degradacao_exemplo.png` | Timeline de 1 município (caso emblemático) |
| `latencia_media_por_ano.png` | Evolução da latência alerta → corte |
| `matriz_transicao_sankey.png` | Diagrama Sankey de transições |
| `map_recorrencia_alertas.png` | Mapa de calor de recorrência |

---

## 7. Critérios de Aceite da Sprint 4

| Critério | Meta | Status |
|----------|------|--------|
| Timeline reconstruída | ≥5 anos (2018-2023) | ⏳ Pendente |
| Matriz de transição | Amazônia Legal completa | ⏳ Pendente |
| Latência média | Documentada (alerta → ação) | ⏳ Pendente |
| Municípios analisados | ≥1.000 | ⏳ Pendente |
| Visualizações | 4 gráficos | ⏳ Pendente |

---

## 8. Plano de Ação Recomendado

### Fase 1: Ingestão (Dias 1-2)

1. [ ] **ETL 4.1:** Baixar PRODES vetorial do TerraBrasilis
2. [ ] **ETL 4.1:** Baixar DETER do TerraBrasilis
3. [ ] **ETL 4.1:** Baixar MapBiomas Fogo (raster ou shapefile)
4. [ ] **ETL 4.1:** Baixar TerraClass
5. [ ] **ETL 4.1:** Converter todos para GeoParquet

### Fase 2: Processamento (Dias 3-4)

6. [ ] **ETL 4.2:** Unir dados espaciais com série Silver
7. [ ] **ETL 4.2:** Construir timeline por município
8. [ ] **ETL 4.2:** Calcular latência entre eventos
9. [ ] **ETL 4.3:** Calcular matriz de transição

### Fase 3: Análise e Visualização (Dia 5)

10. [ ] Gerar 4 visualizações
11. [ ] Calcular métricas resumidas
12. [ ] Criar resumo executivo JSON
13. [ ] Documentar em README.md

---

## 9. Conclusão da Validação

### 9.1 Prontidão para Sprint 4

| Componente | Prontidão | Ação Necessária |
|------------|-----------|-----------------|
| **Dados Silver** | 🟢 100% | Nenhum - dados válidos e consolidados |
| **Dados Gold (Sprint 2)** | 🟢 100% | MVP Econômico concluído |
| **Dados Espaciais (PRODES/DETER)** | 🔴 0% | **Requer ingestão urgente** |
| **MapBiomas/TerraClass** | 🔴 0% | **Requer ingestão urgente** |
| **Scripts ETL 4.x** | 🔴 0% | **Requer desenvolvimento** |

### 9.2 Recomendação

**Opção A (Completa):** Ingerir PRODES/DETER/MapBiomas/TerraClass antes de iniciar Sprint 4
- **Prós:** Análise completa e precisa da rota temporal
- **Contras:** 4-6 horas adicionais de ETL

**Opção B (Proxy):** Usar dados IBAMA como proxy de desmatamento
- **Prós:** Sprint 4 pode iniciar imediatamente
- **Contras:** Análise limitada, não captura todo desmatamento

**Recomendação:** **Opção A** - A ingestão dos dados espaciais é crítica para a credibilidade da análise de transição do uso do solo.

---

## 10. Anexos

### 10.1 Links de Download

| Base | URL | Formato |
|------|-----|---------|
| PRODES | https://terrabrasilis.dpi.inpe.br/downloads/ | Shapefile |
| DETER | https://terrabrasilis.dpi.inpe.br/downloads/ | Shapefile |
| MapBiomas Fogo | https://plataforma.brasil.mapbiomas.org/ | Raster/Shapefile |
| TerraClass | https://www.terraclass.gov.br/download-de-dados | Shapefile/Raster |

### 10.2 Dependências Python

```
geopandas>=0.14.0
rasterio>=1.3.0
shapely>=2.0.0
fiona>=1.9.0
pyproj>=3.6.0
```

---

**Documento criado em:** 29/03/2026  
**Próxima atualização:** Após ingestão ETL 4.1
