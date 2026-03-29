# Sprint 4: Rota Temporal e Transição do Uso do Solo

**Status:** 🟡 PRONTO PARA INICIAR (aguardando ingestão de dados espaciais)  
**Data de Validação:** 29/03/2026  
**Pré-requisitos:** ✅ Sprint 1 (Silver) | ✅ Sprint 2 (Gold MVP)

---

## 1. Visão Geral

### 1.1 Objetivo da Sprint 4

Reconstruir a cronologia da degradação ambiental através de séries temporais, respondendo à pergunta central:

> *"Qual é a rota temporal da conversão do solo? É possível provar a cronologia do crime ambiental mostrando que, na mesma coordenada, houve um alerta de degradação (DETER no ano 1), seguido de incêndio (MapBiomas Fogo no ano 2), consolidado como desmatamento (PRODES no ano 3) e transformado em pastagem (TerraClass no ano 4)?"*

### 1.2 Perguntas de Análise

1. **Sequência de Degradação:** Qual é a latência média entre alerta (DETER) e corte raso (PRODES)?
2. **Matriz de Transição:** Qual percentual de floresta desmatada se transforma em pastagem vs. agricultura?
3. **Recorrência:** Quais municípios têm alta recorrência de alertas de desmatamento?
4. **Eficácia de Fiscalização:** Embargos do IBAMA precedem ou sucedem o desmatamento detectado pelo PRODES?

---

## 2. Status da Validação de Dados

### 2.1 Dados Disponíveis (✅ Validado)

| Camada | Arquivo | Status | Período | Municípios |
|--------|---------|--------|---------|------------|
| **Silver** | `embargos_por_municipio_ano.parquet` | ✅ | 1987-2026 | 3.769 |
| **Silver** | `pib_vab_consolidado.parquet` | ✅ | 2010-2023 | 5.571 |
| **Silver** | `pam_consolidado.parquet` | ✅ | 2020-2024 | 5.510 |
| **Silver** | `ppm_consolidado.parquet` | ✅ | 2021-2024 | 5.568 |
| **Silver** | `serie_historica_2020_2023.parquet` | ✅ | 2020-2023 | 5.571 |
| **Gold** | `ica_ranking.parquet` | ✅ | 2020-2023 | 5.571 |
| **Gold** | `correlacao_delta.parquet` | ✅ | 2020-2023 | - |
| **Gold** | `eficiencia_atividade.parquet` | ✅ | 2020-2023 | 5.571 |
| **Gold** | `ranking_concentracao.parquet` | ✅ | 2020-2023 | 5.571 |

### 2.2 Dados Pendentes (❌ Requer Ingestão)

| Base | Fonte | Formato | Status | Prioridade |
|------|-------|---------|--------|------------|
| **PRODES** | INPE/TerraBrasilis | Shapefile | ❌ | 🔴 Alta |
| **DETER** | INPE/TerraBrasilis | Shapefile | ❌ | 🔴 Alta |
| **MapBiomas Fogo** | MapBiomas | Raster/Shapefile | ❌ | 🟡 Média |
| **TerraClass** | INPE | Shapefile/Raster | ❌ | 🟡 Média |

### 2.3 Métricas de Qualidade (Camada Silver)

| Métrica | Valor | Status |
|---------|-------|--------|
| Nulls em `area_desmatada_ha` | 0% | ✅ |
| Nulls em `area_embargada_ha` | 0% | ✅ |
| Nulls em `vab_agro_mil_reais` (2020-2023) | 0% | ✅ |
| Chaves de integração consistentes | 100% | ✅ |
| Período comum de análise | 2020-2023 (4 anos) | ✅ |
| Municípios com dados integrados | 3.769 (67,8%) | ✅ |

---

## 3. ETLs Necessárias

### 3.1 ETL 4.1: Ingestão de Dados Espaciais Temporais

**Objetivo:** Ingerir PRODES, DETER, MapBiomas Fogo e TerraClass

**Script:** `etl_4_1_ingestao_dados_espaciais.py`

```python
# Pipeline de ingestão
def ingest_prodes():
    """
    Baixar PRODES vetorial do TerraBrasilis
    Extrair: ano, município, área desmatada (km²)
    Converter para Parquet
    """
    pass

def ingest_deter():
    """
    Baixar DETER do TerraBrasilis
    Extrair: data, município, área alerta (ha), tipo
    Converter para Parquet
    """
    pass

def ingest_mapbiomas_fogo():
    """
    Baixar MapBiomas Fogo
    Extrair: ano, município, área queimada (ha)
    Converter para Parquet
    """
    pass

def ingest_terra_class():
    """
    Baixar TerraClass
    Extrair: ano, município, classe de uso (pastagem, agricultura, etc.)
    Converter para Parquet
    """
    pass
```

**Esforço estimado:** 4-6 horas  
**Dependências:** `geopandas`, `rasterio`, `shapely`, `fiona`, `pyproj`

**Artefatos esperados:**
- `data/01_bronze/prodes_desmatamento_anual.parquet`
- `data/01_bronze/deter_alertas_diarios.parquet`
- `data/01_bronze/mapbiomas_fogo_ocorrencias.parquet`
- `data/01_bronze/terra_class_uso_parquet`

---

### 3.2 ETL 4.2: Construção da Timeline de Degradação

**Objetivo:** Ordenar eventos temporalmente por município

**Script:** `etl_4_2_timeline_degradacao.py`

```python
# Pipeline de construção da timeline
def build_timeline():
    """
    Ordenar temporalmente: DETER (alerta) → Fogo → PRODES (corte) → TerraClass (uso)
    Calcular latência entre eventos
    """
    pass

def calculate_latency():
    """
    Calcular dias entre alerta e corte raso
    Agrupar por município e ano
    """
    pass
```

**Esforço estimado:** 3-4 horas

**Artefatos esperados (Camada Gold):**
- `data/03_gold/timeline_degradacao.parquet`
- `data/03_gold/latencia_alerta_corte.parquet`

**Schema esperado:**
```python
# timeline_degradacao.parquet
{
    'cod_ibge': int64,
    'municipio': str,
    'uf': str,
    'ano': int64,
    'sequencia_eventos': str,  # Ex: "DETER_2020 → FOGO_2021 → PRODES_2021 → PASTAGEM_2022"
    'data_alerta': date,
    'data_corte': date,
    'data_fogo': date,
    'classe_final': str  # Pastagem, Agricultura, Solo Exposto
}

# latencia_alerta_corte.parquet
{
    'cod_ibge': int64,
    'ano': int64,
    'data_alerta': date,
    'data_corte_raso': date,
    'latencia_dias': int64,
    'media_latencia_municipio': float64
}
```

---

### 3.3 ETL 4.3: Matriz de Transição de Uso do Solo

**Objetivo:** Calcular transições entre classes de uso

**Script:** `etl_4_3_matriz_transicao.py`

```python
# Pipeline de matriz de transição
def calculate_transitions():
    """
    Calcular transições: Floresta → Alerta → Corte → Pastagem/Agricultura
    Percentual de transição por município
    """
    pass

def aggregate_by_municipio():
    """
    Agrupar transições por município
    Calcular percentuais
    """
    pass
```

**Esforço estimado:** 2-3 horas

**Artefatos esperados (Camada Gold):**
- `data/03_gold/matriz_transicao_uso.parquet`
- `data/03_gold/recorrencia_alertas.parquet`

**Schema esperado:**
```python
# matriz_transicao_uso.parquet
{
    'cod_ibge': int64,
    'municipio': str,
    'uf': str,
    'ano_inicial': int64,
    'ano_final': int64,
    'classe_origem': str,  # Floresta
    'classe_destino': str,  # Pastagem, Agricultura, Solo Exposto
    'area_ha': float64,
    'pct_transicao': float64
}

# recorrencia_alertas.parquet
{
    'cod_ibge': int64,
    'municipio': str,
    'uf': str,
    'num_alertas_total': int64,
    'num_alertas_2020_2023': int64,
    'area_total_alertas_ha': float64,
    'recorrencia_alta': bool  # True se > média + 2std
}
```

---

## 4. Visualizações Esperadas

| Arquivo | Descrição | Tipo |
|---------|-----------|------|
| `timeline_degradacao_exemplo.png` | Timeline de 1 município emblemático | Linha |
| `latencia_media_por_ano.png` | Evolução da latência alerta → corte | Barra |
| `matriz_transicao_sankey.png` | Diagrama Sankey de transições | Sankey |
| `map_recorrencia_alertas.png` | Mapa de calor de recorrência | Mapa |

---

## 5. Critérios de Aceite

| Critério | Meta | Status |
|----------|------|--------|
| Timeline reconstruída | ≥5 anos (2018-2023) | ⏳ |
| Matriz de transição | Amazônia Legal completa | ⏳ |
| Latência média | Documentada (alerta → ação) | ⏳ |
| Municípios analisados | ≥1.000 | ⏳ |
| Visualizações | 4 gráficos | ⏳ |
| Código reprodutível | Scripts testados | ⏳ |

---

## 6. Plano de Execução

### Fase 1: Ingestão (Dias 1-2)

- [ ] **ETL 4.1:** Baixar PRODES vetorial do TerraBrasilis
- [ ] **ETL 4.1:** Baixar DETER do TerraBrasilis
- [ ] **ETL 4.1:** Baixar MapBiomas Fogo
- [ ] **ETL 4.1:** Baixar TerraClass
- [ ] **ETL 4.1:** Converter todos para Parquet

### Fase 2: Processamento (Dias 3-4)

- [ ] **ETL 4.2:** Unir dados espaciais com série Silver
- [ ] **ETL 4.2:** Construir timeline por município
- [ ] **ETL 4.2:** Calcular latência entre eventos
- [ ] **ETL 4.3:** Calcular matriz de transição
- [ ] **ETL 4.3:** Calcular recorrência de alertas

### Fase 3: Análise e Visualização (Dia 5)

- [ ] Gerar 4 visualizações
- [ ] Calcular métricas resumidas
- [ ] Criar resumo executivo JSON
- [ ] Documentar resultados

---

## 7. Estratégia Alternativa (Dados Disponíveis)

Caso a ingestão dos dados espaciais não seja possível imediatamente, podemos usar **embargos do IBAMA como proxy** de desmatamento:

### 7.1 Análise Possível com Dados Atuais

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

### 7.2 Limitações da Estratégia Alternativa

| Vantagem | Limitação |
|----------|-----------|
| ✅ Já disponível na camada Silver | ⚠️ Não cobre todo desmatamento |
| ✅ Série temporal longa (1987-2026) | ⚠️ Viés de fiscalização |
| ✅ 3.769 municípios cobertos | ⚠️ Área embargada ≠ área desmatada |

---

## 8. Links Úteis

### 8.1 Fontes de Dados

| Base | URL | Formato |
|------|-----|---------|
| PRODES | https://terrabrasilis.dpi.inpe.br/downloads/ | Shapefile |
| DETER | https://terrabrasilis.dpi.inpe.br/downloads/ | Shapefile |
| MapBiomas Fogo | https://plataforma.brasil.mapbiomas.org/ | Raster/Shapefile |
| TerraClass | https://www.terraclass.gov.br/download-de-dados | Shapefile/Raster |

### 8.2 Dependências Python

```
geopandas>=0.14.0
rasterio>=1.3.0
shapely>=2.0.0
fiona>=1.9.0
pyproj>=3.6.0
pandas>=2.0.0
numpy>=1.24.0
matplotlib>=3.7.0
seaborn>=0.12.0
```

---

## 9. Referências

- **Sprint 2 (MVP Econômico):** `/fase_2_execucao/sprint_2_gold_mvp/README.md`
- **Plano de Implementação:** `/docs/plano_implementacao_v2.md`
- **Estrutura de Análise:** `/docs/estrutura_analise.md`
- **Validação de Dados:** `VALIDACAO_DADOS_SPRINT4.md`

---

**Última atualização:** 29/03/2026  
**Próxima milestone:** Ingestão ETL 4.1 de dados espaciais
