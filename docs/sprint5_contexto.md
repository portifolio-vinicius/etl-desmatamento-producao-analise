# Sprint 5 - Rastreabilidade da Cadeia Global (Commodities)

## 📊 Status e Contexto dos Dados

**Data de Análise:** 29/03/2026  
**Status:** ⏳ Pendente de Implementação  
**Pré-requisitos:** ✅ Sprint 1 (Silver) e ✅ Sprint 2 (MVP Econômico) concluídas

---

## 1. Resumo Executivo do Projeto

### Sprints Concluídas

| Sprint | Status | Entregáveis |
|--------|--------|-------------|
| **Sprint 0:** Ingestão Parquet | ✅ 100% | 5 fontes em Parquet (COMEX, IBAMA, PAM, PIB, PPM) |
| **Sprint 0.5:** Data Quality | ✅ 100% | 5 notebooks de resumo + metadados |
| **Sprint 1:** Limpeza e Join | ✅ 100% | 6 arquivos Silver padronizados |
| **Sprint 2:** MVP Econômico | ✅ 100% | 8 análises gold + 6 visualizações |
| **Sprint 3:** Inteligência Espacial | ⏳ 0% | Pendente |
| **Sprint 4:** Rota Temporal | ⏳ 0% | Pendente |
| **Sprint 5:** Cadeia Global | ⏳ 0% | **FOCO ATUAL** |

### Principais Descobertas (Sprint 2)

- **ICA:** Apenas 39 municípios (0.7%) têm desmatamento e VAB positivos simultaneamente
- **Correlação ΔDesmatamento × ΔVAB:** -0.0099 (p=0.20) - **fraca/nula**
- **Overlap Top 100:** Apenas 7% dos municípios estão em ambas listas
- **Conclusão:** Desmatamento **NÃO** gera eficiência econômica

---

## 2. Objetivo da Sprint 5

**Pergunta Central:** *"Para onde vai a produção associada ao desmatamento?"*

### Metas Analíticas

1. **Mapear NCMs de commodities** (soja, milho, carne bovina, café, açúcar, celulose, madeira)
2. **Ranking de UFs exportadoras** vs UFs com maior desmatamento
3. **Matriz de destinos** (países compradores por commodity)
4. **Eficiência ambiental da exportação:** USD exportado / ha desmatado

---

## 3. Dados Disponíveis para Sprint 5

### 3.1 COMEX Silver (Pronto para Uso)

**Arquivo:** `data/02_silver/comex_por_uf_ano.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 689 |
| UFs | 29 (inclui "EX" e "ND") |
| Anos | 2023, 2024, 2025 |
| Commodities | Açúcar, Carne Bovina, Café, Celulose, Milho, Soja, Madeira, Outros |
| Colunas | `uf`, `ano`, `tipo_operacao`, `commodity`, `vob_fob_usd`, `peso_kg`, `num_operacoes` |

**✅ Vantagem:** Dados já agregados por UF-ano-commodity

### 3.2 COMEX Bronze (Dados Brutos)

**Arquivos:** `data/01_bronze/comex/comex_stat/EXP_{2023,2024,2025}.parquet`

| Métrica | Valor |
|---------|-------|
| Registros (2023) | 1.563.659 |
| Colunas | 11 |
| NCMs únicos | 8.014 |
| Países únicos | 243 |
| UFs únicas | 28 |

**Colunas disponíveis:**
- `CO_ANO`: Ano
- `CO_MES`: Mês
- `CO_NCM`: Código NCM (8 dígitos)
- `CO_PAIS`: Código do país (comprador)
- `SG_UF_NCM`: UF exportadora
- `KG_LIQUIDO`: Peso líquido (kg)
- `VL_FOB`: Valor FOB (USD)

**⚠️ Limitação:** Não há tabela de referência para NCM → necessário criar mapeamento manual

### 3.3 IBAMA Silver (Para Cruzamento)

**Arquivo:** `data/02_silver/embargos_por_municipio_ano.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 18.355 |
| Municípios | 3.769 |
| Anos | 1987-2026 |
| Colunas | `cod_munici`, `ano`, `num_embargos`, `area_desmatada_ha`, `area_embargada_ha` |

### 3.4 Dimensão de Municípios

**Arquivo:** `data/02_silver/dim_municipio.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 5.570 |
| Colunas | `cod_ibge`, `uf`, `amazonia_legal`, `regiao`, `municipio` |

**✅ Observação:** `cod_ibge` está 100% preenchido (int64).  
**⚠️ Atenção:** A coluna `municipio` contém strings vazias, mas para análise por UF isso não é impeditivo.

---

## 4. Limitações Críticas Identificadas

### 4.1 Ausência de Município no COMEX

**Problema:** COMEX possui apenas UF (`SG_UF_NCM`), não permite rastreio municipal direto.

**Impacto:** 
- ❌ Não é possível cruzar diretamente município desmatador × exportador
- ✅ Rastreio possível apenas no nível **estadual**

**Soluções Alternativas (fora do escopo atual):**
- Nota Fiscal Eletrônica (SEFAZ)
- Guia de Trânsito Animal (GTA)
- Cadastro Ambiental Rural (CAR)

### 4.2 Ausência de Tabela de Referência NCM

**Problema:** Não há tabela de descrição de NCMs nos dados brutos.

**Solução:** Criar mapeamento manual baseado nos códigos NCM de interesse:

```python
NCM_COMMODITIES = {
    # Soja
    '12010000': 'Soja em grão',
    '12011000': 'Soja para semeadura',
    '12019000': 'Outras soja',
    
    # Milho
    '10051000': 'Milho para semeadura',
    '10059000': 'Outros milhos',
    
    # Carne Bovina
    '02011000': 'Carne bovina fresca/congelada, carcaça',
    '02012000': 'Carne bovina fresca/congelada, pedaços',
    '02013000': 'Carne bovina fresca/congelada, desossada',
    '02021000': 'Carne bovina congelada, carcaça',
    '02022000': 'Carne bovina congelada, pedaços',
    '02023000': 'Carne bovina congelada, desossada',
    
    # Café
    '09011100': 'Café não torrado, não descafeinado',
    '09011200': 'Café não torrado, descafeinado',
    '09012100': 'Café torrado, não descafeinado',
    '09012200': 'Café torrado, descafeinado',
    
    # Açúcar
    '17011300': 'Açúcar de cana, bruto',
    '17011400': 'Outros açúcares de cana, bruto',
    '17019100': 'Açúcar de cana, refinado',
    '17019900': 'Outros açúcares de cana',
    
    # Celulose
    '47032100': 'Pasta de celulose, branqueada',
    '47032900': 'Pasta de celulose, não branqueada',
    
    # Madeira
    '44011000': 'Lenha',
    '44031000': 'Madeira em bruto, tratada',
    '44032000': 'Madeira em bruto, não tratada',
    '44061000': 'Dormentes de madeira',
    '44071000': 'Madeira serrada, conífera',
    '44072100': 'Madeira serrada, mogno',
    '44072200': 'Madeira serrada, virola',
    '44072500': 'Madeira serrada, tropical',
}
```

### 4.3 Ausência de Tabela de Países

**Problema:** COMEX usa `CO_PAIS` (código inteiro), sem descrição.

**Top 10 Códigos de País (2023-2025):**

| Código | Frequência 2023 | Frequência 2024 | Frequência 2025 | Provável País |
|--------|-----------------|-----------------|-----------------|---------------|
| 586 | 99.864 | 104.016 | 108.944 | China |
| 249 | 96.546 | 100.473 | 97.777 | Estados Unidos |
| 63 | 84.437 | 92.049 | 107.737 | Argentina |
| 845 | 68.292 | 70.857 | 73.599 | Holanda |
| 580 | 65.994 | 70.776 | 72.586 | Japão |
| 476 | 64.134 | 65.652 | 69.127 | Espanha |
| 158 | 63.861 | 64.900 | 69.392 | Chile |
| 434 | 57.495 | 63.957 | 72.312 | Itália |
| 169 | 48.326 | 51.303 | 54.412 | Colômbia |
| 493 | 46.598 | 48.503 | 49.446 | México |

**Solução:** Criar tabela de referência manual ou buscar API do MDIC

---

## 5. Plano de Execução da Sprint 5

### 5.1 ETL 5.1: Refinar Mapeamento de Commodities

**Ações:**
- [ ] Criar tabela de referência NCM → commodity
- [ ] Filtrar dados brutos COMEX por NCMs de interesse
- [ ] Validar mapeamento com amostra de dados
- [ ] Re-agregar por UF-ano-commodity (se necessário)

**Script:** `fase_2_execucao/sprint_5_cadeia_global/01_etl_mapeamento_ncm.py`

**Output:** `data/02_silver/ncm_commodity_reference.parquet`

### 5.2 ETL 5.2: Criar Tabela de Países

**Ações:**
- [ ] Listar todos os `CO_PAIS` únicos (2023-2025)
- [ ] Mapear código → nome do país (manual ou API)
- [ ] Salvar como tabela de referência

**Script:** `fase_2_execucao/sprint_5_cadeia_global/02_etl_paises_referencia.py`

**Output:** `data/02_silver/pais_reference.parquet`

### 5.3 Análise 5.3: Ranking de UFs Exportadoras

**Ações:**
- [ ] Agregar exportações por UF-commodity (2023-2024)
- [ ] Calcular: total USD, total kg, operações
- [ ] Rank: Top 10 UFs por commodity
- [ ] Visualizar: heatmap UF × commodity

**Script:** `fase_2_execucao/sprint_5_cadeia_global/03_analise_ranking_uf.py`

**Output:** `data/03_gold/ranking_uf_exportadora.parquet`

### 5.4 Análise 5.4: Overlap UF Exportadora × UF Desmatada

**Ações:**
- [ ] Agregar desmatamento por UF (IBAMA → UF via dim_municipio)
- [ ] Rank: Top 10 UFs com maior desmatamento
- [ ] Cruzar: UF exportadora de soja × UF com desmatamento
- [ ] Calcular correlação: exportação × desmatamento

**Script:** `fase_2_execucao/sprint_5_cadeia_global/04_analise_overlap_uf.py`

**Output:** `data/03_gold/uf_exportacao_vs_desmatamento.parquet`

### 5.5 Análise 5.5: Matriz de Destino (Países)

**Ações:**
- [ ] Juntar COMEX com tabela de países
- [ ] Agregar exportações por país-commodity
- [ ] Top 20 países compradores por commodity
- [ ] Visualizar: mapa de fluxo exportador

**Script:** `fase_2_execucao/sprint_5_cadeia_global/05_analise_matriz_paises.py`

**Output:** `data/03_gold/matriz_destino_exportacao.parquet`

### 5.6 Análise 5.6: Eficiência Ambiental da Exportação

**Ações:**
- [ ] Calcular métrica: `USD_exportado / ha_desmatado` por UF
- [ ] Rank: UFs mais eficientes (mais USD, menos desmatamento)
- [ ] Rank: UFs menos eficientes (menos USD, mais desmatamento)
- [ ] Scatter plot: exportação × desmatamento

**Script:** `fase_2_execucao/sprint_5_cadeia_global/06_analise_eficiencia_ambiental.py`

**Output:** `data/03_gold/eficiencia_ambiental_exportacao.parquet`

---

## 6. Artefatos Esperados da Sprint 5

### Camada Gold (Analítica)

| Arquivo | Descrição | Métricas Principais |
|---------|-----------|---------------------|
| `ranking_uf_exportadora.parquet` | Top UFs por commodity | USD, kg, operações |
| `uf_exportacao_vs_desmatamento.parquet` | Overlap exportação × desmatamento | Correlação, ranks |
| `matriz_destino_exportacao.parquet` | Países compradores por commodity | Top 20 destinos |
| `eficiencia_ambiental_exportacao.parquet` | USD/ha desmatado por UF | Ranking eficiência |
| `resumo_executivo_sprint5.json` | Resumo das métricas | Todos KPIs |

### Visualizações

| Arquivo | Descrição |
|---------|-----------|
| `heatmap_uf_commodity.png` | Heatmap: UF × commodity (USD) |
| `bar_ranking_uf_soja.png` | Barras: Top 10 UFs exportadoras de soja |
| `scatter_exportacao_desmatamento.png` | Scatter: exportação × desmatamento por UF |
| `mapa_fluxo_exportacao.png` | Mapa: fluxo Brasil → países |
| `box_eficiencia_ambiental.png` | Boxplot: distribuição USD/ha por UF |

---

## 7. Estrutura de Diretórios da Sprint 5

```
fase_2_execucao/
└── sprint_5_cadeia_global/
    ├── 01_etl_mapeamento_ncm.py
    ├── 02_etl_paises_referencia.py
    ├── 03_analise_ranking_uf.py
    ├── 04_analise_overlap_uf.py
    ├── 05_analise_matriz_paises.py
    ├── 06_analise_eficiencia_ambiental.py
    ├── README.md
    └── notebooks/
        ├── exploracao_comex.ipynb
        └── validacao_mapeamentos.ipynb
```

---

## 8. Critérios de Aceite da Sprint 5

- [ ] 100% das commodities mapeadas por NCM (7 tipos + outros)
- [ ] Tabela de países criada (243+ países)
- [ ] Ranking de UFs exportadoras documentado
- [ ] Overlap UF exportadora × desmatamento calculado
- [ ] Matriz de destinos (países) visualizada
- [ ] Métrica de eficiência ambiental calculada
- [ ] 5 visualizações claras e publicáveis
- [ ] Resumo executivo em JSON

---

## 9. Dependências e Riscos

### Dependências

| Item | Status | Observação |
|------|--------|------------|
| COMEX Silver | ✅ Disponível | 689 registros, 8 commodities |
| IBAMA Silver | ✅ Disponível | 18.355 registros, 3.769 municípios |
| Dimensão Municípios | ✅ Disponível | 5.510 municípios, UF mapeada |
| Tabela NCM | ❌ Pendente | Criar mapeamento manual |
| Tabela Países | ❌ Pendente | Criar mapeamento manual |

### Riscos

| Risco | Impacto | Mitigação |
|-------|---------|-----------|
| COMEX sem município | Alto | Aceitar limitação, trabalhar no nível UF |
| NCM sem descrição | Médio | Criar mapeamento manual baseado em documentação MDIC |
| Países sem descrição | Baixo | Buscar tabela oficial no site do MDIC/Comex Stat |
| Dados 2025 incompletos | Baixo | Focar análise em 2023-2024 (anos completos) |

---

## 10. Próximos Passos Imediatos

1. **Criar estrutura de diretórios** da Sprint 5
2. **Implementar ETL de mapeamento NCM** (prioridade máxima)
3. **Implementar ETL de países** (prioridade alta)
4. **Executar análises 5.3 a 5.6** em sequência
5. **Gerar visualizações** e resumo executivo

---

## 11. Anexos: Código de Exemplo para Início

### 11.1 Carregar Dados Silver

```python
import pandas as pd

# COMEX
comex = pd.read_parquet('data/02_silver/comex_por_uf_ano.parquet')

# IBAMA (agregar por UF)
ibama = pd.read_parquet('data/02_silver/embargos_por_municipio_ano.parquet')
dim_mun = pd.read_parquet('data/02_silver/dim_municipio.parquet')

# Juntar IBAMA com dimensão para obter UF
ibama_uf = ibama.merge(dim_mun[['cod_ibge', 'uf']], 
                       left_on='cod_munici', 
                       right_on='cod_ibge', 
                       how='left')

# Agregar por UF-ano
ibama_uf_agg = ibama_uf.groupby(['uf', 'ano']).agg({
    'num_embargos': 'sum',
    'area_desmatada_ha': 'sum',
    'area_embargada_ha': 'sum'
}).reset_index()
```

### 11.2 Mapeamento NCM → Commodity

```python
NCM_COMMODITIES = {
    # Soja
    '12010000': 'Soja', '12011000': 'Soja', '12019000': 'Soja',
    # Milho
    '10051000': 'Milho', '10059000': 'Milho',
    # Carne Bovina
    '02011000': 'Carne Bovina', '02012000': 'Carne Bovina',
    '02013000': 'Carne Bovina', '02021000': 'Carne Bovina',
    '02022000': 'Carne Bovina', '02023000': 'Carne Bovina',
    # Café
    '09011100': 'Café', '09011200': 'Café',
    '09012100': 'Café', '09012200': 'Café',
    # Açúcar
    '17011300': 'Açúcar', '17011400': 'Açúcar',
    '17019100': 'Açúcar', '17019900': 'Açúcar',
    # Celulose
    '47032100': 'Celulose', '47032900': 'Celulose',
    # Madeira
    '44011000': 'Madeira', '44031000': 'Madeira',
    '44032000': 'Madeira', '44071000': 'Madeira',
    '44072100': 'Madeira', '44072200': 'Madeira',
    '44072500': 'Madeira',
}
```

---

**Documento criado em:** 29/03/2026  
**Próxima atualização:** Após implementação da ETL 5.1
