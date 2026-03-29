# Sprint 6: O Peso da Fiscalização (Efetividade dos Embargos)

## 📊 Status: 🟢 PRONTO PARA EXECUÇÃO (29/03/2026)

**Objetivo:** Medir se os embargos do IBAMA reduzem desmatamento e produção agropecuária nos municípios embargados.

**Período de Análise:** 2021-2023 (3 anos)
**Municípios com Embargos:** 3.769
**Total de Embargos (2021-2023):** ~15.000 (estimado)

---

## 🎯 Perguntas de Pesquisa

1. **Embargos reduzem desmatamento?** → Comparar área desmatada antes/depois
2. **Embargos impactam produção?** → Δ VAB, Δ área plantada, Δ rebanho bovino
3. **Qual o tempo de reação?** → Latência entre embargo e queda de produção
4. **Quem são os reincidentes?** → Top 100 CPF/CNPJ com mais embargos
5. **Embargos são efetivos?** → % de áreas regularizadas vs mantidas

---

## 📁 Dados Disponíveis

### IBAMA - Embargos Ambientais

| Arquivo | Registros | Período | Municípios | Status |
|---------|-----------|---------|------------|--------|
| Bronze: `embargos_ibama_tabular.parquet` | 88.586 | 1987-2025 | 3.769 | ✅ |
| Silver: `embargos_por_municipio_ano.parquet` | 18.355 | 1987-2026 | 3.769 | ✅ |

**Campos Críticos Validados:**
- `cpf_cnpj_e`: 90% preenchido (79.708 registros, 65.653 únicos)
- `sit_desmat`: 100% preenchido (2 valores: 'D', 'N')
- `dat_embarg`: 100% preenchido (formato: dd/mm/yy HH:MM:SS)

### PIB/PAM/PPM - Produção Agropecuária

| Fonte | Arquivo | Período | Municípios | Status |
|-------|---------|---------|------------|--------|
| PIB | `pib_vab_consolidado.parquet` | 2010-2023 | 5.571 | ✅ |
| PAM | `pam_consolidado.parquet` | 2020-2024 | 5.510 | ✅ |
| PPM | `ppm_consolidado.parquet` | 2021-2024 | 5.568 | ✅ |
| Série Histórica | `serie_historica_2020_2023.parquet` | 2020-2023 | 5.571 | ✅ |

---

## 🚀 ETLs Necessárias

### ETL 6.1: Séries Temporais de Fiscalização

**Script:** `etl_6_1_fiscalizacao_series.py`

**Objetivo:** Criar base analítica de embargos por município-ano com métricas de intensidade.

**Tarefas:**
1. Carregar `embargos_por_municipio_ano.parquet`
2. Calcular métricas por município-ano:
   - `num_embargos` (já existe)
   - `area_embargada_ha` (já existe)
   - `embargos_per_1000km2` = num_embargos / area_municipio_km2 * 1000
   - `outlier_fiscalizacao` = True se > 95º percentil
3. Unir com `dim_municipio` para área territorial
4. Salvar: `data/03_gold/fiscalizacao_series_temporais.parquet`

**Output Esperado:**
```python
{
    'cod_ibge': int64,
    'ano': int64,
    'num_embargos': int64,
    'area_embargada_ha': float64,
    'embargos_per_1000km2': float64,
    'outlier_fiscalizacao': bool
}
```

---

### ETL 6.2: Impacto na Produção (Antes vs Depois)

**Script:** `etl_6_2_impacto_producao.py`

**Objetivo:** Medir mudança na produção agropecuária após embargos.

**Tarefas:**
1. Identificar municípios com embargos em 2021-2022
2. Para cada município:
   - `ano_0` = primeiro ano com embargo
   - janela = [ano_0-2, ano_0-1, ano_0, ano_0+1, ano_0+2]
3. Extrair séries temporais:
   - VAB Agro (PIB)
   - Área plantada (PAM - soja, milho)
   - Bovinos (PPM)
4. Calcular variações:
   - `delta_vab_pct` = (vab_depois - vab_antes) / vab_antes * 100
   - `delta_area_pct` = (area_depois - area_antes) / area_antes * 100
   - `delta_bovinos_pct` = (bovinos_depois - bovinos_antes) / bovinos_antes * 100
5. Teste t pareado: antes vs depois
6. Salvar: `data/03_gold/impacto_embargo_producao.parquet`

**Output Esperado:**
```python
{
    'cod_ibge': int64,
    'ano_embargo': int64,
    'vab_antes_mil_reais': float64,
    'vab_depois_mil_reais': float64,
    'delta_vab_pct': float64,
    'area_plantada_antes_ha': float64,
    'area_plantada_depois_ha': float64,
    'delta_area_pct': float64,
    'bovinos_antes_cabecas': int64,
    'bovinos_depois_cabecas': int64,
    'delta_bovinos_pct': float64,
    'p_valor_vab': float64,
    'p_valor_area': float64,
    'p_valor_bovinos': float64
}
```

---

### ETL 6.3: Recorrência de Embargados

**Script:** `etl_6_3_reincidentes.py`

**Objetivo:** Identificar reincidentes em crimes ambientais.

**Tarefas:**
1. Carregar `embargos_ibama_tabular.parquet` (Bronze)
2. Filtrar `cpf_cnpj_e` não nulo (90% disponibilidade)
3. Agrupar por CPF/CNPJ:
   - `num_embargos`: count
   - `anos_com_embargo`: nunique(ano)
   - `recurrence_rate`: num_embargos / anos_com_embargo
   - `uf_principal`: mode(uf)
   - `area_total_embargada`: sum(qtd_area_e)
4. Ordenar por `recurrence_rate`
5. Salvar: `data/03_gold/reincidentes_embargos.parquet`

**Output Esperado:**
```python
{
    'cpf_cnpj': string,
    'num_embargos': int64,
    'anos_com_embargo': int64,
    'recurrence_rate': float64,
    'uf_principal': string,
    'area_total_embargada_ha': float64,
    'municipios_frequentes': list
}
```

---

### ETL 6.4: Situação do Desmatamento

**Script:** `etl_6_4_status_regularizacao.py`

**Objetivo:** Analisar status de regularização dos embargos.

**Tarefas:**
1. Carregar `embargos_ibama_tabular.parquet` (Bronze)
2. Analisar `sit_desmat`:
   - Mapear valores: 'D' = ?, 'N' = ?
   - Calcular distribuição por ano
3. Salvar: `data/03_gold/status_regularizacao_embargos.parquet`

**Output Esperado:**
```python
{
    'situacao': string,  # 'D' ou 'N'
    'descricao': string,
    'count': int64,
    'pct': float64,
    'count_por_ano': dict
}
```

---

## 📊 Artefatos Esperados

### Camada Gold (Analítica)

| Arquivo | Descrição | Tamanho Estimado |
|---------|-----------|------------------|
| `fiscalizacao_series_temporais.parquet` | Embargos/ano por município | ~1 MB |
| `impacto_embargo_producao.parquet` | Δ Produção pré/pós embargo | ~500 KB |
| `reincidentes_embargos.parquet` | Embargados reincidentes | ~2 MB |
| `status_regularizacao_embargos.parquet` | Situação e tempo | ~10 KB |
| `resumo_sprint6.json` | Resumo executivo | ~1 KB |

### Visualizações

| Arquivo | Descrição | Tipo |
|---------|-----------|------|
| `serie_temporal_embargos.png` | Evolução de embargos 2020-2023 | Linha |
| `embargos_por_uf.png` | Mapa de calor por UF | Coroplético |
| `impacto_producao_boxplot.png` | Boxplot: Antes vs Depois | Boxplot |
| `delta_vab_histogram.png` | Distribuição de ΔVAB | Histograma |
| `top50_reincidentes.png` | Barras: Top 50 reincidentes | Barras |
| `map_reincidentes_uf.png` | Mapa de reincidentes por UF | Coroplético |
| `status_regularizacao_pie.png` | Pizza: Situação dos embargos | Pizza |

---

## ✅ Critérios de Aceite

| Critério | Meta | Status |
|----------|------|--------|
| Séries temporais | 100% municípios com embargos | ⏳ Pendente |
| Impacto na produção | p-valor < 0.05 para pelo menos 1 métrica | ⏳ Pendente |
| Reincidentes identificados | Top 100 CPF/CNPJ | ⏳ Pendente |
| Status regularização | 100% embargos classificados | ⏳ Pendente |
| Visualizações | 7 gráficos | ⏳ Pendente |
| Resumo executivo | JSON completo | ⏳ Pendente |

---

## 📂 Estrutura de Diretórios

```
fase_2_execucao/
└── sprint_6_fiscalizacao/
    ├── VALIDACAO_DADOS_SPRINT6.md       # Validação detalhada
    ├── resumo_contexto_sprint6.json     # Resumo em JSON
    ├── README.md                        # Este arquivo
    ├── etl_6_1_fiscalizacao_series.py   # ETL 6.1
    ├── etl_6_2_impacto_producao.py      # ETL 6.2
    ├── etl_6_3_reincidentes.py          # ETL 6.3
    ├── etl_6_4_status_regularizacao.py  # ETL 6.4
    ├── sprint6_analise.py               # Análise principal
    ├── sprint6_visualizacoes.py         # Visualizações
    └── requirements.txt                 # Dependências

data/
├── 01_bronze/
│   └── ibama/ibama_embargos/
│       └── embargos_ibama_tabular.parquet
├── 02_silver/
│   ├── embargos_por_municipio_ano.parquet
│   ├── pib_vab_consolidado.parquet
│   ├── pam_consolidado.parquet
│   ├── ppm_consolidado.parquet
│   └── serie_historica_2020_2023.parquet
└── 03_gold/
    ├── fiscalizacao_series_temporais.parquet
    ├── impacto_embargo_producao.parquet
    ├── reincidentes_embargos.parquet
    ├── status_regularizacao_embargos.parquet
    ├── resumo_sprint6.json
    └── visualizacoes/
        ├── serie_temporal_embargos.png
        ├── impacto_producao_boxplot.png
        └── ...
```

---

## 🧪 Como Reproduzir

```bash
# 1. Ativar ambiente virtual
cd /home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS
source venv/bin/activate

# 2. Validar dados de entrada
python -c "
import pandas as pd
df = pd.read_parquet('data/02_silver/embargos_por_municipio_ano.parquet')
print(f'Embargos: {df.shape[0]} registros')
print(f'Período: {df.ano.min()}-{df.ano.max()}')
print(f'Municípios: {df.cod_munici.nunique()}')
"

# 3. Executar ETLs em sequência
python fase_2_execucao/sprint_6_fiscalizacao/etl_6_1_fiscalizacao_series.py
python fase_2_execucao/sprint_6_fiscalizacao/etl_6_2_impacto_producao.py
python fase_2_execucao/sprint_6_fiscalizacao/etl_6_3_reincidentes.py
python fase_2_execucao/sprint_6_fiscalizacao/etl_6_4_status_regularizacao.py

# 4. Executar análise principal
python fase_2_execucao/sprint_6_fiscalizacao/sprint6_analise.py

# 5. Gerar visualizações
python fase_2_execucao/sprint_6_fiscalizacao/sprint6_visualizacoes.py
```

---

## 📝 Dependências

```
pandas>=2.0.0
numpy>=1.24.0
pyarrow>=14.0.0
scipy>=1.10.0   # Teste t pareado
matplotlib>=3.7.0
seaborn>=0.12.0
```

---

## 🔍 Métricas e Fórmulas

### Intensidade de Fiscalização
```
embargos_per_1000km2 = num_embargos / area_municipio_km2 * 1000
```

### Variação Percentual
```
delta_pct = (valor_depois - valor_antes) / valor_antes * 100
```

### Taxa de Reincidência
```
recurrence_rate = num_embargos / anos_com_embargo
```

### Teste Estatístico
```python
from scipy import stats
t_stat, p_valor = stats.ttest_rel(antes, depois)
# Significativo se p_valor < 0.05
```

---

## 📅 Plano de Execução

### Dia 1: ETLs 6.1 e 6.2
- [ ] ETL 6.1: `fiscalizacao_series_temporais.parquet`
- [ ] ETL 6.2: `impacto_embargo_producao.parquet`
- [ ] Validar: Testes t pareados, p-valores

### Dia 2: ETLs 6.3 e 6.4
- [ ] ETL 6.3: `reincidentes_embargos.parquet`
- [ ] ETL 6.4: `status_regularizacao_embargos.parquet`
- [ ] Validar: CPF/CNPJ, sit_desmat

### Dia 3: Visualizações e Resumo
- [ ] Gerar 7 visualizações
- [ ] Calcular métricas resumidas
- [ ] Criar resumo executivo JSON
- [ ] Documentar em README.md

---

## 🎯 Principais Conclusões Esperadas

1. **Impacto dos embargos na produção:** ΔVAB, Δárea plantada, Δrebanho
2. **Significância estatística:** p-valor < 0.05 para métricas principais
3. **Reincidência:** Top 100 CPF/CNPJ com maior taxa de reincidência
4. **Efetividade:** Embargos reduzem desmatamento? (sim/não/parcial)
5. **Recomendações:** Políticas de fiscalização baseadas em evidências

---

## 📚 Documentos Relacionados

- [VALIDACAO_DADOS_SPRINT6.md](./VALIDACAO_DADOS_SPRINT6.md) - Validação detalhada dos dados
- [resumo_contexto_sprint6.json](./resumo_contexto_sprint6.json) - Resumo em JSON
- [../sprint_2_gold_mvp/README.md](../sprint_2_gold_mvp/README.md) - Sprint 2 (MVP Econômico)
- [../../docs/plano_implementacao_v2.md](../../docs/plano_implementacao_v2.md) - Plano de Implementação

---

**Status:** ✅ PRONTO PARA INICIAR
**Data da Validação:** 29/03/2026
**Próxima Atualização:** Após execução das ETLs 6.x
