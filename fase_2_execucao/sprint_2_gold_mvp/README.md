# Sprint 2: MVP Econômico - Custo-Benefício do Desmatamento

## 📊 Status: ✅ CONCLUÍDO (29/03/2026)

**Objetivo:** Responder à pergunta central: "O desmatamento gera eficiência econômica?"

**Período de Análise:** 2020-2023  
**Municípios Analisados:** 5.571  
**Municípios com Desmatamento > 0:** 123

---

## 🔍 Análises Realizadas

### 2.1 ICA - Índice de Custo Ambiental
**Fórmula:** `ICA = area_desmatada_ha / vab_agro_mil_reais`

**Resultados:**
- ICA Médio: **0.00118 ha/R$ mil**
- ICA Mediana: **0.00057 ha/R$ mil**
- Municípios com ICA válido: **39 (0.7% do total)**

**Interpretação:** Apenas 39 municípios têm desmatamento e VAB positivos simultaneamente, indicando que a relação entre desmatamento e produção agropecuária é limitada a poucos municípios.

---

### 2.2 Correlação ΔDesmatamento × ΔVAB
**Método:** Correlação de Pearson entre variações anuais

**Resultados:**
- Pearson: **-0.0099** (p-valor: 0.20)
- Spearman: **-0.0232** (p-valor: 0.003)
- N observações: **16.713**

**Interpretação:** Correlação **fraca/nula** - desmatamento NÃO está associado a crescimento do VAB agropecuário.

---

### 2.3 Eficiência: Pecuária vs Agricultura

**Pecuária (por ha desmatado):**
- Bovinos por hectare (mediana): **1.280 cabeças/ha**
- VAB por hectare (mediana): **R$ 0 mil/ha** (dados esparsos)

**Agricultura (PAM - por ha plantado):**
- Valor da produção (média): **R$ 8.24 mil/ha**
- Valor da produção (mediana): **R$ 6.35 mil/ha**

**Conclusão:** Agricultura gera mais valor por hectare que pecuária.

---

### 2.4 Concentração Territorial

**Resultados:**
- Top 100 Desmatamento: 100 municípios
- Top 100 VAB Agro: 100 municípios
- **Overlap:** **7 municípios (7%)**
- **Municípios alto desmatamento + baixo VAB:** **34**

**Interpretação:** Apenas 7% dos municípios estão em ambas listas, indicando que desmatamento e produção econômica não andam juntos.

---

### 2.5 Regressão Linear

**Modelo:** `VAB_Agro = f(area_desmatada, ano)`

**Resultados:**
- R²: **0.0616** (6.16% da variância explicada)
- Coeficiente desmatamento: **~0.0** (não significativo)

**Conclusão:** Área desmatada não explica o VAB agropecuário dos municípios.

---

## 📁 Artefatos Gerados (Camada Gold)

| Arquivo | Descrição | Tamanho |
|---------|-----------|---------|
| `ica_ranking.parquet` | Ranking municipal do ICA | 158 KB |
| `correlacao_delta.parquet` | Correlação ΔDesmatamento × ΔVAB | 3 KB |
| `eficiencia_atividade.parquet` | Pecuária vs Agricultura | 241 KB |
| `eficiencia_agricola_pam.parquet` | Eficiência agrícola PAM | 709 KB |
| `ranking_concentracao.parquet` | Rankings desmatamento e VAB | 144 KB |
| `ranking_top100_desmatamento.parquet` | Top 100 desmatamento | 5 KB |
| `ranking_top100_vab.parquet` | Top 100 VAB | 5 KB |
| `regressao_resultados.csv` | Modelo de regressão | 145 B |
| `resumo_executivo.json` | Resumo das métricas | 568 B |

---

## 📊 Visualizações

| Arquivo | Descrição | Tamanho |
|---------|-----------|---------|
| `distribuicao_ica.png` | Histograma e boxplot do ICA | 268 KB |
| `top20_ica_municipios.png` | Barras: Top 20 municípios por ICA | 463 KB |
| `scatter_delta_vab_desmat.png` | Scatter: ΔVAB vs ΔDesmatamento | 325 KB |
| `eficiencia_pecuaria_agricultura.png` | Boxplot: Pecuária vs Agricultura | 283 KB |
| `concentracao_territorial.png` | Scatter e barras: Overlap Top 100 | 2.8 MB |
| `resumo_visual.png` | Dashboard: 4 métricas principais | 418 KB |

---

## ✅ Principais Conclusões

1. **ICA:** Apenas 39 municípios (0.7%) têm desmatamento e VAB positivos simultaneamente
2. **Correlação:** -0.0099 (fraca/nula) - desmatamento NÃO está associado a crescimento do VAB
3. **Overlap:** Apenas 7% dos municípios estão em ambas listas (Top 100 desmatamento e Top 100 VAB)
4. **Municípios problemáticos:** 34 municípios no Top 100 desmatamento têm rank VAB > 2000
5. **Regressão:** R²=0.0616 - área desmatada não explica VAB agropecuário
6. **Resposta à pergunta central:** **Desmatamento NÃO gera eficiência econômica**

---

## 📂 Estrutura de Diretórios

```
fase_2_execucao/
└── sprint_2_gold_mvp/
    ├── sprint2_mvp_economico.py       # Script principal da Sprint 2
    ├── sprint2_mvp_economico.ipynb    # Notebook da Sprint 2
    └── sprint2_visualizacoes.py       # Script de visualizações

data/
├── 02_silver/                         # Dados da Sprint 1
│   ├── serie_historica_2020_2023.parquet
│   ├── pam_consolidado.parquet
│   ├── pib_vab_consolidado.parquet
│   ├── ppm_consolidado.parquet
│   └── embargos_por_municipio_ano.parquet
└── 03_gold/                           # Resultados da Sprint 2
    ├── *.parquet                      # Artefatos analíticos
    ├── resumo_executivo.json          # Resumo em JSON
    └── visualizacoes/                 # Gráficos e dashboards
```

---

## 🚀 Como Reproduzir

```bash
# Ativar ambiente virtual
source venv/bin/activate

# Executar Sprint 2
python fase_2_execucao/sprint_2_gold_mvp/sprint2_mvp_economico.py

# Gerar visualizações
python fase_2_execucao/sprint_2_gold_mvp/sprint2_visualizacoes.py
```

---

## 📝 Dependências

- pandas
- numpy
- pyarrow
- scipy
- statsmodels
- matplotlib
- seaborn

---

## 📅 Próximos Passos

**Sprint 3: Inteligência Espacial e Vazamento (Spillover)**
- Integrar geometrias dos embargos (GeoParquet)
- Criar buffers de 10km ao redor de embargos
- Analisar efeito spillover em municípios adjacentes
- Calcular densidade de embargos por microrregião

---

**Documentação atualizada em:** 29/03/2026  
**Responsável:** Análise de Dados Ambientais
