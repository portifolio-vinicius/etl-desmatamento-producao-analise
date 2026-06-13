---
marp: true
theme: default
paginate: true
size: 16:9
style: |
  section { font-family: 'Segoe UI', sans-serif; }
  h1, h2 { color: #1b5e20; }
  table { font-size: 0.75em; }
  th { background: #1b5e20; color: white; }
  blockquote { border-left: 4px solid #2e7d32; background: #f1f8e9; }
---

# Modelagem Preditiva na Amazônia Legal

**Classificação supervisionada** — desmatamento, embargos e risco municipal

FATEC · Tópicos Especiais em Informática

---

## Contexto

- **Pergunta:** quais municípios merecem monitoramento preventivo no **próximo ano**?
- Fontes: PRODES/DETER, IBAMA, IBGE, CHIRPS, preços agrícolas
- Arquitetura: Bronze → Silver → Gold → **04_modelagem**
- Dataset: `dataset_preditivo_com_precos.parquet`

---

## Dataset

| Aspecto | Valor |
|---------|-------|
| Linhas brutas | 796.560 |
| Após dedup | **22.280** |
| Features | 41 numéricas + 3 categóricas |
| Treino / teste | 11.140 / 5.570 |

**Targets:** desmatamento · embargos · risco (baixo/medio/alto)

---

## Desbalanceamento (treino)

| Target | Classe majoritária | Classe minoritária |
|--------|-------------------|-------------------|
| Desmatamento | 99,5% (sem) | **0,5%** (com) |
| Embargos | 90,1% (sem) | **9,9%** (com) |
| Multiclasse | 99,5% (baixo) | **0,2%** medio/alto |

> F1 weighted alto pode esconder falha total na classe rara.

---

## Metodologia

- Target: `groupby(cod_ibge).shift(-1)`
- Split: treino **2020–2021** · teste **2022** (prevê 2023)
- Modelos: Dummy, Árvore, KNN, Random Forest
- Balanceamento (treino): nenhum, SMOTE, NearMiss
- Validação cruzada estratificada

> Split temporal simula uso real — evita vazamento temporal.

---

## Resumo executivo

| Problema | Melhor modelo | F1 | ROC-AUC |
|----------|---------------|-----|---------|
| Desmatamento | KNN (nenhuma) | 0,99 | 0,76 |
| Embargos | RF + SMOTE | 0,87 | 0,87 |
| Multiclasse | RF + SMOTE | 0,99 | N/A |

**Sempre compare com DummyClassifier e recall da classe positiva.**

---

## Desmatamento — curva ROC

- Melhor F1: **KNN** — 0,9918
- Recall classe 1: **30%** · ROC-AUC: **0,76**
- Dummy: recall classe 1 = **0%**

![bg right:42%](../data/04_modelagem/resultados_metricas/curva_roc_tem_desmatamento_kneighborsclassifier_(nenhuma).png)

---

## Desmatamento — matriz de confusão

![width:480px](../data/04_modelagem/resultados_metricas/matriz_confusao_tem_desmatamento_kneighborsclassifier_(nenhuma).png)

14 de 46 municípios com desmatamento detectados · NearMiss degradou performance

---

## Desmatamento — trade-off ROC

RF+SMOTE tem ROC-AUC **0,93** (melhor discriminação), mas KNN lidera F1 weighted

![width:520px](../data/04_modelagem/resultados_metricas/curva_roc_tem_desmatamento_randomforestclassifier_(smote).png)

---

## Embargos — matriz de confusão

- Melhor: **Random Forest + SMOTE**
- F1: 0,8739 · ~293 de 722 embargos detectados

![bg right:48%](../data/04_modelagem/resultados_metricas/matriz_confusao_tem_embargos_randomforestclassifier_(smote).png)

---

## Embargos — curva ROC

- ROC-AUC: **0,87** (baseline 0,50)
- Recall ~41% · Precision ~56%
- SMOTE melhorou recall da classe positiva

![bg right:45%](../data/04_modelagem/resultados_metricas/curva_roc_tem_embargos_randomforestclassifier_(smote).png)

---

## Multiclasse — matriz de confusão

- F1 weighted: 0,9897 · F1 macro: **0,48**
- Classe **medio**: recall **0%** · **alto**: ~35%

![bg right:50%](../data/04_modelagem/resultados_metricas/matriz_confusao_classe_risco_randomforestclassifier_(smote).png)

---

## Notebooks do professor

| Nb | Tópico | Aplicação |
|----|--------|-----------|
| 17 | Classificação | Dummy, Árvore, KNN, RF |
| 18 | Métricas | Split, ROC, matriz, CV |
| 19 | Multiclasse | `classe_risco_proximo_ano` |

---

## Conclusões

**Funcionou:** ROC-AUC acima do baseline; RF+SMOTE em embargos

**Não funcionou:** F1 weighted enganoso; multiclasse medio/alto ignoradas; NearMiss

**Próximos passos:** mais dados, MapBiomas, tuning de threshold, modelos espaciais

---

# Obrigado!

`docs/relatorio_modelagem_classificacao.md` · `src/modelagem/`
