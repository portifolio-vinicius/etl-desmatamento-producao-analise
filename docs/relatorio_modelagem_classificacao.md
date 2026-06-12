# Relatório de Modelagem de Classificação

**Projeto:** Análise de Desmatamento, Atividade Econômica e Impacto Socioambiental na Amazônia Legal  
**Gerado em:** 2026-06-12 17:56

---

## Resumo executivo

| Problema | Melhor modelo | F1 (weighted) | ROC-AUC | Observação |
|----------|---------------|---------------|---------|------------|
| Desmatamento | KNeighborsClassifier (nenhuma) | 0.9918 | 0.76 | Classe raríssima; F1 weighted enganoso; priorizar ROC-AUC e recall da classe 1 |
| Embargos | RandomForestClassifier (smote) | 0.8739 | 0.87 | Desbalanceamento moderado; SMOTE melhorou recall da classe positiva |
| Risco multiclasse | RandomForestClassifier (smote) | 0.9897 | N/A | Classes medio/alto quase ausentes; F1 macro baixo |


Os F1 weighted elevados em desmatamento e multiclasse refletem predominantemente a classe majoritária.
Para interpretação honesta, compare sempre com o **DummyClassifier** e analise **recall da classe positiva**
e **ROC-AUC** (quando aplicável).

---

## 1. Introdução

Este relatório documenta modelos de classificação supervisionada sobre o dataset consolidado em
`data/04_modelagem/`, alinhados aos notebooks do professor sobre classificação binária (17),
validação e métricas (18) e problemas multiclasse (19).

### Objetivos

- Prever ocorrência de desmatamento no ano seguinte (`tem_desmatamento_proximo_ano`)
- Prever ocorrência de embargos no ano seguinte (`tem_embargos_proximo_ano`)
- Classificar risco de desmatamento futuro em `baixo`, `medio` e `alto`

---

## 2. Descrição da base de dados

| Aspecto | Valor |
|---------|-------|
| Arquivo | `data/04_modelagem/dataset_preditivo_com_precos.parquet` |
| Shape bruto | 796,560 linhas × 51 colunas |
| Após deduplicação (`cod_ibge` + `ano`) | 22,280 linhas |
| Anos disponíveis | 2020–2023 |
| Observações no treino | 11,140 |
| Observações no teste | 5,570 |

### Distribuição das classes no treino

| Target | Distribuição (proporção) |
|--------|--------------------------|
| `tem_desmatamento_proximo_ano` | 0: 99.5%; 1: 0.5% |
| `tem_embargos_proximo_ano` | 0: 90.1%; 1: 9.9% |
| `classe_risco_proximo_ano` | alto: 0.2%; baixo: 99.5%; medio: 0.2% |

O desbalanceamento extremo em desmatamento (~0,5% de positivos) e multiclasse (~0,2% medio/alto)
explica por que acurácia e F1 weighted podem ser altos mesmo quando o modelo quase não detecta
a classe minoritária.

---

## 3. Metodologia

- **Base:** `dataset_preditivo_com_precos.parquet`
- **Deduplicação:** uma observação por `cod_ibge` + `ano` (796,560 → 22,280 linhas)
- **Targets temporais:** `shift(-1)` por município — features do ano *t* preveem o target do ano *t+1*
- **Split temporal:** treino anos 2020-2021 (prevê 2021–2022); teste ano 2022 (prevê 2023)
- **Por que 2023 não entra no teste:** observações de 2023 exigiriam target de 2024, inexistente nos dados
- **Tamanho dos conjuntos:** treino=11,140, teste=5,570
- **Treino com 11,140 observações — abaixo do limite de 50,000; amostragem não foi necessária.**
- **Exclusão de vazamento:** colunas derivadas do target ou do desfecho contemporâneo
- **Modelos:** DummyClassifier, DecisionTree, KNN, RandomForest
- **Balanceamento (apenas treino):** nenhum, SMOTE, NearMiss
- **Validação cruzada:** estratificada no conjunto de treino (F1 weighted)


---

## 4. Classificação binária — Desmatamento

### Resultados comparativos

| Modelo | Balanceamento | Accuracy | Precision | Recall | F1 | ROC-AUC | CV F1 |
|--------|---------------|----------|-----------|--------|----|---------|---------|
| KNeighborsClassifier (nenhuma) | nenhuma | 0.9932 | 0.9918 | 0.9932 | 0.9918 | 0.7562 | 0.9942 |
| RandomForestClassifier (smote) | smote | 0.9910 | 0.9912 | 0.9910 | 0.9911 | 0.9305 | 0.9930 |
| KNeighborsClassifier (smote) | smote | 0.9898 | 0.9913 | 0.9898 | 0.9905 | 0.7886 | 0.9895 |
| RandomForestClassifier (nenhuma) | nenhuma | 0.9926 | 0.9917 | 0.9926 | 0.9899 | 0.9016 | 0.9934 |
| DecisionTreeClassifier (nenhuma) | nenhuma | 0.9912 | 0.9891 | 0.9912 | 0.9899 | 0.6892 | 0.9930 |
| DummyClassifier (nenhuma) | nenhuma | 0.9917 | 0.9836 | 0.9917 | 0.9876 | 0.5000 | 0.9930 |
| DummyClassifier (smote) | smote | 0.9917 | 0.9836 | 0.9917 | 0.9876 | 0.5000 | 0.9930 |
| DummyClassifier (nearmiss) | nearmiss | 0.9917 | 0.9836 | 0.9917 | 0.9876 | 0.5000 | 0.9930 |
| DecisionTreeClassifier (smote) | smote | 0.9722 | 0.9903 | 0.9722 | 0.9800 | 0.8053 | 0.9855 |
| KNeighborsClassifier (nearmiss) | nearmiss | 0.4899 | 0.9904 | 0.4899 | 0.6490 | 0.7307 | 0.5559 |
| RandomForestClassifier (nearmiss) | nearmiss | 0.2034 | 0.9873 | 0.2034 | 0.3273 | 0.7788 | 0.4047 |
| DecisionTreeClassifier (nearmiss) | nearmiss | 0.1445 | 0.9841 | 0.1445 | 0.2412 | 0.5040 | 0.3722 |


### Melhor modelo vs baseline

| Métrica | DummyClassifier | Melhor modelo | Δ |
|---------|-----------------|---------------|---|
| F1 weighted | 0.9876 | 0.9918 | +0.0042 |
| Recall classe positiva | 0.0000 | 0.3043 | +0.3043 |
| F1 classe positiva | 0.0000 | 0.4242 | +0.4242 |
| ROC-AUC | 0.5000 | 0.7562 | — |


### Melhor modelo — métricas detalhadas

- **Modelo:** KNeighborsClassifier (nenhuma)
- **F1 weighted:** 0.9918
- **ROC-AUC:** 0.7562336681043983
- **Precision (classe 1):** 0.7000
- **Recall (classe 1):** 0.3043
- **F1 (classe 1):** 0.4242
- **Suporte (classe 1):** 46

### Figuras

- Matriz de confusão: `../data/04_modelagem/resultados_metricas/matriz_confusao_kneighborsclassifier_(nenhuma).png`
- Curva ROC: `../data/04_modelagem/resultados_metricas/curva_roc_kneighborsclassifier_(nenhuma).png`

### Conclusão

O KNN sem balanceamento obteve o maior F1 weighted (0,99), mas isso decorre da classe negativa
dominante. O recall da classe positiva permanece baixo (~30%), e a ROC-AUC (~0,76) indica
discriminação modesta — bem acima do baseline (0,50), porém longe de um detector confiável.
O DummyClassifier já alcança F1 weighted ~0,99 ao prever sempre "sem desmatamento".
**NearMiss degradou a performance**, como esperado quando a classe positiva é raríssima.
Para fiscalização preventiva, priorize recall da classe 1 e ROC-AUC, não F1 weighted.

---

## 5. Classificação binária — Embargos

### Resultados comparativos

| Modelo | Balanceamento | Accuracy | Precision | Recall | F1 | ROC-AUC | CV F1 |
|--------|---------------|----------|-----------|--------|----|---------|---------|
| RandomForestClassifier (smote) | smote | 0.8822 | 0.8695 | 0.8822 | 0.8739 | 0.8748 | 0.8720 |
| DecisionTreeClassifier (nenhuma) | nenhuma | 0.8873 | 0.8688 | 0.8873 | 0.8659 | 0.7399 | 0.8934 |
| RandomForestClassifier (nenhuma) | nenhuma | 0.8916 | 0.8897 | 0.8916 | 0.8598 | 0.8756 | 0.8993 |
| DecisionTreeClassifier (smote) | smote | 0.8348 | 0.8633 | 0.8348 | 0.8464 | 0.7759 | 0.8437 |
| KNeighborsClassifier (nenhuma) | nenhuma | 0.8729 | 0.8382 | 0.8729 | 0.8329 | 0.6203 | 0.8818 |
| DummyClassifier (nenhuma) | nenhuma | 0.8704 | 0.7576 | 0.8704 | 0.8101 | 0.5000 | 0.8539 |
| DummyClassifier (smote) | smote | 0.8704 | 0.7576 | 0.8704 | 0.8101 | 0.5000 | 0.8539 |
| DummyClassifier (nearmiss) | nearmiss | 0.8704 | 0.7576 | 0.8704 | 0.8101 | 0.5000 | 0.8539 |
| KNeighborsClassifier (smote) | smote | 0.7813 | 0.8043 | 0.7813 | 0.7920 | 0.6222 | 0.8114 |
| DecisionTreeClassifier (nearmiss) | nearmiss | 0.6250 | 0.7759 | 0.6250 | 0.6827 | 0.4777 | 0.7240 |
| RandomForestClassifier (nearmiss) | nearmiss | 0.3562 | 0.8851 | 0.3562 | 0.3981 | 0.7944 | 0.6382 |
| KNeighborsClassifier (nearmiss) | nearmiss | 0.2777 | 0.8133 | 0.2777 | 0.3025 | 0.5560 | 0.4841 |

### Melhor modelo vs baseline

| Métrica | DummyClassifier | Melhor modelo | Δ |
|---------|-----------------|---------------|---|
| F1 weighted | 0.8101 | 0.8739 | +0.0638 |
| Recall classe positiva | 0.0000 | 0.4058 | +0.4058 |
| F1 classe positiva | 0.0000 | 0.4718 | +0.4718 |
| ROC-AUC | 0.5000 | 0.8748 | — |

### Melhor modelo — métricas detalhadas

- **Modelo:** RandomForestClassifier (smote)
- **F1 weighted:** 0.8739
- **ROC-AUC:** 0.8747681598145964
- **Precision (classe 1):** 0.5635
- **Recall (classe 1):** 0.4058
- **F1 (classe 1):** 0.4718
- **Suporte (classe 1):** 722

### Figuras

- Matriz de confusão: `../data/04_modelagem/resultados_metricas/matriz_confusao_randomforestclassifier_(smote).png`
- Curva ROC: `../data/04_modelagem/resultados_metricas/curva_roc_randomforestclassifier_(smote).png`

### Conclusão

Embargos apresentam desbalanceamento moderado (~10% positivos). Random Forest com **SMOTE**
superou os demais (F1 weighted 0,87; ROC-AUC 0,87), melhorando recall da classe positiva
em relação ao baseline. Este é o problema com resultados mais equilibrados e interpretáveis.
NearMiss novamente prejudicou recall da classe majoritária ao forçar undersampling agressivo.

---

## 6. Classificação multiclasse — Risco de desmatamento

Classes:
- **baixo:** sem desmatamento no ano seguinte
- **medio:** desmatamento positivo até a mediana entre municípios com desmatamento
- **alto:** desmatamento acima dessa mediana

### Resultados comparativos

| Modelo | Balanceamento | Accuracy | Precision | Recall | F1 | ROC-AUC | CV F1 |
|--------|---------------|----------|-----------|--------|----|---------|---------|
| RandomForestClassifier (smote) | smote | 0.9921 | 0.9877 | 0.9921 | 0.9897 | N/A | 0.9940 |
| RandomForestClassifier (nenhuma) | nenhuma | 0.9919 | 0.9879 | 0.9919 | 0.9881 | N/A | 0.9932 |

### Melhor modelo — métricas por classe

- **Modelo:** RandomForestClassifier (smote)
- **F1 weighted:** 0.9897
- **Accuracy:** 0.9921
- **F1 macro:** 0.4802

- **Medio:** precision=0.0000, recall=0.0000, f1=0.0000 (suporte=23)
- **Alto:** precision=0.6154, recall=0.3478, f1=0.4444 (suporte=23)

### Figuras

- Matriz de confusão: `../data/04_modelagem/resultados_metricas/matriz_confusao_randomforestclassifier_(smote).png`

### Conclusão

Random Forest com SMOTE lidera pelo F1 weighted (~0,99), mas a classe **medio** permanece
sem recall (0%) e **alto** tem recall ~35%. O F1 macro (~0,48) revela a dificuldade real.
Com apenas ~46 municípios positivos no teste (23 medio + 23 alto), a multiclasse fine-grained
não é viável com 4 anos de painel; agrupamento binário ou mais dados temporais seriam necessários.

---

## 7. Interpretação de negócio

- Compare **sempre** com DummyClassifier: ganhos aparentes em F1 weighted podem ser triviais.
- Em desmatamento, **ROC-AUC** e **recall da classe 1** são mais informativos que acurácia.
- **SMOTE** ajudou em embargos; **NearMiss** piorou ambos os problemas binários neste dataset.
- Para políticas públicas, modelos com alto recall minimizam falsos negativos (municípios de
  risco não monitorados), ao custo de mais falsos positivos (fiscalização em municípios seguros).

---

## 8. Limitações metodológicas

- Apenas 4 anos de observação (2020–2023), restringindo validação cruzada temporal.
- Variáveis meteorológicas (CHIRPS) possuem cobertura limitada em parte dos municípios.
- MapBiomas ainda não está totalmente integrado ao dataset principal.
- Targets do ano seguinte (`shift(-1)`) excluem o último ano disponível do conjunto de teste.
- Classes raras (desmatamento, medio/alto) limitam aprendizado supervisionado robusto.

---

## 9. Artefatos gerados

- `data/04_modelagem/resultados_metricas/metricas_binario_*.json`
- `data/04_modelagem/resultados_metricas/comparativo_binario_*.csv`
- `data/04_modelagem/resultados_metricas/metricas_multiclasse_risco.json`
- `data/04_modelagem/resultados_metricas/matriz_confusao_*.png`
- `data/04_modelagem/resultados_metricas/curva_roc_*.png`
- `data/04_modelagem/modelos/*.pkl`

---

## 10. Comandos de execução

```bash
python src/modelagem/treinar_classificacao_binaria.py --target tem_desmatamento_proximo_ano
python src/modelagem/treinar_classificacao_binaria.py --target tem_embargos_proximo_ano
python src/modelagem/treinar_classificacao_multiclasse.py
python src/modelagem/gerar_relatorio_modelagem.py
```
