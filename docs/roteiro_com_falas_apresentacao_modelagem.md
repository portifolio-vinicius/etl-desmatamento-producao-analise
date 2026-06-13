# Roteiro de Falas — Apresentação de Modelagem

Este roteiro está alinhado com `docs/apresentacao_modelagem.html`.

## Divisão entre 4 apresentadores

Substituam `Pessoa 1`, `Pessoa 2`, `Pessoa 3` e `Pessoa 4` pelos nomes do grupo.

| Apresentador | Slides | Papel na narrativa |
|--------------|--------|--------------------|
| Pessoa 1 | Slides 1–4 | Contextualiza o projeto, apresenta a base, explica metodologia e desbalanceamento |
| Pessoa 2 | Slides 5–7 | Apresenta o resumo executivo e os resultados de desmatamento |
| Pessoa 3 | Slides 8–10 | Apresenta embargos e classificação multiclasse |
| Pessoa 4 | Slides 11–13 | Conecta com os notebooks do professor, conclui e encerra |

**Sugestão geral:** cada pessoa deve encerrar seu bloco chamando a próxima com uma frase curta. Isso evita pausas longas e deixa a apresentação mais natural.

---

## Slide 1 — Modelagem Preditiva na Amazônia Legal

**Responsável:** Pessoa 1

“Boa noite. Nesta apresentação, nós vamos mostrar a etapa de modelagem preditiva do projeto sobre desmatamento, atividade econômica e impacto socioambiental na Amazônia Legal.

A pergunta principal foi: com base nos dados de um ano, conseguimos indicar quais municípios podem ter desmatamento, embargos ou maior risco no ano seguinte?

Para isso, usamos dados públicos de fontes como PRODES/DETER, IBAMA, IBGE, CHIRPS e preços agrícolas. Esses dados foram organizados na arquitetura Bronze, Silver, Gold e, por fim, na camada `04_modelagem`, que é a etapa usada para treinar os modelos.”

---

## Slide 2 — Dataset e preparação

**Responsável:** Pessoa 1

“A modelagem foi feita a partir do arquivo `dataset_preditivo_com_precos.parquet`.

O dataset bruto tinha 796.560 linhas. Porém, havia mais de um registro para o mesmo município e ano. Por isso, fizemos uma deduplicação usando `cod_ibge` e `ano`, chegando a 22.280 observações.

Na prática, isso representa pouco mais de 5.500 municípios observados ao longo de quatro anos. A base usada no modelo tem 41 variáveis numéricas e 3 categóricas, além de três targets: desmatamento no próximo ano, embargos no próximo ano e risco de desmatamento em três classes: baixo, médio e alto.”

---

## Slide 3 — Metodologia

**Responsável:** Pessoa 1

“A metodologia foi construída para evitar vazamento temporal.

Para cada município, usamos `shift(-1)`, ou seja, as informações do ano atual são usadas para prever o resultado do ano seguinte.

O split também foi temporal: treinamos com os anos de 2020 e 2021 e testamos em 2022, que prevê o que aconteceu em 2023. O ano de 2023 não entra como teste porque, nesse caso, precisaríamos conhecer o target de 2024, que não existe na base.

Comparamos modelos simples e alinhados com a disciplina: DummyClassifier, árvore de decisão, KNN e Random Forest. Também testamos três estratégias de balanceamento: nenhuma, SMOTE e NearMiss. O balanceamento foi aplicado apenas no conjunto de treino.”

---

## Slide 4 — Desbalanceamento das classes

**Responsável:** Pessoa 1

“Um ponto muito importante do trabalho é o desbalanceamento das classes.

No target de desmatamento, 99,5% dos exemplos de treino são da classe zero, ou seja, municípios sem desmatamento no ano seguinte. A classe positiva representa só 0,5%.

Em embargos, o problema é menos extremo, mas ainda desbalanceado: cerca de 90% dos casos são negativos e 10% positivos.

Na classificação multiclasse, quase todos os registros ficam na classe baixo risco. As classes médio e alto têm participação muito pequena.

Isso é essencial para interpretar os resultados. Métricas como acurácia e F1 weighted podem parecer muito boas mesmo quando o modelo praticamente não detecta a classe rara.”

**Transição para Pessoa 2:** “Com a base, a metodologia e o desbalanceamento explicados, a Pessoa 2 vai apresentar os principais resultados, começando pelo resumo executivo.”

---

## Slide 5 — Resumo executivo

**Responsável:** Pessoa 2

“Este slide resume os melhores resultados por problema.

Para desmatamento, o melhor F1 weighted foi do KNN sem balanceamento, com valor próximo de 0,99. Mas esse resultado precisa ser lido com cautela, porque a classe negativa domina quase toda a base.

Para embargos, o melhor modelo foi Random Forest com SMOTE, com F1 de 0,87 e ROC-AUC de 0,87. Esse foi o caso com desempenho mais equilibrado.

Na classificação multiclasse, Random Forest com SMOTE também teve F1 weighted alto, mas o resultado por classe mostra dificuldade nas categorias médio e alto.

Por isso, ao interpretar os modelos, não olhamos só F1 weighted. Também comparamos com DummyClassifier e damos atenção ao recall da classe positiva e à ROC-AUC.”

---

## Slide 6 — Desmatamento (binário)

**Responsável:** Pessoa 2

“No problema de desmatamento, o KNN sem balanceamento foi o modelo com maior F1 weighted.

O recall da classe positiva ficou em torno de 30%. Isso significa que o modelo detectou uma parte dos municípios que realmente tiveram desmatamento no ano seguinte, mas ainda deixou muitos casos passarem.

A ROC-AUC ficou em aproximadamente 0,76. Esse valor é melhor que o acaso, que seria 0,50, mas ainda não indica um modelo forte o suficiente para ser usado sozinho em produção.

O ponto mais importante aqui é que o DummyClassifier já consegue um F1 weighted alto simplesmente prevendo sempre a classe majoritária. Então, neste problema, o F1 weighted sozinho é uma métrica enganosa.”

---

## Slide 7 — Desmatamento: matriz de confusão e trade-off ROC

**Responsável:** Pessoa 2

“Na matriz de confusão, conseguimos enxergar melhor a limitação do modelo.

Dos 46 casos positivos de desmatamento no teste, o KNN identificou 14. Os outros 32 foram falsos negativos.

No contexto de fiscalização ambiental, falso negativo é um erro importante, porque significa deixar de sinalizar um município que de fato teve desmatamento.

Ao lado, aparece também a curva ROC do Random Forest com SMOTE. Embora o KNN tenha vencido em F1 weighted, o Random Forest com SMOTE teve ROC-AUC maior, em torno de 0,93. Isso mostra que a escolha do melhor modelo depende da métrica e do objetivo.

Se o objetivo for priorizar municípios para fiscalização, talvez seja melhor usar um modelo com melhor separação e depois ajustar o threshold para aumentar recall.”

**Transição para Pessoa 3:** “Depois do resultado de desmatamento, a Pessoa 3 vai apresentar os resultados para embargos e para o problema multiclasse.”

---

## Slide 8 — Embargos (binário)

**Responsável:** Pessoa 3

“No caso de embargos, o comportamento foi mais equilibrado.

O melhor modelo foi Random Forest com SMOTE. Ele detectou cerca de 293 dos 722 casos positivos no conjunto de teste.

Ainda existem muitos falsos negativos, mas o resultado é mais interpretável que no caso de desmatamento, porque a classe positiva representa cerca de 10% dos dados, e não apenas 0,5%.

Aqui o SMOTE ajudou, porque criou exemplos sintéticos da classe minoritária durante o treino, melhorando a capacidade do modelo de reconhecer municípios com embargos no ano seguinte.”

---

## Slide 9 — Embargos: curva ROC

**Responsável:** Pessoa 3

“A curva ROC reforça que embargos foi o problema com melhor comportamento.

A ROC-AUC ficou em torno de 0,87, bem acima do baseline de 0,50. Isso indica boa capacidade de separação entre municípios com e sem embargos no ano seguinte.

Também houve ganho expressivo em relação ao DummyClassifier: o recall da classe positiva saiu de zero para aproximadamente 41%.

Mesmo assim, o modelo não deve ser visto como substituto da fiscalização. Ele serviria como ferramenta de apoio para priorizar municípios com maior chance de embargos futuros.”

---

## Slide 10 — Risco multiclasse

**Responsável:** Pessoa 3

“No problema multiclasse, tentamos classificar o risco em baixo, médio e alto.

O F1 weighted ficou próximo de 0,99, mas isso acontece porque quase todos os exemplos pertencem à classe baixo. Quando olhamos o F1 macro e o recall por classe, a limitação fica clara.

A classe médio teve recall zero, e a classe alto teve recall em torno de 35%.

Isso mostra que, com apenas quatro anos de dados e tão poucos exemplos positivos, essa divisão em três classes é muito granular. Para uma próxima versão, talvez seja melhor manter um problema binário ou incluir mais anos de dados antes de insistir na multiclasse.”

**Transição para Pessoa 4:** “Para fechar, a Pessoa 4 vai conectar essa modelagem com os notebooks do professor e apresentar as conclusões.”

---

## Slide 11 — Alinhamento com notebooks do professor

**Responsável:** Pessoa 4

“Esta etapa se conecta diretamente aos notebooks do professor.

Do notebook 17, usamos a ideia de classificação supervisionada e comparação de algoritmos.

Do notebook 18, aplicamos avaliação com matriz de confusão, curva ROC, baseline, validação cruzada e métricas de classificação.

Do notebook 19, trouxemos o problema multiclasse, criando a variável `classe_risco_proximo_ano`, com as classes baixo, médio e alto.

Além disso, acrescentamos a discussão sobre desbalanceamento, usando SMOTE e NearMiss, que complementa os conteúdos trabalhados nos notebooks.”

---

## Slide 12 — Limitações e conclusões

**Responsável:** Pessoa 4

“Como conclusão, a modelagem mostrou que existe sinal preditivo na base, mas os resultados precisam ser interpretados com cuidado.

O caso mais promissor foi embargos, em que Random Forest com SMOTE teve desempenho mais equilibrado e ROC-AUC de aproximadamente 0,87.

Para desmatamento, existe discriminação acima do acaso, mas o recall da classe positiva ainda é baixo. O F1 weighted alto é explicado principalmente pelo desbalanceamento.

Na multiclasse, a classe baixo domina os dados, e as classes médio e alto têm poucos exemplos. Por isso, o modelo praticamente ignora a classe médio.

As principais limitações são poucos anos de observação, classes raras, cobertura parcial de algumas variáveis e ausência de uma validação temporal mais robusta. Como próximos passos, seria interessante integrar mais anos, completar MapBiomas, ajustar thresholds para recall e testar modelos espaciais ou longitudinais.”

---

## Slide 13 — Encerramento

**Responsável:** Pessoa 4

“Com isso, o projeto entrega não apenas modelos treinados, mas um pipeline reproduzível de modelagem.

Temos scripts para preparar dados, treinar modelos, gerar métricas, salvar matrizes de confusão, curvas ROC e produzir o relatório final.

O ponto principal é que a modelagem foi feita com preocupação metodológica: evitando vazamento temporal, comparando com baseline e interpretando as métricas de forma honesta diante do desbalanceamento.

Obrigado. Ficamos à disposição para perguntas.”
