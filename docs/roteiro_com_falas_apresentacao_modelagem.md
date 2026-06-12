Sim. Segue um roteiro de fala direto, alinhado com a apresentação atual.

## Slide 1 — Capa / Contexto

“Boa noite. Nesta apresentação eu vou mostrar a etapa de modelagem preditiva do projeto sobre desmatamento, atividade econômica e impacto socioambiental na Amazônia Legal.

A ideia principal foi sair de uma análise apenas descritiva e construir modelos capazes de estimar, com base nos dados de um ano, quais municípios podem apresentar desmatamento, embargos ou maior risco no ano seguinte.

O projeto usa dados públicos como PRODES/DETER, IBAMA, IBGE, CHIRPS e preços agrícolas, organizados na arquitetura Bronze, Silver, Gold e, por fim, na camada de modelagem.”

## Slide 2 — Dataset

“Para a modelagem, usamos o arquivo `dataset_preditivo_com_precos.parquet`.

O dataset original tinha 796 mil linhas, mas havia múltiplos registros para o mesmo município e ano. Por isso, fizemos uma deduplicação usando `cod_ibge` e `ano`, chegando a 22.280 observações.

A base final tem pouco mais de 5.500 municípios ao longo de quatro anos. Trabalhamos com 41 variáveis numéricas e 3 categóricas, e criamos três alvos: desmatamento no próximo ano, embargos no próximo ano e uma classificação multiclasse de risco.”

## Slide 3 — Desbalanceamento

“Um ponto central do trabalho é o forte desbalanceamento das classes.

No caso de desmatamento, aproximadamente 99,5% dos registros de treino são da classe zero, ou seja, sem desmatamento no ano seguinte. A classe positiva representa só 0,5%.

Em embargos o cenário é menos extremo, mas ainda desbalanceado: cerca de 90% sem embargos e 10% com embargos.

Na classificação multiclasse, quase tudo fica na classe baixo risco. As classes médio e alto têm participação muito pequena.

Isso é importante porque métricas como acurácia e F1 weighted podem parecer ótimas mesmo quando o modelo praticamente ignora a classe que mais nos interessa.”

## Slide 4 — Metodologia

“A metodologia foi baseada em target temporal.

Para cada município, usamos `shift(-1)`: as variáveis do ano atual são usadas para prever o resultado do ano seguinte.

O split também foi temporal: treinamos com 2020 e 2021 e testamos em 2022, que prevê o que acontece em 2023. O ano de 2023 não pode ser usado como teste porque, para ele, precisaríamos conhecer o target de 2024, que não existe na base.

Foram comparados modelos simples e interpretáveis para a disciplina: DummyClassifier, árvore de decisão, KNN e Random Forest. Também testamos estratégias de balanceamento como SMOTE e NearMiss, sempre aplicadas apenas no treino.”

## Slide 5 — Resumo executivo

“Este é o resumo dos principais resultados.

Para desmatamento, o melhor F1 weighted foi do KNN sem balanceamento, com 0,99. Mas esse número precisa ser interpretado com cuidado, porque o problema é extremamente desbalanceado.

Para embargos, o melhor modelo foi Random Forest com SMOTE, com F1 de 0,87 e ROC-AUC de 0,87. Este foi o problema com resultado mais equilibrado.

Para multiclasse, Random Forest com SMOTE também teve F1 weighted alto, mas o F1 macro foi baixo, mostrando dificuldade nas classes médio e alto.

Por isso, ao longo da análise, eu comparo os modelos com o DummyClassifier e observo principalmente recall da classe positiva e ROC-AUC.”

## Slide 6 — Desmatamento: Curva ROC

“No problema de desmatamento, o KNN sem balanceamento foi o modelo com maior F1 weighted.

O recall da classe positiva foi de cerca de 30%, ou seja, ele detectou parte dos municípios que realmente tiveram desmatamento no ano seguinte, mas ainda deixou muitos passar.

A ROC-AUC foi de 0,76. Isso indica que o modelo discrimina melhor que o acaso, mas ainda não é forte o suficiente para ser usado sozinho em uma aplicação real.

O ponto importante é que o DummyClassifier já consegue F1 weighted muito alto simplesmente prevendo sempre a classe majoritária. Então o F1 weighted sozinho não é uma métrica confiável aqui.”

## Slide 7 — Desmatamento: Matriz de Confusão

“Na matriz de confusão, vemos melhor o problema.

Dos 46 casos positivos de desmatamento no teste, o modelo identificou 14. Os outros 32 foram falsos negativos.

Isso é crítico no contexto de fiscalização, porque falso negativo significa deixar de sinalizar um município que teve desmatamento.

Mesmo assim, o resultado é melhor que o baseline, que não detectava nenhum caso positivo. A leitura correta é: o modelo tem sinal preditivo, mas ainda é insuficiente para uso operacional.”

## Slide 8 — Desmatamento: Trade-off ROC

“Aqui aparece um ponto interessante.

Embora o KNN tenha vencido no F1 weighted, o Random Forest com SMOTE teve ROC-AUC maior, chegando a aproximadamente 0,93.

Isso mostra que a escolha do melhor modelo depende da métrica e do objetivo. Se a prioridade for ranking de risco e discriminação geral, Random Forest com SMOTE pode ser mais interessante. Se usarmos apenas F1 weighted, o KNN aparece como vencedor.

Para um problema real de fiscalização, provavelmente faríamos ajuste de threshold e escolheríamos o modelo maximizando recall ou uma métrica de custo.”

## Slide 9 — Embargos: Matriz de Confusão

“No caso de embargos, o comportamento foi mais equilibrado.

O melhor modelo foi Random Forest com SMOTE. Ele detectou cerca de 293 dos 722 casos positivos no teste.

Ainda há muitos falsos negativos, mas o desempenho é mais interpretável que no caso de desmatamento, porque a classe positiva representa cerca de 10% dos dados, e não apenas 0,5%.

Aqui o SMOTE ajudou, porque criou exemplos sintéticos da classe minoritária durante o treino e melhorou a capacidade do modelo de reconhecer embargos.”

## Slide 10 — Embargos: Curva ROC

“A curva ROC para embargos reforça esse resultado.

A ROC-AUC ficou em torno de 0,87, bem acima do baseline de 0,50. Isso mostra boa capacidade de separação entre municípios com e sem embargos no ano seguinte.

Mesmo assim, é importante lembrar que o modelo não substitui fiscalização. Ele serviria mais como ferramenta de priorização: indicar municípios mais prováveis para análise ou monitoramento.”

## Slide 11 — Multiclasse: Matriz de Confusão

“No problema multiclasse, tentamos dividir o risco em baixo, médio e alto.

O F1 weighted ficou alto, mas isso acontece porque quase todos os exemplos são da classe baixo. Quando olhamos o F1 macro e o recall por classe, vemos o problema real.

A classe médio teve recall zero, e a classe alto teve recall em torno de 35%.

Isso indica que, com apenas quatro anos de dados e tão poucos exemplos positivos, essa divisão em três classes é muito granular. Para uma próxima versão, talvez seja melhor manter um problema binário ou buscar mais anos de dados.”

## Slide 12 — Notebooks do professor

“Esta etapa se conecta diretamente aos notebooks do professor.

Do notebook 17, usamos a ideia de classificação supervisionada e comparação de algoritmos.

Do notebook 18, aplicamos avaliação com matriz de confusão, ROC-AUC, baseline e validação cruzada.

Do notebook 19, trouxemos o problema multiclasse, criando a variável `classe_risco_proximo_ano`.

Além disso, acrescentamos uma discussão sobre desbalanceamento com SMOTE e NearMiss, que complementa os conteúdos vistos.”

## Slide 13 — Conclusões

“Como conclusão, a modelagem mostrou que existe algum sinal preditivo na base, mas os resultados precisam ser interpretados com cuidado.

O caso mais promissor foi embargos, onde Random Forest com SMOTE teve resultado mais equilibrado.

Para desmatamento, o F1 alto é enganoso. A ROC-AUC mostra discriminação razoável, mas o recall da classe positiva ainda é baixo.

Na multiclasse, a classe baixo domina e as classes médio e alto têm poucos exemplos, então o modelo não consegue aprender bem essas categorias.

Os próximos passos seriam integrar mais anos, completar variáveis como MapBiomas, fazer ajuste de threshold para priorizar recall e avaliar modelos espaciais ou longitudinais.”

## Slide 14 — Encerramento

“Com isso, o projeto entrega não apenas os modelos, mas também um pipeline reproduzível, com scripts de treino, métricas, matrizes de confusão, curvas ROC e relatório final.

O ponto principal é que a modelagem foi feita com preocupação metodológica: evitando vazamento temporal, comparando com baseline e interpretando as métricas de forma honesta diante do desbalanceamento.

Obrigado. Fico à disposição para perguntas.”