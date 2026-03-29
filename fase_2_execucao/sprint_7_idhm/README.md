# Sprint 7: O Paradoxo Socioeconômico (IDHM e Desenvolvimento)

## 🎯 Objetivo
Esta sprint foca em cruzar os dados de desenvolvimento econômico (VAB Agropecuário e Desmatamento) com indicadores de bem-estar social. O objetivo é testar a hipótese de que o desmatamento não gera desenvolvimento social sustentável para a população local.

## 📊 Indicadores Principais
- **IDHM (Índice de Desenvolvimento Humano Municipal):** Educação, Longevidade e Renda.
- **PIB per capita:** Riqueza média produzida por habitante.
- **Taxa de Pobreza:** Percentual da população abaixo da linha de pobreza.

## 🛠️ Metodologia
1. **Interpolação do IDHM:** Como o IDHM é decenal, utilizaremos interpolação linear para os anos entre os censos (2011-2019) e projeções para 2021-2023.
2. **Correlação de Spearman:** Utilizada para medir a força da relação entre Desmatamento Acumulado e IDHM, mitigando o efeito de outliers.
3. **Análise de Quadrantes:** Classificação dos municípios em 4 categorias para visualizar o "Paradoxo do Desmatamento" (Alto Desmatamento e Baixo IDHM).

## 🚀 Próximos Passos
1. **Ingestão:** Baixar os dados do Atlas Brasil e SIDRA IBGE.
2. **ETL 7.1:** Implementar script de interpolação.
3. **Análise:** Gerar rankings de municípios por eficiência socioambiental.

---
**Status:** 🟡 Planejado | **Responsável:** Engenheiro de Dados AI
