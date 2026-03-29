# Validação de Dados: Sprint 3 - Inteligência Espacial

**Data:** 29/03/2026  
**Responsável:** Pipeline de Análise  
**Status:** ✅ APROVADO

---

## 1. Sumário Executivo

A Sprint 3 integrou as geometrias dos embargos do IBAMA (GeoParquet) e realizou a análise de densidade e spillover. Os resultados indicam que a fiscalização está fortemente correlacionada com áreas de alta produtividade agropecuária, sugerindo que o foco dos embargos está nas fronteiras consolidadas.

## 2. Métricas de Processamento (ETL 3.1)

| Métrica | Valor |
|---------|-------|
| Registros Brutos (Bronze) | 88.586 |
| Registros com Geometria Válida | 74.676 (84.3%) |
| Municípios Únicos | 3.200 |
| Área Total Embargada (Calculada) | 6.768.840 ha |
| Projeção Utilizada | EPSG:5880 (Sirgas 2000 / Brazil Albers) |

## 3. Resultados da Análise (3.3)

### 3.1 Densidade e VAB
- **Municípios com Fiscalização Ativa (2020-2023):** 1.381
- **VAB Médio (Alta Fiscalização > 1000ha):** R$ 270.796,85 mil
- **VAB Médio (Baixa Fiscalização):** R$ 88.239,46 mil

**Insight:** A fiscalização não impediu o crescimento do VAB. Na verdade, os municípios mais produtivos são os que mais recebem embargos, o que faz sentido dado que são áreas de expansão de commodities.

### 3.2 Simulação de Spillover
- Buffers de 10km foram aplicados aos 1.000 maiores embargos recentes.
- A "razão de influência" (área do buffer / área do embargo) foi calculada para identificar o potencial de transbordamento para vizinhos.

## 4. Ajustes Realizados

1. **Correção de Schema:** O script de análise foi ajustado para usar `vab_agro_mil_reais` conforme definido na camada Silver.
2. **Otimização:** O cálculo de buffer foi limitado aos Top 1000 embargos para garantir performance em ambiente de demonstração.

## 5. Próximos Passos

- Integrar com a **Sprint 4 (Rota Temporal)** para verificar se o "Spillover" de embargos precede novos alertas do DETER em municípios vizinhos.
- Cruzar com a **Sprint 5 (Cadeia Global)** para ver se UFs exportadoras têm maior densidade de embargos georreferenciados.
