# Validação de Dados: Sprint 6 - O Peso da Fiscalização

## 📊 Resumo Executivo da Sprint

| Métrica | Valor | Descrição |
|---------|-------|-----------|
| **Municípios Analisados** | 1.249 | Municípios com embargos ativos em 2021-2023. |
| **Total de Embargos** | 13.031 | Quantidade total de registros na série temporal. |
| **Taxa de Reincidência** | 14,5% | 9.522 infratores reincidentes em 65.653 únicos. |
| **Taxa de Sucesso (Bovinos)** | 18,9% | Municípios onde o rebanho reduziu pós-embargo (169 de 893). |
| **Impacto Médio (PPM)** | +3,71% | Crescimento médio do rebanho em municípios embargados. |
| **Diretamente Desmatamento** | 62,9% | Embargos classificados como 'D' (Desmatamento). |

---

## 🔍 Detalhamento das Descobertas

### 1. Ineficácia Predominante
A análise de impacto **Antes vs Depois** mostrou que o embargo, isoladamente, **não reduz** a produção na maioria dos casos:
*   **Aumento de Rebanho:** 31,2% (279 municípios) ignoraram o impacto fiscal ou mantiveram o crescimento.
*   **Estabilidade/Zero:** 50,0% não reportaram variação significativa ou dados insuficientes.
*   **Redução:** Apenas 18,9% (169 municípios) apresentaram queda no rebanho bovino.

### 2. Recorrência Extrema
O top 1 infrator (CPF/CNPJ ocultado por segurança) possui **191 embargos**. Isso demonstra que a fiscalização punitiva (TAD - Termo de Apreensão e Depósito) não está sendo impeditiva para grandes degradadores.

---

## 🛠️ Ajustes Realizados na Sprint
1.  **Refinamento do ETL 6.2:** Adicionada a classificação binária `sucesso_embargo` (redução vs aumento).
2.  **Mapeamento `sit_desmat`:** Identificado que 37,1% dos embargos são 'N' (Não desmatamento direto), exigindo cruzamento com MapBiomas no futuro.
3.  **Gold Dataset:** Criação de `fiscalizacao_resumo_kpis.json` para facilitar o dashboard (Sprint 8).

---

**Status Final:** ✅ Concluído e Validado
**Data:** 29/03/2026
