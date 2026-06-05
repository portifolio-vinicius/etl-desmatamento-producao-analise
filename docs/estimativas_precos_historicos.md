# Estimativas de Preços Históricos - CEPEA (2018-2024)

**Fonte:** Farmnews - Dados médios anuais do CEPEA
**Data de extração:** 05/06/2026
**Referência:** https://www.farmnews.com.br/mercado/preco-do-boi-gordo-bezerro-milho-e-soja-media-anual-entre-2018-e-2024/

## Dados Identificados

### Preços Médios Anuais (R$) - Dados Confirmados

| Ano | Boi Gordo (R$/arroba) | Bezerro (R$/cabeça) | Milho (R$/saca) | Soja (R$/saca) | Fonte |
|-----|----------------------|---------------------|----------------|----------------|-------|
| 2020 | - | - | - | - | - |
| 2021 | - | - | ~R$ 88,0* | ~R$ 160,0* | Estimativa |
| 2022 | - | - | ~R$ 88,1* | ~R$ 175,0* | Estimativa |
| 2023 | R$ 255,1 | R$ 2.208,1 | R$ 66,0 | ~R$ 145,0* | Farmnews |
| 2024 | R$ 258,0 | R$ 2.172,6 | R$ 64,2 | ~R$ 129,0* | Farmnews |

*Valores estimados baseados em variações percentuais mencionadas nos artigos

### Cálculo das Estimativas

**Milho:**
- 2023: R$ 66,0/saca (confirmado)
- 2023 teve queda de 25,1% vs 2022 → 2022 ≈ R$ 66,0 / 0,749 = R$ 88,1/saca
- 2021 foi próximo de 2022 (período de alta) → 2021 ≈ R$ 88,0/saca
- 2024: R$ 64,2/saca (confirmado)

**Soja:**
- Recorde abril 2021: R$ 177,1/saca (mensal)
- Recorde janeiro 2022: R$ 179,4/saca (mensal)
- 2024 teve queda de 11% vs 2023
- Se 2024 ≈ R$ 129,0 (preço atual), então 2023 ≈ R$ 145,0
- 2021-2022 foram anos de recorde → estimativa R$ 160-175/saca

### Informações Adicionais do Artigo

**Boi Gordo (Cepea):**
- 2024: R$ 258,0/arroba (média anual)
- 2023: R$ 255,1/arroba (média anual)
- Variação 2024 vs 2023: +1,1%
- Mínima 2024: R$ 215,3/arroba (junho)
- Máxima 2024: R$ 352,7/arroba (novembro)
- Variação mínima-máxima: +63,8%
- Dezembro 2024: R$ 320,3/arroba
- Dezembro 2023: R$ 248,6/arroba

**Bezerro (Cepea, Mato Grosso do Sul):**
- 2024: R$ 2.172,6/cabeça (média anual)
- 2023: R$ 2.208,1/cabeça (média anual)
- Variação 2024 vs 2023: -1,6%
- Terceiro ano consecutivo de queda nominal

**Milho (Cepea):**
- 2024: R$ 64,2/saca (média anual)
- 2023: R$ 66,0/saca (média anual)
- Variação 2024 vs 2023: -2,8%
- Terceiro ano consecutivo de queda nominal

**Soja (Cepea, Paranaguá-PR):**
- 2024: queda de 11,0% vs 2023
- Perspectiva pessimista para 2025 devido a estoques mundiais recordes

### Série Histórica da Soja (2007-presente)

**Fonte:** https://www.farmnews.com.br/mercado/precos-historicos-da-soja-2/

- Valor médio nominal (2007-2024): R$ 59,5/saca
- Série histórica desde 2007 disponível no artigo

## Limitações

- Dados disponíveis apenas para médias anuais
- Tabela completa com valores de 2018-2024 mencionada no artigo, mas não totalmente extraída
- Para dados mensais ou diários, necessário:
  - Download manual do site CEPEA
  - Web scraping específico
  - Acesso a APIs pagas (B3, IMEA, etc.)

## Próximos Passos Sugeridos

1. **Extrair tabela completa** do artigo Farmnews (2018-2024)
2. **Investigar IMEA** para dados históricos de Mato Grosso
3. **Considerar B3 futuros** como proxy para preços de mercado
4. **Usar médias anuais** como estimativa para análise preditiva (se dados diários não disponíveis)

## Notas

- Todos os valores são nominais (não corrigidos pela inflação)
- Preços do CEPEA são referências de mercado físico brasileiro
- Para análise econômica mais robusta, considerar correção pela inflação (IPCA)
