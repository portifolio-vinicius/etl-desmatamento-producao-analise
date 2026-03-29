# Sprint 5 - Rastreabilidade da Cadeia Global (Commodities)

## Visão Geral

Esta sprint tem como objetivo cruzar dados de exportação de commodities com dados de desmatamento para responder à pergunta: **"Para onde vai a produção associada ao desmatamento?"**

## ⚠️ Limitação Crítica

**COMEX não possui código municipal**, apenas UF (`SG_UF_NCM`). Portanto:
- ✅ Rastreio possível apenas no nível **estadual**
- ❌ Não é possível cruzar diretamente município desmatador × exportador

## Dados de Entrada

### Camada Silver

| Arquivo | Localização | Registros |
|---------|-------------|-----------|
| COMEX | `data/02_silver/comex_por_uf_ano.parquet` | 689 |
| IBAMA | `data/02_silver/embargos_por_municipio_ano.parquet` | 18.355 |
| Dimensão Municípios | `data/02_silver/dim_municipio.parquet` | 5.510 |

### Camada Bronze (para refinamento)

| Arquivo | Localização | Registros |
|---------|-------------|-----------|
| COMEX EXP 2023 | `data/01_bronze/comex/comex_stat/EXP_2023.parquet` | 1.563.659 |
| COMEX EXP 2024 | `data/01_bronze/comex/comex_stat/EXP_2024.parquet` | ~1.6M |
| COMEX EXP 2025 | `data/01_bronze/comex/comex_stat/EXP_2025.parquet` | ~1.6M |

## Scripts de ETL e Análise

**Contexto Consolidado:** [resumo_contexto_sprint5.json](./resumo_contexto_sprint5.json)

### Pendentes de Implementação

| Script | Descrição | Status |
|--------|-----------|--------|
| `01_etl_mapeamento_ncm.py` | Criar tabela NCM → commodity | ✅ Concluído |
| `02_etl_paises_referencia.py` | Criar tabela código → país | ✅ Concluído |
| `03_analise_ranking_uf.py` | Ranking de UFs exportadoras | ✅ Concluído |
| `04_analise_overlap_uf.py` | Overlap exportação × desmatamento | ✅ Concluído |
| `05_analise_matriz_paises.py` | Matriz de países compradores | ✅ Concluído |
| `06_analise_eficiencia_ambiental.py` | Métrica USD/ha desmatado | ✅ Concluído |

## Como Executar

```bash
cd /home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS
source venv/bin/activate

# Executar scripts em sequência (após implementação)
python fase_2_execucao/sprint_5_cadeia_global/01_etl_mapeamento_ncm.py
python fase_2_execucao/sprint_5_cadeia_global/02_etl_paises_referencia.py
python fase_2_execucao/sprint_5_cadeia_global/03_analise_ranking_uf.py
python fase_2_execucao/sprint_5_cadeia_global/04_analise_overlap_uf.py
python fase_2_execucao/sprint_5_cadeia_global/05_analise_matriz_paises.py
python fase_2_execucao/sprint_5_cadeia_global/06_analise_eficiencia_ambiental.py
```

## Artefatos Esperados

### Camada Gold

- `data/03_gold/ranking_uf_exportadora.parquet`
- `data/03_gold/uf_exportacao_vs_desmatamento.parquet`
- `data/03_gold/matriz_destino_exportacao.parquet`
- `data/03_gold/eficiencia_ambiental_exportacao.parquet`
- `data/03_gold/resumo_executivo_sprint5.json`

### Visualizações

- `data/04_reports/visualizacoes/heatmap_uf_commodity.png`
- `data/04_reports/visualizacoes/bar_ranking_uf_soja.png`
- `data/04_reports/visualizacoes/scatter_exportacao_desmatamento.png`
- `data/04_reports/visualizacoes/mapa_fluxo_exportacao.png`
- `data/04_reports/visualizacoes/box_eficiencia_ambiental.png`

## Mapeamento de Commodities (NCM)

### NCMs de Interesse

```python
NCM_COMMODITIES = {
    # Soja
    '12010000': 'Soja em grão',
    '12011000': 'Soja para semeadura',
    '12019000': 'Outras soja',
    
    # Milho
    '10051000': 'Milho para semeadura',
    '10059000': 'Outros milhos',
    
    # Carne Bovina
    '02011000': 'Carne bovina fresca/congelada, carcaça',
    '02012000': 'Carne bovina fresca/congelada, pedaços',
    '02013000': 'Carne bovina fresca/congelada, desossada',
    '02021000': 'Carne bovina congelada, carcaça',
    '02022000': 'Carne bovina congelada, pedaços',
    '02023000': 'Carne bovina congelada, desossada',
    
    # Café
    '09011100': 'Café não torrado, não descafeinado',
    '09011200': 'Café não torrado, descafeinado',
    '09012100': 'Café torrado, não descafeinado',
    '09012200': 'Café torrado, descafeinado',
    
    # Açúcar
    '17011300': 'Açúcar de cana, bruto',
    '17011400': 'Outros açúcares de cana, bruto',
    '17019100': 'Açúcar de cana, refinado',
    '17019900': 'Outros açúcares de cana',
    
    # Celulose
    '47032100': 'Pasta de celulose, branqueada',
    '47032900': 'Pasta de celulose, não branqueada',
    
    # Madeira
    '44011000': 'Lenha',
    '44031000': 'Madeira em bruto, tratada',
    '44032000': 'Madeira em bruto, não tratada',
    '44071000': 'Madeira serrada, conífera',
    '44072100': 'Madeira serrada, mogno',
    '44072200': 'Madeira serrada, virola',
    '44072500': 'Madeira serrada, tropical',
}
```

## Top 10 Países Compradores (Códigos)

| Código | Provável País | Frequência (2023) |
|--------|---------------|-------------------|
| 586 | China | 99.864 |
| 249 | Estados Unidos | 96.546 |
| 63 | Argentina | 84.437 |
| 845 | Holanda | 68.292 |
| 580 | Japão | 65.994 |
| 476 | Espanha | 64.134 |
| 158 | Chile | 63.861 |
| 434 | Itália | 57.495 |
| 169 | Colômbia | 48.326 |
| 493 | México | 46.598 |

## Critérios de Aceite

- [ ] Tabela NCM → commodity criada e validada
- [ ] Tabela código → país criada
- [ ] Ranking de UFs exportadoras gerado
- [ ] Overlap exportação × desmatamento calculado
- [ ] Matriz de países compradores gerada
- [ ] Métrica de eficiência ambiental calculada
- [ ] 5 visualizações geradas
- [ ] Resumo executivo em JSON

## Referências

- [Comex Stat - MDIC](https://comexstat.mdic.gov.br/)
- [Documentação NCM](https://www.gov.br/economia/pt-br/assuntos/macroeconomia/comercio-internacional/estatisticas-de-comercio-internacional/classificacoes-e-nomenclaturas)
- [Tabela de Países - IBGE](https://www.ibge.gov.br/explica/codigos-dos-paises.php)

---

**Status:** ⏳ Pendente de Implementação  
**Data de Criação:** 29/03/2026  
**Documentação de Contexto:** `docs/sprint5_contexto.md`
