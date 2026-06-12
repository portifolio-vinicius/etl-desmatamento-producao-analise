# Notebooks de Análise Preditiva

Esta pasta contém notebooks Jupyter organizados para análise preditiva de desmatamento, atividade econômica e impacto socioambiental na Amazônia Legal.

## Estrutura de Notebooks

Cada notebook é autocontido e pode ser executado independentemente usando apenas o dataset principal.

### Notebooks Disponíveis

1. **01_previsao_desmatamento.ipynb** - Previsão de desmatamento para alocação de recursos de fiscalização
2. **02_previsao_embargos.ipynb** - Previsão de risco de embargos para compliance em cadeias de suprimento
3. **03_eficiencia_agricola.ipynb** - Identificação de potencial de melhoria de eficiência agrícola
4. **04_tendencias_temporais.ipynb** - Análise de tendências e projeções de desmatamento
5. **05_dashboard_consolidado.ipynb** - Dashboard consolidado de risco municipal

## Dados Necessários

### Arquivo Principal de Dados
**Caminho:** `../data/04_modelagem/dataset_preditivo_com_precos.parquet`

Este é o único arquivo de dados necessário para executar todos os notebooks. Ele contém:
- 796.560 observações (painel município × ano)
- 51 colunas com features econômicas, ambientais, sociais e meteorológicas
- Período: 2020-2023
- Cobertura: 5.570 municípios brasileiros

### Estrutura de Pastas para Teste em Outro Local

```
pasta_teste/
├── data/
│   └── 04_modelagem/
│       └── dataset_preditivo_com_precos.parquet  # COPIAR ESTE ARQUIVO
├── notebooks_analise_preditiva/
│   ├── 01_previsao_desmatamento.ipynb
│   ├── 02_previsao_embargos.ipynb
│   ├── 03_eficiencia_agricola.ipynb
│   ├── 04_tendencias_temporais.ipynb
│   ├── 05_dashboard_consolidado.ipynb
│   ├── README.md
│   └── requirements.txt
└── output/
    └── (resultados serão salvos aqui)
```

## Como Executar

### 1. Instalar Dependências
```bash
pip install -r requirements.txt
```

### 2. Preparar Dados
Copie o arquivo `dataset_preditivo_com_precos.parquet` para a estrutura de pastas correta.

### 3. Executar Notebooks
Execute os notebooks em ordem sequencial:
1. `01_previsao_desmatamento.ipynb`
2. `02_previsao_embargos.ipynb`
3. `03_eficiencia_agricola.ipynb`
4. `04_tendencias_temporais.ipynb`
5. `05_dashboard_consolidado.ipynb`

Ou execute cada notebook independentemente conforme necessário.

## Dependências

- pandas >= 2.0.0
- numpy >= 1.24.0
- scikit-learn >= 1.3.0
- pyarrow >= 12.0.0
- jupyter >= 1.0.0
- matplotlib >= 3.7.0
- seaborn >= 0.12.0

## Saída dos Notebooks

Cada notebook gera arquivos de resultados na pasta `../data/03_gold/`:

- `ranking_risco_desmatamento_2023.parquet` - Ranking de risco de desmatamento
- `ranking_risco_embargos_2023.parquet` - Ranking de risco de embargos
- `potencial_melhoria_eficiencia_2023.parquet` - Potencial de melhoria de eficiência
- `tendencias_desmatamento_temporal.parquet` - Tendências temporais
- `projecao_desmatamento_2024.parquet` - Projeção de desmatamento 2024
- `dashboard_preditivo_consolidado.parquet` - Dashboard consolidado

## Notas Importantes

- Os notebooks usam caminhos relativos assumindo a estrutura de pastas do projeto original
- Para testar em outro local, ajuste os caminhos no início de cada notebook
- Todos os notebooks incluem validação de dados e tratamento de erros
- Os modelos usam apenas features disponíveis no dataset principal

## Suporte

Para dúvidas ou problemas, consulte a documentação completa em:
`../docs/recomendacoes_estrategicas_preditivas.md`