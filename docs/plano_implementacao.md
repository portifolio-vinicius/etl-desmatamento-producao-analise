# Plano de Implementação: Análise de Desmatamento e Impacto Socioambiental

Para transformar este documento em realidade, é necessário um Plano de Implementação estruturado em etapas lógicas de Engenharia e Análise de Dados (pipeline de ETL e Modelagem). Abaixo, apresento o roteiro de execução para responder a todas as perguntas levantadas.

---

## Fase 1: Arquitetura e Stack Tecnológico

Antes de baixar os dados, defina o ambiente de trabalho para suportar o volume (especialmente os dados espaciais e do MapBiomas, que são pesados).

*   **Linguagem:** Python (ideal para automação e análise espacial).
*   **Bibliotecas Tabulares:** `pandas`, `numpy`, `basedosdados` (para SQL) e `sidrapy` (API do IBGE).
*   **Bibliotecas Espaciais:** `geopandas` (shapefiles), `shapely` (buffers), `rasterio` (imagens MapBiomas).
*   **Visualização:** `matplotlib`, `seaborn` (estatística), `folium` ou `kepler.gl` (mapas interativos).
*   **Armazenamento:** Arquivos **Parquet** locais para o MVP; **PostgreSQL** com extensão **PostGIS** para escala.

---

## Fase 2: Coleta e Engenharia de Dados (ETL)

O foco aqui é garantir que todas as bases falem a mesma língua antes de tentar responder às perguntas.

1.  **Padronização da Chave Primária:** Garanta que todas as bases usem o **Código IBGE de 7 dígitos** como `id_municipio` em formato de texto (string), para não perder zeros à esquerda.
2.  **Alinhamento Temporal:** Defina a janela de estudo (ex: 2012 a 2022). O PRODES mede o desmatamento de agosto a julho; PIB e PAM/PPM são anuais civis. Ajuste as agregações para que os períodos façam sentido.
3.  **Padronização de Projeção Espacial (CRS):** Converta todos os shapefiles para o mesmo Sistema de Referência (recomendado: **SIRGAS 2000 - EPSG:4674**) antes de cálculos de área ou distância.

---

## Fase 3: Execução Analítica (Respondendo às Perguntas)

### A. Eficiência Econômica e o "Custo Ambiental"

*   **Método para o "Desmatamento Ineficiente" e Índice (ICA):**
    1. Calcule o delta ($\Delta$) de desmatamento (PRODES) e o delta do Valor Adicionado da Agropecuária (PIB IBGE) em uma janela de 5 ou 10 anos.
    2. Aplique a fórmula:
       $$ICA_{i} = \frac{\Delta Desmatamento_{i} (ha)}{\Delta VAB\_Agro_{i} (R\$)}$$
    3. Ordene o dataframe decrescentemente; os primeiros da lista são os menos eficientes.
*   **Método para comparar Pecuária vs Agricultura:**
    1. Isole os municípios por dominância de crescimento (Rebanho/PPM vs Área Plantada/PAM).
    2. Calcule a correlação de Pearson entre essas variáveis e o PRODES, medindo o R$ gerado por hectare desmatado.

### B. Dinâmica Espacial e Efeito Vazamento (Spillover)

*   **Método para analisar as UCs:**
    1. Use `geopandas` e a função `buffer()` para criar um polígono de 10 km ao redor das UCs.
    2. Realize um `sjoin` (spatial join) com os polígonos de desmatamento do PRODES.
    3. Compare a densidade de desmatamento dentro da UC, no buffer e no resto do estado.
*   **Método para a Rota Temporal (Análise de Sobreposição):**
    1. Execute uma query temporal por coordenada: Ano 1 (DETER) -> Ano 2 (Fogo) -> Ano 3 (PRODES) -> Ano 4 (TerraClass indicando pasto).

### C. Cadeia de Suprimentos e Mercado Global

*   **Método de Rastreio Comercial:**
    1. Rankeie os 100 municípios líderes em desmatamento (PRODES).
    2. Filtre o Comex Stat por NCM (Soja e Carne Bovina).
    3. Identifique portos de saída e países de destino vinculados a esses municípios críticos via *merge*.
*   **Método de Impacto da Fiscalização:**
    1. Agrupe embargos (IBAMA) por município/ano.
    2. Analise a série temporal da produção (PAM/PPM) nos anos subsequentes ao pico de embargos para identificar quebras estruturais.

### D. O Paradoxo do Desenvolvimento Social

*   **Método de Correlação Socioeconômica:**
    1. Faça um *join* triplo: PRODES + PIB IBGE + Atlas Brasil (Variação IDHM).
    2. Gere um gráfico de dispersão (Scatter Plot): Eixo X (Crescimento PIB), Eixo Y (Variação IDHM), Tamanho da Bolha (Área Desmatada).
    3. Analise se o desmatamento resultou em desenvolvimento social amplo ou apenas concentração de renda.
