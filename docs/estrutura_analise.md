# Estrutura da Análise de Dados: Desmatamento, Economia e Impacto Socioambiental

## 1. Mapa de Dados Abertos e Públicos (O "Dicionário" do Projeto)

Para facilitar a arquitetura de dados, as fontes estão divididas por eixo temático. Todas utilizam o **Código IBGE de 7 dígitos** como chave primária para os cruzamentos.

| Eixo Temático | Base de Dados (Fonte) | O que fornece ao projeto |
| :--- | :--- | :--- |
| **Desmatamento e Fogo** | PRODES (INPE) | Taxa oficial de desmatamento anual por corte raso ($km^2$). |
| | DETER (INPE) | Alertas em tempo quase real, mostrando o ritmo mensal da degradação. |
| | MapBiomas Fogo | Cicatrizes de incêndios, indicando a "limpeza" da área. |
| | Limites de UCs (ICMBio/INPE) | Shapefiles delimitando áreas de proteção integral e uso sustentável. |
| **Uso e Transição do Solo** | MapBiomas (Coleção 10+) | Histórico anual (pixel a pixel) da cobertura do solo (floresta, pasto, agricultura). |
| | TerraClass (INPE) | Mapeamento exato do que ocorreu após o desmatamento (ex: virou solo exposto ou pasto limpo?). |
| **Economia e Agropecuária** | PAM (IBGE) | Produção Agrícola Municipal: área plantada e valor da produção de soja, milho, etc. |
| | PPM (IBGE) | Pesquisa da Pecuária Municipal: evolução do tamanho do rebanho bovino. |
| | PIB Municipal (IBGE) | Valor Adicionado Bruto (VAB) exclusivo da Agropecuária. |
| | Comex Stat (MDIC) | Volume e valor de exportação (soja, carne) por município e país de destino. |
| **Impacto Socioambiental** | Embargos (IBAMA) | Lista de propriedades punidas por crimes ambientais (termômetro de fiscalização). |
| | Atlas Brasil (IPEA/PNUD) | Índice de Desenvolvimento Humano Municipal (IDHM), renda e longevidade local. |

## 2. Detalhamento das Fontes e Links de Acesso

### Eixo: Desmatamento e Fogo

*   **PRODES (INPE)** — Taxa oficial de desmatamento anual:
    *   Portal principal: [TerraBrasilis Downloads](https://terrabrasilis.dpi.inpe.br/downloads/) (vetorial e raster).
    *   [Dados.gov.br - PRODES](https://dados.gov.br/dados/conjuntos-dados/prodes) e via Base dos Dados.
*   **DETER (INPE)** — Alertas em tempo quase real:
    *   Downloads: [TerraBrasilis Downloads](https://terrabrasilis.dpi.inpe.br/downloads/) (shapefiles).
    *   Portal Oficial: [Dados.gov.br - DETER](https://dados.gov.br/dados/conjuntos-dados/deter).
*   **MapBiomas Fogo (e Coleção 10+)**:
    *   Downloads diretos: [MapBiomas Downloads](https://brasil.mapbiomas.org/downloads).
    *   Recortes personalizados: [Plataforma MapBiomas](https://plataforma.brasil.mapbiomas.org).
*   **Limites de Unidades de Conservação (UCs) — ICMBio/INPE**:
    *   Shapefiles oficiais: [ICMBio - Dados Geoespaciais](https://www.gov.br/icmbio/pt-br/assuntos/dados_geoespaciais/mapa-tematico-e-dados-geoestatisticos-das-unidades-de-conservacao-federais).
    *   [Dados.gov.br - Limites UCs](https://dados.gov.br/dados/conjuntos-dados/limites-oficiais-de-unidades-de-conservacao-federais).

### Eixo: Uso e Transição do Solo

*   **MapBiomas (Coleção 10+)**: Veja links acima.
*   **TerraClass (INPE)** — Mapeamento pós-desmatamento:
    *   Portal de download: [TerraClass Download](https://www.terraclass.gov.br/download-de-dados).
    *   Via [TerraBrasilis](https://terrabrasilis.dpi.inpe.br/downloads/).

### Eixo: Economia e Agropecuária

*   **PAM (IBGE)** — Produção Agrícola Municipal:
    *   [Página Oficial IBGE](https://www.ibge.gov.br/estatisticas/economicas/agricultura-e-pecuaria/9117-producao-agricola-municipal-culturas-temporarias-e-permanentes.html).
    *   [FTP IBGE](https://ftp.ibge.gov.br/Producao_Agricola/Producao_Agricola_Municipal/).
    *   [SIDRA](https://sidra.ibge.gov.br) para consultas.
*   **PPM (IBGE)** — Pesquisa da Pecuária Municipal:
    *   [Página Oficial IBGE](https://www.ibge.gov.br/estatisticas/economicas/agricultura-e-pecuaria/9107-producao-da-pecuaria-municipal.html).
    *   [FTP IBGE](https://ftp.ibge.gov.br/Producao_Pecuaria/Producao_da_Pecuaria_Municipal/).
    *   [Dados.gov.br - PPM](https://dados.gov.br/dados/conjuntos-dados/pp-pesquisa-da-pecuaria-municipal).
*   **PIB Municipal (IBGE)**:
    *   [Página Oficial IBGE (2002–2023)](https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html).
*   **Comex Stat (MDIC)** — Exportações por município:
    *   Portal Principal: [Comex Stat](https://comexstat.mdic.gov.br/).
    *   Dados Abertos (CSV): [Comex Stat - Open Data](https://comexstat.mdic.gov.br/pt/geral).

### Eixo: Impacto Socioambiental

*   **Embargos (IBAMA)**:
    *   [Shapefiles - Termos de Embargo](https://dadosabertos.ibama.gov.br/dataset/termos-de-embargo).
    *   [Dados.gov.br - Ibama](https://dados.gov.br/dados/conjuntos-dados/termos-de-embargo).
*   **Atlas Brasil (IPEA/PNUD)** — IDHM e indicadores:
    *   Portal Principal: [Atlas Brasil](http://www.atlasbrasil.org.br/).
    *   [Dados.gov.br - Atlas Brasil](https://dados.gov.br/dados/conjuntos-dados/atlasbrasil).

## 3. Dicas para facilitar os cruzamentos

*   **Chave Comum:** Utilize o **Código IBGE de 7 dígitos** para realizar *joins* (Pandas, SQL, etc).
*   **Agregadores:** A [Base dos Dados](https://basedosdados.org/) oferece muitas destas tabelas já tratadas.
*   **Análise Espacial:** Utilize QGIS para criar buffers de 10 km e análises geográficas complexas.
*   **Cronograma:** Sempre valide o ano de referência (ex: PRODES anual vs DETER mensal).

---

## 4. Perguntas Inteligentes e Correlacionadas

Cruzar essas bases permite ir muito além de relatórios descritivos, possibilitando análises causais e de eficiência. Aqui estão as perguntas mais relevantes e complexas que o seu modelo poderá responder:

### A. Eficiência Econômica e o "Custo Ambiental"

1.  **Existe o "Desmatamento Ineficiente"?** Quais são os municípios que mais destruíram a floresta (PRODES) na última década, mas apresentaram a menor taxa de crescimento no Valor Adicionado da Agropecuária (PIB IBGE)?
2.  **Qual é o verdadeiro Índice de Custo Ambiental (ICA)?** Utilizando a fórmula de custo ambiental, quais são os 50 municípios com a pior relação custo-benefício ecológico?
    $$ICA_{i} = \frac{\Delta Desmatamento_{i} (ha)}{\Delta VAB\_Agro_{i} (R\$)}$$
3.  **A pecuária é o vetor de menor valor agregado?** Cruzando o aumento do rebanho bovino (PPM) com a área desmatada (PRODES), qual é o rendimento em Reais por hectare desmatado comparado aos municípios focados em agricultura (PAM)?

### B. Dinâmica Espacial e Efeito Vazamento (Spillover)

1.  **As Unidades de Conservação contêm ou apenas "empurram" a motosserra?** Ao criar um buffer espacial de 10 km ao redor das UCs, a taxa de desmatamento nessa zona de borda é estatisticamente maior do que a média do resto do estado?
2.  **Qual é a rota temporal da conversão do solo?** É possível provar a cronologia do crime ambiental mostrando que, na mesma coordenada, houve um alerta de degradação (DETER no ano 1), seguido de incêndio (MapBiomas Fogo no ano 2), consolidado como desmatamento (PRODES no ano 3) e transformado em pastagem (TerraClass no ano 4)?

### C. Cadeia de Suprimentos e Mercado Global

1.  **Para onde vai a produção do desmatamento?** Cruzando as taxas mais altas do PRODES com o banco do Comex Stat, quais países são os maiores compradores da soja e da carne exportadas diretamente dos municípios campeões de desmatamento?
2.  **A fiscalização altera a produção local?** Nos municípios com maior número de embargos do IBAMA, houve queda subsequente na produção declarada na PAM/PPM ou o mercado encontrou formas de escoar essa produção de forma invisível?

### D. O Paradoxo do Desenvolvimento Social

1.  **Desmatar enriquece a população local?** Os municípios da Amazônia Legal que registraram os maiores crescimentos no PIB Agropecuário associados ao desmatamento (PRODES + PIB IBGE) tiveram um aumento proporcional no IDHM e na renda per capita (Atlas IPEA), ou a riqueza ficou concentrada, gerando passivo ambiental sem desenvolvimento social?
