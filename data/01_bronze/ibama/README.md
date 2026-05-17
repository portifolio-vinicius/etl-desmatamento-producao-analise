# Documentação: ibama

**Pasta:** `ibama`

**Descrição:** Dados brutos da camada Bronze para ibama

---

## Resumo Geral

- **Total de arquivos:** 2
- **Total de linhas:** 177,172
- **Tamanho total:** 93.12 MB

---

## Detalhes por Arquivo

### 1. embargos_ibama_tabular.parquet

**Tipo:** parquet
**Tamanho:** 20.22 MB
**Linhas:** 88,586
**Colunas:** 38

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| objectid | int64 | 0 | 0.0% | 88,586 | 1.00 | 88586.00 | 44293.50 | 44293.50 |
| seq_tad | int64 | 0 | 0.0% | 87,533 | 0.00 | 1876749.00 | 1231892.22 | 1491242.00 |
| num_tad | object | 0 | 0.0% | 87,698 | N/A | N/A | N/A | N/A |
| serie_tad | object | 24,615 | 27.79% | 5 | N/A | N/A | N/A | N/A |
| operacao | object | 79,121 | 89.32% | 747 | N/A | N/A | N/A | N/A |
| origem_geo | object | 0 | 0.0% | 3 | N/A | N/A | N/A | N/A |
| cod_uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| cod_munici | int64 | 0 | 0.0% | 3,769 | 1100015.00 | 9999999.00 | 2485295.27 | 1716208.00 |
| municipio | object | 0 | 0.0% | 3,633 | N/A | N/A | N/A | N/A |
| nome_imove | object | 77,314 | 87.28% | 9,318 | N/A | N/A | N/A | N/A |
| des_locali | object | 10,199 | 11.51% | 71,720 | N/A | N/A | N/A | N/A |
| nome_embar | object | 5,368 | 6.06% | 64,673 | N/A | N/A | N/A | N/A |
| cpf_cnpj_e | object | 8,878 | 10.02% | 65,653 | N/A | N/A | N/A | N/A |
| sit_desmat | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| tipo_area | object | 53,557 | 60.46% | 7 | N/A | N/A | N/A | N/A |
| num_auto_i | object | 13,443 | 15.18% | 74,387 | N/A | N/A | N/A | N/A |
| serie_auto | object | 31,927 | 36.04% | 8 | N/A | N/A | N/A | N/A |
| cod_tipo_b | object | 33,902 | 38.27% | 40 | N/A | N/A | N/A | N/A |
| des_tipo_b | object | 33,902 | 38.27% | 40 | N/A | N/A | N/A | N/A |
| unid_contr | object | 20 | 0.02% | 133 | N/A | N/A | N/A | N/A |
| ordem_fisc | object | 40,581 | 45.81% | 6,247 | N/A | N/A | N/A | N/A |
| cd_acao_fi | object | 65,599 | 74.05% | 22,811 | N/A | N/A | N/A | N/A |
| num_proces | object | 6,410 | 7.24% | 81,270 | N/A | N/A | N/A | N/A |
| des_tad | object | 1,535 | 1.73% | 82,502 | N/A | N/A | N/A | N/A |
| des_infrac | object | 19,771 | 22.32% | 246 | N/A | N/A | N/A | N/A |
| num_longit | object | 12,865 | 14.52% | 60,753 | N/A | N/A | N/A | N/A |
| num_latitu | object | 11,844 | 13.37% | 57,074 | N/A | N/A | N/A | N/A |
| dat_embarg | object | 0 | 0.0% | 84,145 | N/A | N/A | N/A | N/A |
| dat_impres | object | 18,647 | 21.05% | 21,714 | N/A | N/A | N/A | N/A |
| dat_ult_al | object | 437 | 0.49% | 34,155 | N/A | N/A | N/A | N/A |
| num_long00 | float64 | 12,865 | 14.52% | 59,642 | -96.54 | 0.00 | -51.56 | -53.74 |
| num_lati00 | float64 | 11,844 | 13.37% | 56,566 | -135.50 | 62.62 | -9.57 | -9.01 |
| qtd_area_d | float64 | 61,284 | 69.18% | 14,470 | 0.00 | 1876484.75 | 503.40 | 20.59 |
| qtd_area_e | float64 | 32,305 | 36.47% | 46,656 | 0.00 | 68744.00 | 121.82 | 24.17 |
| dat_ult_00 | object | 437 | 0.49% | 34,289 | N/A | N/A | N/A | N/A |
| st_area(sh | float64 | 13,908 | 15.7% | 54,740 | 0.00 | 0.04 | 0.00 | 0.00 |
| st_perimet | float64 | 13,908 | 15.7% | 69,349 | 0.00 | 14.73 | 0.03 | 0.01 |

#### Exemplos de Valores (Colunas Categóricas)

**num_tad:** 756611, 602347, 729878, 746952, 728729, ...

**serie_tad:** E, C, A, D, B

**operacao:** ONDA VERDE P11, ONDA VERDE, CONTROLE REMOTO P1, PONTA DO ABUNÃ V, PONTA DO ABUNÃ III, ...

**origem_geo:** Polígono, Sem Geometria, Ponto

**cod_uf:** 13, 29, 15, 31, 51, ...

**uf:** AM, BA, PA, MG, MT, ...

**municipio:** Lábrea, Santa Cruz Cabrália, Altamira, Manga, Manicoré, ...

**nome_imove:** Zona Rural, distrito de S A MATUPI, Fazenda Jacuba, Fazenda Mata Verde., Fazenda Conquista Senepol., Fazenda Onassis I., ...

**des_locali:** Coordenadas de Referencia 09°21'43'' S - 66°35'05'' W, Sítio Santa Bárbara, Estrada Vicinal Celeste, km 180, Altamira PA., fazenda agropasto, estrada São João das Missões/Manga, município de Manga MG, coordenada da entrada 14°52'05,29''-S 44°04'37,98''-W., AREA DE PROTEÇÃO AMBIENTAL ( APA TRIUNFO DO  XINGU ) ALTAMIRA/PA, ...

**nome_embar:** DESCONHECIDO, WELLINGTON RODRIGUES DANTAS, EMILIO CARLO NOGUEIRA BATAGIN, REGINALDO FRAGA GUEDES, CORIOLANO RODRIGUES DA SILVA, ...

**cpf_cnpj_e:** 04449165551, 08972866857, 03493521600, 03649890178, 87837307291, ...

**sit_desmat:** D, N

**tipo_area:** Desmatamento, Outros, Queimada, Atividade, Não se Aplica, Desmatamento e Queim, Não se aplica

**num_auto_i:** 9168921, 9126094, 9137930, 9125606, 9131912, ...

**serie_auto:** E, D, A, B, a, O, C, d

**cod_tipo_b:** 1, 4, 5, 3, 4, 3, ...

**des_tipo_b:** Mata Atlantica, Amazonia, Caatinga, Cerrado, Amazonia, Cerrado, ...

**unid_contr:** DIFIS - Manaus/AM, Gerência Executiva do Ibama em Eunápolis/BA, Unidade Técnica Nível 1 em Altamira/PA, DIFIS - Belo Horizonte/MG, DIFIS - Cuiabá/MT, ...

**ordem_fisc:** DF590151, DF590154, MG048208, DF590142, DF590156, ...

**cd_acao_fi:** HX0MG1R, 2WVGK2X, JADRJZ5, J6DZZSV, HUM65BU, ...

**num_proces:** 02024102709201736, 02059100167201786, 02048101405201708, 02566100124201771, 02018103067201798, ...

**des_tad:** Artigo 78 da Lei 5.172/66 § unico , Artigo 50 da lei federal 9.605/98, Artigo 16,101,108 § 2° do decreto federal 6.514/08, artigo 51 da lei federal 12.651/12, fica embargado por edital apartir dessa data e hora uma área de 244,43 há desmatada, const, Fica embargado o uso e as atividades que possam dificultar ou impedir a regeneração natural de uma área de 11,5 Hectares delimitada pelas Coordenadas geográficas apresentadas no memorial descritivo em anexo, com o objetivo de impedir a continuidade d, Fica embargadas todas as atividades produtivas, excetuando-se as de proteção ambientais e as determinadas e ou autorizadas pelas autoridades competentes, na area objeto do AI n 9126094-E. Lei Federal 9605/98 70 1° 72 II VII Decreto Federal 6514/08 3 I, Lei Federal 9605/98 70 1° 72 II,VII Decreto Federal 6514/08 3 II,VII 50 1° Lei 11.428/2006 2°.    Fica embargada a área de 28,6386 hectares, objeto do AI n° 9137930-E nas coordenadas dos vértices, 14° 52'05,29'' S 44°04'18,88'' W, 14°52'14,77''S, Lei Federal 9605/98 70 1° 72 II,VII Decreto Federal 6514/08 3 II,VII 50 e 93    FICA EMBARGADA A ÁREA DE 704,59 , REFERENTE AO AI N° 9125606-E , PARA POSSIBILITAR A COMPLETA REGENERAÇÃO DA VEGETAÇÃO NATIVA , NO POLIGONO DE COORDENADAS CENTRAIS S 0, ...

**des_infrac:** Infração da Flora(Não Classificada-Móvel), Infração de Licenciamento(Não Classificada-Móvel), Infração de pesca(Não Classificada-Móvel), Infração da Fauna(Não Classificada-Móvel), Infração de Administração Ambiental(Não Classificada-Móvel), ...

**num_longit:** 66° 35' 04.999'' W, 39° 10' 52.000'' W, 54° 51' 47.002'' W, 44° 03' 59.000'' W, 53° 25' 14.002'' W, ...

**num_latitu:** 09° 21' 42.998'' S, 16° 10' 41.999'' S, 06° 28' 54.001'' S, 14° 52' 05.002'' S, 06° 06' 14.000'' S, ...

**dat_embarg:** 11/07/17 14:48:00, 11/07/17 16:49:00, 12/07/17 08:40:00, 12/07/17 10:58:00, 12/07/17 15:44:00, ...

**dat_impres:** 11/07/17 14:53:30, 11/07/17 17:08:59, 12/07/17 16:16:08, 12/07/17 11:26:27, 12/07/17 16:03:03, ...

**dat_ult_al:** 28/01/19 13:36:23, 03/10/17 16:34:45, 17/07/17 12:27:36, 08/08/17 08:46:25, 17/07/17 11:02:28, ...

**dat_ult_00:** 28/01/19 13:36:23, 03/10/17 16:34:45, 17/07/17 12:27:36, 08/08/17 08:46:25, 17/07/17 11:02:28, ...

---

### 2. embargos_ibama_full.geoparquet

**Tipo:** geoparquet
**Tamanho:** 72.9 MB
**Linhas:** 88,586
**Colunas:** 39

**CRS:** {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "SIRGAS 2000", "datum": {"type": "GeodeticReferenceFrame", "name": "Sistema de Referencia Geocentrico para las AmericaS 2000", "ellipsoid": {"name": "GRS 1980", "semi_major_axis": 6378137, "inverse_flattening": 298.257222101}}, "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}, {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]}, "scope": "Horizontal component of 3D system.", "area": "Latin America - Central America and South America - onshore and offshore. Brazil - onshore and offshore.", "bbox": {"south_latitude": -59.87, "west_longitude": -122.19, "north_latitude": 32.72, "east_longitude": -25.28}, "id": {"authority": "EPSG", "code": 4674}}

#### Geometria

**Tipos de geometria:**
- Polygon: 65,856
- MultiPolygon: 8,820

**Extensão (bounds):** [-90.70501283699997, -82.85206494599998, -0.6058355909999591, 62.61667597200005]

#### Colunas

| Nome | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Mediana |
|------|------|-------|---------|--------|-----|-----|-------|--------|
| objectid | int64 | 0 | 0.0% | 88,586 | 1.00 | 88586.00 | 44293.50 | 44293.50 |
| seq_tad | int64 | 0 | 0.0% | 87,533 | 0.00 | 1876749.00 | 1231892.22 | 1491242.00 |
| num_tad | object | 0 | 0.0% | 87,698 | N/A | N/A | N/A | N/A |
| serie_tad | object | 24,615 | 27.79% | 5 | N/A | N/A | N/A | N/A |
| operacao | object | 79,121 | 89.32% | 747 | N/A | N/A | N/A | N/A |
| origem_geo | object | 0 | 0.0% | 3 | N/A | N/A | N/A | N/A |
| cod_uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| uf | object | 0 | 0.0% | 27 | N/A | N/A | N/A | N/A |
| cod_munici | int64 | 0 | 0.0% | 3,769 | 1100015.00 | 9999999.00 | 2485295.27 | 1716208.00 |
| municipio | object | 0 | 0.0% | 3,633 | N/A | N/A | N/A | N/A |
| nome_imove | object | 77,314 | 87.28% | 9,318 | N/A | N/A | N/A | N/A |
| des_locali | object | 10,199 | 11.51% | 71,720 | N/A | N/A | N/A | N/A |
| nome_embar | object | 5,368 | 6.06% | 64,673 | N/A | N/A | N/A | N/A |
| cpf_cnpj_e | object | 8,878 | 10.02% | 65,653 | N/A | N/A | N/A | N/A |
| sit_desmat | object | 0 | 0.0% | 2 | N/A | N/A | N/A | N/A |
| tipo_area | object | 53,557 | 60.46% | 7 | N/A | N/A | N/A | N/A |
| num_auto_i | object | 13,443 | 15.18% | 74,387 | N/A | N/A | N/A | N/A |
| serie_auto | object | 31,927 | 36.04% | 8 | N/A | N/A | N/A | N/A |
| cod_tipo_b | object | 33,902 | 38.27% | 40 | N/A | N/A | N/A | N/A |
| des_tipo_b | object | 33,902 | 38.27% | 40 | N/A | N/A | N/A | N/A |
| unid_contr | object | 20 | 0.02% | 133 | N/A | N/A | N/A | N/A |
| ordem_fisc | object | 40,581 | 45.81% | 6,247 | N/A | N/A | N/A | N/A |
| cd_acao_fi | object | 65,599 | 74.05% | 22,811 | N/A | N/A | N/A | N/A |
| num_proces | object | 6,410 | 7.24% | 81,270 | N/A | N/A | N/A | N/A |
| des_tad | object | 1,535 | 1.73% | 82,502 | N/A | N/A | N/A | N/A |
| des_infrac | object | 19,771 | 22.32% | 246 | N/A | N/A | N/A | N/A |
| num_longit | object | 12,865 | 14.52% | 60,753 | N/A | N/A | N/A | N/A |
| num_latitu | object | 11,844 | 13.37% | 57,074 | N/A | N/A | N/A | N/A |
| dat_embarg | object | 0 | 0.0% | 84,145 | N/A | N/A | N/A | N/A |
| dat_impres | object | 18,647 | 21.05% | 21,714 | N/A | N/A | N/A | N/A |
| dat_ult_al | object | 437 | 0.49% | 34,155 | N/A | N/A | N/A | N/A |
| num_long00 | float64 | 12,865 | 14.52% | 59,642 | -96.54 | 0.00 | -51.56 | -53.74 |
| num_lati00 | float64 | 11,844 | 13.37% | 56,566 | -135.50 | 62.62 | -9.57 | -9.01 |
| qtd_area_d | float64 | 61,284 | 69.18% | 14,470 | 0.00 | 1876484.75 | 503.40 | 20.59 |
| qtd_area_e | float64 | 32,305 | 36.47% | 46,656 | 0.00 | 68744.00 | 121.82 | 24.17 |
| dat_ult_00 | object | 437 | 0.49% | 34,289 | N/A | N/A | N/A | N/A |
| st_area(sh | float64 | 13,908 | 15.7% | 54,740 | 0.00 | 0.04 | 0.00 | 0.00 |
| st_perimet | float64 | 13,908 | 15.7% | 69,349 | 0.00 | 14.73 | 0.03 | 0.01 |

#### Exemplos de Valores (Colunas Categóricas)

**num_tad:** 756611, 602347, 729878, 746952, 728729, ...

**serie_tad:** E, C, A, D, B

**operacao:** ONDA VERDE P11, ONDA VERDE, CONTROLE REMOTO P1, PONTA DO ABUNÃ V, PONTA DO ABUNÃ III, ...

**origem_geo:** Polígono, Sem Geometria, Ponto

**cod_uf:** 13, 29, 15, 31, 51, ...

**uf:** AM, BA, PA, MG, MT, ...

**municipio:** Lábrea, Santa Cruz Cabrália, Altamira, Manga, Manicoré, ...

**nome_imove:** Zona Rural, distrito de S A MATUPI, Fazenda Jacuba, Fazenda Mata Verde., Fazenda Conquista Senepol., Fazenda Onassis I., ...

**des_locali:** Coordenadas de Referencia 09°21'43'' S - 66°35'05'' W, Sítio Santa Bárbara, Estrada Vicinal Celeste, km 180, Altamira PA., fazenda agropasto, estrada São João das Missões/Manga, município de Manga MG, coordenada da entrada 14°52'05,29''-S 44°04'37,98''-W., AREA DE PROTEÇÃO AMBIENTAL ( APA TRIUNFO DO  XINGU ) ALTAMIRA/PA, ...

**nome_embar:** DESCONHECIDO, WELLINGTON RODRIGUES DANTAS, EMILIO CARLO NOGUEIRA BATAGIN, REGINALDO FRAGA GUEDES, CORIOLANO RODRIGUES DA SILVA, ...

**cpf_cnpj_e:** 04449165551, 08972866857, 03493521600, 03649890178, 87837307291, ...

**sit_desmat:** D, N

**tipo_area:** Desmatamento, Outros, Queimada, Atividade, Não se Aplica, Desmatamento e Queim, Não se aplica

**num_auto_i:** 9168921, 9126094, 9137930, 9125606, 9131912, ...

**serie_auto:** E, D, A, B, a, O, C, d

**cod_tipo_b:** 1, 4, 5, 3, 4, 3, ...

**des_tipo_b:** Mata Atlantica, Amazonia, Caatinga, Cerrado, Amazonia, Cerrado, ...

**unid_contr:** DIFIS - Manaus/AM, Gerência Executiva do Ibama em Eunápolis/BA, Unidade Técnica Nível 1 em Altamira/PA, DIFIS - Belo Horizonte/MG, DIFIS - Cuiabá/MT, ...

**ordem_fisc:** DF590151, DF590154, MG048208, DF590142, DF590156, ...

**cd_acao_fi:** HX0MG1R, 2WVGK2X, JADRJZ5, J6DZZSV, HUM65BU, ...

**num_proces:** 02024102709201736, 02059100167201786, 02048101405201708, 02566100124201771, 02018103067201798, ...

**des_tad:** Artigo 78 da Lei 5.172/66 § unico , Artigo 50 da lei federal 9.605/98, Artigo 16,101,108 § 2° do decreto federal 6.514/08, artigo 51 da lei federal 12.651/12, fica embargado por edital apartir dessa data e hora uma área de 244,43 há desmatada, const, Fica embargado o uso e as atividades que possam dificultar ou impedir a regeneração natural de uma área de 11,5 Hectares delimitada pelas Coordenadas geográficas apresentadas no memorial descritivo em anexo, com o objetivo de impedir a continuidade d, Fica embargadas todas as atividades produtivas, excetuando-se as de proteção ambientais e as determinadas e ou autorizadas pelas autoridades competentes, na area objeto do AI n 9126094-E. Lei Federal 9605/98 70 1° 72 II VII Decreto Federal 6514/08 3 I, Lei Federal 9605/98 70 1° 72 II,VII Decreto Federal 6514/08 3 II,VII 50 1° Lei 11.428/2006 2°.    Fica embargada a área de 28,6386 hectares, objeto do AI n° 9137930-E nas coordenadas dos vértices, 14° 52'05,29'' S 44°04'18,88'' W, 14°52'14,77''S, Lei Federal 9605/98 70 1° 72 II,VII Decreto Federal 6514/08 3 II,VII 50 e 93    FICA EMBARGADA A ÁREA DE 704,59 , REFERENTE AO AI N° 9125606-E , PARA POSSIBILITAR A COMPLETA REGENERAÇÃO DA VEGETAÇÃO NATIVA , NO POLIGONO DE COORDENADAS CENTRAIS S 0, ...

**des_infrac:** Infração da Flora(Não Classificada-Móvel), Infração de Licenciamento(Não Classificada-Móvel), Infração de pesca(Não Classificada-Móvel), Infração da Fauna(Não Classificada-Móvel), Infração de Administração Ambiental(Não Classificada-Móvel), ...

**num_longit:** 66° 35' 04.999'' W, 39° 10' 52.000'' W, 54° 51' 47.002'' W, 44° 03' 59.000'' W, 53° 25' 14.002'' W, ...

**num_latitu:** 09° 21' 42.998'' S, 16° 10' 41.999'' S, 06° 28' 54.001'' S, 14° 52' 05.002'' S, 06° 06' 14.000'' S, ...

**dat_embarg:** 11/07/17 14:48:00, 11/07/17 16:49:00, 12/07/17 08:40:00, 12/07/17 10:58:00, 12/07/17 15:44:00, ...

**dat_impres:** 11/07/17 14:53:30, 11/07/17 17:08:59, 12/07/17 16:16:08, 12/07/17 11:26:27, 12/07/17 16:03:03, ...

**dat_ult_al:** 28/01/19 13:36:23, 03/10/17 16:34:45, 17/07/17 12:27:36, 08/08/17 08:46:25, 17/07/17 11:02:28, ...

**dat_ult_00:** 28/01/19 13:36:23, 03/10/17 16:34:45, 17/07/17 12:27:36, 08/08/17 08:46:25, 17/07/17 11:02:28, ...

---



## Análises Estatísticas Avançadas

### Distribuição e Forma dos Dados

| Variável | Skewness | Kurtosis | CV | P25 | P75 | IQR |
|----------|----------|----------|----|-----|-----|-----|
| objectid | 0.000 | -1.200 | 0.577 | 22147.25 | 66439.75 | 44292.50 |
| seq_tad | -0.656 | -1.067 | 0.491 | 708378.00 | 1782378.75 | 1074000.75 |
| cod_munici | 0.834 | -0.635 | 0.560 | 1302702.00 | 3203205.00 | 1900503.00 |
| num_long00 | 1.882 | 4.689 | -0.278 | -61.45 | -46.67 | 14.78 |
| num_lati00 | -1.252 | 8.347 | -0.716 | -11.58 | -5.77 | 5.81 |
| qtd_area_d | 77.055 | 6746.155 | 37.190 | 6.00 | 62.00 | 56.00 |
| qtd_area_e | 43.584 | 2761.168 | 6.117 | 5.90 | 78.00 | 72.10 |
| st_area(sh | 45.458 | 3213.607 | 6.386 | 0.00 | 0.00 | 0.00 |
| st_perimet | 60.104 | 5471.575 | 4.299 | 0.00 | 0.03 | 0.03 |

**Interpretação:**
- **Skewness > 1**: Distribuição altamente assimétrica à direita
- **Skewness < -1**: Distribuição altamente assimétrica à esquerda
- **Kurtosis > 3**: Distribuição com caudas pesadas (leptocúrtica)
- **CV > 1**: Alta variabilidade relativa

### Correlações Fortes (|r| > 0.7)

| Variável 1 | Variável 2 | Correlação |
|-----------|-----------|------------|
| qtd_area_e | st_area(sh | 0.736 |

**Atenção:** Correlações fortes podem indicar multicolinearidade ou relações causais que merecem investigação.

### Análise de Outliers (Método IQR)

| Variável | Q1 | Q3 | IQR | Lower Bound | Upper Bound | N Outliers | % Outliers |
|----------|----|----|-----|-------------|-------------|------------|-------------|
| objectid | 22147.25 | 66439.75 | 44292.50 | -44291.50 | 132878.50 | 0 | 0.00% |
| seq_tad | 708378.00 | 1782378.75 | 1074000.75 | -902623.12 | 3393379.88 | 0 | 0.00% |
| cod_munici | 1302702.00 | 3203205.00 | 1900503.00 | -1548052.50 | 6053959.50 | 7 | 0.01% |
| num_long00 | -61.45 | -46.67 | 14.78 | -83.62 | -24.50 | 3358 | 4.43% |
| num_lati00 | -11.58 | -5.77 | 5.81 | -20.30 | 2.95 | 7191 | 9.37% |
| qtd_area_d | 6.00 | 62.00 | 56.00 | -78.00 | 146.00 | 3486 | 12.77% |
| qtd_area_e | 5.90 | 78.00 | 72.10 | -102.24 | 186.14 | 6940 | 12.33% |
| st_area(sh | 0.00 | 0.00 | 0.00 | -0.00 | 0.00 | 9910 | 13.27% |
| st_perimet | 0.00 | 0.03 | 0.03 | -0.05 | 0.08 | 6235 | 8.35% |

### Qualidade de Dados

**Duplicatas:** 0 (0.00%)

#### Cardinalidade das Colunas

| Coluna | Cardinalidade | Razão Cardinalidade |
|--------|---------------|---------------------|
| objectid | 88,586 | 1.0000 (Alta - quase única) |
| seq_tad | 87,533 | 0.9881 (Alta - quase única) |
| num_tad | 87,698 | 0.9900 (Alta - quase única) |
| serie_tad | 5 | 0.0001 (Muito Baixa - categórica) |
| operacao | 747 | 0.0084 (Muito Baixa - categórica) |
| origem_geo | 3 | 0.0000 (Muito Baixa - categórica) |
| cod_uf | 27 | 0.0003 (Muito Baixa - categórica) |
| uf | 27 | 0.0003 (Muito Baixa - categórica) |
| cod_munici | 3,769 | 0.0425 (Baixa) |
| municipio | 3,633 | 0.0410 (Baixa) |
| nome_imove | 9,318 | 0.1052 (Média) |
| des_locali | 71,720 | 0.8096 (Média-Alta) |
| nome_embar | 64,673 | 0.7301 (Média-Alta) |
| cpf_cnpj_e | 65,653 | 0.7411 (Média-Alta) |
| sit_desmat | 2 | 0.0000 (Muito Baixa - categórica) |
| tipo_area | 7 | 0.0001 (Muito Baixa - categórica) |
| num_auto_i | 74,387 | 0.8397 (Média-Alta) |
| serie_auto | 8 | 0.0001 (Muito Baixa - categórica) |
| cod_tipo_b | 40 | 0.0005 (Muito Baixa - categórica) |
| des_tipo_b | 40 | 0.0005 (Muito Baixa - categórica) |
| unid_contr | 133 | 0.0015 (Muito Baixa - categórica) |
| ordem_fisc | 6,247 | 0.0705 (Baixa) |
| cd_acao_fi | 22,811 | 0.2575 (Média) |
| num_proces | 81,270 | 0.9174 (Alta - quase única) |
| des_tad | 82,502 | 0.9313 (Alta - quase única) |
| des_infrac | 246 | 0.0028 (Muito Baixa - categórica) |
| num_longit | 60,753 | 0.6858 (Média-Alta) |
| num_latitu | 57,074 | 0.6443 (Média-Alta) |
| dat_embarg | 84,145 | 0.9499 (Alta - quase única) |
| dat_impres | 21,714 | 0.2451 (Média) |
| dat_ult_al | 34,155 | 0.3856 (Média) |
| num_long00 | 59,642 | 0.6733 (Média-Alta) |
| num_lati00 | 56,566 | 0.6385 (Média-Alta) |
| qtd_area_d | 14,470 | 0.1633 (Média) |
| qtd_area_e | 46,656 | 0.5267 (Média-Alta) |
| dat_ult_00 | 34,289 | 0.3871 (Média) |
| st_area(sh | 54,740 | 0.6179 (Média-Alta) |
| st_perimet | 69,349 | 0.7828 (Média-Alta) |

