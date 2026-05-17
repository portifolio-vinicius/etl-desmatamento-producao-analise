# Análise de Dados de Desmatamento e Produção Agropecuária

Este projeto analisa a relação entre desmatamento e produção agropecuária no Brasil, utilizando dados públicos de múltiplas fontes (IBGE, IBAMA, COMEX, etc.).

## Estrutura do Projeto

```
SABADO-TE-ANALISE-DADOS/
├── data/                          # Dados em diferentes camadas
│   ├── 01_bronze/                # Dados brutos das fontes originais
│   ├── 02_silver/                # Dados limpos e padronizados
│   ├── 03_gold/                  # Dados analíticos e agregados
│   └── 04_reports/               # Relatórios consolidados
├── src/                          # Scripts de processamento
│   ├── utils/                    # Módulos compartilhados
│   ├── ingestao/                 # Scripts de download de dados
│   ├── transformacao/            # Scripts ETL para camada Silver
│   └── analise/                  # Scripts de análise e visualizações
├── notebooks_exploratorios/      # Notebooks de teste e exploração
├── docs/                         # Documentação técnica
├── logs/                         # Arquivos de log
└── requirements.txt              # Dependências Python
```

## Camadas de Dados

### Bronze (01_bronze/)
Dados brutos das fontes originais, sem transformações significativas.

### Silver (02_silver/)
Dados limpos, padronizados e integrados. Principais datasets:
- `pam_consolidado.parquet` - Produção Agrícola Municipal
- `pib_vab_consolidado.parquet` - Valor Adicionado Bruto Agropecuário
- `ppm_consolidado.parquet` - Produção da Pecuária Municipal
- `embargos_por_municipio_ano.parquet` - Embargos IBAMA
- `comex_por_uf_ano.parquet` - Comércio Exterior por UF
- `dim_municipio.parquet` - Dimensão de municípios
- `serie_historica_2020_2023.parquet` - Série histórica unificada

### Gold (03_gold/)
Dados analíticos com métricas e indicadores calculados:
- `ica_municipal.parquet` - Índice de Custo Ambiental
- `correlacao_delta.parquet` - Correlação ΔDesmatamento vs ΔVAB
- `densidade_fiscalizacao_municipal.parquet` - Densidade de fiscalização

## Scripts Principais

### Ingestão
- `src/ingestao/download_dados_parquet.py` - Download de dados das fontes originais

### Transformação (Silver)
- `src/transformacao/pam/etl_pam_consolidado.py` - ETL PAM
- `src/transformacao/pib/etl_pib_vab_consolidado.py` - ETL PIB
- `src/transformacao/ppm/etl_ppm_consolidado.py` - ETL PPM
- `src/transformacao/ibama/etl_embargos_municipio_ano.py` - ETL IBAMA
- `src/transformacao/comex/etl_comex_por_uf_ano.py` - ETL COMEX
- `src/transformacao/dimensao/etl_dim_municipio.py` - ETL Dimensão Municípios
- `src/transformacao/serie_historica/etl_serie_historica_comum.py` - Série histórica unificada

### Análise (Gold)
- `src/analise/eficiencia_economica/mvp_economico.py` - Análise de eficiência econômica
- `src/analise/inteligencia_espacial/analise_buffer_spillover.py` - Análise espacial

## Instalação e Execução com Docker

Este projeto usa Docker para garantir que rode em qualquer computador sem problemas de dependências.

### Pré-requisitos
- Docker instalado (https://docs.docker.com/get-docker/)
- Docker Compose instalado (https://docs.docker.com/compose/install/)

### Download de Dados (Ingestão)
```bash
docker-compose run app
```

### Scripts de Transformação
```bash
docker-compose run app python src/transformacao/pam/etl_pam_consolidado.py
docker-compose run app python src/transformacao/pib/etl_pib_vab_consolidado.py
docker-compose run app python src/transformacao/ppm/etl_ppm_consolidado.py
docker-compose run app python src/transformacao/ibama/etl_embargos_municipio_ano.py
docker-compose run app python src/transformacao/comex/etl_comex_por_uf_ano.py
docker-compose run app python src/transformacao/dimensao/etl_dim_municipio.py
docker-compose run app python src/transformacao/serie_historica/etl_serie_historica_comum.py
```

### Scripts de Análise
```bash
docker-compose run app python src/analise/eficiencia_economica/mvp_economico.py
```

### Jupyter Notebook (Opcional)
```bash
docker-compose up jupyter
# Acesse em http://localhost:8888
```

### Streamlit (Opcional)
```bash
docker-compose up streamlit
# Acesse em http://localhost:8501
```

### Parar Serviços
```bash
docker-compose down
```

### Método Alternativo: Instalação Local

**Pré-requisitos de Sistema:**
- Python 3.11+
- GDAL, PROJ, GEOS (para geopandas)

**Linux:**
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-pip gdal-bin libgdal-dev libgeos-dev libproj-dev
pip install -r requirements.txt
```

**macOS:**
```bash
brew install python gdal
pip install -r requirements.txt
```

**Windows (Recomendado usar Docker):**
```bash
# Usar conda é mais fácil no Windows
conda create -n analise-dados python=3.11
conda activate analise-dados
conda install -c conda-forge geopandas
pip install -r requirements.txt
```

## Execução Local

### Download de Dados
```bash
python src/ingestao/download_dados_parquet.py
```

### Transformação para Silver
Executar os scripts em ordem na pasta `src/transformacao/`:
1. `pam/etl_pam_consolidado.py`
2. `pib/etl_pib_vab_consolidado.py`
3. `ppm/etl_ppm_consolidado.py`
4. `ibama/etl_embargos_municipio_ano.py`
5. `comex/etl_comex_por_uf_ano.py`
6. `dimensao/etl_dim_municipio.py`
7. `serie_historica/etl_serie_historica_comum.py`

### Análise
```bash
python src/analise/eficiencia_economica/mvp_economico.py
```

## Documentação

- `docs/estrutura_analise.md` - Estrutura da análise
- `docs/plano_implementacao.md` - Plano de implementação
- `docs/plano_implementacao_v2.md` - Plano de implementação atualizado
- `docs/resumo_transformacoes_dados.md` - Resumo das transformações de dados

## Dependências Principais

- pandas - Manipulação de dados
- sidrapy - API do IBGE SIDRA
- geopandas - Dados geoespaciais
- pyarrow - Formato Parquet
- matplotlib/seaborn - Visualizações
- scikit-learn - Machine learning
- statsmodels - Análise estatística

## Licença

Este projeto é um projeto acadêmico para análise de dados públicos.
