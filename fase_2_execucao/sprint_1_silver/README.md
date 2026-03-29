# Sprint 1 - Fundação de Dados Tabulares (Camada Silver)

## Visão Geral

A **Sprint 1** teve como objetivo transformar os dados brutos da camada Bronze em uma estrutura padronizada e analítica na camada Silver, integrando 5 fontes de dados diferentes.

## Artefatos Gerados

### 1. PAM - Produção Agrícola Municipal
**Arquivo:** `pam/data/silver/pam_consolidado.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 27.505 |
| Municípios | 5.510 |
| Anos | 2020-2024 |
| Tamanho | 0,58 MB |

**Colunas:**
- `chave_municipio`: Chave única (municipio_uf)
- `municipio`: Nome do município
- `uf`: Unidade Federativa
- `ano`: Ano de referência
- `tipo_lavoura`: Temporária/Permanente
- `produto`: Nome da cultura
- `area_plantada_ha`: Área plantada (hectares)
- `area_colhida_ha`: Área colhida (hectares)
- `valor_producao_mil_reais`: Valor da produção (R$ mil)

**Transformações aplicadas:**
- Limpeza de linhas de cabeçalho codificadas
- Extração de município e UF do campo D1N
- Pivotamento Long → Wide
- Agregação por município/ano/produto

---

### 2. PIB - Valor Adicionado Bruto Agropecuária
**Arquivo:** `pib/data/silver/pib_vab_consolidado.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 77.994 |
| Municípios | 5.571 |
| Anos | 2010-2023 |
| Tamanho | 0,38 MB |

**Colunas:**
- `cod_ibge`: Código IBGE do município (7 dígitos)
- `ano`: Ano de referência
- `vab_agro_mil_reais`: VAB da agropecuária (R$ mil)

**Transformações aplicadas:**
- Extração de código IBGE via regex `(\d{7})`
- Conversão de valores ".." → null
- Consolidação de 14 anos em arquivo único

---

### 3. PPM - Pecuária Municipal
**Arquivo:** `ppm/data/silver/ppm_consolidado.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 267.264 |
| Municípios | 5.568 |
| Categorias | 12 |
| Anos | 2021-2024 |
| Tamanho | 0,24 MB |

**Categorias de rebanho:**
- Bovinos (936M cabeças em 2024)
- Suínos, Ovinos, Caprinos
- Equinos, Asininos, Muares
- Galinhas, Galináceos, Codornas, Coelhos

**Transformações aplicadas:**
- Leitura de 12 categorias separadas
- Extração de código IBGE
- Consolidação em arquivo único

---

### 4. IBAMA - Embargos Ambientais
**Arquivo:** `ibama/data/silver/embargos_por_municipio_ano.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 18.355 |
| Municípios | 3.769 |
| Anos | 1987-2026 |
| Tamanho | 0,20 MB |

**Colunas:**
- `cod_munici`: Código IBGE do município
- `ano`: Ano do embargo
- `num_embargos`: Quantidade de embargos
- `area_desmatada_ha`: Área desmatada (hectares)
- `area_embargada_ha`: Área embargada (hectares)

**Transformações aplicadas:**
- Conversão de dat_embarg para datetime
- Extração de ano
- Agregação por município/ano

---

### 5. COMEX - Exportações/Importações
**Arquivo:** `comex/data/silver/comex_por_uf_ano.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 689 |
| UFs | 29 |
| Commodities | 7 |
| Anos | 2023-2025 |
| Tamanho | 0,02 MB |

**Commodities mapeadas:**
- Soja, Milho, Carne Bovina, Café, Açúcar, Celulose, Madeira

**Colunas:**
- `uf`: Unidade Federativa
- `ano`: Ano de referência
- `tipo_operacao`: Exportação/Importação
- `commodity`: Nome da commodity
- `vob_fob_usd`: Valor em USD
- `peso_kg`: Peso líquido (kg)
- `num_operacoes`: Quantidade de operações

**⚠️ Limitação:** COMEX não possui código municipal, apenas UF

---

### 6. Dimensão de Municípios
**Arquivo:** `dimensao/data/silver/dim_municipio.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 5.510 |
| Tamanho | 0,07 MB |

**Colunas:**
- `municipio`: Nome do município
- `uf`: Sigla da UF
- `cod_ibge`: Código IBGE (preencher via API)
- `uf_codigo`: Código numérico da UF
- `regiao`: Região do Brasil
- `amazonia_legal`: Flag (True/False)

**⚠️ Nota:** `cod_ibge` está null - requer integração com API do IBGE

---

### 7. Série Histórica Comum (2020-2023)
**Arquivo:** `dados_analiticos/data/silver/serie_historica_2020_2023.parquet`

| Métrica | Valor |
|---------|-------|
| Registros | 22.284 |
| Municípios | 5.571 |
| Colunas | 18 |
| Tamanho | 0,26 MB |

**Colunas integradas:**
- `cod_ibge`, `ano`
- `vab_agro_mil_reais` (PIB)
- `ppm_*_cabecas` (PPM - 12 categorias)
- `num_embargos`, `area_desmatada_ha`, `area_embargada_ha` (IBAMA)

**Fontes integradas:**
- ✅ PIB: por cod_ibge
- ✅ PPM: por cod_ibge (pivotado)
- ✅ IBAMA: por cod_munici → cod_ibge
- ⚠️ PAM: separado (usa chave_municipio)
- ⚠️ COMEX: separado (nível UF)

---

## Scripts de ETL

| ETL | Script | Status |
|-----|--------|--------|
| 1.1 PAM | `notebooks/sprint1-silver/pam/01_etl_pam_consolidado.py` | ✅ |
| 1.2 PIB | `notebooks/sprint1-silver/pib/01_etl_pib_vab_consolidado.py` | ✅ |
| 1.3 PPM | `notebooks/sprint1-silver/ppm/01_etl_ppm_consolidado.py` | ✅ |
| 1.4 IBAMA | `notebooks/sprint1-silver/ibama/01_etl_embargos_municipio_ano.py` | ✅ |
| 1.5 COMEX | `notebooks/sprint1-silver/comex/01_etl_comex_por_uf_ano.py` | ✅ |
| 1.6 Dimensão | `notebooks/sprint1-silver/dimensao/01_etl_dim_municipio.py` | ✅ |
| 1.7 Série Histórica | `notebooks/sprint1-silver/02_etl_serie_historica_comum.py` | ✅ |

---

## Descobertas Críticas

### 1. Formato "Long" do IBGE/SIDRA
PAM, PIB e PPM usam formato long com colunas codificadas:
- `NC, NN, MC, MN`: Níveis hierárquicos
- `V`: Valor (string, ".." para ausentes)
- `D1C/D1N`: Município (código/nome)
- `D2C/D2N`: Variável (código/nome)
- `D3C/D3N`: Ano (código/nome)
- `D4C/D4N`: Produto/Categoria (código/nome)

### 2. Chaves de Join Diferentes
- **PAM:** `chave_municipio` (municipio_uf) - sem cod_ibge
- **PIB/PPM:** `cod_ibge` (inteiro, 7 dígitos)
- **IBAMA:** `cod_munici` (inteiro, equivalente a cod_ibge)
- **COMEX:** `uf` (sigla) - sem município

### 3. Limitação de Rastreio
COMEX não tem código municipal → rastreio de commodities apenas no nível estadual

### 4. Período Comum
Série histórica comum disponível: **2020-2023** (4 anos)

---

## Próximos Passos (Sprint 2)

1. **Cálculo do ICA** (Índice de Custo Ambiental)
2. **Delta Desmatamento vs Delta PIB Agro**
3. **Pecuária vs Agricultura** (eficiência por hectare)
4. **Concentração Territorial** (Top 100 municípios)
5. **Validação Estatística** (regressão)

---

## Como Executar

```bash
cd /home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS
source venv/bin/activate

# Executar todas as ETLs em sequência
python notebooks/sprint1-silver/pam/01_etl_pam_consolidado.py
python notebooks/sprint1-silver/pib/01_etl_pib_vab_consolidado.py
python notebooks/sprint1-silver/ppm/01_etl_ppm_consolidado.py
python notebooks/sprint1-silver/ibama/01_etl_embargos_municipio_ano.py
python notebooks/sprint1-silver/comex/01_etl_comex_por_uf_ano.py
python notebooks/sprint1-silver/dimensao/01_etl_dim_municipio.py
python notebooks/sprint1-silver/02_etl_serie_historica_comum.py
```

---

**Status:** ✅ Sprint 1 Concluída  
**Data:** 28/03/2026  
**Próxima Sprint:** Sprint 2 - MVP Analítico Econômico
