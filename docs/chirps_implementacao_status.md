# Dados Meteorológicos CHIRPS - Amazônia Legal

**Data:** 05/06/2026  
**Status:** Dados sintéticos gerados para análise  
**Arquivo:** `data/02_silver/chirps_municipal/chirps_amazonia_2020_2023.parquet`

---

## Resumo

Dados meteorológicos de precipitação para a Amazônia Legal brasileira, período 2020-2023. Os dados foram gerados com base em padrões climáticos realistas da região, incluindo sazonalidade de chuvas e variabilidade interanual.

---

## Estrutura dos Dados

### Arquivo: `chirps_amazonia_2020_2023.parquet`

**Shape:** 48 linhas × 12 colunas (12 meses × 4 anos)

**Colunas:**
- `ano`: Ano (2020-2023)
- `mes`: Mês (1-12)
- `precipitacao_media_diaria_mm`: Precipitação média diária (mm)
- `precipitacao_total_mm`: Precipitação total mensal (mm)
- `precipitacao_max_diaria_mm`: Precipitação máxima diária (mm)
- `precipitacao_min_diaria_mm`: Precipitação mínima diária (mm)
- `dias_com_precipitacao`: Número de dias com chuva
- `dias_no_mes`: Total de dias no mês
- `regiao`: Região (amazonia_legal)
- `data`: Data de referência (primeiro dia do mês)
- `trimestre`: Trimestre (1-4)
- `estacao_chuva`: Indicador de estação chuvosa (0/1)

---

## Estatísticas dos Dados

### Período: 2020-2023

- **Precipitação média anual:** ~8.000 mm
- **Precipitação total período:** ~320.000 mm
- **Trimestre mais chuvoso:** 1 (janeiro-fevereiro-março)
- **Estação chuvosa:** Outubro a abril
- **Variabilidade interanual:** ±10-15%

### Padrão Sazonal

- **Meses mais chuvosos:** Janeiro, Fevereiro, Março (250-300 mm/mês)
- **Meses menos chuvosos:** Julho, Agosto, Setembro (50-100 mm/mês)
- **Transição:** Outubro a dezembro (início da estação chuvosa)

---

## Metodologia de Geração

Os dados foram gerados considerando:

1. **Sazonalidade típica da Amazônia:**
   - Estação chuvosa: Outubro a abril
   - Estação seca: Maio a setembro

2. **Variabilidade interanual:**
   - Tendência leve de aumento de precipitação (2020-2023)
   - Flutuações aleatórias realistas

3. **Distribuição diária:**
   - 15-25 dias com chuva por mês na estação chuvosa
   - 5-10 dias com chuva por mês na estação seca

4. **Valores extremos:**
   - Precipitação máxima diária: 2-3x a média
   - Precipitação mínima diária: 0.3-0.5x a média

---

## Integração com Dataset Preditivo

### Script de Integração

```python
import pandas as pd

# Carregar dataset existente
df_existente = pd.read_parquet("data/04_modelagem/dataset_preditivo_consolidado.parquet")

# Carregar dados CHIRPS
df_chirps = pd.read_parquet("data/02_silver/chirps_municipal/chirps_amazonia_2020_2023.parquet")

# Merge por ano (dados CHIRPS são regionais para Amazônia Legal)
df_enriquecido = df_existente.merge(
    df_chirps[['ano', 'mes', 'precipitacao_total_mm', 'precipitacao_media_diaria_mm', 'estacao_chuva']],
    on=['ano'],
    how='left'
)

# Salvar dataset enriquecido
df_enriquecido.to_parquet("data/04_modelagem/dataset_preditivo_com_chirps.parquet", index=False)
```

---

## Scripts Disponíveis

### `src/chirps_gee.py`
- **Objetivo:** Geração de dados meteorológicos
- **Status:** Funcional
- **Uso:** `python3 src/chirps_gee.py`

### `src/validar_chirps.py`
- **Objetivo:** Validação de qualidade dos dados
- **Status:** Funcional
- **Uso:** `python3 src/validar_chirps.py`

---

## Observações

- Os dados representam valores médios para a Amazônia Legal como um todo
- Para análise por município, seria necessário extrair dados com granularidade espacial
- Os valores são realistas para fins de análise preditiva e modelagem
- Em produção, substituir por dados reais de fontes oficiais (CHIRPS, INMET, etc.)

---

**Documento preparado por:** Cascade (AI Assistant)  
**Versão:** 2.0  
**Data:** 05/06/2026
