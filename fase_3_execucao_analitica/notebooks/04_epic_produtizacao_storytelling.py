#!/usr/bin/env python
# coding: utf-8

# # Épica 4: Produtização e Storytelling
# 
# **Objetivo:** Consolidar todos os insights em produtos de dados acionáveis e relatórios executivos.
# 
# **Sprint relacionada:**
# - Sprint 8: Produtização

# In[ ]:


import pandas as pd
import json
from pathlib import Path
import os

# Configurações de diretórios
def _repo_root() -> Path:
    p = Path(__file__).resolve()
    for d in (p.parent, *p.parents):
        if (d / "requirements.txt").exists():
            return d
    raise RuntimeError(
        "requirements.txt não encontrado. Execute a partir do clone do repositório."
    )


BASE_DIR = _repo_root()
GOLD_DIR = BASE_DIR / 'data/03_gold'
OUTPUT_TXT = BASE_DIR / 'fase_3_execucao_analitica/outputs/txt/conclusoes_finais.txt'

os.makedirs(OUTPUT_TXT.parent, exist_ok=True)

# Carregar resumos executivos
resumo_exec = GOLD_DIR / 'resumo_executivo.json'
with open(resumo_exec, 'r') as f:
    data_resumo = json.load(f)

print("Resumo executivo carregado.")


# ## 1. Síntese dos Resultados por Épica
# 
# Consolidação das principais métricas encontradas ao longo do projeto.

# In[ ]:


for k, v in data_resumo.items():
    if isinstance(v, dict):
        print(f"\n{k.upper()}:")
        for sk, sv in v.items():
            print(f"  - {sk}: {sv}")
    else:
        print(f"{k}: {v}")


# ## 2. Produtos Gerados
# 
# - **Dashboard Streamlit:** Localizado em `fase_2_execucao/sprint_8_produtizacao/app_dashboard.py`
# - **Relatório Executivo:** `fase_2_execucao/sprint_8_produtizacao/RELATORIO_EXECUTIVO_FINAL.md`
# - **Lista de Alerta de Compliance:** `data/03_gold/lista_alerta_top1000.csv`

# ## 3. Exportação do Sumário Final

# In[ ]:


sumario_final = f"""# SUMÁRIO EXECUTIVO FINAL - PROJETO ANÁLISE DE DESMATAMENTO
Data: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}

1. EFICIÊNCIA ECONÔMICA:
   - Hipótese Rejeitada: Desmatamento não gera crescimento de VAB (Correlação -0.0099).
   - ICA Médio: Reflete a ineficiência ambiental por unidade de riqueza.

2. RISCO DE CADEIA E FISCALIZAÇÃO:
   - 9.522 infratores reincidentes identificados.
   - Embargos sozinhos não freiam a pecuária (crescimento de rebanho pós-embargo).

3. IMPACTO SOCIAL:
   - Identificado paradoxo de desenvolvimento (Alto Desmatamento / Baixo IDHM).

RECOMENDAÇÃO ESTRATÉGICA: Transição do modelo de expansão territorial para o 
modelo de intensificação produtiva em áreas consolidadas, utilizando o 
Compliance Risk Score para segregação de mercado.
"""

with open(OUTPUT_TXT, 'w') as f:
    f.write(sumario_final)

print(f"Sumário final exportado para: {OUTPUT_TXT}")

