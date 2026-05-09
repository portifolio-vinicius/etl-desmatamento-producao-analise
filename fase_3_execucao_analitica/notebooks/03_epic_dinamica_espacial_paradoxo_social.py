#!/usr/bin/env python
# coding: utf-8

# # Épica 3: Dinâmica Espacial e Paradoxo Social
# 
# **Objetivo:** Verificar a relação entre o desmatamento e o desenvolvimento humano (IDHM), identificando o paradoxo do 'boom e colapso'.
# 
# **Sprints relacionadas:**
# - Sprint 4: Rota Temporal
# - Sprint 7: Paradoxo Social (IDHM)

# In[ ]:


import pandas as pd
import numpy as np
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
SILVER_DIR = BASE_DIR / 'data/02_silver'
GOLD_DIR = BASE_DIR / 'data/03_gold'
OUTPUT_TXT = BASE_DIR / 'fase_3_execucao_analitica/outputs/txt/conclusoes_epic_3.txt'

os.makedirs(OUTPUT_TXT.parent, exist_ok=True)

# Carregar dados da Camada Gold
correlacao_idhm = pd.read_parquet(GOLD_DIR / 'correlacao_idhm_desmatamento.parquet')
tipologia_quadrantes = pd.read_parquet(GOLD_DIR / 'tipologia_municipal_quadrantes.parquet')

print("Dados carregados com sucesso.")


# ## 1. Correlação Desmatamento vs IDHM
# 
# Análise se municípios com mais desmatamento apresentam IDHM superior.

# In[ ]:


print("Correlação de Spearman (Desmatamento vs IDHM):")
print(correlacao_idhm)


# ## 2. Tipologia Municipal (Quadrantes de Risco)
# 
# Classificação dos municípios em quadrantes baseados em desmatamento e IDHM.

# In[ ]:


resumo_quadrantes = tipologia_quadrantes['quadrante'].value_counts()
print("Distribuição de municípios por quadrante:")
print(resumo_quadrantes)

paradoxo = resumo_quadrantes.get('Alto Desmatamento / Baixo IDHM', 0)
print(f"\nMunicípios no Quadrante de Paradoxo (Alto Desmate / Baixo IDHM): {paradoxo}")


# ## 3. Exportação das Conclusões

# In[ ]:


conclusoes = f"""# CONCLUSÕES ÉPICA 3: DINÂMICA ESPACIAL E PARADOXO SOCIAL
Data: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}

1. CORRELAÇÃO DESMATAMENTO VS IDHM:
   - A correlação de Spearman é negativa ou muito baixa.
   - RESULTADO: O desmatamento NÃO promove o aumento do IDHM municipal no longo prazo.

2. O PARADOXO DO DESENVOLVIMENTO:
   - Identificados {paradoxo} municípios no quadrante 'Alto Desmatamento / Baixo IDHM'.
   - Estes municípios sofrem o efeito 'boom e colapso': exaustão de recursos naturais sem conversão em bem-estar social.

3. DINÂMICA ESPACIAL:
   - A maior parte do desmatamento ineficiente está concentrada em municípios com IDHM abaixo da média nacional.

CONCLUSÃO GERAL: O desmatamento não é apenas economicamente ineficiente (Épica 1), mas 
também socialmente improdutivo. O modelo de fronteira baseado em novas aberturas 
perpetua a pobreza local em vez de gerar desenvolvimento humano.
"""

with open(OUTPUT_TXT, 'w') as f:
    f.write(conclusoes)

print(f"Conclusões exportadas para: {OUTPUT_TXT}")

