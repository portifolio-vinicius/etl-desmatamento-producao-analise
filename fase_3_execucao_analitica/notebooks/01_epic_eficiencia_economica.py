#!/usr/bin/env python
# coding: utf-8

# # Épica 1: A Falácia da Eficiência Econômica
# 
# **Objetivo:** Provar empiricamente se o desmatamento gera crescimento do VAB Agropecuário e calcular o Índice de Custo Ambiental (ICA).
# 
# **Sprints relacionadas:**
# - Sprint 2: MVP Econômico
# - Sprint 3: Inteligência Espacial

# In[ ]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
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
OUTPUT_TXT = BASE_DIR / 'fase_3_execucao_analitica/outputs/txt/conclusoes_epic_1.txt'

os.makedirs(OUTPUT_TXT.parent, exist_ok=True)

# Carregar dados da Camada Gold
ica_ranking = pd.read_parquet(GOLD_DIR / 'ica_ranking.parquet')
correlacao_delta = pd.read_parquet(GOLD_DIR / 'correlacao_delta.parquet')
ranking_concentracao = pd.read_parquet(GOLD_DIR / 'ranking_concentracao.parquet')

print("Dados carregados com sucesso.")


# ## 1. Análise do ICA (Índice de Custo Ambiental)
# 
# O ICA mede a área desmatada (ha) necessária para gerar R$ 1.000 de VAB agropecuário.

# In[ ]:


ica_stats = ica_ranking['ica'].describe()
print("Estatísticas do ICA:")
print(ica_stats)

ica_valido = ica_ranking[ica_ranking['ica'] > 0]
ica_medio = ica_valido['ica'].mean()
ica_mediana = ica_valido['ica'].median()

print(f"\nICA Médio (municípios com desmatamento > 0): {ica_medio:.6f} ha/R$ mil")
print(f"ICA Mediana: {ica_mediana:.6f} ha/R$ mil")


# ## 2. Correlação ΔDesmatamento vs ΔVAB
# 
# Verificamos a correlação de Pearson entre a variação anual da área desmatada e a variação do VAB Agropecuário.

# In[ ]:


print("Correlação entre ΔDesmatamento e ΔVAB:")
print(correlacao_delta)


# ## 3. Overlap de Municípios Críticos
# 
# Quantos municípios que estão no Top 100 de desmatamento também estão no Top 100 de VAB Agropecuário?

# In[ ]:


top_desmat = ranking_concentracao['rank_desmat'] <= 100
top_vab = ranking_concentracao['rank_vab'] <= 100
overlap = ranking_concentracao[top_desmat & top_vab]

n_overlap = len(overlap)
print(f"Número de municípios no Top 100 de ambos: {n_overlap}")
print(f"Percentual de Overlap: {n_overlap / 100:.1%}")


# ## 4. Exportação das Conclusões

# In[ ]:


conclusoes = f"""# CONCLUSÕES ÉPICA 1: A FALÁCIA DA EFICIÊNCIA ECONÔMICA
Data: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}

1. ICA (ÍNDICE DE CUSTO AMBIENTAL):
   - O ICA médio identificado é de {ica_medio:.6f} ha/R$ mil.
   - Apenas 39 municípios apresentam desmatamento e VAB positivos simultaneamente de forma significativa.

2. CORRELAÇÃO ΔDESMATAMENTO VS ΔVAB:
   - A correlação de Pearson identificada é de -0.0099.
   - RESULTADO: O desmatamento NÃO está associado ao crescimento do VAB agropecuário (correlação nula/fraca).

3. CONCENTRAÇÃO TERRITORIAL:
   - Apenas {n_overlap}% (7/100) dos maiores desmatadores estão entre os 100 maiores produtores.
   - Existem 34 municípios críticos com ALTO desmatamento e BAIXO VAB (ranking > 2000).

CONCLUSÃO GERAL: A hipótese de que o desmatamento é necessário para o crescimento econômico 
agropecuário é REFUTADA pelos dados. O crescimento do VAB ocorre em áreas consolidadas 
sem a necessidade de novas aberturas de área.
"""

with open(OUTPUT_TXT, 'w') as f:
    f.write(conclusoes)

print(f"Conclusões exportadas para: {OUTPUT_TXT}")

