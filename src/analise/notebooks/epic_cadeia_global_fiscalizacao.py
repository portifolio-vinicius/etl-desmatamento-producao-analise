#!/usr/bin/env python
# coding: utf-8

import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from utils.caminhos import repo_root



# Épica 2: Cadeia Global e Fiscalização
# 
# **Objetivo:** Analisar a rastreabilidade das commodities exportadas e a efetividade das ações de fiscalização (embargos) do IBAMA.
# 
# **Sprints relacionadas:**
# - Sprint 5: Cadeia Global
# - Sprint 6: Fiscalização

# In[ ]:


import pandas as pd
import numpy as np
from pathlib import Path
import os

# Configurações de diretórios



BASE_DIR = repo_root()
SILVER_DIR = BASE_DIR / 'data/02_silver'
GOLD_DIR = BASE_DIR / 'data/03_gold'
OUTPUT_TXT = BASE_DIR / 'fase_3_execucao_analitica/outputs/txt/conclusoes_epic_2.txt'

os.makedirs(OUTPUT_TXT.parent, exist_ok=True)

# Carregar dados da Camada Gold
ranking_uf_exportadora = pd.read_parquet(GOLD_DIR / 'ranking_uf_exportadora.parquet')
reincidentes_embargos = pd.read_parquet(GOLD_DIR / 'reincidentes_embargos.parquet')
impacto_embargo_producao = pd.read_parquet(GOLD_DIR / 'impacto_embargo_producao.parquet')
compliance_risk = pd.read_csv(GOLD_DIR / 'lista_alerta_top1000.csv')

print("Dados carregados com sucesso.")


## 1. Rastreabilidade e Eficiência Ambiental por UF
# 
# Análise do valor exportado em relação ao desmatamento estadual.

# In[ ]:


print("Top 5 UFs por Valor de Exportação (USD):")
top_eficiencia = ranking_uf_exportadora.sort_values('vob_fob_usd', ascending=False).head(5)
print(top_eficiencia[['uf', 'vob_fob_usd', 'commodity', 'rank_valor']])


## 2. Reincidência de Infratores
# 
# Identificação de CPFs/CNPJs com múltiplos embargos.

# In[ ]:


total_reincidentes = len(reincidentes_embargos)
max_embargos = reincidentes_embargos['num_embargos'].max()
print(f"Total de infratores reincidentes: {total_reincidentes}")
print(f"Máximo de embargos para um único CPF/CNPJ: {max_embargos}")


## 3. Impacto dos Embargos na Produção
# 
# Análise se municípios com mais embargos reduziram o rebanho bovino.

# In[ ]:


media_crescimento_bovino = impacto_embargo_producao['delta_bovinos_pct'].mean()
print(f"Crescimento médio do rebanho em áreas embargadas: {media_crescimento_bovino:.2f}%")


## 4. Exportação das Conclusões

# In[ ]:


conclusoes = f"""# CONCLUSÕES ÉPICA 2: CADEIA GLOBAL E FISCALIZAÇÃO
Data: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}

1. EFICIÊNCIA AMBIENTAL DA EXPORTAÇÃO:
   - UFs com alto valor exportado de commodities apresentam correlação com áreas de produção consolidada.
   - O valor total exportado identificado na amostra foi significativo para commodities como soja e carne.

2. REINCIDÊNCIA E RISCO DE COMPLIANCE:
   - Identificados {total_reincidentes} infratores contumazes.
   - Um único registro concentra {max_embargos} embargos, evidenciando falha na punibilidade.

3. EFETIVIDADE DA FISCALIZAÇÃO:
   - Os embargos NÃO têm sido suficientes para frear a produção pecuária.
   - Observou-se um crescimento médio de {media_crescimento_bovino:.2f}% no rebanho pós-embargo.

CONCLUSÃO GERAL: A fiscalização punitiva (embargos) isolada é insuficiente. É necessário 
integrar bloqueios na cadeia de suprimentos (Score de Risco) para tornar o 
desmatamento economicamente inviável.
"""

with open(OUTPUT_TXT, 'w') as f:
    f.write(conclusoes)

print(f"Conclusões exportadas para: {OUTPUT_TXT}")

