#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sprint 2: Visualizações - MVP Econômico

Gera visualizações para as análises da Sprint 2
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
import json

warnings.filterwarnings('ignore')

# Configurar estilo
sns.set_style('whitegrid')
sns.set_context('talk')
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 12
plt.rcParams['axes.labelsize'] = 14
plt.rcParams['axes.titlesize'] = 16

# Paths
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
GOLD_DIR = BASE_DIR / 'data/03_gold'
VIZ_DIR = GOLD_DIR / 'visualizacoes'
VIZ_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 60)
print("SPRINT 2: VISUALIZAÇÕES")
print("=" * 60)
print(f"\nDiretório de visualizações: {VIZ_DIR}\n")

# Carregar dados
print("Carregando dados...")
ica_df = pd.read_parquet(GOLD_DIR / 'ica_ranking.parquet')
correlacao = pd.read_parquet(GOLD_DIR / 'correlacao_delta.parquet')
eficiencia = pd.read_parquet(GOLD_DIR / 'eficiencia_atividade.parquet')
ranking = pd.read_parquet(GOLD_DIR / 'ranking_concentracao.parquet')
serie_hist = pd.read_parquet(BASE_DIR / 'data/02_silver' / 'serie_historica_2020_2023.parquet')

print(f"✓ ICA: {ica_df.shape}")
print(f"✓ Eficiência: {eficiencia.shape}")
print(f"✓ Ranking: {ranking.shape}")
print(f"✓ Série Histórica: {serie_hist.shape}")

# ============================================================================
# 1. DISTRIBUIÇÃO DO ICA
# ============================================================================
print("\n" + "=" * 60)
print("1. DISTRIBUIÇÃO DO ICA")
print("=" * 60)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Histograma do ICA (apenas valores > 0)
ica_valido = ica_df[ica_df['ica'] > 0]['ica']
if len(ica_valido) > 0:
    axes[0].hist(ica_valido, bins=30, edgecolor='black', alpha=0.7, color='steelblue')
    axes[0].set_xlabel('ICA (ha desmatados / R$ mil VAB)')
    axes[0].set_ylabel('Frequência')
    axes[0].set_title('Distribuição do ICA (Municípios com Desmatamento > 0)')
    axes[0].axvline(ica_valido.mean(), color='red', linestyle='--', linewidth=2, label=f'Média: {ica_valido.mean():.6f}')
    axes[0].axvline(ica_valido.median(), color='green', linestyle='--', linewidth=2, label=f'Mediana: {ica_valido.median():.6f}')
    axes[0].legend()
else:
    axes[0].text(0.5, 0.5, 'Sem dados válidos', ha='center', va='center', transform=axes[0].transAxes)
    axes[0].set_title('Distribuição do ICA')

# ICA por ano (boxplot)
ica_por_ano = ica_df[ica_df['ica'] > 0].groupby('ano')['ica'].describe()
if len(ica_por_ano) > 0:
    ica_df[ica_df['ica'] > 0].boxplot(column='ica', by='ano', ax=axes[1])
    axes[1].set_xlabel('Ano')
    axes[1].set_ylabel('ICA')
    axes[1].set_title('ICA por Ano')
    plt.suptitle('')

plt.tight_layout()
plt.savefig(VIZ_DIR / 'distribuicao_ica.png', dpi=300, bbox_inches='tight')
plt.show()
print(f"✓ Distribuição ICA salva: {VIZ_DIR / 'distribuicao_ica.png'}")

# ============================================================================
# 2. TOP MUNICÍPIOS POR ICA
# ============================================================================
print("\n" + "=" * 60)
print("2. TOP MUNICÍPIOS POR ICA")
print("=" * 60)

# Agrupar por município e calcular média
ica_mun = ica_df[ica_df['ica'] > 0].groupby('cod_ibge').agg({
    'ica': 'mean',
    'area_desmatada_ha': 'sum',
    'vab_agro_mil_reais': 'sum'
}).reset_index()

ica_mun = ica_mun.sort_values('ica', ascending=False).head(20)

fig, ax = plt.subplots(figsize=(12, 10))
bars = ax.barh(range(len(ica_mun)), ica_mun['ica'], color='steelblue')
ax.set_yticks(range(len(ica_mun)))
ax.set_yticklabels([f"Cod: {cod}" for cod in ica_mun['cod_ibge']])
ax.set_xlabel('ICA Médio (ha/R$ mil)')
ax.set_title('TOP 20 Municípios Mais Ineficientes (Maior ICA)')
ax.invert_yaxis()

# Adicionar valores nas barras
for i, (idx, row) in enumerate(ica_mun.iterrows()):
    ax.text(row['ica'] * 1.01, i, f"{row['ica']:.6f}", va='center', fontsize=9)

plt.tight_layout()
plt.savefig(VIZ_DIR / 'top20_ica_municipios.png', dpi=300, bbox_inches='tight')
plt.show()
print(f"✓ Top 20 ICA salvo: {VIZ_DIR / 'top20_ica_municipios.png'}")

# ============================================================================
# 3. SCATTER PLOT - DELTA VAB VS DELTA DESMATAMENTO
# ============================================================================
print("\n" + "=" * 60)
print("3. SCATTER PLOT - DELTA VAB VS DELTA DESMATAMENTO")
print("=" * 60)

# Recriar dados de delta
delta_df = serie_hist[['cod_ibge', 'ano', 'vab_agro_mil_reais', 'area_desmatada_ha']].copy()
delta_df = delta_df.sort_values(['cod_ibge', 'ano'])
delta_df['delta_desmatamento'] = delta_df.groupby('cod_ibge')['area_desmatada_ha'].diff()
delta_df['delta_vab'] = delta_df.groupby('cod_ibge')['vab_agro_mil_reais'].diff()
delta_df = delta_df.dropna(subset=['delta_desmatamento', 'delta_vab'])

fig, ax = plt.subplots(figsize=(12, 10))

# Limitar outliers para visualização
dados_plot = delta_df[['delta_desmatamento', 'delta_vab']].dropna()
q1_desmat = dados_plot['delta_desmatamento'].quantile(0.01)
q99_desmat = dados_plot['delta_desmatamento'].quantile(0.99)
q1_vab = dados_plot['delta_vab'].quantile(0.01)
q99_vab = dados_plot['delta_vab'].quantile(0.99)

dados_plot_filtered = dados_plot[
    (dados_plot['delta_desmatamento'].between(q1_desmat, q99_desmat)) &
    (dados_plot['delta_vab'].between(q1_vab, q99_vab))
]

ax.scatter(dados_plot_filtered['delta_vab'], dados_plot_filtered['delta_desmatamento'], 
           alpha=0.3, s=50, color='steelblue', edgecolors='gray', linewidth=0.5)

# Linha de tendência
from scipy.stats import linregress
slope, intercept, r_value, p_value, std_err = linregress(
    dados_plot_filtered['delta_vab'], dados_plot_filtered['delta_desmatamento']
)
x_line = np.array([dados_plot_filtered['delta_vab'].min(), dados_plot_filtered['delta_vab'].max()])
y_line = slope * x_line + intercept
ax.plot(x_line, y_line, 'r--', linewidth=2, label=f'Tendência (R²={r_value**2:.4f})')

ax.axhline(0, color='gray', linestyle='-', linewidth=0.5)
ax.axvline(0, color='gray', linestyle='-', linewidth=0.5)
ax.set_xlabel('Variação do VAB Agro (R$ mil)')
ax.set_ylabel('Variação da Área Desmatada (ha)')
ax.set_title('ΔVAB vs ΔDesmatamento (2020-2023)\nCorrelação: Fraca/Nula')
ax.legend()

plt.tight_layout()
plt.savefig(VIZ_DIR / 'scatter_delta_vab_desmat.png', dpi=300, bbox_inches='tight')
plt.show()
print(f"✓ Scatter plot salvo: {VIZ_DIR / 'scatter_delta_vab_desmat.png'}")

# ============================================================================
# 4. EFICIÊNCIA PECUÁRIA VS AGRICULTURA
# ============================================================================
print("\n" + "=" * 60)
print("4. EFICIÊNCIA PECUÁRIA VS AGRICULTURA")
print("=" * 60)

# Carregar eficiência agrícola
eficiencia_agri = pd.read_parquet(GOLD_DIR / 'eficiencia_agricola_pam.parquet')

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Eficiência pecuária por ano
eficiencia_group = eficiencia.groupby('ano').agg({
    'bovinos_por_ha': ['mean', 'median'],
    'vab_por_ha': ['mean', 'median']
}).reset_index()

# Aplanar colunas multi-index
eficiencia_group.columns = ['ano', 'bovinos_mean', 'bovinos_median', 'vab_mean', 'vab_median']

# VAB por ha desmatado por ano
axes[0].bar(['2020', '2021', '2022', '2023'], eficiencia_group['vab_mean'], 
            color='steelblue', alpha=0.7, edgecolor='black')
axes[0].set_xlabel('Ano')
axes[0].set_ylabel('VAB por ha desmatado (R$ mil)')
axes[0].set_title('VAB Agro por Hectare Desmatado')
axes[0].tick_params(axis='x', rotation=0)

# Boxplot comparativo
dados_boxplot = pd.concat([
    eficiencia[['ano', 'bovinos_por_ha']].dropna().assign(Atividade='Pecuária (bovinos/ha)'),
    eficiencia_agri[['ano', 'valor_agri_por_ha_plantada']].dropna().rename(
        columns={'valor_agri_por_ha_plantada': 'valor'}
    ).assign(Atividade='Agricultura (R$/ha)')
])

dados_boxplot_filtered = dados_boxplot[
    (dados_boxplot[dados_boxplot.columns[1]] > dados_boxplot[dados_boxplot.columns[1]].quantile(0.01)) &
    (dados_boxplot[dados_boxplot.columns[1]] < dados_boxplot[dados_boxplot.columns[1]].quantile(0.99))
]

# Renomear para boxplot
dados_boxplot_filtered = dados_boxplot_filtered.rename(columns={dados_boxplot_filtered.columns[1]: 'Valor'})

sns.boxplot(data=dados_boxplot_filtered, x='Atividade', y='Valor', ax=axes[1],
            palette=['steelblue', 'coral'])
axes[1].set_xlabel('')
axes[1].set_ylabel('Eficiência (valores)')
axes[1].set_title('Comparação: Pecuária vs Agricultura')
axes[1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig(VIZ_DIR / 'eficiencia_pecuaria_agricultura.png', dpi=300, bbox_inches='tight')
plt.show()
print(f"✓ Eficiência comparada salva: {VIZ_DIR / 'eficiencia_pecuaria_agricultura.png'}")

# ============================================================================
# 5. CONCENTRAÇÃO TERRITORIAL
# ============================================================================
print("\n" + "=" * 60)
print("5. CONCENTRAÇÃO TERRITORIAL")
print("=" * 60)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Scatter: Rank Desmatamento vs Rank VAB
axes[0].scatter(ranking['rank_desmat'], ranking['rank_vab'], alpha=0.3, s=30, color='steelblue')
axes[0].plot([0, max(ranking['rank_desmat'])], [0, max(ranking['rank_vab'])], 'r--', linewidth=2)
axes[0].set_xlabel('Rank Desmatamento')
axes[0].set_ylabel('Rank VAB Agro')
axes[0].set_title('Rank Desmatamento vs Rank VAB Agro\n(Quanto menor o rank, maior o valor)')
axes[0].invert_xaxis()
axes[0].invert_yaxis()

# Highlight Top 100
top100_desmat = ranking[ranking['rank_desmat'] <= 100]
top100_vab = ranking[ranking['rank_vab'] <= 100]
overlap = ranking[(ranking['rank_desmat'] <= 100) & (ranking['rank_vab'] <= 100)]

axes[0].scatter(top100_desmat['rank_desmat'], top100_desmat['rank_vab'], 
                alpha=0.5, s=50, color='orange', label='Top 100 Desmat', edgecolors='gray')
axes[0].scatter(top100_vab['rank_desmat'], top100_vab['rank_vab'], 
                alpha=0.5, s=50, color='green', label='Top 100 VAB', edgecolors='gray')
if len(overlap) > 0:
    axes[0].scatter(overlap['rank_desmat'], overlap['rank_vab'], 
                    alpha=0.8, s=80, color='red', label='Overlap', edgecolors='black', linewidth=2)
axes[0].legend()

# Barras: Overlap
categorias = ['Top 100\nDesmat', 'Top 100\nVAB', 'Overlap']
valores = [100, 100, len(overlap)]
cores = ['orange', 'green', 'red']

axes[1].bar(categorias, valores, color=cores, edgecolor='black', alpha=0.7)
axes[1].set_ylabel('Quantidade de Municípios')
axes[1].set_title(f'Concentração Territorial\nOverlap: {len(overlap)} municípios ({len(overlap)}%)')
axes[1].text(2, valores[2] + 5, f'{len(overlap)} ({len(overlap)/100*100:.1f}%)', 
             ha='center', va='bottom', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.savefig(VIZ_DIR / 'concentracao_territorial.png', dpi=300, bbox_inches='tight')
plt.show()
print(f"✓ Concentração territorial salva: {VIZ_DIR / 'concentracao_territorial.png'}")

# ============================================================================
# 6. RESUMO VISUAL
# ============================================================================
print("\n" + "=" * 60)
print("6. RESUMO VISUAL")
print("=" * 60)

# Carregar resumo executivo
with open(GOLD_DIR / 'resumo_executivo.json', 'r') as f:
    resumo = json.load(f)

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# 1. ICA
ica_text = f"ICA Médio:\n{resumo['ica_medio']:.6f}\n\n"
ica_text += f"ICA Mediana:\n{resumo['ica_mediana']:.6f}"
axes[0, 0].text(0.5, 0.5, ica_text, ha='center', va='center', 
                fontsize=18, fontweight='bold', color='steelblue')
axes[0, 0].set_title('Índice de Custo Ambiental (ICA)')
axes[0, 0].axis('off')

# 2. Correlação
correlacao_text = f"Pearson: {resumo['correlacao_pearson']:.4f}\n"
correlacao_text += f"P-valor: {resumo['p_valor_correlacao']:.2e}\n"
correlacao_text += f"\n{resumo['interpretacao_correlacao']}"
axes[0, 1].text(0.5, 0.5, correlacao_text, ha='center', va='center', 
                fontsize=14, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
axes[0, 1].set_title('Correlação ΔDesmatamento × ΔVAB')
axes[0, 1].axis('off')

# 3. Concentração
overlap_pct = resumo['overlap_top100_pct']
alto_desmat_baixo_vab = resumo['municipios_alto_desmat_baixo_vab']
axes[1, 0].pie([overlap_pct, 100-overlap_pct], labels=['Overlap', 'Restante'], 
               colors=['red', 'lightgray'], autopct='%1.1f%%')
axes[1, 0].set_title(f'Overlap Top 100 (Desmat × VAB)\n{alto_desmat_baixo_vab} mun. alto desmat + baixo VAB')

# 4. Regressão
reg_text = f"R²: {resumo['r_squared_regressao']:.4f}\n"
reg_text += f"Coef. Desmat: {resumo['coef_desmatamento']:.6f}\n"
if resumo['p_valor_desmatamento']:
    reg_text += f"P-valor: {resumo['p_valor_desmatamento']:.4e}"
else:
    reg_text += f"P-valor: N/A"
axes[1, 1].text(0.5, 0.5, reg_text, ha='center', va='center', 
                fontsize=16, bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
axes[1, 1].set_title('Regressão Linear')
axes[1, 1].axis('off')

plt.suptitle('RESUMO EXECUTIVO - SPRINT 2: MVP ECONÔMICO\nPeríodo: 2020-2023', 
             fontsize=18, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(VIZ_DIR / 'resumo_visual.png', dpi=300, bbox_inches='tight')
plt.show()
print(f"✓ Resumo visual salvo: {VIZ_DIR / 'resumo_visual.png'}")

# ============================================================================
# LISTAR VISUALIZAÇÕES
# ============================================================================
print("\n" + "=" * 60)
print("VISUALIZAÇÕES GERADAS")
print("=" * 60)

viz_files = list(VIZ_DIR.glob('*.png'))
print(f"\nTotal de visualizações: {len(viz_files)}\n")
for viz in sorted(viz_files):
    tamanho_kb = viz.stat().st_size / 1024
    print(f"  📊 {viz.name}: {tamanho_kb:.1f} KB")

print("\n" + "=" * 60)
print("✅ VISUALIZAÇÕES DA SPRINT 2 CONCLUÍDAS!")
print("=" * 60)
