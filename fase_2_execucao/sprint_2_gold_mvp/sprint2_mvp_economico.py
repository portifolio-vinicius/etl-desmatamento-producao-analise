#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sprint 2: MVP Econômico - Custo-Benefício do Desmatamento

Objetivo: Responder à pergunta central: "O desmatamento gera eficiência econômica?"

Análises Incluídas:
1. ICA (Índice de Custo Ambiental) - hectares desmatados por R$ 1.000 de VAB agropecuário
2. Delta Desmatamento vs Delta PIB Agro - correlação entre variações anuais
3. Eficiência Pecuária vs Agricultura - R$/ha e cabeças/ha
4. Concentração Territorial - Top 100 desmatamento vs Top 100 VAB
5. Validação Estatística - regressão linear
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
import json

warnings.filterwarnings('ignore')

# Configurar pandas
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 20)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

# Paths
BASE_DIR = Path('/home/vinicius/Downloads/estudo/fatec/SABADO-TE-ANALISE-DADOS')
SILVER_DIR = BASE_DIR / 'data/02_silver'
GOLD_DIR = BASE_DIR / 'data/03_gold'

# Criar diretório gold se não existir
GOLD_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 60)
print("SPRINT 2: MVP ECONÔMICO - CUSTO-BENEFÍCIO DO DESMATAMENTO")
print("=" * 60)
print(f"\nDiretório Gold: {GOLD_DIR}\n")

# ============================================================================
# 1. CARREGAR DADOS
# ============================================================================
print("\n" + "=" * 60)
print("1. CARREGANDO DADOS")
print("=" * 60)

print("\nCarregando dados...")

# Série histórica principal (VAB + PPM + IBAMA) - JÁ INTEGRADA
serie_hist = pd.read_parquet(SILVER_DIR / 'serie_historica_2020_2023.parquet')
print(f"✓ Série Histórica: {serie_hist.shape}")

# PAM - Produção Agrícola (para análise de eficiência agrícola)
pam = pd.read_parquet(SILVER_DIR / 'pam_consolidado.parquet')
print(f"✓ PAM: {pam.shape}")

# COMEX por UF (para contexto)
comex = pd.read_parquet(SILVER_DIR / 'comex_por_uf_ano.parquet')
print(f"✓ COMEX: {comex.shape}")

# Criar dimensão de municípios a partir da série histórica
# Usar cod_ibge como chave primária
dim_mun = serie_hist[['cod_ibge']].drop_duplicates().copy()
dim_mun = dim_mun[dim_mun['cod_ibge'] > 0].reset_index(drop=True)
print(f"✓ Dim Município (criada): {dim_mun.shape}")

print("\n" + "=" * 60)
print("RESUMO DOS DADOS")
print("=" * 60)
print(f"Período: 2020-2023")
print(f"Municípios na série histórica: {serie_hist['cod_ibge'].nunique()}")
print(f"Municípios com cod_ibge válido: {(serie_hist['cod_ibge'] > 0).sum()}")
print(f"Municípios com desmatamento > 0: {(serie_hist['area_desmatada_ha'] > 0).sum()}")

# ============================================================================
# 2. ANÁLISE 2.1: ICA - ÍNDICE DE CUSTO AMBIENTAL
# ============================================================================
print("\n" + "=" * 60)
print("2. ANÁLISE 2.1: ICA - ÍNDICE DE CUSTO AMBIENTAL")
print("=" * 60)

ica_df = serie_hist[['cod_ibge', 'ano', 'vab_agro_mil_reais', 'area_desmatada_ha', 'area_embargada_ha']].copy()

# ICA: área desmatada por R$ 1.000 de VAB
# Evitar divisão por zero
ica_df['ica'] = np.where(
    ica_df['vab_agro_mil_reais'] > 0,
    ica_df['area_desmatada_ha'] / ica_df['vab_agro_mil_reais'],
    np.nan
)

print(f"\nICA calculado para {ica_df['cod_ibge'].nunique()} municípios")
print(f"\nEstatísticas do ICA (2020-2023):")
print(ica_df['ica'].describe())

# Filtrar apenas municípios com ICA válido (VAB > 0 e desmatamento > 0)
ica_valido = ica_df[ica_df['ica'].notna() & (ica_df['ica'] > 0)]
print(f"\nMunicípios com ICA válido: {ica_valido['cod_ibge'].nunique()}")

if len(ica_valido) > 0:
    print(f"\nICA médio (apenas válidos): {ica_valido['ica'].mean():.6f}")
    print(f"ICA mediana (apenas válidos): {ica_valido['ica'].median():.6f}")

# Salvar ICA ranking
ica_df.to_parquet(GOLD_DIR / 'ica_ranking.parquet', index=False)
print(f"\n✓ ICA ranking salvo: {GOLD_DIR / 'ica_ranking.parquet'}")

# Ranking de municípios por ICA (média 2020-2023)
ica_ranking = ica_df.groupby('cod_ibge').agg({
    'ano': 'count',
    'ica': 'mean',
    'area_desmatada_ha': 'sum',
    'vab_agro_mil_reais': 'sum'
}).reset_index()

ica_ranking.columns = ['cod_ibge', 'anos_observados', 'ica_medio', 'area_desmatada_total_ha', 'vab_agro_total_mil_reais']

# Ordenar por ICA (maior = mais ineficiente)
ica_ranking = ica_ranking.sort_values('ica_medio', ascending=False)

print("\n=== TOP 20 MUNICÍPIOS MAIS INEFICIENTES (Maior ICA) ===")
print(ica_ranking[['cod_ibge', 'ica_medio', 'area_desmatada_total_ha', 'vab_agro_total_mil_reais']].head(20))

# ============================================================================
# 3. ANÁLISE 2.2: DELTA DESMATAMENTO VS DELTA PIB AGRO
# ============================================================================
print("\n" + "=" * 60)
print("3. ANÁLISE 2.2: DELTA DESMATAMENTO VS DELTA PIB AGRO")
print("=" * 60)

delta_df = serie_hist[['cod_ibge', 'ano', 'vab_agro_mil_reais', 'area_desmatada_ha']].copy()
delta_df = delta_df.sort_values(['cod_ibge', 'ano'])

# Calcular variação ano a ano
delta_df['delta_desmatamento'] = delta_df.groupby('cod_ibge')['area_desmatada_ha'].diff()
delta_df['delta_vab'] = delta_df.groupby('cod_ibge')['vab_agro_mil_reais'].diff()

# Remover primeiro ano (sem delta)
delta_df = delta_df.dropna(subset=['delta_desmatamento', 'delta_vab'])

print(f"\nDeltas calculados para {delta_df['cod_ibge'].nunique()} municípios")
print(f"\nEstatísticas Delta Desmatamento:")
print(delta_df['delta_desmatamento'].describe())
print(f"\nEstatísticas Delta VAB:")
print(delta_df['delta_vab'].describe())

# Calcular correlação de Pearson
from scipy import stats

dados_correlacao = delta_df[['delta_desmatamento', 'delta_vab']].dropna()
pearson_corr, p_valor = stats.pearsonr(dados_correlacao['delta_desmatamento'], dados_correlacao['delta_vab'])
spearman_corr, p_valor_spearman = stats.spearmanr(dados_correlacao['delta_desmatamento'], dados_correlacao['delta_vab'])

print("\n=== CORRELAÇÃO DELTA DESMATAMENTO × DELTA VAB ===")
print(f"Correlação de Pearson: {pearson_corr:.4f} (p-valor: {p_valor:.4e})")
print(f"Correlação de Spearman: {spearman_corr:.4f} (p-valor: {p_valor_spearman:.4e})")
print(f"N observações: {len(dados_correlacao)}")

# Interpretação
if pearson_corr > 0.3:
    interp = "Positiva moderada/forte - desmatamento associado a crescimento do VAB"
elif pearson_corr < -0.3:
    interp = "Negativa moderada/forte - desmatamento associado a redução do VAB"
else:
    interp = "Fraca/nula - desmatamento NÃO está associado a crescimento do VAB"

print(f"\nInterpretação: {interp}")

# Salvar resultados da correlação
correlacao_result = pd.DataFrame({
    'metrica': ['pearson', 'spearman'],
    'correlacao': [pearson_corr, spearman_corr],
    'p_valor': [p_valor, p_valor_spearman],
    'n_observacoes': [len(dados_correlacao), len(dados_correlacao)]
})
correlacao_result.to_parquet(GOLD_DIR / 'correlacao_delta.parquet', index=False)
print(f"\n✓ Correlação salva: {GOLD_DIR / 'correlacao_delta.parquet'}")

# ============================================================================
# 4. ANÁLISE 2.3: EFICIÊNCIA PECUÁRIA VS AGRICULTURA
# ============================================================================
print("\n" + "=" * 60)
print("4. ANÁLISE 2.3: EFICIÊNCIA PECUÁRIA VS AGRICULTURA")
print("=" * 60)

ppm_cols = [col for col in serie_hist.columns if col.startswith('ppm_')]
print(f"Categorias PPM disponíveis: {len(ppm_cols)}")

# Focar em bovinos (categoria mais relevante para desmatamento)
eficiencia_df = serie_hist[['cod_ibge', 'ano', 'vab_agro_mil_reais', 'area_desmatada_ha', 'ppm_bovinos_cabecas']].copy()

# Calcular eficiência pecuária
# Nota: area_desmatada_ha é acumulada/embargada, não é área de pasto
# Usaremos como proxy de área utilizada
eficiencia_df['bovinos_por_ha'] = np.where(
    eficiencia_df['area_desmatada_ha'] > 0,
    eficiencia_df['ppm_bovinos_cabecas'] / eficiencia_df['area_desmatada_ha'],
    np.nan
)

# Valor por hectare
eficiencia_df['vab_por_ha'] = np.where(
    eficiencia_df['area_desmatada_ha'] > 0,
    eficiencia_df['vab_agro_mil_reais'] / eficiencia_df['area_desmatada_ha'],
    np.nan
)

print(f"\n=== EFICIÊNCIA PECUÁRIA (por ha desmatado) ===")
print(f"Bovinos por hectare (média): {eficiencia_df['bovinos_por_ha'].mean():,.2f} cabeças/ha")
print(f"Bovinos por hectare (mediana): {eficiencia_df['bovinos_por_ha'].median():,.2f} cabeças/ha")

print(f"\n=== VAB POR HECTARE (por ha desmatado) ===")
print(f"VAB por hectare (média): R$ {eficiencia_df['vab_por_ha'].mean():,.2f} mil/ha")
print(f"VAB por hectare (mediana): R$ {eficiencia_df['vab_por_ha'].median():,.2f} mil/ha")

# Integrar com PAM para comparação Agricultura
# Pivotar PAM para ter colunas de área e produção por município-ano
pam_pivot = pam.pivot_table(
    index=['municipio', 'uf', 'ano'],
    values=['area_plantada_ha', 'area_colhida_ha', 'valor_producao_mil_reais'],
    aggfunc='sum'
).reset_index()

print(f"\nPAM pivotado: {pam_pivot.shape}")

# Unir PAM com eficiência por municipio+uf+ano
# Primeiro, precisamos de um mapeamento cod_ibge -> municipio+uf
# Vamos usar o IBAMA silver que tem essa informação

ibama_silver = pd.read_parquet(SILVER_DIR / 'embargos_por_municipio_ano.parquet')
print(f"IBAMA silver: {ibama_silver.shape}")

# IBAMA tem cod_munici mas não tem nome
# Precisamos de uma fonte externa para mapear cod_ibge -> municipio

# Para a Sprint 2, vamos simplificar e usar apenas os dados numéricos
# sem necessidade de nome do município

# Calcular eficiência agrícola diretamente do PAM
pam_pivot['valor_agri_por_ha_plantada'] = np.where(
    pam_pivot['area_plantada_ha'] > 0,
    pam_pivot['valor_producao_mil_reais'] / pam_pivot['area_plantada_ha'],
    np.nan
)

print(f"\n=== EFICIÊNCIA AGRÍCOLA (PAM) ===")
print(f"Valor da produção por ha plantado (média): R$ {pam_pivot['valor_agri_por_ha_plantada'].mean():,.2f} mil/ha")
print(f"Valor da produção por ha plantado (mediana): R$ {pam_pivot['valor_agri_por_ha_plantada'].median():,.2f} mil/ha")

# Comparação Pecuária vs Agricultura por ano
comparacao = eficiencia_df.groupby('ano').agg({
    'bovinos_por_ha': 'mean',
    'vab_por_ha': 'mean'
}).reset_index()

comparacao.columns = ['ano', 'bovinos_por_ha_medio', 'vab_por_ha_medio']

print("\n=== COMPARAÇÃO ANUAL: PECUÁRIA vs AGRICULTURA ===")
print(comparacao)

# Salvar eficiência
eficiencia_df.to_parquet(GOLD_DIR / 'eficiencia_atividade.parquet', index=False)
pam_pivot.to_parquet(GOLD_DIR / 'eficiencia_agricola_pam.parquet', index=False)
print(f"\n✓ Eficiência salva: {GOLD_DIR / 'eficiencia_atividade.parquet'}")
print(f"✓ Eficiência agrícola salva: {GOLD_DIR / 'eficiencia_agricola_pam.parquet'}")

# ============================================================================
# 5. ANÁLISE 2.4: CONCENTRAÇÃO TERRITORIAL
# ============================================================================
print("\n" + "=" * 60)
print("5. ANÁLISE 2.4: CONCENTRAÇÃO TERRITORIAL")
print("=" * 60)

# Ranking de desmatamento (2020-2023)
rank_desmat = serie_hist.groupby('cod_ibge').agg({
    'area_desmatada_ha': 'sum',
    'area_embargada_ha': 'sum'
}).reset_index()
rank_desmat = rank_desmat.sort_values('area_desmatada_ha', ascending=False)
rank_desmat['rank_desmat'] = range(1, len(rank_desmat) + 1)

# Ranking de VAB Agro (2020-2023)
rank_vab = serie_hist.groupby('cod_ibge').agg({
    'vab_agro_mil_reais': 'sum'
}).reset_index()
rank_vab = rank_vab.sort_values('vab_agro_mil_reais', ascending=False)
rank_vab['rank_vab'] = range(1, len(rank_vab) + 1)

# Unir rankings
ranking_consolidado = rank_desmat.merge(rank_vab, on='cod_ibge', how='outer')

# Top 100 Desmatamento
top100_desmat = ranking_consolidado.nsmallest(100, 'rank_desmat')[['cod_ibge', 'area_desmatada_ha', 'rank_desmat', 'rank_vab']]

# Top 100 VAB
top100_vab = ranking_consolidado.nsmallest(100, 'rank_vab')[['cod_ibge', 'vab_agro_mil_reais', 'rank_desmat', 'rank_vab']]

# Overlap: quantos estão em ambas as listas?
overlap = set(top100_desmat['cod_ibge']) & set(top100_vab['cod_ibge'])
overlap_pct = len(overlap) / 100 * 100

print("\n=== CONCENTRAÇÃO TERRITORIAL ===")
print(f"Municípios no Top 100 Desmatamento: 100")
print(f"Municípios no Top 100 VAB: 100")
print(f"Overlap (em ambas listas): {len(overlap)} ({overlap_pct:.1f}%)")

# Perfil: municípios que desmatam muito mas têm baixo VAB
# Definir "baixo VAB" como rank_vab > 2000 (metade inferior)
alto_desmat_baixo_vab = top100_desmat[top100_desmat['rank_vab'] > 2000]
print(f"\n=== ALTO DESMATAMENTO + BAIXO VAB ===")
print(f"Municípios no Top 100 desmatamento com rank VAB > 2000: {len(alto_desmat_baixo_vab)}")
print(alto_desmat_baixo_vab.head(20))

# Salvar rankings
ranking_consolidado.to_parquet(GOLD_DIR / 'ranking_concentracao.parquet', index=False)
top100_desmat.to_parquet(GOLD_DIR / 'ranking_top100_desmatamento.parquet', index=False)
top100_vab.to_parquet(GOLD_DIR / 'ranking_top100_vab.parquet', index=False)
print(f"\n✓ Rankings salvos em {GOLD_DIR}")

# ============================================================================
# 6. ANÁLISE 2.5: VALIDAÇÃO ESTATÍSTICA - REGRESSÃO LINEAR
# ============================================================================
print("\n" + "=" * 60)
print("6. ANÁLISE 2.5: VALIDAÇÃO ESTATÍSTICA - REGRESSÃO LINEAR")
print("=" * 60)

import statsmodels.api as sm

# Preparar dados para regressão
regressao_df = serie_hist[['cod_ibge', 'ano', 'vab_agro_mil_reais', 'area_desmatada_ha']].copy()

# Remover outliers extremos (top 1% de área desmatada)
threshold = regressao_df['area_desmatada_ha'].quantile(0.99)
regressao_df = regressao_df[regressao_df['area_desmatada_ha'] <= threshold]

print(f"Dados para regressão: {regressao_df.shape}")

# Variáveis
X = regressao_df[['area_desmatada_ha', 'ano']]
y = regressao_df['vab_agro_mil_reais']

# Adicionar constante
X = sm.add_constant(X)

# Ajustar modelo
modelo = sm.OLS(y, X).fit()

print("\n=== RESULTADOS DA REGRESSÃO LINEAR (SIMPLES) ===")
print(f"R²: {modelo.rsquared:.4f}")
print(f"R² Ajustado: {modelo.rsquared_adj:.4f}")
print(f"\nCoeficientes:")
print(f"  Intercepto: {modelo.params['const']:.2f}")
print(f"  area_desmatada_ha: {modelo.params['area_desmatada_ha']:.6f} (p={modelo.pvalues['area_desmatada_ha']:.4e})")
print(f"  ano: {modelo.params['ano']:.2f} (p={modelo.pvalues['ano']:.4e})")

# Salvar resultados
resultado_regressao = pd.DataFrame({
    'modelo': ['simples'],
    'r_squared': [modelo.rsquared],
    'r_squared_adj': [modelo.rsquared_adj],
    'coef_desmatamento': [modelo.params['area_desmatada_ha']],
    'p_valor_desmatamento': [modelo.pvalues['area_desmatada_ha']],
    'n_observacoes': [modelo.nobs]
})

resultado_regressao.to_csv(GOLD_DIR / 'regressao_resultados.csv', index=False)
print(f"\n✓ Resultados da regressão salvos em {GOLD_DIR}")

# ============================================================================
# 7. RESUMO EXECUTIVO
# ============================================================================
print("\n" + "=" * 60)
print("7. RESUMO EXECUTIVO")
print("=" * 60)

# Calcular ICA válido para o resumo
ica_medio_valido = ica_valido['ica'].mean() if len(ica_valido) > 0 else np.nan
ica_mediana_valido = ica_valido['ica'].median() if len(ica_valido) > 0 else np.nan

resumo = {
    'periodo_analise': '2020-2023',
    'municipios_analisados': int(serie_hist['cod_ibge'].nunique()),
    'municipios_com_desmatamento': int((serie_hist['area_desmatada_ha'] > 0).sum()),
    'ica_medio': float(ica_medio_valido) if not np.isnan(ica_medio_valido) else None,
    'ica_mediana': float(ica_mediana_valido) if not np.isnan(ica_mediana_valido) else None,
    'correlacao_pearson': float(pearson_corr),
    'p_valor_correlacao': float(p_valor),
    'interpretacao_correlacao': interp,
    'overlap_top100_pct': float(overlap_pct),
    'municipios_alto_desmat_baixo_vab': int(len(alto_desmat_baixo_vab)),
    'r_squared_regressao': float(modelo.rsquared),
    'coef_desmatamento': float(modelo.params['area_desmatada_ha']),
    'p_valor_desmatamento': float(modelo.pvalues['area_desmatada_ha'])
}

print("\n" + "=" * 60)
print("RESUMO EXECUTIVO - SPRINT 2: MVP ECONÔMICO")
print("=" * 60)
print(f"\nPeríodo de análise: {resumo['periodo_analise']}")
print(f"Municípios analisados: {resumo['municipios_analisados']}")
print(f"Municípios com desmatamento > 0: {resumo['municipios_com_desmatamento']}")
print(f"\n--- ICA (Índice de Custo Ambiental) ---")
if resumo['ica_medio']:
    print(f"ICA médio (válidos): {resumo['ica_medio']:.6f} ha/R$ mil")
    print(f"ICA mediana (válidos): {resumo['ica_mediana']:.6f} ha/R$ mil")
else:
    print("ICA: Sem dados válidos suficientes (poucos municípios com desmatamento e VAB)")
print(f"\n--- Correlação ΔDesmatamento × ΔVAB ---")
print(f"Correlação de Pearson: {resumo['correlacao_pearson']:.4f}")
print(f"P-valor: {resumo['p_valor_correlacao']:.4e}")
print(f"Interpretação: {resumo['interpretacao_correlacao']}")
print(f"\n--- Concentração Territorial ---")
print(f"Overlap Top 100 (Desmatamento × VAB): {resumo['overlap_top100_pct']:.1f}%")
print(f"Municípios alto desmatamento + baixo VAB: {resumo['municipios_alto_desmat_baixo_vab']}")
print(f"\n--- Regressão Linear ---")
print(f"R²: {resumo['r_squared_regressao']:.4f}")
print(f"Coeficiente desmatamento: {resumo['coef_desmatamento']:.6f}")
print(f"P-valor desmatamento: {resumo['p_valor_desmatamento']:.4e}")
print("\n" + "=" * 60)

# Salvar resumo
with open(GOLD_DIR / 'resumo_executivo.json', 'w') as f:
    json.dump(resumo, f, indent=2)
print(f"\n✓ Resumo executivo salvo: {GOLD_DIR / 'resumo_executivo.json'}")

# ============================================================================
# 8. ARTEFATOS GERADOS
# ============================================================================
print("\n" + "=" * 60)
print("8. ARTEFATOS GERADOS - CAMADA GOLD")
print("=" * 60)

artefatos = list(GOLD_DIR.glob('*'))
print(f"\nTotal de artefatos: {len(artefatos)}\n")
for artefato in sorted(artefatos):
    tamanho_mb = artefato.stat().st_size / (1024 * 1024)
    print(f"  📄 {artefato.name}: {tamanho_mb:.2f} MB")

print("\n" + "=" * 60)
print("✅ SPRINT 2 CONCLUÍDA COM SUCESSO!")
print("=" * 60)
