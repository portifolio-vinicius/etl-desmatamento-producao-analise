"""
ETL 4.2: Construção da Timeline de Degradação
=============================================

Este script constrói a sequência temporal de degradação ambiental,
ordenando eventos de múltiplas fontes:
- DETER: Alertas (primeiro sinal)
- MapBiomas Fogo: Ocorrências de incêndio
- PRODES: Corte raso consolidado
- TerraClass: Uso final do solo

Autor: Pipeline de Análise de Desmatamento
Data: 29/03/2026
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json

# Configuração
BASE_DIR = Path(__file__).parent.parent.parent
BRONZE_DIR = BASE_DIR / 'data' / '01_bronze'
SILVER_DIR = BASE_DIR / 'data' / '02_silver'
GOLD_DIR = BASE_DIR / 'data' / '03_gold'

# Paths de entrada
PRODES_FILE = BRONZE_DIR / 'prodes' / 'prodes_desmatamento_anual.parquet'
DETER_FILE = BRONZE_DIR / 'deter' / 'deter_alertas_diarios.parquet'
FOGO_FILE = BRONZE_DIR / 'mapbiomas_fogo' / 'mapbiomas_fogo_ocorrencias.parquet'
TERRACLASS_FILE = BRONZE_DIR / 'terra_class' / 'terra_class_uso.parquet'

# Paths de saída
TIMELINE_FILE = GOLD_DIR / 'temporal' / 'timeline_degradacao.parquet'
LATENCIA_FILE = GOLD_DIR / 'temporal' / 'latencia_alerta_corte.parquet'
RECORRENCIA_FILE = GOLD_DIR / 'temporal' / 'recorrencia_alertas.parquet'

# Criar diretório de saída
GOLD_DIR / 'temporal'.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("ETL 4.2: CONSTRUÇÃO DA TIMELINE DE DEGRADAÇÃO")
print("=" * 80)
print(f"Data de execução: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
print()


# =============================================================================
# CARREGAMENTO DE DADOS
# =============================================================================

def load_all_data():
    """Carregar todos os dados das fontes espaciais"""
    print("\n📥 CARREGANDO DADOS DAS FONTES")
    print("-" * 60)
    
    dados = {}
    
    # PRODES
    if PRODES_FILE.exists():
        dados['prodes'] = pd.read_parquet(PRODES_FILE)
        print(f"   ✅ PRODES: {len(dados['prodes']):,} registros")
    else:
        print(f"   ❌ PRODES não encontrado: {PRODES_FILE}")
        dados['prodes'] = pd.DataFrame()
    
    # DETER
    if DETER_FILE.exists():
        dados['deter'] = pd.read_parquet(DETER_FILE)
        print(f"   ✅ DETER: {len(dados['deter']):,} registros")
    else:
        print(f"   ❌ DETER não encontrado: {DETER_FILE}")
        dados['deter'] = pd.DataFrame()
    
    # MapBiomas Fogo
    if FOGO_FILE.exists():
        dados['fogo'] = pd.read_parquet(FOGO_FILE)
        print(f"   ✅ MapBiomas Fogo: {len(dados['fogo']):,} registros")
    else:
        print(f"   ❌ MapBiomas Fogo não encontrado: {FOGO_FILE}")
        dados['fogo'] = pd.DataFrame()
    
    # TerraClass
    if TERRACLASS_FILE.exists():
        dados['terra_class'] = pd.read_parquet(TERRACLASS_FILE)
        print(f"   ✅ TerraClass: {len(dados['terra_class']):,} registros")
    else:
        print(f"   ❌ TerraClass não encontrado: {TERRACLASS_FILE}")
        dados['terra_class'] = pd.DataFrame()
    
    return dados


# =============================================================================
# CONSTRUÇÃO DA TIMELINE
# =============================================================================

def build_timeline(dados):
    """
    Construir timeline unificada de degradação por município-ano.
    
    Sequência esperada:
    1. DETER (alerta) → 2. Fogo → 3. PRODES (corte raso) → 4. TerraClass (uso)
    """
    print("\n🔧 CONSTRUINDO TIMELINE DE DEGRADAÇÃO")
    print("-" * 60)
    
    timeline_registros = []
    
    # Processar por município
    cod_ibges = set()
    for fonte, df in dados.items():
        if len(df) > 0 and 'cod_ibge' in df.columns:
            cod_ibges.update(df['cod_ibge'].unique())
    
    print(f"   Municípios processando: {len(cod_ibges):,}")
    
    for cod_ibge in cod_ibges:
        # Dados do município
        prodes_mun = dados['prodes'][dados['prodes']['cod_ibge'] == cod_ibge] if len(dados['prodes']) > 0 else pd.DataFrame()
        deter_mun = dados['deter'][dados['deter']['cod_ibge'] == cod_ibge] if len(dados['deter']) > 0 else pd.DataFrame()
        fogo_mun = dados['fogo'][dados['fogo']['cod_ibge'] == cod_ibge] if len(dados['fogo']) > 0 else pd.DataFrame()
        terra_mun = dados['terra_class'][dados['terra_class']['cod_ibge'] == cod_ibge] if len(dados['terra_class']) > 0 else pd.DataFrame()
        
        # Obter informações do município
        municipio = prodes_mun['municipio'].iloc[0] if len(prodes_mun) > 0 else \
                    deter_mun['municipio'].iloc[0] if len(deter_mun) > 0 else \
                    fogo_mun['municipio'].iloc[0] if len(fogo_mun) > 0 else \
                    terra_mun['municipio'].iloc[0] if len(terra_mun) > 0 else 'Desconhecido'
        
        uf = prodes_mun['uf'].iloc[0] if len(prodes_mun) > 0 else \
             deter_mun['uf'].iloc[0] if len(deter_mun) > 0 else \
             fogo_mun['uf'].iloc[0] if len(fogo_mun) > 0 else \
             terra_mun['uf'].iloc[0] if len(terra_mun) > 0 else 'Desconhecido'
        
        # Anos de análise
        anos = set()
        if len(prodes_mun) > 0:
            anos.update(prodes_mun['ano'].unique())
        if len(deter_mun) > 0:
            anos.update(deter_mun['ano'].unique())
        if len(fogo_mun) > 0:
            anos.update(fogo_mun['ano'].unique())
        if len(terra_mun) > 0:
            anos.update(terra_mun['ano'].unique())
        
        for ano in sorted(anos):
            evento = []
            areas = {}
            
            # Verificar DETER (alerta)
            deter_ano = deter_mun[deter_mun['ano'] == ano] if len(deter_mun) > 0 else pd.DataFrame()
            if len(deter_ano) > 0:
                evento.append('DETER')
                areas['deter_ha'] = deter_ano['area_ha'].sum()
            
            # Verificar Fogo
            fogo_ano = fogo_mun[fogo_mun['ano'] == ano] if len(fogo_mun) > 0 else pd.DataFrame()
            if len(fogo_ano) > 0:
                evento.append('FOGO')
                areas['fogo_ha'] = fogo_ano['area_queimada_ha'].sum()
            
            # Verificar PRODES (corte raso)
            prodes_ano = prodes_mun[prodes_mun['ano'] == ano] if len(prodes_mun) > 0 else pd.DataFrame()
            if len(prodes_ano) > 0:
                evento.append('PRODES')
                areas['prodes_ha'] = prodes_ano['area_desmatada_ha'].sum()
            
            # Verificar TerraClass (uso) - bienal
            terra_ano = terra_mun[terra_mun['ano'] == ano] if len(terra_mun) > 0 else pd.DataFrame()
            if len(terra_ano) > 0:
                evento.append('TERRACLASS')
                # Classe dominante
                classe_dominante = terra_ano.groupby('classe_uso')['area_ha'].sum().idxmax()
                areas['classe_uso'] = classe_dominante
                areas['terra_class_ha'] = terra_ano['area_ha'].sum()
            
            if len(evento) > 0:
                timeline_registros.append({
                    'cod_ibge': cod_ibge,
                    'municipio': municipio,
                    'uf': uf,
                    'ano': ano,
                    'sequencia_eventos': ' → '.join(evento) if evento else None,
                    'num_eventos': len(evento),
                    **areas
                })
    
    timeline_df = pd.DataFrame(timeline_registros)
    print(f"   Registros na timeline: {len(timeline_df):,}")
    print(f"   Municípios na timeline: {timeline_df['cod_ibge'].nunique():,}")
    
    return timeline_df


# =============================================================================
# CÁLCULO DE LATÊNCIA
# =============================================================================

def calculate_latency(dados):
    """
    Calcular latência entre alerta (DETER) e corte raso (PRODES).
    
    Para dados simulados (anual), a latência é medida em anos.
    Para dados reais (com datas), seria medida em dias.
    """
    print("\n⏱️  CALCULANDO LATÊNCIA ALERTA → CORTE RASO")
    print("-" * 60)
    
    latencia_registros = []
    
    # Processar apenas municípios com ambos DETER e PRODES
    if len(dados['deter']) == 0 or len(dados['prodes']) == 0:
        print("   ⚠️  Dados insuficientes para calcular latência")
        return pd.DataFrame()
    
    deter_mun = dados['deter'].groupby(['cod_ibge', 'municipio', 'uf'])['ano'].min().reset_index()
    deter_mun.columns = ['cod_ibge', 'municipio', 'uf', 'ano_primeiro_alerta']
    
    prodes_mun = dados['prodes'].groupby(['cod_ibge', 'municipio', 'uf'])['ano'].min().reset_index()
    prodes_mun.columns = ['cod_ibge', 'municipio', 'uf', 'ano_primeiro_corte']
    
    # Unir
    latencia_df = deter_mun.merge(prodes_mun, on=['cod_ibge', 'municipio', 'uf'], how='inner')
    
    # Calcular latência em anos
    latencia_df['latencia_anos'] = latencia_df['ano_primeiro_corte'] - latencia_df['ano_primeiro_alerta']
    
    # Converter para dias (aproximado, para comparação)
    latencia_df['latencia_dias'] = latencia_df['latencia_anos'] * 365
    
    # Classificar latência
    def classificar_latencia(latencia):
        if latencia < 0:
            return 'Corte antes do alerta'
        elif latencia == 0:
            return 'Mesmo ano'
        elif latencia == 1:
            return '1 ano'
        elif latencia <= 3:
            return '2-3 anos'
        else:
            return '> 3 anos'
    
    latencia_df['latencia_categoria'] = latencia_df['latencia_anos'].apply(classificar_latencia)
    
    print(f"   Municípios com latência calculada: {len(latencia_df):,}")
    print(f"   Latência média: {latencia_df['latencia_anos'].mean():.2f} anos")
    print(f"   Latência mediana: {latencia_df['latencia_anos'].median():.2f} anos")
    
    # Distribuição
    print("   Distribuição da latência:")
    for cat in latencia_df['latencia_categoria'].unique():
        count = len(latencia_df[latencia_df['latencia_categoria'] == cat])
        pct = count / len(latencia_df) * 100
        print(f"      - {cat}: {count:,} ({pct:.1f}%)")
    
    return latencia_df


# =============================================================================
# RECORRÊNCIA DE ALERTAS
# =============================================================================

def calculate_recorrencia(dados):
    """
    Calcular recorrência de alertas por município.
    """
    print("\n🔁 CALCULANDO RECORRÊNCIA DE ALERTAS")
    print("-" * 60)
    
    if len(dados['deter']) == 0:
        print("   ⚠️  Dados DETER insuficientes")
        return pd.DataFrame()
    
    # Agrupar por município
    recorrencia = dados['deter'].groupby(['cod_ibge', 'municipio', 'uf']).agg(
        num_alertas_total=('area_ha', 'count'),
        area_total_alertas_ha=('area_ha', 'sum'),
        ano_primeiro_alerta=('ano', 'min'),
        ano_ultimo_alerta=('ano', 'max'),
    ).reset_index()
    
    # Calcular anos de alerta
    recorrencia['anos_alerta'] = recorrencia['ano_ultimo_alerta'] - recorrencia['ano_primeiro_alerta'] + 1
    
    # Calcular média de alertas por ano
    recorrencia['alertas_por_ano'] = recorrencia['num_alertas_total'] / recorrencia['anos_alerta']
    
    # Classificar recorrência (alta se > média + 2std)
    media = recorrencia['alertas_por_ano'].mean()
    std = recorrencia['alertas_por_ano'].std()
    threshold = media + 2 * std
    
    recorrencia['recorrencia_alta'] = recorrencia['alertas_por_ano'] > threshold
    recorrencia['recorrencia_media'] = (recorrencia['alertas_por_ano'] >= media) & (~recorrencia['recorrencia_alta'])
    recorrencia['recorrencia_baixa'] = recorrencia['alertas_por_ano'] < media
    
    print(f"   Municípios analisados: {len(recorrencia):,}")
    print(f"   Alertas totais: {recorrencia['num_alertas_total'].sum():,}")
    print(f"   Área total alertada: {recorrencia['area_total_alertas_ha'].sum():,.0f} ha")
    print(f"   Municípios com recorrência alta: {recorrencia['recorrencia_alta'].sum():,}")
    
    return recorrencia


# =============================================================================
# ANÁLISE DE TRANSIÇÃO
# =============================================================================

def analyze_transition(timeline_df):
    """
    Analisar transições de uso do solo baseado na timeline.
    """
    print("\n🔄 ANALISANDO TRANSIÇÕES DE USO DO SOLO")
    print("-" * 60)
    
    if len(timeline_df) == 0:
        return pd.DataFrame()
    
    # Filtrar municípios com TerraClass
    timeline_com_terra = timeline_df[timeline_df['sequencia_eventos'].str.contains('TERRACLASS', na=False)]
    
    if len(timeline_com_terra) == 0:
        print("   ⚠️  Sem dados de TerraClass para análise")
        return pd.DataFrame()
    
    # Agrupar por município para analisar sequência completa
    transicoes = []
    
    for cod_ibge in timeline_com_terra['cod_ibge'].unique():
        mun_data = timeline_com_terra[timeline_com_terra['cod_ibge'] == cod_ibge].sort_values('ano')
        
        if len(mun_data) < 2:
            continue
        
        municipio = mun_data['municipio'].iloc[0]
        uf = mun_data['uf'].iloc[0]
        
        # Analisar cada par de anos consecutivos
        for i in range(len(mun_data) - 1):
            ano_atual = mun_data.iloc[i]
            ano_seguinte = mun_data.iloc[i + 1]
            
            # Determinar estado atual e seguinte
            if 'classe_uso' in ano_atual and pd.notna(ano_atual['classe_uso']):
                estado_atual = ano_atual['classe_uso']
            elif 'PRODES' in str(ano_atual['sequencia_eventos']):
                estado_atual = 'Corte Raso'
            elif 'FOGO' in str(ano_atual['sequencia_eventos']):
                estado_atual = 'Fogo'
            elif 'DETER' in str(ano_atual['sequencia_eventos']):
                estado_atual = 'Alerta'
            else:
                continue
            
            if 'classe_uso' in ano_seguinte and pd.notna(ano_seguinte['classe_uso']):
                estado_seguinte = ano_seguinte['classe_uso']
            elif 'PRODES' in str(ano_seguinte['sequencia_eventos']):
                estado_seguinte = 'Corte Raso'
            else:
                continue
            
            transicoes.append({
                'cod_ibge': cod_ibge,
                'municipio': municipio,
                'uf': uf,
                'ano_inicial': ano_atual['ano'],
                'ano_final': ano_seguinte['ano'],
                'estado_origem': estado_atual,
                'estado_destino': estado_seguinte,
                'area_ha': ano_seguinte.get('terra_class_ha', 0)
            })
    
    transicoes_df = pd.DataFrame(transicoes)
    
    if len(transicoes_df) > 0:
        print(f"   Transições identificadas: {len(transicoes_df):,}")
        print("   Top 5 transições:")
        top_transicoes = transicoes_df.groupby(['estado_origem', 'estado_destino']).size().nlargest(5)
        for (origem, destino), count in top_transicoes.items():
            print(f"      - {origem} → {destino}: {count:,}")
    else:
        print("   ⚠️  Sem transições identificadas")
    
    return transicoes_df


# =============================================================================
# PRINCIPAL
# =============================================================================

def main():
    """Pipeline principal da ETL 4.2"""
    
    # Carregar dados
    dados = load_all_data()
    
    # Verificar se há dados suficientes
    if all(len(df) == 0 for df in dados.values()):
        print("\n❌ Erro: Nenhum dado encontrado para processar")
        return
    
    # Construir timeline
    timeline_df = build_timeline(dados)
    
    # Calcular latência
    latencia_df = calculate_latency(dados)
    
    # Calcular recorrência
    recorrencia_df = calculate_recorrencia(dados)
    
    # Analisar transições
    transicoes_df = analyze_transition(timeline_df)
    
    # Salvar resultados
    print("\n💾 SALVANDO RESULTADOS")
    print("-" * 60)
    
    # Timeline
    if len(timeline_df) > 0:
        timeline_df.to_parquet(TIMELINE_FILE, index=False)
        print(f"   ✅ Timeline: {TIMELINE_FILE.relative_to(BASE_DIR)}")
    
    # Latência
    if len(latencia_df) > 0:
        latencia_df.to_parquet(LATENCIA_FILE, index=False)
        print(f"   ✅ Latência: {LATENCIA_FILE.relative_to(BASE_DIR)}")
    
    # Recorrência
    if len(recorrencia_df) > 0:
        recorrencia_df.to_parquet(RECORRENCIA_FILE, index=False)
        print(f"   ✅ Recorrência: {RECORRENCIA_FILE.relative_to(BASE_DIR)}")
    
    # Resumo
    resumo = {
        'data_processamento': datetime.now().isoformat(),
        'timeline': {
            'registros': len(timeline_df),
            'municipios': int(timeline_df['cod_ibge'].nunique()) if len(timeline_df) > 0 else 0,
            'arquivo': str(TIMELINE_FILE.relative_to(BASE_DIR)) if len(timeline_df) > 0 else None
        },
        'latencia': {
            'municipios': len(latencia_df),
            'latencia_media_anos': round(latencia_df['latencia_anos'].mean(), 2) if len(latencia_df) > 0 else None,
            'arquivo': str(LATENCIA_FILE.relative_to(BASE_DIR)) if len(latencia_df) > 0 else None
        },
        'recorrencia': {
            'municipios': len(recorrencia_df),
            'recorrencia_alta': int(recorrencia_df['recorrencia_alta'].sum()) if len(recorrencia_df) > 0 else 0,
            'arquivo': str(RECORRENCIA_FILE.relative_to(BASE_DIR)) if len(recorrencia_df) > 0 else None
        }
    }
    
    resumo_file = Path(__file__).parent / 'etl_4.2_resumo.json'
    with open(resumo_file, 'w', encoding='utf-8') as f:
        json.dump(resumo, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Resumo: {resumo_file.relative_to(BASE_DIR)}")
    
    print("\n" + "=" * 80)
    print("ETL 4.2 CONCLUÍDA COM SUCESSO")
    print("=" * 80)
    
    return resumo


if __name__ == '__main__':
    main()
