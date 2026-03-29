import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def gerar_visualizacoes_sprint6():
    print("Gerando visualizações para o Sprint 6...")
    
    # 1. Carregar dados
    df_impacto = pd.read_parquet('data/03_gold/impacto_embargo_producao.parquet')
    df_reincidentes = pd.read_parquet('data/03_gold/reincidentes_embargos.parquet')
    df_fiscal = pd.read_parquet('data/03_gold/fiscalizacao_series_temporais.parquet')
    
    output_dir = 'data/03_gold/visualizacoes'
    os.makedirs(output_dir, exist_ok=True)
    
    # Visualização 1: Boxplot Antes vs Depois (Bovinos)
    plt.figure(figsize=(10, 6))
    melted_bov = df_impacto.melt(id_vars=['cod_ibge'], value_vars=['bovinos_antes', 'bovinos_depois'], 
                                  var_name='Periodo', value_name='Cabecas')
    sns.boxplot(x='Periodo', y='Cabecas', data=melted_bov)
    plt.yscale('log')
    plt.title('Impacto do Embargo no Rebanho Bovino (Antes vs Depois)')
    plt.savefig(os.path.join(output_dir, 'impacto_producao_boxplot.png'))
    plt.close()
    
    # Visualização 2: Histograma do Delta % (Bovinos)
    plt.figure(figsize=(10, 6))
    sns.histplot(df_impacto['delta_bovinos_pct'], bins=50, kde=True)
    plt.axvline(0, color='red', linestyle='--')
    plt.title('Distribuição da Variação do Rebanho Bovino (%) Após Embargo')
    plt.xlabel('Delta %')
    plt.savefig(os.path.join(output_dir, 'delta_bovinos_histogram.png'))
    plt.close()
    
    # Visualização 3: Top 20 Reincidentes
    plt.figure(figsize=(12, 8))
    top_20 = df_reincidentes.head(20)
    sns.barplot(x='num_embargos', y='cpf_cnpj_e', data=top_20, hue='uf_principal', dodge=False)
    plt.title('Top 20 Infratores Reincidentes (Número de Embargos)')
    plt.savefig(os.path.join(output_dir, 'top20_reincidentes.png'))
    plt.close()
    
    # Visualização 4: Evolução de Embargos por UF (Top 5 UFs)
    plt.figure(figsize=(12, 6))
    top_ufs = df_fiscal.groupby('uf')['num_embargos'].sum().nlargest(5).index
    df_top_ufs = df_fiscal[df_fiscal['uf'].isin(top_ufs)]
    sns.lineplot(x='ano', y='num_embargos', hue='uf', data=df_top_ufs, marker='o')
    plt.title('Evolução de Embargos nas 5 UFs com Mais Fiscalização')
    plt.savefig(os.path.join(output_dir, 'serie_temporal_embargos.png'))
    plt.close()
    
    # Visualização 5: Sucesso vs Aumento (Eficácia)
    plt.figure(figsize=(10, 6))
    contagem_eficacia = pd.Series({
        'Redução (Sucesso)': df_impacto['sucesso_embargo'].sum(),
        'Aumento (Descumprimento)': df_impacto['aumento_pos_embargo'].sum(),
        'Estável (±1%)': len(df_impacto) - df_impacto['sucesso_embargo'].sum() - df_impacto['aumento_pos_embargo'].sum()
    })
    contagem_eficacia.plot(kind='pie', autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c', '#95a5a6'])
    plt.title('Eficácia do Embargo no Rebanho Bovino (n=893)')
    plt.ylabel('')
    plt.savefig(os.path.join(output_dir, 'eficacia_embargo_pizza.png'))
    plt.close()
    
    print(f"Visualizações salvas em: {output_dir}")

if __name__ == "__main__":
    gerar_visualizacoes_sprint6()
