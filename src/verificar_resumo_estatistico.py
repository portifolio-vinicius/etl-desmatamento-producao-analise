"""
Script para gerar resumos estatísticos de todas as camadas de dados (Bronze, Silver, Gold, Reports).

Este script analisa estatisticamente os dados em cada camada e gera relatórios com:
- Resumo geral de cada camada
- Estatísticas descritivas por dataset
- Qualidade de dados (nulos, duplicados)
- Distribuição de variáveis
- Consistência temporal e espacial
"""

import os
from pathlib import Path
from typing import Dict, List, Any, Tuple
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import json
import numpy as np
from datetime import datetime

# Configurações
BASE_DIR = Path("/app")
DATA_DIR = BASE_DIR / "data"
SCRIPT_DIR = Path(__file__).parent

CAMADAS = {
    "01_bronze": "Dados brutos das fontes originais",
    "02_silver": "Dados limpos e padronizados",
    "03_gold": "Dados analíticos com métricas calculadas",
    "04_reports": "Relatórios consolidados"
}


def analisar_arquivo_parquet(caminho_arquivo: Path) -> Dict[str, Any]:
    """
    Analisa um arquivo parquet e retorna estatísticas detalhadas.
    
    Args:
        caminho_arquivo: Caminho para o arquivo parquet
        
    Returns:
        Dicionário com estatísticas detalhadas
    """
    try:
        # Ler metadados
        parquet_file = pq.ParquetFile(caminho_arquivo)
        
        info = {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
            "num_linhas": parquet_file.metadata.num_rows,
            "num_colunas": parquet_file.metadata.num_columns,
            "colunas": [],
            "qualidade": {},
            "estatisticas": {}
        }
        
        # Ler amostra para análise
        df = pd.read_parquet(caminho_arquivo)
        
        # Verificar duplicados
        num_duplicados = df.duplicated().sum()
        info["qualidade"]["num_duplicados"] = int(num_duplicados)
        info["qualidade"]["percentual_duplicados"] = round(num_duplicados / len(df) * 100, 2)
        
        # Análise de colunas
        for col in df.columns:
            col_info = {
                "nome": col,
                "tipo": str(df[col].dtype),
                "nulos": int(df[col].isna().sum()),
                "percentual_nulos": round(df[col].isna().sum() / len(df) * 100, 2),
                "valores_unicos": int(df[col].nunique()),
            }
            
            # Estatísticas para colunas numéricas
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info["min"] = float(df[col].min()) if not df[col].isna().all() else None
                col_info["max"] = float(df[col].max()) if not df[col].isna().all() else None
                col_info["media"] = float(df[col].mean()) if not df[col].isna().all() else None
                col_info["mediana"] = float(df[col].median()) if not df[col].isna().all() else None
                col_info["desvio_padrao"] = float(df[col].std()) if not df[col].isna().all() else None
                col_info["q25"] = float(df[col].quantile(0.25)) if not df[col].isna().all() else None
                col_info["q75"] = float(df[col].quantile(0.75)) if not df[col].isna().all() else None
                col_info["cv"] = round(col_info["desvio_padrao"] / col_info["media"] * 100, 2) if col_info["media"] and col_info["media"] != 0 else None
            
            # Exemplos para colunas categóricas
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                valores_unicos = df[col].dropna().unique()
                if len(valores_unicos) <= 10:
                    col_info["exemplos_valores"] = [str(v) for v in valores_unicos[:10]]
                else:
                    col_info["exemplos_valores"] = [str(v) for v in valores_unicos[:5]] + ["..."]
                
                # Distribuição de frequência para top 10
                freq = df[col].value_counts().head(10)
                col_info["top_valores_freq"] = {str(k): int(v) for k, v in freq.items()}
            
            info["colunas"].append(col_info)
        
        # Estatísticas gerais do dataset
        info["estatisticas"]["total_nulos"] = int(df.isna().sum().sum())
        info["estatisticas"]["percentual_geral_nulos"] = round(df.isna().sum().sum() / (len(df) * len(df.columns)) * 100, 2)
        
        return info
        
    except Exception as e:
        return {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "erro": str(e),
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
        }


def analisar_arquivo_geoparquet(caminho_arquivo: Path) -> Dict[str, Any]:
    """
    Analisa um arquivo geoparquet e retorna estatísticas detalhadas.
    
    Args:
        caminho_arquivo: Caminho para o arquivo geoparquet
        
    Returns:
        Dicionário com estatísticas detalhadas
    """
    try:
        gdf = gpd.read_parquet(caminho_arquivo)
        
        info = {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "tipo": "geoparquet",
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
            "num_linhas": len(gdf),
            "num_colunas": len(gdf.columns),
            "crs": str(gdf.crs) if gdf.crs else "N/A",
            "colunas": [],
            "geometria": {},
            "qualidade": {},
            "estatisticas": {}
        }
        
        # Verificar duplicados
        num_duplicados = gdf.duplicated().sum()
        info["qualidade"]["num_duplicados"] = int(num_duplicados)
        info["qualidade"]["percentual_duplicados"] = round(num_duplicados / len(gdf) * 100, 2)
        
        # Análise de colunas não-geométricas
        for col in gdf.columns:
            if col == "geometry":
                continue
                
            col_info = {
                "nome": col,
                "tipo": str(gdf[col].dtype),
                "nulos": int(gdf[col].isna().sum()),
                "percentual_nulos": round(gdf[col].isna().sum() / len(gdf) * 100, 2),
                "valores_unicos": int(gdf[col].nunique()),
            }
            
            if pd.api.types.is_numeric_dtype(gdf[col]):
                col_info["min"] = float(gdf[col].min()) if not gdf[col].isna().all() else None
                col_info["max"] = float(gdf[col].max()) if not gdf[col].isna().all() else None
                col_info["media"] = float(gdf[col].mean()) if not gdf[col].isna().all() else None
                col_info["mediana"] = float(gdf[col].median()) if not gdf[col].isna().all() else None
                col_info["desvio_padrao"] = float(gdf[col].std()) if not gdf[col].isna().all() else None
            
            if pd.api.types.is_object_dtype(gdf[col]) or pd.api.types.is_string_dtype(gdf[col]):
                valores_unicos = gdf[col].dropna().unique()
                if len(valores_unicos) <= 10:
                    col_info["exemplos_valores"] = [str(v) for v in valores_unicos[:10]]
                else:
                    col_info["exemplos_valores"] = [str(v) for v in valores_unicos[:5]] + ["..."]
            
            info["colunas"].append(col_info)
        
        # Informações sobre geometria
        if "geometry" in gdf.columns:
            info["geometria"]["tipo_geometria"] = gdf.geometry.type.value_counts().to_dict()
            info["geometria"]["extensao_total"] = gdf.geometry.total_bounds.tolist()
            
            # Calcular área total se for polígono
            if any(t in ["Polygon", "MultiPolygon"] for t in gdf.geometry.type.unique()):
                info["geometria"]["area_total_km2"] = round(gdf.geometry.area.sum() / 1_000_000, 2)
        
        return info
        
    except Exception as e:
        return {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "erro": str(e),
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
        }


def analisar_arquivo_csv(caminho_arquivo: Path) -> Dict[str, Any]:
    """
    Analisa um arquivo CSV e retorna estatísticas detalhadas.
    
    Args:
        caminho_arquivo: Caminho para o arquivo CSV
        
    Returns:
        Dicionário com estatísticas detalhadas
    """
    try:
        df = pd.read_csv(caminho_arquivo)
        
        info = {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "tipo": "csv",
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
            "num_linhas": len(df),
            "num_colunas": len(df.columns),
            "colunas": [],
            "qualidade": {},
            "estatisticas": {}
        }
        
        # Verificar duplicados
        num_duplicados = df.duplicated().sum()
        info["qualidade"]["num_duplicados"] = int(num_duplicados)
        info["qualidade"]["percentual_duplicados"] = round(num_duplicados / len(df) * 100, 2)
        
        # Análise de colunas
        for col in df.columns:
            col_info = {
                "nome": col,
                "tipo": str(df[col].dtype),
                "nulos": int(df[col].isna().sum()),
                "percentual_nulos": round(df[col].isna().sum() / len(df) * 100, 2),
                "valores_unicos": int(df[col].nunique()),
            }
            
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info["min"] = float(df[col].min()) if not df[col].isna().all() else None
                col_info["max"] = float(df[col].max()) if not df[col].isna().all() else None
                col_info["media"] = float(df[col].mean()) if not df[col].isna().all() else None
                col_info["mediana"] = float(df[col].median()) if not df[col].isna().all() else None
                col_info["desvio_padrao"] = float(df[col].std()) if not df[col].isna().all() else None
            
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                valores_unicos = df[col].dropna().unique()
                if len(valores_unicos) <= 10:
                    col_info["exemplos_valores"] = [str(v) for v in valores_unicos[:10]]
                else:
                    col_info["exemplos_valores"] = [str(v) for v in valores_unicos[:5]] + ["..."]
            
            info["colunas"].append(col_info)
        
        return info
        
    except Exception as e:
        return {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "erro": str(e),
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
        }


def analisar_arquivo_json(caminho_arquivo: Path) -> Dict[str, Any]:
    """
    Analisa um arquivo JSON e retorna estatísticas detalhadas.
    
    Args:
        caminho_arquivo: Caminho para o arquivo JSON
        
    Returns:
        Dicionário com estatísticas detalhadas
    """
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        info = {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "tipo": "json",
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
            "estrutura": type(data).__name__,
            "qualidade": {},
            "estatisticas": {}
        }
        
        if isinstance(data, dict):
            info["num_chaves"] = len(data)
            info["chaves"] = list(data.keys())[:20]  # Primeiras 20 chaves
        elif isinstance(data, list):
            info["num_itens"] = len(data)
            if data and isinstance(data[0], dict):
                info["colunas"] = list(data[0].keys())[:20]
        
        return info
        
    except Exception as e:
        return {
            "nome_arquivo": caminho_arquivo.name,
            "caminho_relativo": str(caminho_arquivo.relative_to(DATA_DIR)),
            "erro": str(e),
            "tamanho_mb": round(caminho_arquivo.stat().st_size / (1024 * 1024), 2),
        }


def processar_camada(nome_camada: str, descricao_camada: str) -> Dict[str, Any]:
    """
    Processa uma camada completa e retorna estatísticas agregadas.
    
    Args:
        nome_camada: Nome da camada (ex: 01_bronze)
        descricao_camada: Descrição da camada
        
    Returns:
        Dicionário com estatísticas agregadas da camada
    """
    camada_dir = DATA_DIR / nome_camada
    
    if not camada_dir.exists():
        return {
            "nome_camada": nome_camada,
            "erro": "Diretório não encontrado"
        }
    
    print(f"\n{'='*60}")
    print(f"Processando camada: {nome_camada}")
    print(f"Descrição: {descricao_camada}")
    print(f"{'='*60}")
    
    # Encontrar todos os arquivos de dados
    arquivos_dados = []
    
    for ext in ["*.parquet", "*.geoparquet", "*.csv", "*.json"]:
        arquivos_dados.extend(camada_dir.rglob(ext))
    
    if not arquivos_dados:
        print(f"  Nenhum arquivo de dados encontrado")
        return {
            "nome_camada": nome_camada,
            "descricao": descricao_camada,
            "total_arquivos": 0,
            "arquivos": []
        }
    
    print(f"  Encontrados {len(arquivos_dados)} arquivos")
    
    # Analisar cada arquivo
    analises = []
    for arquivo in sorted(arquivos_dados):
        print(f"  Analisando: {arquivo.relative_to(DATA_DIR)}")
        
        if arquivo.suffix == ".geoparquet":
            analise = analisar_arquivo_geoparquet(arquivo)
        elif arquivo.suffix == ".parquet":
            analise = analisar_arquivo_parquet(arquivo)
        elif arquivo.suffix == ".csv":
            analise = analisar_arquivo_csv(arquivo)
        elif arquivo.suffix == ".json":
            analise = analisar_arquivo_json(arquivo)
        else:
            continue
        
        analises.append(analise)
    
    # Calcular estatísticas agregadas
    total_arquivos = len(analises)
    total_linhas = sum(a.get("num_linhas", 0) for a in analises if "num_linhas" in a)
    total_tamanho_mb = sum(a.get("tamanho_mb", 0) for a in analises)
    arquivos_com_erro = sum(1 for a in analises if "erro" in a)
    
    # Resumo de qualidade
    total_duplicados = sum(a.get("qualidade", {}).get("num_duplicados", 0) for a in analises)
    total_nulos = sum(a.get("estatisticas", {}).get("total_nulos", 0) for a in analises)
    
    resumo_camada = {
        "nome_camada": nome_camada,
        "descricao": descricao_camada,
        "total_arquivos": total_arquivos,
        "total_linhas": total_linhas,
        "total_tamanho_mb": round(total_tamanho_mb, 2),
        "arquivos_com_erro": arquivos_com_erro,
        "total_duplicados": total_duplicados,
        "total_nulos": total_nulos,
        "arquivos": analises
    }
    
    return resumo_camada


def gerar_relatorio_markdown(camadas_analisadas: List[Dict[str, Any]]) -> str:
    """
    Gera relatório markdown com resumos estatísticos de todas as camadas.
    
    Args:
        camadas_analisadas: Lista de análises das camadas
        
    Returns:
        String com conteúdo markdown
    """
    markdown = "# Resumo Estatístico das Camadas de Dados\n\n"
    markdown += f"**Data de geração:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n"
    markdown += "---\n\n"
    
    # Resumo executivo
    markdown += "## Resumo Executivo\n\n"
    total_arquivos = sum(c.get("total_arquivos", 0) for c in camadas_analisadas)
    total_linhas = sum(c.get("total_linhas", 0) for c in camadas_analisadas)
    total_tamanho = sum(c.get("total_tamanho_mb", 0) for c in camadas_analisadas)
    total_erros = sum(c.get("arquivos_com_erro", 0) for c in camadas_analisadas)
    
    markdown += f"- **Total de arquivos:** {total_arquivos:,}\n"
    markdown += f"- **Total de linhas:** {total_linhas:,}\n"
    markdown += f"- **Tamanho total:** {total_tamanho:.2f} MB\n"
    markdown += f"- **Arquivos com erro:** {total_erros}\n\n"
    markdown += "---\n\n"
    
    # Detalhes por camada
    for camada in camadas_analisadas:
        if "erro" in camada:
            markdown += f"## {camada['nome_camada']}\n\n"
            markdown += f"**Erro:** {camada['erro']}\n\n"
            markdown += "---\n\n"
            continue
        
        markdown += f"## {camada['nome_camada']}: {camada['descricao']}\n\n"
        
        # Resumo da camada
        markdown += "### Resumo\n\n"
        markdown += f"- **Arquivos:** {camada['total_arquivos']}\n"
        markdown += f"- **Linhas:** {camada['total_linhas']:,}\n"
        markdown += f"- **Tamanho:** {camada['total_tamanho_mb']:.2f} MB\n"
        markdown += f"- **Arquivos com erro:** {camada['arquivos_com_erro']}\n"
        markdown += f"- **Total de duplicados:** {camada['total_duplicados']:,}\n"
        markdown += f"- **Total de nulos:** {camada['total_nulos']:,}\n\n"
        
        # Detalhes por arquivo
        markdown += "### Arquivos\n\n"
        
        for arquivo in camada['arquivos']:
            if "erro" in arquivo:
                markdown += f"#### {arquivo['nome_arquivo']}\n\n"
                markdown += f"**Erro:** {arquivo['erro']}\n"
                markdown += f"**Tamanho:** {arquivo['tamanho_mb']} MB\n\n"
                continue
            
            markdown += f"#### {arquivo['nome_arquivo']}\n\n"
            markdown += f"- **Tipo:** {arquivo.get('tipo', 'parquet')}\n"
            markdown += f"- **Tamanho:** {arquivo['tamanho_mb']} MB\n"
            linhas = arquivo.get('num_linhas', 'N/A')
            if linhas != 'N/A':
                markdown += f"- **Linhas:** {linhas:,}\n"
            else:
                markdown += f"- **Linhas:** N/A\n"
            markdown += f"- **Colunas:** {arquivo.get('num_colunas', 'N/A')}\n"
            
            if "crs" in arquivo:
                markdown += f"- **CRS:** {arquivo['crs']}\n"
            
            if "qualidade" in arquivo and arquivo["qualidade"]:
                markdown += "\n**Qualidade:**\n"
                if arquivo["qualidade"].get("num_duplicados", 0) > 0:
                    markdown += f"- Duplicados: {arquivo['qualidade']['num_duplicados']:,} ({arquivo['qualidade']['percentual_duplicados']}%)\n"
            
            markdown += "\n"
        
        markdown += "---\n\n"
    
    return markdown


def main():
    """
    Função principal para processar todas as camadas.
    """
    print("="*60)
    print("VERIFICAÇÃO E RESUMO ESTATÍSTICO DAS CAMADAS DE DADOS")
    print("="*60)
    print(f"Diretório base: {DATA_DIR}\n")
    
    # Verificar se o diretório existe
    if not DATA_DIR.exists():
        print(f"Erro: Diretório {DATA_DIR} não encontrado")
        return
    
    # Processar cada camada
    camadas_analisadas = []
    for nome_camada, descricao in CAMADAS.items():
        resultado = processar_camada(nome_camada, descricao)
        camadas_analisadas.append(resultado)
    
    # Gerar relatório markdown
    print("\n" + "="*60)
    print("Gerando relatório markdown...")
    print("="*60)
    
    relatorio = gerar_relatorio_markdown(camadas_analisadas)
    
    # Salvar relatório
    relatorio_path = SCRIPT_DIR / "resumo_estatistico_camadas.md"
    with open(relatorio_path, "w", encoding="utf-8") as f:
        f.write(relatorio)
    
    print(f"\nRelatório gerado: {relatorio_path}")
    
    # Salvar JSON para análise programática
    json_path = SCRIPT_DIR / "resumo_estatistico_camadas.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(camadas_analisadas, f, indent=2, ensure_ascii=False)
    
    print(f"Dados JSON salvos: {json_path}")
    print("\nProcessamento concluído!")


if __name__ == "__main__":
    main()
