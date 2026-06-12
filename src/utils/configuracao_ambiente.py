"""
Módulo de Configuração de Ambiente para Notebooks de Análise Preditiva

Este módulo fornece funções compartilhadas para configurar o ambiente de execução
dos notebooks, detectando automaticamente se está rodando no Google Colab ou localmente,
e ajustando os caminhos de arquivos conforme necessário.

Tipagem: Usa type hints avançados e tipos customizados do módulo types.py
"""

import os
import sys
from pathlib import Path
from typing import Optional, Tuple

# Importar tipos customizados
from .types import (
    CaminhoArquivo,
    Ano,
    Probabilidade,
    UF,
    NivelRisco
)


def detectar_ambiente_colab() -> bool:
    """
    Detecta se o notebook está rodando no Google Colab.
    
    Returns:
        bool: True se estiver no Colab, False caso contrário
        
    Examples:
        >>> if detectar_ambiente_colab():
        ...     print("Rodando no Colab")
    """
    try:
        import google.colab
        return True
    except ImportError:
        return False


def configurar_caminhos_ambiente(caminho_relativo: str = '../') -> Tuple[CaminhoArquivo, CaminhoArquivo]:
    """
    Configura os caminhos base e de dados baseados no ambiente de execução.
    
    Args:
        caminho_relativo: Caminho relativo para uso local (padrão: '../')
        
    Returns:
        Tuple[CaminhoArquivo, CaminhoArquivo]: (caminho_base, caminho_dados)
        
    Examples:
        >>> caminho_base, caminho_dados = configurar_caminhos_ambiente()
        >>> print(f"Base: {caminho_base}, Dados: {caminho_dados}")
    """
    if detectar_ambiente_colab():
        # No Colab, usa o diretório /content/
        caminho_base: CaminhoArquivo = '/content/'
        caminho_dados: CaminhoArquivo = '/content/data/'
    else:
        # Local, usa caminho relativo
        caminho_base: CaminhoArquivo = caminho_relativo
        caminho_dados: CaminhoArquivo = os.path.join(caminho_relativo, 'data/')
    
    return caminho_base, caminho_dados


def montar_google_drive(caminho_drive: CaminhoArquivo = '/content/drive') -> bool:
    """
    Monta o Google Drive no Colab.
    
    Args:
        caminho_drive: Caminho onde o Drive será montado
        
    Returns:
        bool: True se o Drive foi montado com sucesso, False caso contrário
        
    Examples:
        >>> if montar_google_drive():
        ...     print("Drive montado com sucesso")
    """
    if not detectar_ambiente_colab():
        return False
    
    try:
        from google.colab import drive
        drive.mount(caminho_drive)
        print(f"✓ Google Drive montado em {caminho_drive}")
        return True
    except Exception as e:
        print(f"⚠️  Erro ao montar Google Drive: {e}")
        return False


def fazer_upload_arquivo_colab(
    nome_arquivo: str,
    diretorio_destino: CaminhoArquivo = '/content/data/04_modelagem/',
    usar_drive: bool = False,
    caminho_drive: CaminhoArquivo = '/content/drive/MyDrive'
) -> Optional[CaminhoArquivo]:
    """
    Faz upload de um arquivo no Google Colab e move para o diretório correto.
    Pode usar upload direto ou buscar do Google Drive.
    
    Args:
        nome_arquivo: Nome do arquivo esperado (ex: 'dataset_preditivo_com_precos.parquet')
        diretorio_destino: Diretório onde o arquivo deve ser movido
        usar_drive: Se True, tenta buscar o arquivo do Google Drive
        caminho_drive: Caminho base do Google Drive
        
    Returns:
        Optional[CaminhoArquivo]: Caminho do arquivo movido ou None se não estiver no Colab
        
    Examples:
        >>> caminho = fazer_upload_arquivo_colab('dados.parquet')
        >>> if caminho:
        ...     print(f"Arquivo carregado: {caminho}")
    """
    if not detectar_ambiente_colab():
        return None
    
    print("📤 Ambiente Google Colab detectado")
    
    # Cria diretórios necessários
    os.makedirs(diretorio_destino, exist_ok=True)
    os.makedirs('/content/data/03_gold', exist_ok=True)
    
    caminho_destino: CaminhoArquivo = os.path.join(diretorio_destino, nome_arquivo)
    
    # Se o arquivo já existe no destino, retorna o caminho
    if os.path.exists(caminho_destino):
        print(f"✓ Arquivo já existe: {caminho_destino}")
        return caminho_destino
    
    # Tenta buscar do Google Drive se solicitado
    if usar_drive:
        caminho_drive_arquivo: CaminhoArquivo = os.path.join(caminho_drive, nome_arquivo)
        if os.path.exists(caminho_drive_arquivo):
            import shutil
            shutil.copy(caminho_drive_arquivo, caminho_destino)
            print(f"✓ Arquivo copiado do Google Drive: {caminho_destino}")
            return caminho_destino
        else:
            print(f"⚠️  Arquivo não encontrado no Google Drive: {caminho_drive_arquivo}")
            print("Tentando upload manual...")
    
    # Faz upload manual
    print(f"Por favor, faça upload do arquivo: {nome_arquivo}")
    
    from google.colab import files
    
    uploaded = files.upload()
    
    # Move o arquivo para o local correto
    caminho_arquivo: Optional[CaminhoArquivo] = None
    for filename in uploaded.keys():
        if filename.endswith('.parquet'):
            caminho_destino = os.path.join(diretorio_destino, filename)
            os.rename(filename, caminho_destino)
            print(f"✓ Arquivo movido para: {caminho_destino}")
            caminho_arquivo = caminho_destino
        else:
            print(f"⚠️  Arquivo {filename} não é um parquet, ignorando")
    
    if caminho_arquivo:
        print("\n✓ Upload concluído!")
    else:
        print("\n⚠️  Nenhum arquivo parquet foi carregado")
    
    return caminho_arquivo


def obter_caminho_arquivo(
    caminho_relativo: str,
    ambiente_colab: Optional[bool] = None
) -> CaminhoArquivo:
    """
    Retorna o caminho completo do arquivo baseado no ambiente.
    
    Args:
        caminho_relativo: Caminho relativo do arquivo (ex: 'data/04_modelagem/dataset.parquet')
        ambiente_colab: Forçar detecção de ambiente (None = auto-detect)
        
    Returns:
        CaminhoArquivo: Caminho completo do arquivo
        
    Examples:
        >>> caminho = obter_caminho_arquivo('data/04_modelagem/dataset.parquet')
        >>> print(caminho)
    """
    if ambiente_colab is None:
        ambiente_colab = detectar_ambiente_colab()
    
    if ambiente_colab:
        # No Colab, remove o '../' inicial se existir
        if caminho_relativo.startswith('../'):
            caminho_relativo = caminho_relativo[3:]
        return f'/content/{caminho_relativo}'
    else:
        # Local, mantém caminho relativo
        return caminho_relativo


def configurar_notebook_para_colab(
    arquivo_dados: str = 'dataset_preditivo_com_precos.parquet',
    forcar_upload: bool = False,
    usar_drive: bool = False
) -> Tuple[CaminhoArquivo, CaminhoArquivo]:
    """
    Configura o notebook para execução no Colab, incluindo upload de arquivos se necessário.
    
    Args:
        arquivo_dados: Nome do arquivo de dados principal
        forcar_upload: Forçar upload mesmo se o arquivo já existir
        usar_drive: Se True, tenta montar e usar Google Drive
        
    Returns:
        Tuple[CaminhoArquivo, CaminhoArquivo]: (caminho_dados, caminho_saida)
        
    Examples:
        >>> caminho_dados, caminho_saida = configurar_notebook_para_colab()
        >>> print(f"Dados: {caminho_dados}, Saída: {caminho_saida}")
    """
    eh_colab = detectar_ambiente_colab()
    
    if eh_colab:
        print("📤 Ambiente Google Colab detectado")
        caminho_dados: CaminhoArquivo = f'/content/data/04_modelagem/{arquivo_dados}'
        caminho_saida: CaminhoArquivo = '/content/data/03_gold/'
        
        # Tenta montar Google Drive se solicitado
        if usar_drive:
            montar_google_drive()
        
        # Verifica se o arquivo já existe
        if not forcar_upload and os.path.exists(caminho_dados):
            print(f"✓ Arquivo já existe: {caminho_dados}")
        else:
            print(f"⚠️  Arquivo não encontrado: {caminho_dados}")
            fazer_upload_arquivo_colab(arquivo_dados, usar_drive=usar_drive)
    else:
        print("✓ Ambiente local detectado - usando caminhos relativos")
        caminho_dados: CaminhoArquivo = f'../data/04_modelagem/{arquivo_dados}'
        caminho_saida: CaminhoArquivo = '../data/03_gold/'
    
    return caminho_dados, caminho_saida


def adicionar_src_ao_path() -> None:
    """
    Adiciona o diretório src ao Python path para importação de módulos.
    Útil para notebooks que precisam importar funções de src/.
    
    Examples:
        >>> adicionar_src_ao_path()
        >>> from src.utils.predicao import carregar_dados
    """
    caminho_projeto = Path.cwd()
    
    # Se estiver em notebooks_analise_preditiva, volta para o root
    if 'notebooks_analise_preditiva' in str(caminho_projeto):
        caminho_projeto = caminho_projeto.parent
    
    caminho_src = caminho_projeto / 'src'
    
    if str(caminho_src) not in sys.path:
        sys.path.insert(0, str(caminho_src))
        print(f"✓ {caminho_src} adicionado ao Python path")


# ============================================================================
# CONSTANTES DE CONFIGURAÇÃO (TIPADAS)
# ============================================================================

# UFs da Amazônia Legal
UFS_AMAZONIA_LEGAL: list[UF] = ['AC', 'AM', 'AP', 'MA', 'MT', 'PA', 'RO', 'RR', 'TO']

# Anos para divisão temporal
ANO_LIMITE_TREINO: Ano = 2021
ANO_TESTE: Ano = 2022
ANO_PREVISAO: Ano = 2023

# Thresholds para classificação de risco (tipados)
THRESHOLD_CRITICO: Probabilidade = 0.7
THRESHOLD_ALTO: Probabilidade = 0.5
THRESHOLD_MODERADO: Probabilidade = 0.3

# Níveis de risco (tipados)
NIVEIS_RISCO: list[NivelRisco] = ['Baixo', 'Moderado', 'Alto', 'Crítico']
