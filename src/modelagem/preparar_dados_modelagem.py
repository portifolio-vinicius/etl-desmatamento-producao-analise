#!/usr/bin/env python
# coding: utf-8

"""
Preparação de dados para modelagem de classificação.

Gera targets temporais (ano seguinte), seleciona features sem vazamento
e aplica split temporal treino (2020-2022) / teste (2023).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, Tuple

import numpy as np
import pandas as pd

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.caminhos import CaminhosDados, repo_root

logger = logging.getLogger(__name__)

TargetBinario = Literal["tem_desmatamento_proximo_ano", "tem_embargos_proximo_ano"]
TargetMulticlasse = Literal["classe_risco_proximo_ano"]

COLUNAS_IDENTIFICADORAS = {"cod_ibge", "municipio", "ano"}
COLUNAS_VAZAMENTO_DESMATAMENTO = {
    "area_desmatada_ha",
    "tem_desmatamento",
    "log_area_desmatada",
    "risco_desmatamento",
}
COLUNAS_VAZAMENTO_EMBARGOS = {
    "num_embargos",
    "area_embargada_ha",
    "tem_embargos",
    "log_num_embargos",
    "log_area_embargada",
}
COLUNAS_TARGET = {
    "tem_desmatamento_proximo_ano",
    "tem_embargos_proximo_ano",
    "classe_risco_proximo_ano",
    "area_desmatada_proximo_ano",
}
COLUNAS_CATEGORICAS = {"uf", "regiao", "idhm_categoria"}
ANOS_TREINO = (2020, 2021)
ANO_TESTE = 2022
MAX_LINHAS_TREINO = 50_000


@dataclass
class ConjuntosModelagem:
    """Conjuntos de treino e teste para modelagem."""

    X_treino: pd.DataFrame
    X_teste: pd.DataFrame
    y_treino: pd.Series
    y_teste: pd.Series
    colunas_numericas: list[str]
    colunas_categoricas: list[str]
    target: str


def carregar_dataset_modelagem(
    nome_arquivo: str = "dataset_preditivo_com_precos.parquet",
    caminho_base: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Carrega dataset da camada de modelagem.

    Args:
        nome_arquivo: Nome do arquivo Parquet.
        caminho_base: Diretório base; padrão é data/04_modelagem.

    Returns:
        DataFrame carregado.
    """
    base = caminho_base or (repo_root() / CaminhosDados.MODELAGEM_DIR)
    caminho = base / nome_arquivo

    if not caminho.exists():
        caminho_fallback = base / "dataset_preditivo_consolidado.parquet"
        if caminho_fallback.exists():
            logger.warning(
                "Arquivo %s não encontrado; usando %s",
                nome_arquivo,
                caminho_fallback.name,
            )
            caminho = caminho_fallback
        else:
            raise FileNotFoundError(f"Dataset não encontrado: {caminho}")

    df = pd.read_parquet(caminho)
    logger.info("Dataset carregado: %s shape=%s", caminho.name, df.shape)
    return df


def deduplicar_painel_municipal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante uma observação por município e ano.

    Args:
        df: Dataset bruto da camada de modelagem.

    Returns:
        DataFrame deduplicado por cod_ibge e ano.
    """
    if "cod_ibge" not in df.columns or "ano" not in df.columns:
        return df

    antes = len(df)
    df_dedup = df.sort_values(["cod_ibge", "ano"]).drop_duplicates(
        subset=["cod_ibge", "ano"],
        keep="last",
    )
    if len(df_dedup) < antes:
        logger.info(
            "Painel deduplicado: %d -> %d linhas (cod_ibge + ano)",
            antes,
            len(df_dedup),
        )
    return df_dedup


def criar_targets_temporais(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria targets do ano seguinte por município.

    Args:
        df: DataFrame com cod_ibge, ano e variáveis base.

    Returns:
        DataFrame enriquecido com colunas target.
    """
    df = df.sort_values(["cod_ibge", "ano"]).copy()

    df["tem_desmatamento_proximo_ano"] = df.groupby("cod_ibge")[
        "tem_desmatamento"
    ].shift(-1)
    df["tem_embargos_proximo_ano"] = df.groupby("cod_ibge")["tem_embargos"].shift(
        -1
    )
    df["area_desmatada_proximo_ano"] = df.groupby("cod_ibge")[
        "area_desmatada_ha"
    ].shift(-1)

    df["classe_risco_proximo_ano"] = _classificar_risco(
        df["area_desmatada_proximo_ano"]
    )

    return df


def _classificar_risco(area_proximo_ano: pd.Series) -> pd.Series:
    """
    Classifica risco de desmatamento do ano seguinte em baixo/medio/alto.

    Args:
        area_proximo_ano: Série com área desmatada do ano seguinte.

    Returns:
        Série categórica com classes de risco.
    """
    area = area_proximo_ano.fillna(0)
    limite_medio = area[area > 0].quantile(0.5) if (area > 0).any() else 0.0

    def rotular(valor: float) -> str:
        if pd.isna(valor) or valor <= 0:
            return "baixo"
        if valor <= limite_medio:
            return "medio"
        return "alto"

    return area.apply(rotular).astype("category")


def _colunas_excluidas_features(target: str) -> set[str]:
    """Retorna colunas que não devem entrar como features."""
    excluidas = COLUNAS_IDENTIFICADORAS | COLUNAS_TARGET

    if target == "tem_desmatamento_proximo_ano":
        excluidas |= COLUNAS_VAZAMENTO_DESMATAMENTO
    elif target == "tem_embargos_proximo_ano":
        excluidas |= COLUNAS_VAZAMENTO_EMBARGOS
    elif target == "classe_risco_proximo_ano":
        excluidas |= COLUNAS_VAZAMENTO_DESMATAMENTO

    return excluidas


def selecionar_features(
    df: pd.DataFrame, target: str
) -> Tuple[pd.DataFrame, pd.Series, list[str], list[str]]:
    """
    Separa features e target, removendo colunas com vazamento.

    Args:
        df: DataFrame com targets já criados.
        target: Nome da coluna alvo.

    Returns:
        Tupla (X, y, colunas_numericas, colunas_categoricas).
    """
    if target not in df.columns:
        raise ValueError(f"Target '{target}' não encontrado no dataset.")

    df_valido = df.dropna(subset=[target]).copy()
    excluidas = _colunas_excluidas_features(target)

    colunas_features = [c for c in df_valido.columns if c not in excluidas]
    X = df_valido[colunas_features].copy()
    y = df_valido[target].copy()

    colunas_categoricas = [
        c for c in X.columns if c in COLUNAS_CATEGORICAS or X[c].dtype.name == "category"
    ]
    colunas_numericas = [c for c in X.columns if c not in colunas_categoricas]

    for col in colunas_categoricas:
        X[col] = X[col].astype(str).replace("nan", "desconhecido")

    for col in colunas_numericas:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)

    if target in ("tem_desmatamento_proximo_ano", "tem_embargos_proximo_ano"):
        y = y.astype(int)

    logger.info(
        "Features selecionadas para %s: %d numéricas, %d categóricas, n=%d",
        target,
        len(colunas_numericas),
        len(colunas_categoricas),
        len(X),
    )
    return X, y, colunas_numericas, colunas_categoricas


def split_temporal(
    df: pd.DataFrame,
    target: str,
    anos_treino: Tuple[int, ...] = ANOS_TREINO,
    ano_teste: int = ANO_TESTE,
) -> ConjuntosModelagem:
    """
    Aplica split temporal treino/teste.

    Args:
        df: DataFrame completo com coluna ano.
        target: Nome da coluna alvo.
        anos_treino: Anos usados no treino.
        ano_teste: Ano reservado para teste.

    Returns:
        ConjuntosModelagem com treino e teste.
    """
    df_targets = criar_targets_temporais(deduplicar_painel_municipal(df))
    X, y, cols_num, cols_cat = selecionar_features(df_targets, target)

    indices = df_targets.dropna(subset=[target]).index
    anos = df_targets.loc[indices, "ano"]

    mascara_treino = anos.isin(anos_treino)
    mascara_teste = anos == ano_teste

    X_treino = X.loc[mascara_treino].reset_index(drop=True)
    y_treino = y.loc[mascara_treino].reset_index(drop=True)
    X_teste = X.loc[mascara_teste].reset_index(drop=True)
    y_teste = y.loc[mascara_teste].reset_index(drop=True)

    if len(X_treino) == 0 or len(X_teste) == 0:
        raise ValueError(
            f"Split temporal inválido para {target}: "
            f"treino={len(X_treino)}, teste={len(X_teste)}"
        )

    logger.info(
        "Split temporal %s: treino=%d (anos %s), teste=%d (ano %d)",
        target,
        len(X_treino),
        anos_treino,
        len(X_teste),
        ano_teste,
    )
    logger.info(
        "Distribuição target treino: %s",
        y_treino.value_counts(normalize=True).round(3).to_dict(),
    )

    return ConjuntosModelagem(
        X_treino=X_treino,
        X_teste=X_teste,
        y_treino=y_treino,
        y_teste=y_teste,
        colunas_numericas=cols_num,
        colunas_categoricas=cols_cat,
        target=target,
    )


def amostrar_treino_estratificado(
    X_treino: pd.DataFrame,
    y_treino: pd.Series,
    max_linhas: int = MAX_LINHAS_TREINO,
    random_state: int = 42,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Reduz o treino por amostragem estratificada quando necessário.

    Args:
        X_treino: Features de treino.
        y_treino: Target de treino.
        max_linhas: Máximo de linhas no treino.
        random_state: Semente para reprodutibilidade.

    Returns:
        Tupla (X_treino, y_treino), possivelmente amostrada.
    """
    if len(X_treino) <= max_linhas:
        return X_treino, y_treino

    from sklearn.model_selection import train_test_split

    try:
        X_amostra, _, y_amostra, _ = train_test_split(
            X_treino,
            y_treino,
            train_size=max_linhas,
            stratify=y_treino,
            random_state=random_state,
        )
    except ValueError:
        X_amostra = X_treino.sample(n=max_linhas, random_state=random_state)
        y_amostra = y_treino.loc[X_amostra.index]

    logger.warning(
        "Treino reduzido por amostragem estratificada: %d -> %d linhas",
        len(X_treino),
        len(X_amostra),
    )
    return X_amostra.reset_index(drop=True), y_amostra.reset_index(drop=True)


def obter_diretorio_resultados(caminho_base: Optional[Path] = None) -> Path:
    """Retorna diretório para salvar métricas e figuras."""
    base = caminho_base or (repo_root() / CaminhosDados.MODELAGEM_DIR)
    diretorio = base / "resultados_metricas"
    diretorio.mkdir(parents=True, exist_ok=True)
    return diretorio


def obter_diretorio_modelos(caminho_base: Optional[Path] = None) -> Path:
    """Retorna diretório para salvar modelos treinados."""
    base = caminho_base or (repo_root() / CaminhosDados.MODELAGEM_DIR)
    diretorio = base / "modelos"
    diretorio.mkdir(parents=True, exist_ok=True)
    return diretorio
