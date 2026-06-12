#!/usr/bin/env python
# coding: utf-8

"""
Avaliação de modelos de classificação.

Centraliza métricas, matriz de confusão, curvas ROC, validação cruzada
e persistência de resultados.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.base import ClassifierMixin
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    RocCurveDisplay,
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.pipeline import Pipeline

logger = logging.getLogger(__name__)


def calcular_metricas(
    y_verdadeiro: Union[pd.Series, np.ndarray],
    y_predito: Union[pd.Series, np.ndarray],
    y_proba: Optional[np.ndarray] = None,
    tipo_problema: str = "binario",
) -> Dict[str, Any]:
    """
    Calcula métricas de classificação.

    Args:
        y_verdadeiro: Rótulos reais.
        y_predito: Rótulos preditos.
        y_proba: Probabilidades da classe positiva (binário) ou matriz (multiclasse).
        tipo_problema: 'binario' ou 'multiclasse'.

    Returns:
        Dicionário com métricas calculadas.
    """
    metricas: Dict[str, Any] = {
        "accuracy": float(accuracy_score(y_verdadeiro, y_predito)),
        "precision": float(
            precision_score(y_verdadeiro, y_predito, average="weighted", zero_division=0)
        ),
        "recall": float(
            recall_score(y_verdadeiro, y_predito, average="weighted", zero_division=0)
        ),
        "f1": float(f1_score(y_verdadeiro, y_predito, average="weighted", zero_division=0)),
    }

    if tipo_problema == "binario" and y_proba is not None:
        try:
            metricas["roc_auc"] = float(roc_auc_score(y_verdadeiro, y_proba))
        except ValueError:
            metricas["roc_auc"] = None

    metricas["classification_report"] = classification_report(
        y_verdadeiro, y_predito, zero_division=0, output_dict=True
    )
    metricas["confusion_matrix"] = confusion_matrix(y_verdadeiro, y_predito).tolist()

    return metricas


def validacao_cruzada(
    pipeline: Pipeline,
    X: pd.DataFrame,
    y: pd.Series,
    n_folds: int = 3,
) -> Dict[str, Any]:
    """
    Executa validação cruzada estratificada no conjunto de treino.

    Args:
        pipeline: Pipeline sklearn completo.
        X: Features de treino.
        y: Target de treino.
        n_folds: Número de folds (limitado pelos anos disponíveis).

    Returns:
        Métricas agregadas da validação cruzada.
    """
    n_classes = y.nunique()
    folds_efetivos = min(n_folds, len(y), max(n_classes, 2))
    if folds_efetivos < 2:
        logger.warning("Validação cruzada ignorada: folds insuficientes.")
        return {}

    cv = StratifiedKFold(n_splits=folds_efetivos, shuffle=True, random_state=42)
    scoring = {
        "accuracy": "accuracy",
        "precision": "precision_weighted",
        "recall": "recall_weighted",
        "f1": "f1_weighted",
    }

    try:
        resultados = cross_validate(
            pipeline,
            X,
            y,
            cv=cv,
            scoring=scoring,
            n_jobs=-1,
            error_score="raise",
        )
    except Exception as erro:
        logger.warning("Validação cruzada falhou: %s", erro)
        return {}

    resumo = {}
    for chave in scoring:
        valores = resultados[f"test_{chave}"]
        resumo[f"cv_{chave}_media"] = float(np.mean(valores))
        resumo[f"cv_{chave}_desvio"] = float(np.std(valores))

    logger.info("Validação cruzada (%d folds): f1=%.4f", folds_efetivos, resumo["cv_f1_media"])
    return resumo


def salvar_matriz_confusao(
    y_verdadeiro: Union[pd.Series, np.ndarray],
    y_predito: Union[pd.Series, np.ndarray],
    caminho_saida: Path,
    titulo: str,
) -> None:
    """
    Salva figura da matriz de confusão.

    Args:
        y_verdadeiro: Rótulos reais.
        y_predito: Rótulos preditos.
        caminho_saida: Caminho do arquivo PNG.
        titulo: Título do gráfico.
    """
    fig, ax = plt.subplots(figsize=(8, 6))
    ConfusionMatrixDisplay.from_predictions(y_verdadeiro, y_predito, ax=ax)
    ax.set_title(titulo)
    fig.tight_layout()
    fig.savefig(caminho_saida, dpi=120)
    plt.close(fig)
    logger.info("Matriz de confusão salva: %s", caminho_saida)


def salvar_curva_roc(
    y_verdadeiro: Union[pd.Series, np.ndarray],
    y_proba: np.ndarray,
    caminho_saida: Path,
    titulo: str,
) -> None:
    """
    Salva curva ROC para classificação binária.

    Args:
        y_verdadeiro: Rótulos reais.
        y_proba: Probabilidade da classe positiva.
        caminho_saida: Caminho do arquivo PNG.
        titulo: Título do gráfico.
    """
    if len(np.unique(y_verdadeiro)) < 2:
        logger.warning("Curva ROC não gerada: apenas uma classe no teste.")
        return

    fig, ax = plt.subplots(figsize=(8, 6))
    RocCurveDisplay.from_predictions(y_verdadeiro, y_proba, ax=ax)
    ax.set_title(titulo)
    fig.tight_layout()
    fig.savefig(caminho_saida, dpi=120)
    plt.close(fig)
    logger.info("Curva ROC salva: %s", caminho_saida)


def extrair_probabilidades_positivas(
    modelo: Pipeline, X: pd.DataFrame
) -> Optional[np.ndarray]:
    """
    Extrai probabilidades da classe positiva para modelos binários.

    Args:
        modelo: Pipeline treinado com predict_proba.
        X: Features de teste.

    Returns:
        Array de probabilidades ou None se indisponível.
    """
    if not hasattr(modelo, "predict_proba"):
        return None

    proba = modelo.predict_proba(X)
    if proba.shape[1] < 2:
        return None
    return proba[:, 1]


def avaliar_modelo_completo(
    pipeline: Pipeline,
    X_treino: pd.DataFrame,
    y_treino: pd.Series,
    X_teste: pd.DataFrame,
    y_teste: pd.Series,
    nome_modelo: str,
    diretorio_saida: Path,
    tipo_problema: str = "binario",
    executar_cv: bool = True,
) -> Dict[str, Any]:
    """
    Treina, avalia e persiste resultados de um modelo.

    Args:
        pipeline: Pipeline sklearn.
        X_treino: Features de treino.
        y_treino: Target de treino.
        X_teste: Features de teste.
        y_teste: Target de teste.
        nome_modelo: Identificador do modelo (slug).
        diretorio_saida: Diretório para artefatos.
        tipo_problema: 'binario' ou 'multiclasse'.
        executar_cv: Se True, roda validação cruzada no treino.

    Returns:
        Dicionário consolidado de resultados.
    """
    pipeline.fit(X_treino, y_treino)
    y_predito = pipeline.predict(X_teste)

    y_proba = None
    if tipo_problema == "binario":
        y_proba = extrair_probabilidades_positivas(pipeline, X_teste)

    metricas = calcular_metricas(y_teste, y_predito, y_proba, tipo_problema)
    resultado: Dict[str, Any] = {
        "modelo": nome_modelo,
        "tipo_problema": tipo_problema,
        "n_treino": int(len(X_treino)),
        "n_teste": int(len(X_teste)),
        **metricas,
    }

    if executar_cv:
        resultado.update(validacao_cruzada(pipeline, X_treino, y_treino))

    slug = nome_modelo.lower().replace(" ", "_")
    salvar_matriz_confusao(
        y_teste,
        y_predito,
        diretorio_saida / f"matriz_confusao_{slug}.png",
        f"Matriz de Confusão - {nome_modelo}",
    )

    if tipo_problema == "binario" and y_proba is not None:
        salvar_curva_roc(
            y_teste,
            y_proba,
            diretorio_saida / f"curva_roc_{slug}.png",
            f"Curva ROC - {nome_modelo}",
        )

    logger.info(
        "%s | accuracy=%.4f f1=%.4f",
        nome_modelo,
        metricas["accuracy"],
        metricas["f1"],
    )
    return resultado


def salvar_resultados_json(
    resultados: Union[Dict[str, Any], List[Dict[str, Any]]],
    caminho: Path,
) -> None:
    """
    Persiste resultados em JSON.

    Args:
        resultados: Resultado único ou lista de resultados.
        caminho: Caminho do arquivo de saída.
    """
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as arquivo:
        json.dump(resultados, arquivo, indent=2, ensure_ascii=False, default=str)
    logger.info("Resultados salvos em %s", caminho)


def salvar_comparativo_csv(
    resultados: List[Dict[str, Any]],
    caminho: Path,
) -> None:
    """
    Salva comparativo de modelos em CSV.

    Args:
        resultados: Lista de dicionários de métricas.
        caminho: Caminho do arquivo CSV.
    """
    linhas = []
    for item in resultados:
        linhas.append(
            {
                "modelo": item.get("modelo"),
                "estrategia_balanceamento": item.get("estrategia_balanceamento", "nenhuma"),
                "accuracy": item.get("accuracy"),
                "precision": item.get("precision"),
                "recall": item.get("recall"),
                "f1": item.get("f1"),
                "roc_auc": item.get("roc_auc"),
                "cv_f1_media": item.get("cv_f1_media"),
            }
        )
    pd.DataFrame(linhas).to_csv(caminho, index=False)
    logger.info("Comparativo salvo em %s", caminho)
