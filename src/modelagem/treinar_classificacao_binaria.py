#!/usr/bin/env python
# coding: utf-8

"""
Treinamento de modelos de classificação binária.

Compara DummyClassifier, DecisionTree, KNN e RandomForest com e sem
balanceamento (SMOTE / NearMiss), conforme notebooks 17 e 18 do professor.
"""

from __future__ import annotations

import argparse
import logging
import pickle
import sys
from pathlib import Path
from typing import Dict, List, Optional

from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline
from imblearn.under_sampling import NearMiss
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.tree import DecisionTreeClassifier

sys.path.insert(0, str(Path(__file__).parent.parent))

from modelagem.avaliar_modelos import (
    avaliar_modelo_completo,
    salvar_comparativo_csv,
    salvar_resultados_json,
)
from modelagem.preparar_dados_modelagem import (
    TargetBinario,
    amostrar_treino_estratificado,
    carregar_dataset_modelagem,
    obter_diretorio_modelos,
    obter_diretorio_resultados,
    split_temporal,
)
from utils.logging_config import configurar_logging

logger = logging.getLogger(__name__)

TARGETS_VALIDOS = ("tem_desmatamento_proximo_ano", "tem_embargos_proximo_ano")


def criar_preprocessador(
    colunas_numericas: List[str],
    colunas_categoricas: List[str],
) -> ColumnTransformer:
    """
    Cria transformador de colunas numéricas e categóricas.

    Args:
        colunas_numericas: Nomes das colunas numéricas.
        colunas_categoricas: Nomes das colunas categóricas.

    Returns:
        ColumnTransformer configurado.
    """
    transformadores = []

    if colunas_numericas:
        transformadores.append(
            (
                "numericas",
                MinMaxScaler(),
                colunas_numericas,
            )
        )

    if colunas_categoricas:
        transformadores.append(
            (
                "categoricas",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                colunas_categoricas,
            )
        )

    return ColumnTransformer(transformers=transformadores, remainder="drop")


def criar_pipeline(
    classificador,
    preprocessador: ColumnTransformer,
    estrategia_balanceamento: Optional[str] = None,
) -> Pipeline:
    """
    Monta pipeline com preprocessamento, balanceamento opcional e classificador.

    Args:
        classificador: Estimador sklearn.
        preprocessador: ColumnTransformer.
        estrategia_balanceamento: None, 'smote' ou 'nearmiss'.

    Returns:
        Pipeline sklearn ou imblearn.
    """
    passos = [("preprocessador", preprocessador)]

    if estrategia_balanceamento == "smote":
        passos.append(("balanceamento", SMOTE(random_state=42)))
    elif estrategia_balanceamento == "nearmiss":
        passos.append(("balanceamento", NearMiss(version=1)))

    passos.append(("classificador", classificador))

    if estrategia_balanceamento:
        return ImbPipeline(passos)

    return Pipeline(passos)


def obter_modelos_base(incluir_knn: bool = True) -> Dict[str, object]:
    """Retorna dicionário de classificadores base."""
    modelos = {
        "DummyClassifier": DummyClassifier(strategy="most_frequent"),
        "DecisionTreeClassifier": DecisionTreeClassifier(
            random_state=42, max_depth=8
        ),
        "RandomForestClassifier": RandomForestClassifier(
            n_estimators=75, random_state=42, n_jobs=-1, max_depth=12
        ),
    }
    if incluir_knn:
        modelos["KNeighborsClassifier"] = KNeighborsClassifier(n_neighbors=5)
    return modelos


def treinar_target(
    target: TargetBinario,
    dataset: Optional[str] = None,
) -> List[dict]:
    """
    Treina e compara modelos para um target binário.

    Args:
        target: Coluna alvo.
        dataset: Nome opcional do arquivo Parquet.

    Returns:
        Lista de resultados por modelo/estratégia.
    """
    nome_dataset = dataset or "dataset_preditivo_com_precos.parquet"
    df = carregar_dataset_modelagem(nome_dataset)
    conjuntos = split_temporal(df, target)
    X_treino, y_treino = amostrar_treino_estratificado(
        conjuntos.X_treino,
        conjuntos.y_treino,
    )

    diretorio_resultados = obter_diretorio_resultados()
    diretorio_modelos = obter_diretorio_modelos()
    preprocessador = criar_preprocessador(
        conjuntos.colunas_numericas,
        conjuntos.colunas_categoricas,
    )

    estrategias = {
        "nenhuma": None,
        "smote": "smote",
        "nearmiss": "nearmiss",
    }

    resultados: List[dict] = []
    melhor_f1 = -1.0
    melhor_pipeline = None
    melhor_nome = ""

    incluir_knn = len(conjuntos.X_teste) <= 20_000
    if not incluir_knn:
        logger.warning(
            "KNN omitido: conjunto de teste grande (%d linhas).",
            len(conjuntos.X_teste),
        )

    for nome_modelo, classificador in obter_modelos_base(incluir_knn).items():
        for nome_estrategia, estrategia in estrategias.items():
            slug = f"{target}_{nome_modelo}_{nome_estrategia}"
            pipeline = criar_pipeline(
                clone(classificador),
                clone(preprocessador),
                estrategia,
            )

            try:
                resultado = avaliar_modelo_completo(
                    pipeline=pipeline,
                    X_treino=X_treino,
                    y_treino=y_treino,
                    X_teste=conjuntos.X_teste,
                    y_teste=conjuntos.y_teste,
                    nome_modelo=f"{nome_modelo} ({nome_estrategia})",
                    diretorio_saida=diretorio_resultados,
                    tipo_problema="binario",
                )
            except Exception as erro:
                logger.error("Falha em %s: %s", slug, erro)
                continue

            resultado["target"] = target
            resultado["estrategia_balanceamento"] = nome_estrategia
            resultados.append(resultado)

            if resultado["f1"] > melhor_f1:
                melhor_f1 = resultado["f1"]
                melhor_pipeline = pipeline
                melhor_nome = slug

    if melhor_pipeline is not None:
        melhor_pipeline.fit(X_treino, y_treino)
        caminho_modelo = diretorio_modelos / f"{melhor_nome}.pkl"
        with open(caminho_modelo, "wb") as arquivo:
            pickle.dump(melhor_pipeline, arquivo)
        logger.info("Melhor modelo salvo: %s (f1=%.4f)", caminho_modelo, melhor_f1)

    slug_target = target.replace("_proximo_ano", "")
    salvar_resultados_json(
        resultados,
        diretorio_resultados / f"metricas_binario_{slug_target}.json",
    )
    salvar_comparativo_csv(
        resultados,
        diretorio_resultados / f"comparativo_binario_{slug_target}.csv",
    )

    return resultados


def main() -> None:
    """Ponto de entrada do script."""
    configurar_logging(nome_arquivo="modelagem_binaria.log", nivel=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser(
        description="Treina modelos de classificação binária."
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=TARGETS_VALIDOS,
        help="Coluna alvo para previsão do ano seguinte.",
    )
    parser.add_argument(
        "--dataset",
        default="dataset_preditivo_com_precos.parquet",
        help="Arquivo Parquet em data/04_modelagem/.",
    )
    args = parser.parse_args()

    logger.info("Iniciando treinamento binário: target=%s", args.target)
    resultados = treinar_target(args.target, args.dataset)
    logger.info("Treinamento concluído: %d experimentos", len(resultados))


if __name__ == "__main__":
    main()
