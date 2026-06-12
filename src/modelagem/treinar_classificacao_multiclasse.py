#!/usr/bin/env python
# coding: utf-8

"""
Treinamento de classificação multiclasse para risco de desmatamento.

Prevê classe de risco do ano seguinte (baixo, medio, alto) usando
RandomForestClassifier, conforme notebook 19 do professor.
"""

from __future__ import annotations

import argparse
import logging
import pickle
import sys
from pathlib import Path
from typing import List, Optional

from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder

sys.path.insert(0, str(Path(__file__).parent.parent))

from modelagem.avaliar_modelos import (
    avaliar_modelo_completo,
    salvar_comparativo_csv,
    salvar_resultados_json,
)
from modelagem.preparar_dados_modelagem import (
    amostrar_treino_estratificado,
    carregar_dataset_modelagem,
    obter_diretorio_modelos,
    obter_diretorio_resultados,
    split_temporal,
)
from modelagem.treinar_classificacao_binaria import criar_preprocessador
from utils.logging_config import configurar_logging

logger = logging.getLogger(__name__)

TARGET = "classe_risco_proximo_ano"


def criar_pipeline_multiclasse(
    preprocessador: ColumnTransformer,
    usar_smote: bool = True,
) -> ImbPipeline:
    """
    Cria pipeline Random Forest para classificação multiclasse.

    Args:
        preprocessador: Transformador de features.
        usar_smote: Se True, aplica SMOTE no treino.

    Returns:
        Pipeline imblearn com classificador.
    """
    classificador = RandomForestClassifier(
        n_estimators=150,
        random_state=42,
        n_jobs=-1,
        max_depth=14,
        class_weight="balanced",
    )

    passos = [("preprocessador", preprocessador), ("classificador", classificador)]
    if usar_smote:
        passos.insert(1, ("balanceamento", SMOTE(random_state=42)))

    return ImbPipeline(passos)


def treinar_multiclasse(dataset: Optional[str] = None) -> List[dict]:
    """
    Treina modelos multiclasse com e sem SMOTE.

    Args:
        dataset: Nome opcional do arquivo Parquet.

    Returns:
        Lista de resultados comparativos.
    """
    nome_dataset = dataset or "dataset_preditivo_com_precos.parquet"
    df = carregar_dataset_modelagem(nome_dataset)
    conjuntos = split_temporal(df, TARGET)
    X_treino, y_treino = amostrar_treino_estratificado(
        conjuntos.X_treino,
        conjuntos.y_treino,
    )

    preprocessador = criar_preprocessador(
        conjuntos.colunas_numericas,
        conjuntos.colunas_categoricas,
    )

    diretorio_resultados = obter_diretorio_resultados()
    diretorio_modelos = obter_diretorio_modelos()
    resultados: List[dict] = []
    melhor_f1 = -1.0
    melhor_pipeline = None

    for usar_smote, nome_estrategia in ((False, "nenhuma"), (True, "smote")):
        pipeline = criar_pipeline_multiclasse(
            clone(preprocessador),
            usar_smote=usar_smote,
        )
        nome_modelo = f"RandomForestClassifier ({nome_estrategia})"

        try:
            resultado = avaliar_modelo_completo(
                pipeline=pipeline,
                X_treino=X_treino,
                y_treino=y_treino,
                X_teste=conjuntos.X_teste,
                y_teste=conjuntos.y_teste,
                nome_modelo=nome_modelo,
                diretorio_saida=diretorio_resultados,
                tipo_problema="multiclasse",
                prefixo_figuras=TARGET.replace("_proximo_ano", ""),
            )
        except Exception as erro:
            logger.error("Falha em multiclasse (%s): %s", nome_estrategia, erro)
            continue

        resultado["target"] = TARGET
        resultado["estrategia_balanceamento"] = nome_estrategia
        resultados.append(resultado)

        if resultado["f1"] > melhor_f1:
            melhor_f1 = resultado["f1"]
            melhor_pipeline = pipeline

    if melhor_pipeline is not None:
        melhor_pipeline.fit(X_treino, y_treino)
        caminho_modelo = diretorio_modelos / "classe_risco_proximo_ano_random_forest.pkl"
        with open(caminho_modelo, "wb") as arquivo:
            pickle.dump(melhor_pipeline, arquivo)
        logger.info("Modelo multiclasse salvo: %s", caminho_modelo)

    salvar_resultados_json(
        resultados,
        diretorio_resultados / "metricas_multiclasse_risco.json",
    )
    salvar_comparativo_csv(
        resultados,
        diretorio_resultados / "comparativo_multiclasse_risco.csv",
    )

    return resultados


def main() -> None:
    """Ponto de entrada do script."""
    configurar_logging(nome_arquivo="modelagem_multiclasse.log", nivel=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser(
        description="Treina classificação multiclasse de risco de desmatamento."
    )
    parser.add_argument(
        "--dataset",
        default="dataset_preditivo_com_precos.parquet",
        help="Arquivo Parquet em data/04_modelagem/.",
    )
    args = parser.parse_args()

    logger.info("Iniciando treinamento multiclasse: target=%s", TARGET)
    resultados = treinar_multiclasse(args.dataset)
    logger.info("Treinamento concluído: %d experimentos", len(resultados))


if __name__ == "__main__":
    main()
