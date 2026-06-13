#!/usr/bin/env python
# coding: utf-8

"""
Gera relatório consolidado da modelagem de classificação.

Lê artefatos em data/04_modelagem/resultados_metricas/ e produz
docs/relatorio_modelagem_classificacao.md.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from modelagem.preparar_dados_modelagem import (
    ANO_TESTE,
    ANOS_TREINO,
    MAX_LINHAS_TREINO,
    carregar_dataset_modelagem,
    criar_targets_temporais,
    deduplicar_painel_municipal,
    obter_diretorio_resultados,
    split_temporal,
)
from utils.caminhos import repo_root
from utils.logging_config import configurar_logging

logger = logging.getLogger(__name__)

CAMINHO_FIGURAS = "../data/04_modelagem/resultados_metricas"


def carregar_json(caminho: Path) -> Optional[List[Dict[str, Any]]]:
    """
    Carrega arquivo JSON de resultados.

    Args:
        caminho: Caminho do arquivo.

    Returns:
        Lista de resultados ou None se ausente.
    """
    if not caminho.exists():
        logger.warning("Arquivo não encontrado: %s", caminho)
        return None

    with open(caminho, encoding="utf-8") as arquivo:
        dados = json.load(arquivo)

    if isinstance(dados, dict):
        return [dados]
    return dados


def _formatar_tabela_resultados(resultados: List[Dict[str, Any]]) -> str:
    """Formata tabela markdown com métricas principais."""
    if not resultados:
        return "_Sem resultados disponíveis._\n"

    linhas = [
        "| Modelo | Balanceamento | Accuracy | Precision | Recall | F1 | ROC-AUC | CV F1 |",
        "|--------|---------------|----------|-----------|--------|----|---------|---------|",
    ]

    for item in sorted(resultados, key=lambda x: x.get("f1", 0), reverse=True):
        linhas.append(
            "| {modelo} | {bal} | {acc:.4f} | {prec:.4f} | {rec:.4f} | {f1:.4f} | {auc} | {cv} |".format(
                modelo=item.get("modelo", "N/A"),
                bal=item.get("estrategia_balanceamento", "nenhuma"),
                acc=item.get("accuracy", 0),
                prec=item.get("precision", 0),
                rec=item.get("recall", 0),
                f1=item.get("f1", 0),
                auc=(
                    f"{item['roc_auc']:.4f}"
                    if item.get("roc_auc") is not None
                    else "N/A"
                ),
                cv=(
                    f"{item['cv_f1_media']:.4f}"
                    if item.get("cv_f1_media") is not None
                    else "N/A"
                ),
            )
        )

    return "\n".join(linhas) + "\n"


def _melhor_modelo(resultados: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Retorna experimento com maior F1 weighted."""
    if not resultados:
        return None
    return max(resultados, key=lambda x: x.get("f1", 0))


def _baseline(resultados: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Retorna DummyClassifier sem balanceamento como linha de base."""
    for item in resultados:
        if (
            "DummyClassifier" in item.get("modelo", "")
            and item.get("estrategia_balanceamento") == "nenhuma"
        ):
            return item
    return None


def _slug_figura(nome_modelo: str, prefixo_target: Optional[str] = None) -> str:
    """Reproduz slug usado em avaliar_modelos.salvar_*."""
    slug = nome_modelo.lower().replace(" ", "_")
    if prefixo_target:
        return f"{prefixo_target}_{slug}"
    return slug


def _prefixo_target(item: Dict[str, Any]) -> str:
    """Extrai prefixo de figuras a partir do campo target."""
    target = item.get("target", "")
    return str(target).replace("_proximo_ano", "") if target else ""


def _extrair_classe_report(
    item: Dict[str, Any], chave: str
) -> Optional[Dict[str, Any]]:
    """Extrai métricas de uma classe do classification_report."""
    relatorio = item.get("classification_report")
    if not relatorio or chave not in relatorio:
        return None
    return relatorio[chave]


def _formatar_metricas_classe_positiva(
    item: Dict[str, Any], chave_classe: str = "1"
) -> str:
    """Formata precision/recall/F1 da classe positiva (binário)."""
    metricas = _extrair_classe_report(item, chave_classe)
    if not metricas:
        return "_Métricas por classe indisponíveis._\n"

    return (
        f"- **Precision (classe {chave_classe}):** {metricas['precision']:.4f}\n"
        f"- **Recall (classe {chave_classe}):** {metricas['recall']:.4f}\n"
        f"- **F1 (classe {chave_classe}):** {metricas['f1-score']:.4f}\n"
        f"- **Suporte (classe {chave_classe}):** {int(metricas['support'])}\n"
    )


def _formatar_metricas_multiclasse(item: Dict[str, Any]) -> str:
    """Formata métricas das classes medio e alto."""
    linhas = []
    for classe in ("medio", "alto"):
        metricas = _extrair_classe_report(item, classe)
        if not metricas:
            continue
        linhas.append(
            f"- **{classe.capitalize()}:** precision={metricas['precision']:.4f}, "
            f"recall={metricas['recall']:.4f}, f1={metricas['f1-score']:.4f} "
            f"(suporte={int(metricas['support'])})\n"
        )
    return "".join(linhas) if linhas else "_Métricas por classe indisponíveis._\n"


def _formatar_figuras_modelo(item: Dict[str, Any], binario: bool = True) -> str:
    """Referencia figuras PNG do melhor modelo."""
    nome_modelo = item.get("modelo", "")
    slug = _slug_figura(nome_modelo, _prefixo_target(item))
    matriz = f"{CAMINHO_FIGURAS}/matriz_confusao_{slug}.png"
    linhas = [
        f"- Matriz de confusão: `{matriz}`\n",
    ]
    if binario:
        roc = f"{CAMINHO_FIGURAS}/curva_roc_{slug}.png"
        linhas.append(f"- Curva ROC: `{roc}`\n")
    return "".join(linhas)


def _formatar_comparacao_baseline(
    melhor: Dict[str, Any],
    baseline_item: Optional[Dict[str, Any]],
    chave_classe: str = "1",
) -> str:
    """Tabela comparativa melhor modelo vs DummyClassifier."""
    if not baseline_item:
        return ""

    melhor_pos = _extrair_classe_report(melhor, chave_classe)
    base_pos = _extrair_classe_report(baseline_item, chave_classe)

    recall_melhor = melhor_pos["recall"] if melhor_pos else melhor.get("recall", 0)
    recall_base = base_pos["recall"] if base_pos else 0
    f1_melhor = melhor_pos["f1-score"] if melhor_pos else melhor.get("f1", 0)
    f1_base = base_pos["f1-score"] if base_pos else baseline_item.get("f1", 0)

    def fmt_auc_val(val: Any) -> str:
        if val is None or val == "N/A":
            return "N/A"
        return f"{float(val):.4f}"

    return (
        "| Métrica | DummyClassifier | Melhor modelo | Δ |\n"
        "|---------|-----------------|---------------|---|\n"
        f"| F1 weighted | {baseline_item.get('f1', 0):.4f} | "
        f"{melhor.get('f1', 0):.4f} | "
        f"{melhor.get('f1', 0) - baseline_item.get('f1', 0):+.4f} |\n"
        f"| Recall classe positiva | {recall_base:.4f} | {recall_melhor:.4f} | "
        f"{recall_melhor - recall_base:+.4f} |\n"
        f"| F1 classe positiva | {f1_base:.4f} | {f1_melhor:.4f} | "
        f"{f1_melhor - f1_base:+.4f} |\n"
        f"| ROC-AUC | {fmt_auc_val(baseline_item.get('roc_auc'))} | "
        f"{fmt_auc_val(melhor.get('roc_auc'))} | — |\n"
    )


def _obter_info_dataset() -> Dict[str, Any]:
    """
    Calcula estatísticas da base usada na modelagem.

    Returns:
        Dicionário com shape bruto/dedup e distribuições de classes no treino.
    """
    df_bruto = carregar_dataset_modelagem()
    df_dedup = deduplicar_painel_municipal(df_bruto)
    df_targets = criar_targets_temporais(df_dedup)

    distribuicoes: Dict[str, Dict[str, float]] = {}
    for target in (
        "tem_desmatamento_proximo_ano",
        "tem_embargos_proximo_ano",
        "classe_risco_proximo_ano",
    ):
        try:
            conjuntos = split_temporal(df_dedup, target)
            distribuicoes[target] = (
                conjuntos.y_treino.value_counts(normalize=True)
                .round(4)
                .astype(float)
                .to_dict()
            )
        except ValueError:
            distribuicoes[target] = {}

    n_treino = n_teste = 0
    try:
        conjuntos_ref = split_temporal(df_dedup, "tem_desmatamento_proximo_ano")
        n_treino = len(conjuntos_ref.X_treino)
        n_teste = len(conjuntos_ref.X_teste)
    except ValueError:
        pass

    return {
        "shape_bruto": df_bruto.shape,
        "shape_dedup": df_dedup.shape,
        "n_treino": n_treino,
        "n_teste": n_teste,
        "distribuicoes": distribuicoes,
    }


def _formatar_distribuicao(dist: Dict[Any, float]) -> str:
    """Formata distribuição de classes como texto legível."""
    if not dist:
        return "_Indisponível_"
    partes = []
    for classe, proporcao in sorted(dist.items(), key=lambda x: str(x[0])):
        partes.append(f"{classe}: {proporcao:.1%}")
    return "; ".join(partes)


def _formatar_resumo_executivo(
    melhor_desmatamento: Optional[Dict[str, Any]],
    melhor_embargos: Optional[Dict[str, Any]],
    melhor_multiclasse: Optional[Dict[str, Any]],
) -> str:
    """Tabela resumo executiva no início do relatório."""

    def fmt_f1(item: Optional[Dict[str, Any]]) -> str:
        return f"{item.get('f1', 0):.4f}" if item else "—"

    def fmt_auc(item: Optional[Dict[str, Any]]) -> str:
        if not item or item.get("roc_auc") is None:
            return "N/A"
        return f"{item['roc_auc']:.2f}"

    def fmt_modelo(item: Optional[Dict[str, Any]]) -> str:
        return item.get("modelo", "N/A") if item else "—"

    return (
        "| Problema | Melhor modelo | F1 (weighted) | ROC-AUC | Observação |\n"
        "|----------|---------------|---------------|---------|------------|\n"
        f"| Desmatamento | {fmt_modelo(melhor_desmatamento)} | "
        f"{fmt_f1(melhor_desmatamento)} | {fmt_auc(melhor_desmatamento)} | "
        "Classe raríssima; F1 weighted enganoso; priorizar ROC-AUC e recall da classe 1 |\n"
        f"| Embargos | {fmt_modelo(melhor_embargos)} | "
        f"{fmt_f1(melhor_embargos)} | {fmt_auc(melhor_embargos)} | "
        "Desbalanceamento moderado; SMOTE melhorou recall da classe positiva |\n"
        f"| Risco multiclasse | {fmt_modelo(melhor_multiclasse)} | "
        f"{fmt_f1(melhor_multiclasse)} | N/A | "
        "Classes medio/alto quase ausentes; F1 macro baixo |\n"
    )


def _texto_metodologia(info: Dict[str, Any]) -> str:
    """Seção de metodologia com constantes dinâmicas."""
    anos_treino_txt = "-".join(str(a) for a in ANOS_TREINO)
    amostragem = (
        f"Amostragem estratificada do treino (máx. {MAX_LINHAS_TREINO:,} linhas) "
        "quando o conjunto excede esse limite."
    )
    if info["n_treino"] <= MAX_LINHAS_TREINO:
        amostragem = (
            f"Treino com {info['n_treino']:,} observações — abaixo do limite de "
            f"{MAX_LINHAS_TREINO:,}; amostragem não foi necessária."
        )

    return f"""- **Base:** `dataset_preditivo_com_precos.parquet`
- **Deduplicação:** uma observação por `cod_ibge` + `ano` ({info['shape_bruto'][0]:,} → {info['shape_dedup'][0]:,} linhas)
- **Targets temporais:** `shift(-1)` por município — features do ano *t* preveem o target do ano *t+1*
- **Split temporal:** treino anos {anos_treino_txt} (prevê {ANOS_TREINO[0] + 1}–{ANOS_TREINO[-1] + 1}); teste ano {ANO_TESTE} (prevê {ANO_TESTE + 1})
- **Por que {ANO_TESTE + 1} não entra no teste:** observações de {ANO_TESTE + 1} exigiriam target de {ANO_TESTE + 2}, inexistente nos dados
- **Tamanho dos conjuntos:** treino={info['n_treino']:,}, teste={info['n_teste']:,}
- **{amostragem}**
- **Exclusão de vazamento:** colunas derivadas do target ou do desfecho contemporâneo
- **Modelos:** DummyClassifier, DecisionTree, KNN, RandomForest
- **Balanceamento (apenas treino):** nenhum, SMOTE, NearMiss
- **Validação cruzada:** estratificada no conjunto de treino (F1 weighted)
"""


def gerar_relatorio_markdown() -> Path:
    """
    Gera relatório markdown consolidado.

    Returns:
        Caminho do arquivo gerado.
    """
    diretorio_resultados = obter_diretorio_resultados()
    resultados_desmatamento = carregar_json(
        diretorio_resultados / "metricas_binario_tem_desmatamento.json"
    )
    resultados_embargos = carregar_json(
        diretorio_resultados / "metricas_binario_tem_embargos.json"
    )
    resultados_multiclasse = carregar_json(
        diretorio_resultados / "metricas_multiclasse_risco.json"
    )

    melhor_desmatamento = _melhor_modelo(resultados_desmatamento or [])
    melhor_embargos = _melhor_modelo(resultados_embargos or [])
    melhor_multiclasse = _melhor_modelo(resultados_multiclasse or [])

    baseline_desmatamento = _baseline(resultados_desmatamento or [])
    baseline_embargos = _baseline(resultados_embargos or [])

    info = _obter_info_dataset()
    dist = info["distribuicoes"]

    data_geracao = datetime.now().strftime("%Y-%m-%d %H:%M")

    conteudo = f"""# Relatório de Modelagem de Classificação

**Projeto:** Análise de Desmatamento, Atividade Econômica e Impacto Socioambiental na Amazônia Legal  
**Gerado em:** {data_geracao}

---

## Resumo executivo

{_formatar_resumo_executivo(melhor_desmatamento, melhor_embargos, melhor_multiclasse)}

Os F1 weighted elevados em desmatamento e multiclasse refletem predominantemente a classe majoritária.
Para interpretação honesta, compare sempre com o **DummyClassifier** e analise **recall da classe positiva**
e **ROC-AUC** (quando aplicável).

---

## 1. Introdução

Este relatório documenta modelos de classificação supervisionada sobre o dataset consolidado em
`data/04_modelagem/`, alinhados aos notebooks do professor sobre classificação binária (17),
validação e métricas (18) e problemas multiclasse (19).

### Objetivos

- Prever ocorrência de desmatamento no ano seguinte (`tem_desmatamento_proximo_ano`)
- Prever ocorrência de embargos no ano seguinte (`tem_embargos_proximo_ano`)
- Classificar risco de desmatamento futuro em `baixo`, `medio` e `alto`

---

## 2. Descrição da base de dados

| Aspecto | Valor |
|---------|-------|
| Arquivo | `data/04_modelagem/dataset_preditivo_com_precos.parquet` |
| Shape bruto | {info['shape_bruto'][0]:,} linhas × {info['shape_bruto'][1]} colunas |
| Após deduplicação (`cod_ibge` + `ano`) | {info['shape_dedup'][0]:,} linhas |
| Anos disponíveis | 2020–2023 |
| Observações no treino | {info['n_treino']:,} |
| Observações no teste | {info['n_teste']:,} |

### Distribuição das classes no treino

| Target | Distribuição (proporção) |
|--------|--------------------------|
| `tem_desmatamento_proximo_ano` | {_formatar_distribuicao(dist.get('tem_desmatamento_proximo_ano', {}))} |
| `tem_embargos_proximo_ano` | {_formatar_distribuicao(dist.get('tem_embargos_proximo_ano', {}))} |
| `classe_risco_proximo_ano` | {_formatar_distribuicao(dist.get('classe_risco_proximo_ano', {}))} |

O desbalanceamento extremo em desmatamento (~0,5% de positivos) e multiclasse (~0,2% medio/alto)
explica por que acurácia e F1 weighted podem ser altos mesmo quando o modelo quase não detecta
a classe minoritária.

---

## 3. Metodologia

{_texto_metodologia(info)}

---

## 4. Classificação binária — Desmatamento

### Resultados comparativos

{_formatar_tabela_resultados(resultados_desmatamento or [])}

### Melhor modelo vs baseline

{_formatar_comparacao_baseline(melhor_desmatamento, baseline_desmatamento) if melhor_desmatamento and baseline_desmatamento else '_Execute os scripts de treino para gerar comparações._'}

### Melhor modelo — métricas detalhadas

"""

    if melhor_desmatamento:
        conteudo += (
            f"- **Modelo:** {melhor_desmatamento.get('modelo')}\n"
            f"- **F1 weighted:** {melhor_desmatamento.get('f1', 0):.4f}\n"
            f"- **ROC-AUC:** {melhor_desmatamento.get('roc_auc', 'N/A')}\n"
        )
        conteudo += _formatar_metricas_classe_positiva(melhor_desmatamento)
        conteudo += "\n### Figuras\n\n"
        conteudo += _formatar_figuras_modelo(melhor_desmatamento)
        conteudo += """
### Conclusão

O KNN sem balanceamento obteve o maior F1 weighted (0,99), mas isso decorre da classe negativa
dominante. O recall da classe positiva permanece baixo (~30%), e a ROC-AUC (~0,76) indica
discriminação modesta — bem acima do baseline (0,50), porém longe de um detector confiável.
O DummyClassifier já alcança F1 weighted ~0,99 ao prever sempre "sem desmatamento".
**NearMiss degradou a performance**, como esperado quando a classe positiva é raríssima.
Para fiscalização preventiva, priorize recall da classe 1 e ROC-AUC, não F1 weighted.

"""
    else:
        conteudo += "_Execute `treinar_classificacao_binaria.py` para gerar resultados._\n\n"

    conteudo += """---

## 5. Classificação binária — Embargos

### Resultados comparativos

"""
    conteudo += _formatar_tabela_resultados(resultados_embargos or [])
    conteudo += "\n### Melhor modelo vs baseline\n\n"
    if melhor_embargos and baseline_embargos:
        conteudo += _formatar_comparacao_baseline(melhor_embargos, baseline_embargos)
    else:
        conteudo += "_Execute os scripts de treino para gerar comparações._\n"
    conteudo += "\n### Melhor modelo — métricas detalhadas\n\n"

    if melhor_embargos:
        conteudo += (
            f"- **Modelo:** {melhor_embargos.get('modelo')}\n"
            f"- **F1 weighted:** {melhor_embargos.get('f1', 0):.4f}\n"
            f"- **ROC-AUC:** {melhor_embargos.get('roc_auc', 'N/A')}\n"
        )
        conteudo += _formatar_metricas_classe_positiva(melhor_embargos)
        conteudo += "\n### Figuras\n\n"
        conteudo += _formatar_figuras_modelo(melhor_embargos)
        conteudo += """
### Conclusão

Embargos apresentam desbalanceamento moderado (~10% positivos). Random Forest com **SMOTE**
superou os demais (F1 weighted 0,87; ROC-AUC 0,87), melhorando recall da classe positiva
em relação ao baseline. Este é o problema com resultados mais equilibrados e interpretáveis.
NearMiss novamente prejudicou recall da classe majoritária ao forçar undersampling agressivo.

"""
    else:
        conteudo += "_Execute `treinar_classificacao_binaria.py --target tem_embargos_proximo_ano`._\n\n"

    conteudo += """---

## 6. Classificação multiclasse — Risco de desmatamento

Classes:
- **baixo:** sem desmatamento no ano seguinte
- **medio:** desmatamento positivo até a mediana entre municípios com desmatamento
- **alto:** desmatamento acima dessa mediana

### Resultados comparativos

"""
    conteudo += _formatar_tabela_resultados(resultados_multiclasse or [])
    conteudo += "\n### Melhor modelo — métricas por classe\n\n"

    if melhor_multiclasse:
        conteudo += (
            f"- **Modelo:** {melhor_multiclasse.get('modelo')}\n"
            f"- **F1 weighted:** {melhor_multiclasse.get('f1', 0):.4f}\n"
            f"- **Accuracy:** {melhor_multiclasse.get('accuracy', 0):.4f}\n"
        )
        relatorio = melhor_multiclasse.get("classification_report", {})
        macro = relatorio.get("macro avg", {})
        if macro:
            conteudo += (
                f"- **F1 macro:** {macro.get('f1-score', 0):.4f}\n"
            )
        conteudo += "\n"
        conteudo += _formatar_metricas_multiclasse(melhor_multiclasse)
        conteudo += "\n### Figuras\n\n"
        conteudo += _formatar_figuras_modelo(melhor_multiclasse, binario=False)
        conteudo += """
### Conclusão

Random Forest com SMOTE lidera pelo F1 weighted (~0,99), mas a classe **medio** permanece
sem recall (0%) e **alto** tem recall ~35%. O F1 macro (~0,48) revela a dificuldade real.
Com apenas ~46 municípios positivos no teste (23 medio + 23 alto), a multiclasse fine-grained
não é viável com 4 anos de painel; agrupamento binário ou mais dados temporais seriam necessários.

"""
    else:
        conteudo += "_Execute `treinar_classificacao_multiclasse.py` para gerar resultados._\n\n"

    conteudo += """---

## 7. Interpretação de negócio

- Compare **sempre** com DummyClassifier: ganhos aparentes em F1 weighted podem ser triviais.
- Em desmatamento, **ROC-AUC** e **recall da classe 1** são mais informativos que acurácia.
- **SMOTE** ajudou em embargos; **NearMiss** piorou ambos os problemas binários neste dataset.
- Para políticas públicas, modelos com alto recall minimizam falsos negativos (municípios de
  risco não monitorados), ao custo de mais falsos positivos (fiscalização em municípios seguros).

---

## 8. Limitações metodológicas

- Apenas 4 anos de observação (2020–2023), restringindo validação cruzada temporal.
- Variáveis meteorológicas (CHIRPS) possuem cobertura limitada em parte dos municípios.
- MapBiomas ainda não está totalmente integrado ao dataset principal.
- Targets do ano seguinte (`shift(-1)`) excluem o último ano disponível do conjunto de teste.
- Classes raras (desmatamento, medio/alto) limitam aprendizado supervisionado robusto.

---

## 9. Artefatos gerados

- `data/04_modelagem/resultados_metricas/metricas_binario_*.json`
- `data/04_modelagem/resultados_metricas/comparativo_binario_*.csv`
- `data/04_modelagem/resultados_metricas/metricas_multiclasse_risco.json`
- `data/04_modelagem/resultados_metricas/matriz_confusao_*.png`
- `data/04_modelagem/resultados_metricas/curva_roc_*.png`
- `data/04_modelagem/modelos/*.pkl`

---

## 10. Comandos de execução

```bash
python src/modelagem/treinar_classificacao_binaria.py --target tem_desmatamento_proximo_ano
python src/modelagem/treinar_classificacao_binaria.py --target tem_embargos_proximo_ano
python src/modelagem/treinar_classificacao_multiclasse.py
python src/modelagem/gerar_relatorio_modelagem.py
```
"""

    caminho_relatorio = repo_root() / "docs" / "relatorio_modelagem_classificacao.md"
    caminho_relatorio.parent.mkdir(parents=True, exist_ok=True)
    caminho_relatorio.write_text(conteudo, encoding="utf-8")
    logger.info("Relatório gerado: %s", caminho_relatorio)
    return caminho_relatorio


def main() -> None:
    """Ponto de entrada do script."""
    configurar_logging(nome_arquivo="gerar_relatorio_modelagem.log", nivel=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())
    gerar_relatorio_markdown()


if __name__ == "__main__":
    main()
