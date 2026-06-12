# Guia de Implementação de Otimizações

## Visão Geral

Este guia fornece instruções passo a passo para implementar e utilizar as otimizações de performance no projeto de análise preditiva.

## Estrutura de Arquivos

```
src/utils/
├── __init__.py                  # Módulo original (CPU-only)
├── predicao.py                  # Módulo original (CPU-only)
└── predicao_otimizado.py        # Módulo otimizado (GPU + CPU)

docs/
├── otimizacoes_performance.md   # Análise técnica das otimizações
└── guia_otimizacoes.md          # Este arquivo (guia de implementação)
```

## Passo 1: Escolha da Versão do Módulo

### Opção A: Manter Compatibilidade (Recomendado para Início)

Continue usando o módulo original `predicao.py` e implemente otimizações gradualmente:

```python
# notebooks_analise_preditiva/01_previsao_desmatamento.ipynb
from src.utils.predicao import (
    carregar_dados,
    filtrar_amazonia_legal,
    preparar_features_modelo,
    dividir_dados_temporalmente,
    calcular_pesos_classes,
    treinar_modelo_random_forest,
    avaliar_modelo
)
```

### Opção B: Usar Versão Otimizada (Recomendado para Performance)

Mude para o módulo otimizado quando tiver GPU disponível:

```python
# notebooks_analise_preditiva/01_previsao_desmatamento.ipynb
from src.utils.predicao_otimizado import (
    carregar_dados,
    filtrar_amazonia_legal,
    preparar_features_modelo,
    dividir_dados_temporalmente,
    calcular_pesos_classes,
    treinar_modelo_random_forest,
    avaliar_modelo,
    mostrar_info_hardware  # Nova função
)

# Mostrar informações de hardware
mostrar_info_hardware()
```

### Opção C: Implementação Híbrida (Flexível)

Use condicionais para escolher automaticamente:

```python
# notebooks_analise_preditiva/01_previsao_desmatamento.ipynb
try:
    from src.utils.predicao_otimizado import (
        carregar_dados,
        treinar_modelo_random_forest,
        GPU_AVAILABLE
    )
    print("✓ Usando módulo otimizado")
except ImportError:
    from src.utils.predicao import (
        carregar_dados,
        treinar_modelo_random_forest
    )
    GPU_AVAILABLE = False
    print("⚠️  Usando módulo original (GPU não disponível)")

# Usar condicionalmente
df = carregar_dados(caminho, colunas, usar_gpu=GPU_AVAILABLE)
modelo = treinar_modelo_random_forest(X, y, pesos, usar_gpu=GPU_AVAILABLE)
```

## Passo 2: Instalação de Dependências

### Ambiente Local (CPU-only)

```bash
# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Instalar dependências básicas
pip install pandas numpy scikit-learn pyarrow
```

### Ambiente Local com GPU (RAPIDS)

```bash
# Usar conda (recomendado para RAPIDS)
conda create -n analise-preditiva python=3.10
conda activate analise-preditiva

# Instalar RAPIDS
conda install -c rapidsai -c nvidia -c conda-forge \
    cudf cuml cupy dask-cudf

# Instalar dependências adicionais
pip install dask[complete] psutil pynvml
```

### Google Colab com GPU

```python
# No início do notebook
!git clone https://github.com/rapidsai/rapidsai-csp-utils.git
!python rapidsai-csp-utils/colab/pip-install.py

!pip install dask[complete] psutil pynvml
```

## Passo 3: Atualização dos Notebooks

### Exemplo: Atualizar 01_previsao_desmatamento.ipynb

#### Célula de Configuração (Substituir)

```python
# ============================================================================
# CONFIGURAÇÃO DE AMBIENTE (COMPARTILHADO)
# ============================================================================

import sys
import os
from pathlib import Path

# Adicionar caminho do módulo compartilhado
sys.path.append(os.path.join(os.getcwd(), '..'))

# Tentar usar módulo otimizado, fallback para original
try:
    from src.utils.predicao_otimizado import (
        detectar_ambiente_colab,
        montar_google_drive,
        configurar_caminhos_dados,
        UFS_AMAZONIA_LEGAL,
        ANO_LIMITE_TREINO,
        ANO_TESTE,
        ANO_PREVISAO,
        GPU_AVAILABLE,
        mostrar_info_hardware
    )
    MODO_OTIMIZADO = True
    print("✓ Módulo otimizado carregado")
except ImportError:
    from src.utils.predicao import (
        detectar_ambiente_colab,
        montar_google_drive,
        configurar_caminhos_dados,
        UFS_AMAZONIA_LEGAL,
        ANO_LIMITE_TREINO,
        ANO_TESTE,
        ANO_PREVISAO
    )
    GPU_AVAILABLE = False
    MODO_OTIMIZADO = False
    print("⚠️  Módulo original carregado (otimizações não disponíveis)")

# Mostrar informações de hardware se disponível
if MODO_OTIMIZADO:
    mostrar_info_hardware()
```

#### Célula de Importações (Adicionar)

```python
## 1. Configuração e Importações
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, precision_recall_curve, auc
from sklearn.utils.class_weight import compute_class_weight
import warnings
warnings.filterwarnings('ignore')

# Importar funções do módulo compartilhado
if MODO_OTIMIZADO:
    from src.utils.predicao_otimizado import (
        carregar_dados,
        filtrar_amazonia_legal,
        preparar_features_modelo,
        dividir_dados_temporalmente,
        calcular_pesos_classes,
        treinar_modelo_random_forest,
        avaliar_modelo
    )
else:
    from src.utils.predicao import (
        carregar_dados,
        filtrar_amazonia_legal,
        preparar_features_modelo,
        dividir_dados_temporalmente,
        calcular_pesos_classes,
        treinar_modelo_random_forest,
        avaliar_modelo
    )
```

#### Célula de Carregamento (Modificar)

```python
# Carregar dados com otimização se disponível
df = carregar_dados(
    CAMINHO_DADOS, 
    colunas_essenciais=['cod_ibge', 'ano', 'tem_desmatamento'],
    usar_gpu=GPU_AVAILABLE if MODO_OTIMIZADO else False
)
```

#### Célula de Treinamento (Modificar)

```python
# Treinar modelo com otimização se disponível
modelo = treinar_modelo_random_forest(
    X_train, 
    y_train, 
    pesos_classes,
    usar_gpu=GPU_AVAILABLE if MODO_OTIMIZADO else False,
    random_state=42
)
```

## Passo 4: Teste de Performance

### Script de Teste

Crie `testar_otimizacoes.py`:

```python
"""
Script para testar otimizações de performance
"""
import time
import sys
sys.path.append('..')

def testar_modulo_original():
    """Testa performance do módulo original"""
    print("="*50)
    print("TESTANDO MÓDULO ORIGINAL (CPU-only)")
    print("="*50)
    
    from src.utils.predicao import (
        carregar_dados,
        filtrar_amazonia_legal,
        calcular_tendencias_por_municipio
    )
    
    # Carregar dados
    inicio = time.time()
    df = carregar_dados(
        '../data/04_modelagem/dataset_preditivo_com_precos.parquet',
        colunas_essenciais=['cod_ibge', 'ano', 'area_desmatada_ha']
    )
    tempo_carregamento = time.time() - inicio
    print(f"Carregamento: {tempo_carregamento:.2f}s")
    
    # Filtrar
    inicio = time.time()
    df_filtrado = filtrar_amazonia_legal(df)
    tempo_filtragem = time.time() - inicio
    print(f"Filtragem: {tempo_filtragem:.2f}s")
    
    # Calcular tendências
    inicio = time.time()
    tendencias = calcular_tendencias_por_municipio(df_filtrado)
    tempo_tendencias = time.time() - inicio
    print(f"Tendências: {tempo_tendencias:.2f}s")
    
    total = tempo_carregamento + tempo_filtragem + tempo_tendencias
    print(f"Total: {total:.2f}s")
    print()
    
    return total

def testar_modulo_otimizado():
    """Testa performance do módulo otimizado"""
    print("="*50)
    print("TESTANDO MÓDULO OTIMIZADO (GPU + CPU)")
    print("="*50)
    
    try:
        from src.utils.predicao_otimizado import (
            carregar_dados,
            filtrar_amazonia_legal,
            calcular_tendencias_por_municipio,
            GPU_AVAILABLE,
            mostrar_info_hardware
        )
        
        mostrar_info_hardware()
        
        # Carregar dados
        inicio = time.time()
        df = carregar_dados(
            '../data/04_modelagem/dataset_preditivo_com_precos.parquet',
            colunas_essenciais=['cod_ibge', 'ano', 'area_desmatada_ha'],
            usar_gpu=GPU_AVAILABLE
        )
        tempo_carregamento = time.time() - inicio
        print(f"Carregamento: {tempo_carregamento:.2f}s")
        
        # Filtrar
        inicio = time.time()
        df_filtrado = filtrar_amazonia_legal(df)
        tempo_filtragem = time.time() - inicio
        print(f"Filtragem: {tempo_filtragem:.2f}s")
        
        # Calcular tendências
        inicio = time.time()
        tendencias = calcular_tendencias_por_municipio(df_filtrado, usar_vetorizado=True)
        tempo_tendencias = time.time() - inicio
        print(f"Tendências: {tempo_tendencias:.2f}s")
        
        total = tempo_carregamento + tempo_filtragem + tempo_tendencias
        print(f"Total: {total:.2f}s")
        print()
        
        return total
    except ImportError as e:
        print(f"Erro ao importar módulo otimizado: {e}")
        return None

if __name__ == "__main__":
    tempo_original = testar_modulo_original()
    tempo_otimizado = testar_modulo_otimizado()
    
    if tempo_otimizado is not None:
        speedup = tempo_original / tempo_otimizado
        print("="*50)
        print(f"SPEEDUP: {speedup:.2f}x")
        print(f"Tempo economizado: {tempo_original - tempo_otimizado:.2f}s")
        print("="*50)
```

### Executar Teste

```bash
cd notebooks_analise_preditiva
python testar_otimizacoes.py
```

## Passo 5: Monitoramento de Performance

### Durante Execução

Adicione logging de tempo nos notebooks:

```python
import time

def log_tempo(nome_operacao):
    """Decorator para medir tempo de execução"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            inicio = time.time()
            resultado = func(*args, **kwargs)
            tempo = time.time() - inicio
            print(f"⏱️  {nome_operacao}: {tempo:.2f}s")
            return resultado
        return wrapper
    return decorator

# Usar decorator
@log_tempo("Carregamento de dados")
def carregar_e_preparar():
    df = carregar_dados(caminho, colunas)
    df = filtrar_amazonia_legal(df)
    return df
```

### Monitoramento de Recursos

```python
import psutil

def mostrar_uso_recursos():
    """Mostra uso atual de recursos"""
    # CPU
    cpu_percent = psutil.cpu_percent(interval=1)
    print(f"CPU: {cpu_percent}%")
    
    # Memória
    mem = psutil.virtual_memory()
    print(f"RAM: {mem.percent}% ({mem.used/1024**3:.2f}GB / {mem.total/1024**3:.2f}GB)")
    
    # GPU (se disponível)
    try:
        import pynvml
        pynvml.nvmlInit()
        handle = pynvml.nvmlDeviceGetHandleByIndex(0)
        util = pynvml.nvmlDeviceGetUtilizationRates(handle)
        print(f"GPU: {util.gpu}%")
    except:
        pass
```

## Passo 6: Ajuste de Parâmetros

### Para Datasets Grandes

```python
# Aumentar chunk_size para Dask
CHUNK_SIZE = 500000  # Padrão: 100000

# Aumentar n_estimators para melhor accuracy
n_estimators = 200  # Padrão: 100

# Aumentar max_depth para modelos mais complexos
max_depth = 15  # Padrão: 10
```

### Para Memória Limitada

```python
# Reduzir chunk_size
CHUNK_SIZE = 50000

# Reduzir n_estimators
n_estimators = 50

# Usar dtypes menores
df = df.astype({
    'cod_ibge': 'int32',
    'ano': 'int16',
    'area_desmatada_ha': 'float32'
})
```

## Passo 7: Validação de Resultados

### Comparar Resultados

```python
def comparar_resultados(df_original, df_otimizado):
    """Compara se resultados são iguais"""
    # Converter para pandas se necessário
    if hasattr(df_original, 'to_pandas'):
        df_original = df_original.to_pandas()
    if hasattr(df_otimizado, 'to_pandas'):
        df_otimizado = df_otimizado.to_pandas()
    
    # Comparar shapes
    assert df_original.shape == df_otimizado.shape, "Shapes diferentes"
    
    # Comparar valores (com tolerância para float)
    np.testing.assert_allclose(
        df_original.values, 
        df_otimizado.values,
        rtol=1e-5,
        atol=1e-8
    )
    
    print("✓ Resultados idênticos")
```

## Troubleshooting

### Problema: ImportError ao importar cuDF

**Solução**: Verificar instalação do RAPIDS
```bash
conda install -c rapidsai -c nvidia -c conda-forge cudf
```

### Problema: CUDA out of memory

**Solução**: Reduzir chunk_size ou usar CPU-only
```python
df = carregar_dados(caminho, colunas, usar_gpu=False)
```

### Problema: Dask muito lento

**Solução**: Ajustar chunk_size
```python
# Aumentar para menos overhead
CHUNK_SIZE = 500000

# Ou diminuir para mais paralelismo
CHUNK_SIZE = 50000
```

### Problema: Resultados diferentes entre versões

**Solução**: Verificar random_state e dtypes
```python
# Garantir reprodutibilidade
np.random.seed(42)
random_state = 42

# Usar mesmos dtypes
df = df.astype({'coluna': 'float64'})
```

## Próximos Passos

1. **Implementar gradualmente**: Comece com otimizações simples (dtypes, vetorização)
2. **Testar continuamente**: Valide resultados após cada mudança
3. **Monitorar performance**: Use logging para identificar gargalos
4. **Ajustar parâmetros**: Otimize para seu hardware específico
5. **Documentar**: Registe o que funciona para seu caso de uso

## Suporte

Para problemas ou dúvidas:
- Consulte `docs/otimizacoes_performance.md` para análise técnica
- Use `mostrar_info_hardware()` para diagnosticar problemas
- Execute `benchmark_operacoes()` para comparar performance
