# Notebooks de Análise por Eixo com BDD

## Estrutura

Este diretório contém notebooks Jupyter organizados por eixo de análise, cada um seguindo o padrão **BDD (Behavior Driven Development)** em português para documentar as análises e as matemáticas implícitas em linguagem de negócio.

## Eixos de Análise

### Eixos Originais (Dados Disponíveis)

1. **eixo1_desmatamento_fogo.ipynb**
   - Fontes: PRODES (INPE), DETER (INPE), MapBiomas Fogo, Limites de UCs
   - Análises: Tendência temporal do desmatamento, correlação DETER-PRODES

2. **eixo2_uso_transicao_solo.ipynb**
   - Fontes: MapBiomas (Coleção 10+), TerraClass (INPE)
   - Análises: Matriz de transição de uso do solo, destinação pós-desmatamento

3. **eixo3_economia_agropecuaria.ipynb**
   - Fontes: PAM (IBGE), PPM (IBGE), PIB Municipal (IBGE), Comex Stat (MDIC)
   - Análises: Índice de Custo Ambiental (ICA), eficiência pecuária vs agricultura

4. **eixo4_impacto_socioambiental.ipynb**
   - Fontes: Embargos (IBAMA), Atlas Brasil (IPEA/PNUD)
   - Análises: Impacto dos embargos na produção, correlação desmatamento-IDHM

### Eixos Expandidos (Dados Adicionais Necessários)

5. **eixo5_dinamica_demografica.ipynb**
   - Fontes (potenciais): Censo Demográfico (IBGE), RAIS/CAGED (MTE)
   - Status: Requer integração de dados adicionais
   - Análises planejadas: Crescimento populacional vs desmatamento, rotatividade de emprego

## Padrão BDD em Cada Análise

Cada análise dentro dos notebooks segue este padrão:

### 1. Especificação BDD (Gherkin em Português)

```
**Feature**: [Descrição da funcionalidade]

**Scenario**: [Descrição do cenário específico]

- **GIVEN** que tenho [condição inicial]
- **AND** que tenho [condição adicional]
- **WHEN** eu [ação realizada]
- **AND** eu [ação adicional]
- **THEN** devo [resultado esperado]
- **AND** devo [resultado adicional]
```

### 2. Matemática em Linguagem de Negócio

Cada análise inclui uma seção explicando as fórmulas matemáticas em linguagem acessível:

- **Conceitos**: Explicação de termos técnicos (ex: delta, correlação, regressão)
- **Fórmulas**: Apresentação da fórmula matemática
- **Interpretação**: O que o resultado significa em termos de negócio
- **Limitações**: Restrições e viéses metodológicos

### 3. Código Python

Células de código executável que implementam a análise descrita no BDD.

### 4. Visualizações

Gráficos e tabelas para apresentar os resultados.

### 5. Conclusão

Interpretação dos resultados em linguagem de negócio e implicações para tomada de decisão.

## Como Usar os Notebooks

### Executar um Notebook

```bash
# Usando Docker
docker-compose run app jupyter notebook

# Navegar para: notebooks_analise_por_eixo/
# Abrir o notebook desejado
```

### Executar Localmente

```bash
# Ativar ambiente virtual
source venv/bin/activate  # ou conda activate analise-dados

# Iniciar Jupyter
jupyter notebook notebooks_analise_por_eixo/
```

### Estrutura de Células

1. **Markdown (BDD)**: Documentação da análise em linguagem de negócio
2. **Code (Importação)**: Importar bibliotecas necessárias
3. **Code (Carregamento)**: Carregar dados das camadas Silver/Gold
4. **Code (Cálculo)**: Implementar a análise matemática
5. **Code (Visualização)**: Criar gráficos
6. **Markdown (Conclusão)**: Interpretar resultados

## Benefícios do Padrão BDD

### Para Analistas de Dados
- **Clareza**: Especificação clara do que a análise deve fazer
- **Rastreabilidade**: Cada passo é documentado e justificado
- **Reprodutibilidade**: Processo bem definido pode ser replicado

### Para Engenheiros de Dados
- **Validação**: Especificações podem ser usadas para testes automatizados
- **Documentação**: Código auto-documentado com BDD
- **Manutenção**: Facilita identificação de problemas e correções

### Para Estatísticos
- **Rigor**: Matemática explícita em linguagem de negócio
- **Validação**: Hipóteses claramente definidas antes da análise
- **Transparência**: Limitações e pressupostos documentados

### Para Gestores de Negócio
- **Compreensão**: Linguagem acessível sem jargão técnico excessivo
- **Decisão**: Resultados interpretados em termos de impacto
- **Confiança**: Metodologia transparente e justificada

## Exemplo de Uso

### Antes da Análise (BDD)

```markdown
**Feature**: Calcular o Índice de Custo Ambiental (ICA)

**Scenario**: Identificar municípios com pior relação custo-benefício ecológico

- **GIVEN** que tenho dados de desmatamento (PRODES) por município em hectares
- **AND** que tenho dados de Valor Adicionado Bruto Agropecuário (PIB) por município em R$
- **WHEN** eu calculo o delta de desmatamento entre 2010 e 2020
- **AND** calculo o delta de VAB Agropecuário no mesmo período
- **AND** aplico a fórmula: ICA = ΔDesmatamento (ha) / ΔVAB_Agro (R$)
- **THEN** devo obter um ranking de municípios por ICA
- **AND** municípios com ICA alto são ineficientes (muito desmatamento, pouco ganho econômico)
```

### Matemática em Linguagem de Negócio

```markdown
**Fórmula do ICA:**
- ICA = ΔDesmatamento (ha) / ΔVAB_Agro (R$)
- Mede hectares desmatados por cada R$ 1 de VAB gerado
- ICA alto = muito desmatamento para pouco ganho econômico (ineficiente)
- ICA baixo = pouco desmatamento para muito ganho econômico (eficiente)
```

## Próximos Passos

1. **Completar Eixos Expandidos**: Integrar dados para Eixos 5-10
2. **Automatizar Testes**: Converter especificações BDD em testes automatizados
3. **Dashboard**: Criar dashboard interativo com resultados dos notebooks
4. **Documentação**: Expandir documentação de cada eixo com mais detalhes

## Referências

- [Behavior Driven Development (BDD)](https://en.wikipedia.org/wiki/Behavior-driven_development)
- [Gherkin Syntax](https://cucumber.io/docs/gherkin/reference/)
- [Docs do Projeto](../docs/estrutura_analise_expandida.md)
