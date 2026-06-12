# Como Usar Google Drive no Google Colab

Este documento explica como configurar os notebooks para usar o Google Drive no Google Colab, evitando a necessidade de fazer upload do arquivo de dados em cada sessão.

## Opção 1: Upload Manual (Padrão)

Por padrão, os notebooks estão configurados com `USAR_GOOGLE_DRIVE = False`. Neste modo:

1. Execute a primeira célula do notebook
2. O Colab detecta que está no ambiente Colab
3. Abre a interface de upload
4. Selecione o arquivo `dataset_preditivo_com_precos.parquet` no seu computador
5. O arquivo é movido automaticamente para `/content/data/04_modelagem/`

**Desvantagem:** Precisa fazer upload em cada nova sessão do Colab.

## Opção 2: Usar Google Drive (Recomendado)

Para usar o Google Drive, siga estes passos:

### Passo 1: Fazer upload do arquivo para o Google Drive

1. Abra o Google Drive no seu navegador
2. Faça upload do arquivo `dataset_preditivo_com_precos.parquet` para a raiz do seu Google Drive (pasta "MyDrive")

### Passo 2: Ativar o uso do Google Drive no notebook

No início de cada notebook, altere a variável:

```python
# OPÇÃO: Usar Google Drive no Colab?
# Se True, monta o Google Drive e busca o arquivo de lá
# Se False, faz upload manual do arquivo
USAR_GOOGLE_DRIVE = True  # Altere de False para True
```

### Passo 3: Executar o notebook

1. Execute a primeira célula
2. O Colab pedirá autorização para acessar o Google Drive
3. Clique no link, faça login na sua conta Google
4. Copie o código de autorização e cole no campo indicado
5. O arquivo será copiado automaticamente do Google Drive para o Colab

**Vantagem:** O arquivo fica disponível em todas as sessões do Colab sem precisar fazer upload novamente.

## Fluxo de Trabalho Recomendado

### Primeira vez no Colab:

1. Fazer upload do arquivo para o Google Drive (uma única vez)
2. Ativar `USAR_GOOGLE_DRIVE = True` no notebook
3. Autorizar o acesso ao Google Drive
4. Executar o notebook normalmente

### Sessões subsequentes:

1. Ativar `USAR_GOOGLE_DRIVE = True` no notebook
2. Autorizar o acesso ao Google Drive (se necessário)
3. Executar o notebook normalmente

## Compartilhamento entre Notebooks

Uma vez que o arquivo está no Google Drive:

- Todos os 6 notebooks de análise preditiva podem usar o mesmo arquivo
- Basta ativar `USAR_GOOGLE_DRIVE = True` em cada notebook
- Não precisa fazer upload para cada notebook

## Solução de Problemas

### Erro: "Arquivo não encontrado no Google Drive"

**Causa:** O arquivo não está na raiz do Google Drive (pasta "MyDrive")

**Solução:**
1. Verifique se o arquivo `dataset_preditivo_com_precos.parquet` está na raiz do seu Google Drive
2. Se estiver em uma subpasta, mova para a raiz ou ajuste o parâmetro `caminho_drive` na função

### Erro: "Erro ao montar Google Drive"

**Causa:** Problema de conexão ou autorização

**Solução:**
1. Tente executar a célula novamente
2. Verifique se você tem permissão para acessar o Google Drive
3. Se o problema persistir, use o upload manual (`USAR_GOOGLE_DRIVE = False`)

## Ambiente Local

No ambiente local (não Colab), a configuração do Google Drive é ignorada automaticamente e os caminhos relativos são usados normalmente.
