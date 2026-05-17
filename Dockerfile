# Dockerfile otimizado para análise de dados
# Single-stage build para reduzir tempo de build

FROM python:3.11-slim

# Instalar dependências de sistema necessárias para geopandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements.txt primeiro para cache do Docker
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar apenas código necessário
COPY src/ ./src/

# Criar diretórios necessários
RUN mkdir -p data/01_bronze data/02_silver data/03_gold data/04_reports logs

# Definir volumes para dados persistentes
VOLUME ["/app/data", "/app/logs"]

# Comando padrão
CMD ["python", "src/ingestao/download_dados_parquet.py"]
