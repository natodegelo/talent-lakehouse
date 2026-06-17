# Imagem base slim — menor footprint, sem pacotes desnecessários
FROM python:3.12-slim

# Evita prompts interativos durante instalação de pacotes do sistema
ENV DEBIAN_FRONTEND=noninteractive

# Diretório de trabalho dentro do container
WORKDIR /app

# Instala dependências do sistema necessárias para psycopg3
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências Python primeiro — aproveita cache do Docker
# Se o requirements.txt não mudar, essa layer não é reconstruída
COPY requirements.txt .
RUN pip install --no-cache-dir \
    google-cloud-bigquery==3.25.0 \
    google-cloud-storage==2.18.0 \
    psycopg[binary]==3.1.19 \
    pandas==2.2.2 \
    pyarrow==16.1.0 \
    python-dotenv==1.0.1 \
    requests==2.32.0

# Copia os scripts de ingestão
COPY ingestion/ ./ingestion/

# No Cloud Run Jobs, o comando é passado como argumento via --args
# Exemplos:
#   --args="python,ingestion/postgres_to_gcs.py"
#   --args="python,ingestion/api_to_gcs.py"
#   --args="python,ingestion/gcs_to_bq.py"
ENTRYPOINT ["python"]
