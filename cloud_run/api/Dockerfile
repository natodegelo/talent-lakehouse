FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    psycopg[binary]==3.1.19 \
    fastapi==0.111.0 \
    uvicorn==0.30.1 \
    python-dotenv==1.0.1

COPY api/ ./api/

# Cloud Run injeta a variável PORT automaticamente
# O uvicorn deve escutar nessa porta
ENV PORT=8080
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT}"]
