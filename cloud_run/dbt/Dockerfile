FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

RUN pip install --no-cache-dir \
    dbt-bigquery==1.8.0 \
    dbt-core==1.8.0

# Copia o projeto dbt
COPY dbt/ ./dbt/

# Copia o profiles.yml para dentro da imagem
# Em produção, o profiles.yml vem de um Secret Manager — aqui simulamos com COPY
COPY cloud_run/dbt/profiles.yml /root/.dbt/profiles.yml

# Exemplos de uso via --args no Cloud Run Job:
#   --args="dbt,run,--project-dir,/app/dbt"
#   --args="dbt,test,--project-dir,/app/dbt"
#   --args="dbt,docs,generate,--project-dir,/app/dbt"
ENTRYPOINT ["dbt"]
