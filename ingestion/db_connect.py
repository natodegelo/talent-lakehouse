import os
import psycopg
from dotenv import load_dotenv

load_dotenv(encoding='utf-8')

INSTANCE_CONNECTION_NAME = os.getenv('INSTANCE_CONNECTION_NAME')


def get_postgres_conn():
    """
    Retorna uma conexão psycopg3 com o PostgreSQL.

    Detecta o ambiente automaticamente:
      - Local (dev): conecta via TCP usando POSTGRES_HOST/PORT
      - Cloud Run:   conecta via Unix socket do Cloud SQL connector
                     O socket é montado automaticamente pelo Cloud Run
                     quando CLOUD_SQL_CONNECTION_NAME está configurado no job/service

    Por que Unix socket no Cloud Run:
      - Mais seguro — sem exposição de IP público
      - Sem necessidade de Cloud SQL Proxy no container
      - O Cloud Run injeta o socket automaticamente via configuração do job
    """
    if INSTANCE_CONNECTION_NAME:
        # Cloud Run — usa Unix socket
        # O socket fica em /cloudsql/<instance_connection_name>
        host = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"
        conn = psycopg.connect(
            host=host,
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
        )
    else:
        # Local — usa TCP
        conn = psycopg.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
        )

    return conn
