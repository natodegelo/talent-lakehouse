import psycopg
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import os
import sys
import json
from datetime import datetime, timezone
import io

# Garante que a raiz do projeto está no path — necessário para imports
# quando o script é executado diretamente (python ingestion/postgres_to_gcs.py)
# ou como módulo dentro do container (python ingestion/postgres_to_gcs.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingestion.checkpoint import load_checkpoint as gcs_load_checkpoint
from ingestion.checkpoint import save_checkpoint as gcs_save_checkpoint
from ingestion.db_connect import get_postgres_conn

load_dotenv(encoding='utf-8')

PROJECT       = os.getenv('GCP_PROJECT')
BUCKET_RAW    = os.getenv('GCS_BUCKET_RAW')

BATCH_SIZE = 50000
TABLES = ['candidates', 'jobs']


def get_conn():
    return get_postgres_conn()


def load_checkpoint() -> dict:
    data = gcs_load_checkpoint('postgres_checkpoint.json')
    if data:
        return data
    return {
        'last_load_type': 'none',
        'candidates': {'updated_at': '2000-01-01T00:00:00'},
        'jobs':       {'updated_at': '2000-01-01T00:00:00'},
    }


def save_checkpoint(checkpoint: dict):
    gcs_save_checkpoint('postgres_checkpoint.json', checkpoint)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    for col in ['created_at', 'updated_at', 'stage_date']:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    if 'salary' in df.columns:
        df['salary'] = pd.to_numeric(df['salary'], errors='coerce')

    return df


def upload_to_gcs(df: pd.DataFrame, table: str, dt: datetime, part: int) -> str:
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(BUCKET_RAW)

    suffix = f"_part_{part:04d}" if part > 0 else ""
    path = (
        f"{table}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        f"/{table}_{dt.strftime('%Y%m%d_%H%M%S')}{suffix}.parquet"
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob = bucket.blob(path)
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    print(f"    → gs://{BUCKET_RAW}/{path}  ({len(df):,} rows)")
    return path


def full_load_table(conn, table: str, dt: datetime) -> str:
    print(f"\n  [FULL LOAD] {table}")

    max_updated = None
    part = 1
    offset = 0

    while True:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {table}
            ORDER BY updated_at
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        cursor.close()

        if not rows:
            break

        df = pd.DataFrame(rows, columns=cols)

        batch_max = df['updated_at'].max()
        if max_updated is None or batch_max > max_updated:
            max_updated = batch_max

        df = normalize_dataframe(df)
        upload_to_gcs(df, table, dt, part)

        offset += len(rows)
        part += 1
        print(f"    {offset:,} registros extraídos...")

        if len(rows) < BATCH_SIZE:
            break

    print(f"  [FULL LOAD] {table} concluído — {offset:,} registros totais")
    return max_updated.isoformat() if max_updated else '2000-01-01T00:00:00'


def incremental_load_table(conn, table: str, checkpoint_ts: str, dt: datetime) -> str | None:
    print(f"\n  [INCREMENTAL] {table} desde {checkpoint_ts}")

    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT * FROM {table}
        WHERE updated_at > %s
        ORDER BY updated_at
        LIMIT {BATCH_SIZE}
    """, (checkpoint_ts,))
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    cursor.close()

    if not rows:
        print(f"  Nenhum registro novo em {table}")
        return None

    df = pd.DataFrame(rows, columns=cols)
    max_updated = df['updated_at'].max()

    df = normalize_dataframe(df)
    upload_to_gcs(df, table, dt, part=0)

    return max_updated.isoformat()


def run():
    print(f"[{datetime.now()}] Iniciando pipeline PostgreSQL → GCS")

    checkpoint = load_checkpoint()
    is_full_load = checkpoint['last_load_type'] != 'full'

    if is_full_load:
        print("\n>>> MODO: FULL LOAD — extraindo todas as tabelas do zero\n")
    else:
        print("\n>>> MODO: INCREMENTAL — extraindo apenas registros novos\n")

    conn = get_conn()
    dt = datetime.now(timezone.utc)

    for table in TABLES:
        if is_full_load:
            max_updated = full_load_table(conn, table, dt)
            checkpoint[table]['updated_at'] = max_updated
        else:
            max_updated = incremental_load_table(
                conn, table, checkpoint[table]['updated_at'], dt
            )
            if max_updated:
                checkpoint[table]['updated_at'] = max_updated

    conn.close()

    if is_full_load:
        checkpoint['last_load_type'] = 'full'

    save_checkpoint(checkpoint)
    print(f"\n[{datetime.now()}] Pipeline concluído.")


if __name__ == '__main__':
    run()