import psycopg
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import os
import json
from datetime import datetime, timezone
import io

load_dotenv(encoding='utf-8')

PROJECT       = os.getenv('GCP_PROJECT')
BUCKET_RAW    = os.getenv('GCS_BUCKET_RAW')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB   = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')

CHECKPOINT_FILE = 'ingestion/checkpoints/postgres_checkpoint.json'

# Tamanho de cada batch lido do PostgreSQL e salvo como um arquivo Parquet.
# Mantemos 50k mesmo no full load para não explodir memória com 2M registros.
# Cada batch vira um arquivo separado no GCS.
BATCH_SIZE = 50000

TABLES = ['candidates', 'jobs']


def get_conn():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
    )


def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    # Checkpoint ausente = primeira execução = full load obrigatório
    return {
        'last_load_type': 'none',   # 'none' | 'full' | 'incremental'
        'candidates': {'updated_at': '2000-01-01T00:00:00'},
        'jobs':       {'updated_at': '2000-01-01T00:00:00'},
    }


def save_checkpoint(checkpoint: dict):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que o Parquet gerado respeita o contrato de tipos da camada raw:
      - Strings e UUIDs → object (STRING no Parquet)
      - Timestamps      → STRING ISO 8601 (cast para TIMESTAMP acontece na silver)
      - birth_date      → STRING ISO 8601 (cast para DATE acontece na silver)
      - jobscore        → float64 (Pandas não suporta INTEGER nulo)
      - salary          → float64 (idem; Decimal do PostgreSQL vira str no loop object)
      - profile_complete→ bool (sem ambiguidade, BigQuery aceita direto)
    """
    # 1. Converte colunas object (UUIDs, strings, Decimal) para str
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    # 2. Timestamps → STRING ISO 8601
    for col in ['created_at', 'updated_at', 'stage_date']:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # 3. salary: o passo 1 converteu Decimal → str; desfaz para float
    if 'salary' in df.columns:
        df['salary'] = pd.to_numeric(df['salary'], errors='coerce')

    # 4. jobscore: PostgreSQL INTEGER com NULL → Pandas já carrega como float64
    #    Não precisamos fazer nada; documentamos apenas que FLOAT é intencional.
    #    A silver faz: SAFE_CAST(jobscore AS INT64)

    return df


def upload_to_gcs(df: pd.DataFrame, table: str, dt: datetime, part: int) -> str:
    """
    Salva o DataFrame como Parquet no GCS.
    O sufixo _part_NNNN diferencia os múltiplos arquivos do full load.
    No incremental, part=0 e o nome fica idêntico ao comportamento anterior.
    """
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
    """
    Extrai a tabela completa em batches de BATCH_SIZE usando OFFSET paginado.
    Cada batch é normalizado e salvo como um arquivo Parquet separado no GCS.

    Retorna o max(updated_at) encontrado para salvar no checkpoint.

    Por que OFFSET e não cursor keyset (WHERE updated_at > último)?
      No full load não temos uma âncora confiável — updated_at pode ter duplicatas
      e a ordenação por ele não é estável para paginação. OFFSET é simples e correto
      para uma carga única que não vai se repetir.
    """
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

        # Captura max_updated ANTES de normalizar (depois vira string)
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
    """
    Extrai apenas os registros com updated_at > checkpoint_ts.
    Retorna o novo max(updated_at) ou None se não havia registros novos.

    Um único batch por execução é intencional no incremental:
    o pipeline foi pensado para rodar com frequência (Cloud Scheduler).
    Se o volume de novidades em uma janela for > 50k, basta diminuir o intervalo
    do scheduler ou aumentar o BATCH_SIZE.
    """
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

    # Só marca 'full' depois que todas as tabelas foram extraídas com sucesso.
    # Se o processo morrer no meio, na próxima execução refaz o full load.
    if is_full_load:
        checkpoint['last_load_type'] = 'full'

    save_checkpoint(checkpoint)
    print(f"\n[{datetime.now()}] Pipeline concluído.")


if __name__ == '__main__':
    run()