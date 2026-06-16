import psycopg
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import os
import json
from datetime import datetime, timezone
import io

load_dotenv(encoding='utf-8')

# Config
PROJECT = os.getenv('GCP_PROJECT')
BUCKET_RAW = os.getenv('GCS_BUCKET_RAW')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')

CHECKPOINT_FILE = 'ingestion/checkpoints/postgres_checkpoint.json'
BATCH_SIZE = 50000

def get_conn():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
    )

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {
        'candidates': '2000-01-01T00:00:00',
        'jobs': '2000-01-01T00:00:00',
    }

def save_checkpoint(checkpoint):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def upload_to_gcs(df, bucket_name, table, dt):
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(bucket_name)

    path = f"{table}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{table}_{dt.strftime('%Y%m%d_%H%M%S')}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob = bucket.blob(path)
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    print(f"  Uploaded {len(df)} rows → gs://{bucket_name}/{path}")
    return path

def extract_table(conn, table, checkpoint_ts):
    print(f"\nExtraindo {table} desde {checkpoint_ts}...")
    
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
    
    df = pd.DataFrame(rows, columns=cols)
    
    # Converte UUID para string
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
    
    return df

def run():
    print(f"[{datetime.now()}] Iniciando pipeline PostgreSQL → GCS")
    
    checkpoint = load_checkpoint()
    conn = get_conn()
    dt = datetime.now(timezone.utc)
    
    tables = ['candidates', 'jobs']
    
    for table in tables:
        df = extract_table(conn, table, checkpoint[table])
        
        if df.empty:
            print(f"  Nenhum registro novo em {table}")
            continue
        
        # Atualiza checkpoint com o maior updated_at do batch
        max_updated = df['updated_at'].max()
        upload_to_gcs(df, BUCKET_RAW, table, dt)
        checkpoint[table] = max_updated.isoformat()
    
    conn.close()
    save_checkpoint(checkpoint)
    print(f"\n[{datetime.now()}] Pipeline concluído.")

if __name__ == '__main__':
    run()