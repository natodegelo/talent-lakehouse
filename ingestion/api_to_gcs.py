import requests
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import os
import json
from datetime import datetime, timezone
import io

load_dotenv(encoding='utf-8')

PROJECT    = os.getenv('GCP_PROJECT')
BUCKET_RAW = os.getenv('GCS_BUCKET_RAW')
API_BASE   = os.getenv('API_BASE_URL', 'http://localhost:8000')

CHECKPOINT_FILE = 'ingestion/checkpoints/api_checkpoint.json'

# Máximo aceito pelo endpoint /processes
API_LIMIT = 10000

# Cada arquivo Parquet no GCS agrupa N páginas da API.
# 10 páginas × 10k = 100k registros por arquivo — equilibrio entre
# número de arquivos no GCS e memória usada por batch.
PAGES_PER_FILE = 10


def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    # last_created_at None = nunca rodou = full load
    return {
        'last_load_type': 'none',
        'processes': {'last_created_at': None},
    }


def save_checkpoint(checkpoint: dict):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)


def upload_to_gcs(records: list, dt: datetime, part: int) -> str:
    """
    Converte lista de dicts para DataFrame, normaliza tipos e salva Parquet no GCS.

    A API já devolve strings ISO para stage_date e created_at — não precisamos
    de conversão de timestamp aqui. UUIDs já vêm como string. Nada de Decimal.
    O Parquet de processes é o mais simples dos três.
    """
    df = pd.DataFrame(records)

    # Garante que todos os campos estão como STRING (a API pode devolver None)
    for col in ['process_id', 'candidate_id', 'job_id', 'stage', 'stage_date', 'created_at']:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    client = storage.Client(project=PROJECT)
    bucket = client.bucket(BUCKET_RAW)

    suffix = f"_part_{part:04d}" if part > 0 else ""
    path = (
        f"processes/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        f"/processes_{dt.strftime('%Y%m%d_%H%M%S')}{suffix}.parquet"
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob = bucket.blob(path)
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    print(f"    → gs://{BUCKET_RAW}/{path}  ({len(df):,} rows)")
    return path


def fetch_page(offset: int, since: str | None) -> list:
    """
    Faz uma chamada ao endpoint /processes e retorna os registros.
    since é passado apenas no modo incremental.
    """
    params = {'limit': API_LIMIT, 'offset': offset}
    if since:
        params['since'] = since

    resp = requests.get(f"{API_BASE}/processes", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()['data']


def full_load(checkpoint: dict, dt: datetime) -> str | None:
    """
    Pagina a API do offset 0 até esgotar os registros.
    Agrupa PAGES_PER_FILE páginas em um único arquivo Parquet.
    Retorna o max(created_at) encontrado para o checkpoint.
    """
    print("\n  [FULL LOAD] processes")

    max_created_at = None
    part = 1
    offset = 0
    total = 0
    buffer = []

    while True:
        records = fetch_page(offset=offset, since=None)

        if not records:
            break

        buffer.extend(records)
        offset += len(records)
        total += len(records)

        # Atualiza max_created_at com o maior valor do batch
        batch_max = max(r['created_at'] for r in records if r['created_at'])
        if max_created_at is None or batch_max > max_created_at:
            max_created_at = batch_max

        # A cada PAGES_PER_FILE páginas, salva um arquivo e limpa o buffer
        if len(buffer) >= API_LIMIT * PAGES_PER_FILE:
            upload_to_gcs(buffer, dt, part)
            part += 1
            buffer = []
            print(f"    {total:,} registros extraídos...")

        if len(records) < API_LIMIT:
            # Última página — menos registros que o limit = fim dos dados
            break

    # Salva o restante que ficou no buffer
    if buffer:
        upload_to_gcs(buffer, dt, part)
        print(f"    {total:,} registros extraídos...")

    print(f"  [FULL LOAD] processes concluído — {total:,} registros totais")
    return max_created_at


def incremental_load(checkpoint: dict, dt: datetime) -> str | None:
    """
    Usa o parâmetro since para buscar apenas registros novos.
    Pagina até esgotar — no incremental o volume é pequeno (novos desde última execução).
    Salva tudo em um único arquivo Parquet.
    """
    since = checkpoint['processes']['last_created_at']
    print(f"\n  [INCREMENTAL] processes desde {since}")

    records = []
    offset = 0
    max_created_at = None

    while True:
        page = fetch_page(offset=offset, since=since)

        if not page:
            break

        records.extend(page)
        offset += len(page)

        batch_max = max(r['created_at'] for r in page if r['created_at'])
        if max_created_at is None or batch_max > max_created_at:
            max_created_at = batch_max

        if len(page) < API_LIMIT:
            break

    if not records:
        print(f"  Nenhum registro novo em processes")
        return None

    upload_to_gcs(records, dt, part=0)
    print(f"  [INCREMENTAL] {len(records):,} registros novos")
    return max_created_at


def run():
    print(f"[{datetime.now()}] Iniciando pipeline API → GCS")

    checkpoint = load_checkpoint()
    is_full = checkpoint['last_load_type'] != 'full'
    dt = datetime.now(timezone.utc)

    if is_full:
        max_created_at = full_load(checkpoint, dt)
    else:
        max_created_at = incremental_load(checkpoint, dt)

    if max_created_at:
        checkpoint['processes']['last_created_at'] = max_created_at
        if is_full:
            checkpoint['last_load_type'] = 'full'
        save_checkpoint(checkpoint)

    print(f"\n[{datetime.now()}] Pipeline concluído.")


if __name__ == '__main__':
    run()