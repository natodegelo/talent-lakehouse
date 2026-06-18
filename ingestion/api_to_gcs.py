import requests
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import os
import sys
import json
import urllib.request
from datetime import datetime, timezone
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingestion.checkpoint import load_checkpoint as gcs_load_checkpoint
from ingestion.checkpoint import save_checkpoint as gcs_save_checkpoint

load_dotenv(encoding='utf-8')

PROJECT    = os.getenv('GCP_PROJECT')
BUCKET_RAW = os.getenv('GCS_BUCKET_RAW')
API_BASE   = os.getenv('API_BASE_URL', 'http://localhost:8000')

API_LIMIT    = 10000
PAGES_PER_FILE = 10


def load_checkpoint() -> dict:
    data = gcs_load_checkpoint('api_checkpoint.json')
    if data:
        return data
    return {
        'last_load_type': 'none',
        'processes': {'last_created_at': None},
    }


def save_checkpoint(checkpoint: dict):
    gcs_save_checkpoint('api_checkpoint.json', checkpoint)


def get_auth_headers() -> dict:
    if not os.getenv('INSTANCE_CONNECTION_NAME'):
        return {}
    try:
        req = urllib.request.Request(
            'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity'
            f'?audience={API_BASE}',
            headers={'Metadata-Flavor': 'Google'}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            token = resp.read().decode()
        return {'Authorization': f'Bearer {token}'}
    except Exception as e:
        print(f"  [WARN] Não foi possível obter identity token: {e}")
        return {}


def upload_to_gcs(records: list, dt: datetime, part: int) -> str:
    df = pd.DataFrame(records)

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
    params = {'limit': API_LIMIT, 'offset': offset}
    if since:
        params['since'] = since

    resp = requests.get(
        f"{API_BASE}/processes",
        params=params,
        headers=get_auth_headers(),
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()['data']


def full_load(checkpoint: dict, dt: datetime) -> str | None:
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

        batch_max = max(r['created_at'] for r in records if r['created_at'])
        if max_created_at is None or batch_max > max_created_at:
            max_created_at = batch_max

        if len(buffer) >= API_LIMIT * PAGES_PER_FILE:
            upload_to_gcs(buffer, dt, part)
            part += 1
            buffer = []
            print(f"    {total:,} registros extraídos...")

        if len(records) < API_LIMIT:
            break

    if buffer:
        upload_to_gcs(buffer, dt, part)
        print(f"    {total:,} registros extraídos...")

    print(f"  [FULL LOAD] processes concluído — {total:,} registros totais")
    return max_created_at


def incremental_load(checkpoint: dict, dt: datetime) -> str | None:
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