import os
import sys
from datetime import datetime, timezone
from google.cloud import bigquery, storage
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingestion.checkpoint import load_checkpoint as gcs_load_checkpoint
from ingestion.checkpoint import save_checkpoint as gcs_save_checkpoint

load_dotenv(encoding='utf-8')

PROJECT        = os.getenv('GCP_PROJECT')
BUCKET_RAW     = os.getenv('GCS_BUCKET_RAW')
BQ_DATASET_RAW = os.getenv('BQ_DATASET_RAW', 'raw')

SCHEMAS = {
    'candidates': [
        bigquery.SchemaField('candidate_id',    'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('full_name',        'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('email',            'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('phone',            'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('birth_date',       'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('state',            'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('city',             'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('education_level',  'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('jobscore',         'FLOAT',   mode='NULLABLE'),
        bigquery.SchemaField('profile_complete', 'BOOLEAN', mode='NULLABLE'),
        bigquery.SchemaField('created_at',       'STRING',  mode='NULLABLE'),
        bigquery.SchemaField('updated_at',       'STRING',  mode='NULLABLE'),
    ],
    'jobs': [
        bigquery.SchemaField('job_id',     'STRING', mode='NULLABLE'),
        bigquery.SchemaField('title',      'STRING', mode='NULLABLE'),
        bigquery.SchemaField('company',    'STRING', mode='NULLABLE'),
        bigquery.SchemaField('job_type',   'STRING', mode='NULLABLE'),
        bigquery.SchemaField('state',      'STRING', mode='NULLABLE'),
        bigquery.SchemaField('city',       'STRING', mode='NULLABLE'),
        bigquery.SchemaField('salary',     'FLOAT',  mode='NULLABLE'),
        bigquery.SchemaField('status',     'STRING', mode='NULLABLE'),
        bigquery.SchemaField('created_at', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('updated_at', 'STRING', mode='NULLABLE'),
    ],
    'processes': [
        bigquery.SchemaField('process_id',   'STRING', mode='NULLABLE'),
        bigquery.SchemaField('candidate_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('job_id',       'STRING', mode='NULLABLE'),
        bigquery.SchemaField('stage',        'STRING', mode='NULLABLE'),
        bigquery.SchemaField('stage_date',   'STRING', mode='NULLABLE'),
        bigquery.SchemaField('created_at',   'STRING', mode='NULLABLE'),
    ],
}


def load_checkpoint() -> dict:
    data = gcs_load_checkpoint('gcs_to_bq_checkpoint.json')
    if data:
        return data
    return {table: {'last_file': None} for table in SCHEMAS}


def save_checkpoint(checkpoint: dict):
    gcs_save_checkpoint('gcs_to_bq_checkpoint.json', checkpoint)


def list_new_blobs(storage_client, table: str, last_file: str | None) -> list:
    blobs = list(storage_client.list_blobs(BUCKET_RAW, prefix=f"{table}/"))
    blobs = [b for b in blobs if b.name.endswith('.parquet')]
    blobs.sort(key=lambda b: b.name)
    if last_file:
        blobs = [b for b in blobs if b.name > last_file]
    return blobs


def load_blob_to_bq(bq_client, blob, table: str, write_disposition) -> int:
    table_ref = f"{PROJECT}.{BQ_DATASET_RAW}.{table}"
    uri = f"gs://{blob.bucket.name}/{blob.name}"

    mode_label = "TRUNCATE" if write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE else "APPEND"
    print(f"  [{mode_label}] {blob.name} → {table_ref}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=SCHEMAS[table],
        write_disposition=write_disposition,
    )

    job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    job.result()

    print(f"    ✓ {job.output_rows:,} linhas carregadas")
    return job.output_rows


def run():
    print(f"[{datetime.now()}] Iniciando pipeline GCS → BigQuery")

    bq_client      = bigquery.Client(project=PROJECT)
    storage_client = storage.Client(project=PROJECT)
    checkpoint     = load_checkpoint()

    total_loaded = 0

    for table in SCHEMAS:
        print(f"\nProcessando tabela: {table}")

        last_file     = checkpoint[table].get('last_file')
        is_first_load = last_file is None

        if is_first_load:
            print(f"  MODO: FULL LOAD — primeiro arquivo usa TRUNCATE")
        else:
            print(f"  MODO: INCREMENTAL — todos os arquivos usam APPEND")

        blobs = list_new_blobs(storage_client, table, last_file)

        if not blobs:
            print(f"  Nenhum arquivo novo para {table}")
            continue

        print(f"  {len(blobs)} arquivo(s) encontrado(s)")

        for i, blob in enumerate(blobs):
            disposition = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if is_first_load and i == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            )

            rows = load_blob_to_bq(bq_client, blob, table, disposition)
            total_loaded += rows

            checkpoint[table]['last_file'] = blob.name
            save_checkpoint(checkpoint)

    print(f"\n[{datetime.now()}] Pipeline concluído. Total: {total_loaded:,} linhas carregadas.")


if __name__ == '__main__':
    run()