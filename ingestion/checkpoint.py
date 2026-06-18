import json
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv(encoding='utf-8')

PROJECT    = os.getenv('GCP_PROJECT')
BUCKET_RAW = os.getenv('GCS_BUCKET_RAW')


def load_checkpoint(filename: str) -> dict | None:
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(BUCKET_RAW)
    blob = bucket.blob(f"checkpoints/{filename}")
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def save_checkpoint(filename: str, checkpoint: dict):
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(BUCKET_RAW)
    blob = bucket.blob(f"checkpoints/{filename}")
    blob.upload_from_string(
        json.dumps(checkpoint, indent=2),
        content_type='application/json'
    )
