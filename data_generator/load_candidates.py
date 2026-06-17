import psycopg
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from generate_candidates import generate_candidate

load_dotenv(encoding='utf-8')

BATCH_SIZE = 10000
TOTAL = 2_000_000

conn = psycopg.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

cursor = conn.cursor()

print(f"Gerando {TOTAL:,} candidatos em batches de {BATCH_SIZE:,}...")

inserted = 0
batch = []

for i in range(TOTAL):
    c = generate_candidate()
    batch.append((
        c['candidate_id'], c['full_name'], c['email'], c['phone'],
        c['birth_date'], c['state'], c['city'], c['education_level'],
        c['jobscore'], c['profile_complete'], c['created_at'], c['updated_at']
    ))

    if len(batch) == BATCH_SIZE:
        cursor.executemany("""
            INSERT INTO candidates VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, batch)
        conn.commit()
        inserted += len(batch)
        batch = []
        print(f"  {inserted:,} / {TOTAL:,} inseridos...")

if batch:
    cursor.executemany("""
        INSERT INTO candidates VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, batch)
    conn.commit()
    inserted += len(batch)

cursor.close()
conn.close()
print(f"\nConcluído! {inserted:,} candidatos inseridos.")