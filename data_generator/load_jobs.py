import psycopg
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from generate_jobs import generate_job

load_dotenv(encoding='utf-8')

BATCH_SIZE = 10000
TOTAL = 500_000

conn = psycopg.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_id UUID PRIMARY KEY,
        title VARCHAR(255),
        company VARCHAR(255),
        job_type VARCHAR(50),
        state VARCHAR(100),
        city VARCHAR(100),
        salary DECIMAL(10,2),
        status VARCHAR(50),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
""")
conn.commit()

print(f"Gerando {TOTAL:,} vagas em batches de {BATCH_SIZE:,}...")

inserted = 0
batch = []

for i in range(TOTAL):
    j = generate_job()
    batch.append((
        j['job_id'], j['title'], j['company'], j['job_type'],
        j['state'], j['city'], j['salary'], j['status'],
        j['created_at'], j['updated_at']
    ))

    if len(batch) == BATCH_SIZE:
        cursor.executemany("""
            INSERT INTO jobs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, batch)
        conn.commit()
        inserted += len(batch)
        batch = []
        print(f"  {inserted:,} / {TOTAL:,} inseridos...")

if batch:
    cursor.executemany("""
        INSERT INTO jobs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, batch)
    conn.commit()
    inserted += len(batch)

cursor.close()
conn.close()
print(f"\nConcluído! {inserted:,} vagas inseridas.")