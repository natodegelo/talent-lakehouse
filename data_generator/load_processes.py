import psycopg
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from generate_processes import generate_process

load_dotenv(encoding='utf-8')

BATCH_SIZE = 10000
TOTAL_PROCESSES = 3_000_000

conn = psycopg.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS processes (
        process_id UUID PRIMARY KEY,
        candidate_id UUID,
        job_id UUID,
        stage VARCHAR(50),
        stage_date TIMESTAMP,
        created_at TIMESTAMP
    );
""")
conn.commit()

print("Carregando IDs de candidatos e vagas...")
cursor.execute("SELECT candidate_id FROM candidates LIMIT 100000")
candidate_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT job_id FROM jobs LIMIT 50000")
job_ids = [row[0] for row in cursor.fetchall()]

print(f"Gerando ~{TOTAL_PROCESSES:,} registros de processos...")

inserted = 0
batch = []
target = TOTAL_PROCESSES

while inserted < target:
    records = generate_process(candidate_ids, job_ids)
    for r in records:
        batch.append((
            r['process_id'], r['candidate_id'], r['job_id'],
            r['stage'], r['stage_date'], r['created_at']
        ))

    if len(batch) >= BATCH_SIZE:
        cursor.executemany("""
            INSERT INTO processes VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, batch)
        conn.commit()
        inserted += len(batch)
        batch = []
        print(f"  {inserted:,} / {target:,} inseridos...")

if batch:
    cursor.executemany("""
        INSERT INTO processes VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, batch)
    conn.commit()
    inserted += len(batch)

cursor.close()
conn.close()
print(f"\nConcluído! {inserted:,} registros de processos inseridos.")