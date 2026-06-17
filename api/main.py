from fastapi import FastAPI, Query
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv(encoding='utf-8')

app = FastAPI(title="Talent Lakehouse API", version="1.0.0")

def get_conn():
    from ingestion.db_connect import get_postgres_conn
    return get_postgres_conn()

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/processes")
def get_processes(
    limit: int = Query(default=1000, le=10000),
    offset: int = Query(default=0),
    since: str = Query(default=None)
):
    conn = get_conn()
    cursor = conn.cursor()

    if since:
        cursor.execute("""
            SELECT process_id, candidate_id, job_id, stage, stage_date, created_at
            FROM processes
            WHERE created_at > %s
            ORDER BY created_at
            LIMIT %s OFFSET %s
        """, (since, limit, offset))
    else:
        cursor.execute("""
            SELECT process_id, candidate_id, job_id, stage, stage_date, created_at
            FROM processes
            ORDER BY created_at
            LIMIT %s OFFSET %s
        """, (limit, offset))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return {
        "total": len(rows),
        "offset": offset,
        "data": [
            {
                "process_id": str(r[0]),
                "candidate_id": str(r[1]),
                "job_id": str(r[2]),
                "stage": r[3],
                "stage_date": r[4].isoformat() if r[4] else None,
                "created_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ]
    }