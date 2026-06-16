import psycopg
from dotenv import load_dotenv
import os

load_dotenv(encoding='utf-8')

conn = psycopg.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS candidates (
        candidate_id UUID PRIMARY KEY,
        full_name VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        birth_date DATE,
        state VARCHAR(100),
        city VARCHAR(100),
        education_level VARCHAR(50),
        jobscore INTEGER,
        profile_complete BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
""")

conn.commit()
cursor.close()
conn.close()

print("Tabela candidates criada com sucesso.")