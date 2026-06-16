import uuid
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('pt_BR')

STAGES = ['applied', 'screening', 'interview', 'approved', 'rejected']

def generate_process(candidate_ids, job_ids):
    candidate_id = random.choice(candidate_ids)
    job_id = random.choice(job_ids)
    
    # Simula jornada por etapas — nem todos chegam ao final
    num_stages = random.choices(
        [1, 2, 3, 4, 5],
        weights=[40, 25, 20, 10, 5]
    )[0]
    
    stages = STAGES[:num_stages]
    # Ultimo stage pode ser approved ou rejected
    if num_stages > 1 and stages[-1] not in ['approved', 'rejected']:
        stages[-1] = random.choice(['approved', 'rejected'])
    
    records = []
    stage_date = fake.date_time_between(start_date='-1y', end_date='now')
    
    for stage in stages:
        records.append({
            'process_id': str(uuid.uuid4()),
            'candidate_id': candidate_id,
            'job_id': job_id,
            'stage': stage,
            'stage_date': stage_date,
            'created_at': stage_date,
        })
        stage_date += timedelta(days=random.randint(1, 14))
    
    return records

if __name__ == '__main__':
    fake_candidates = [str(uuid.uuid4()) for _ in range(10)]
    fake_jobs = [str(uuid.uuid4()) for _ in range(5)]
    
    print("Gerando amostra de processos para validação...")
    procs = generate_process(fake_candidates, fake_jobs)
    for p in procs:
        print(p)
    print("\nOK — script funcionando.")