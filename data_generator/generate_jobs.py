import uuid
import random
from faker import Faker
from datetime import datetime

fake = Faker('pt_BR')

STATE_DIRTY = {
    'SP': ['SP', 'São Paulo', 'são paulo', 'S.P.', 'Sao Paulo'],
    'RJ': ['RJ', 'Rio de Janeiro', 'rio de janeiro', 'R.J.', 'Rio'],
    'MG': ['MG', 'Minas Gerais', 'minas gerais', 'M.G.', 'Minas'],
    'BA': ['BA', 'Bahia', 'bahia', 'B.A.', 'BA.'],
    'PR': ['PR', 'Paraná', 'parana', 'P.R.', 'Pr'],
    'RS': ['RS', 'Rio Grande do Sul', 'rio grande do sul', 'R.S.', 'RS.'],
    'PE': ['PE', 'Pernambuco', 'pernambuco', 'P.E.', 'Pernamb.'],
    'CE': ['CE', 'Ceará', 'ceara', 'C.E.', 'Ce'],
    'PA': ['PA', 'Pará', 'para', 'P.A.', 'Pa'],
    'GO': ['GO', 'Goiás', 'goias', 'G.O.', 'Go'],
}

COMPANIES = ['Demà', 'Magazine Luiza', 'Ambev', 'Natura', 'Bradesco', 
             'Itaú', 'Renner', 'Carrefour', 'GPA', 'Localiza']

JOB_TITLES = [
    'Aprendiz Administrativo', 'Aprendiz Comercial', 
    'Aprendiz de Logística', 'Aprendiz de TI',
    'Aprendiz Financeiro', 'Aprendiz de RH',
    'Estágio em Marketing', 'Estágio em Dados',
]

JOB_TYPES = ['aprendiz', 'estagio']
STATUS = ['open', 'closed', 'filled']

def generate_job():
    state_key = random.choice(list(STATE_DIRTY.keys()))
    dirty_state = random.choice(STATE_DIRTY[state_key])
    
    salary = round(random.uniform(700, 1500), 2)
    if random.random() < 0.05:
        salary = None

    return {
        'job_id': str(uuid.uuid4()),
        'title': random.choice(JOB_TITLES),
        'company': random.choice(COMPANIES),
        'job_type': random.choice(JOB_TYPES),
        'state': dirty_state,
        'city': fake.city(),
        'salary': salary,
        'status': random.choice(STATUS),
        'created_at': fake.date_time_between(start_date='-2y', end_date='now'),
        'updated_at': datetime.now(),
    }

if __name__ == '__main__':
    print("Gerando amostra de 5 vagas para validação...")
    for i in range(5):
        j = generate_job()
        print(j)
    print("\nOK — script funcionando.")