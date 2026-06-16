import uuid
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('pt_BR')

# Formatos sujos de estado intencionais
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

EDUCATION_LEVELS = ['fundamental', 'médio', 'superior', 'técnico']

def dirty_phone():
    """Gera telefone em formato sujo."""
    ddd = random.choice(['11', '21', '31', '41', '51', '61', '71', '81', '91'])
    number = '9' + ''.join([str(random.randint(0, 9)) for _ in range(7)])
    fmt = random.choice([
        f'({ddd}) {number[:5]}-{number[5:]}',
        f'{ddd}{number}',
        f'+55{ddd}{number}',
        f'{ddd} {number}',
        f'({ddd}){number}',
    ])
    return fmt

def dirty_email(name):
    """Gera email, alguns inválidos."""
    clean = fake.email()
    if random.random() < 0.03:  # 3% inválidos
        return random.choice([
            name.lower().replace(' ', '.'),
            clean.replace('@', ''),
            f'{name.lower()}@',
        ])
    return clean

def generate_candidate():
    state_key = random.choice(list(STATE_DIRTY.keys()))
    dirty_state = random.choice(STATE_DIRTY[state_key])
    name = fake.name()

    # 2% duplicatas intencionais
    candidate_id = str(uuid.uuid4())

    jobscore = random.randint(0, 100)
    if random.random() < 0.05:
        jobscore = None  # 5% nulos

    return {
        'candidate_id': candidate_id,
        'full_name': name,
        'email': dirty_email(name),
        'phone': dirty_phone(),
        'birth_date': fake.date_of_birth(minimum_age=14, maximum_age=24),
        'state': dirty_state,
        'city': fake.city(),
        'education_level': random.choice(EDUCATION_LEVELS),
        'jobscore': jobscore,
        'profile_complete': random.choice([True, False]),
        'created_at': fake.date_time_between(start_date='-2y', end_date='now'),
        'updated_at': datetime.now(),
    }

if __name__ == '__main__':
    print("Gerando amostra de 5 candidatos para validação...")
    for i in range(5):
        c = generate_candidate()
        print(c)
    print("\nOK — script funcionando.")