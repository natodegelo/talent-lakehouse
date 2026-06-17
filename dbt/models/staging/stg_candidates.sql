with source as (
    select * from {{ source('raw', 'candidates') }}
)

select
    candidate_id,
    full_name,
    email,
    phone,
    birth_date,
    state,
    city,
    education_level,
    jobscore,
    profile_complete,
    created_at,
    updated_at
from source
