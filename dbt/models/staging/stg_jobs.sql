with source as (
    select * from {{ source('raw', 'jobs') }}
)

select
    job_id,
    title,
    company,
    job_type,
    state,
    city,
    salary,
    status,
    created_at,
    updated_at
from source
