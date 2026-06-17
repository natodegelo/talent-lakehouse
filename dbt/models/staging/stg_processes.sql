with source as (
    select * from {{ source('raw', 'processes') }}
)

select
    process_id,
    candidate_id,
    job_id,
    stage,
    stage_date,
    created_at
from source
