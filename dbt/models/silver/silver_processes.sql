with source as (
    select * from {{ ref('stg_processes') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by process_id
        order by created_at desc
    ) = 1
)

select
    process_id,
    candidate_id,
    job_id,
    stage,
    safe.parse_timestamp('%Y-%m-%dT%H:%M:%S', stage_date) as stage_date,
    safe.parse_timestamp('%Y-%m-%dT%H:%M:%S', created_at) as created_at
from deduplicated
