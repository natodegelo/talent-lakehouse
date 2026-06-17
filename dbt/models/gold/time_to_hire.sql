with silver as (
    select * from {{ ref('silver_processes') }}
),

applied as (
    select candidate_id, job_id, min(stage_date) as applied_at
    from silver
    where stage = 'applied'
    group by 1, 2
),

approved as (
    select candidate_id, job_id, min(stage_date) as approved_at
    from silver
    where stage = 'approved'
    group by 1, 2
),

joined as (
    select
        appl.candidate_id,
        appl.job_id,
        appl.applied_at,
        appv.approved_at,
        date_diff(date(appv.approved_at), date(appl.applied_at), day) as days_to_hire
    from applied appl
    inner join approved appv
        on appl.candidate_id = appv.candidate_id
        and appl.job_id = appv.job_id
    where appv.approved_at >= appl.applied_at
)

select
    round(avg(days_to_hire), 1) as avg_days_to_hire,
    min(days_to_hire)           as min_days_to_hire,
    max(days_to_hire)           as max_days_to_hire,
    count(*)                    as total_hired
from joined
