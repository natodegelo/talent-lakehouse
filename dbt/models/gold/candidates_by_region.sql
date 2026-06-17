with silver as (
    select * from {{ ref('silver_candidates') }}
)

select
    state_normalized                                    as state,
    count(*)                                            as total_candidates,
    countif(profile_complete = true)                    as complete_profiles,
    round(countif(profile_complete = true) / count(*) * 100, 2) as profile_completion_pct,
    round(avg(jobscore), 2)                             as avg_jobscore,
    countif(email_valid = true)                         as valid_emails,
    round(countif(email_valid = true) / count(*) * 100, 2) as email_valid_pct
from silver
group by 1
order by 2 desc
