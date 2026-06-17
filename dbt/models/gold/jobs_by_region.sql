with silver as (
    select * from {{ ref('silver_jobs') }}
)

select
    state_normalized                                as state,
    count(*)                                        as total_jobs,
    countif(status = 'open')                        as open_jobs,
    countif(status = 'filled')                      as filled_jobs,
    countif(status = 'closed')                      as closed_jobs,
    countif(job_type = 'aprendiz')                  as aprendiz_jobs,
    countif(job_type = 'estagio')                   as estagio_jobs,
    round(avg(salary), 2)                           as avg_salary
from silver
group by 1
order by 2 desc
