with silver as (
    select * from {{ ref('silver_processes') }}
),

-- Cada combinação candidate_id + job_id é um processo seletivo único
-- Um processo pode ter múltiplas linhas (uma por etapa)
-- Queremos contar quantos processos passaram por cada etapa

stage_counts as (
    select
        stage,
        count(distinct concat(candidate_id, '-', job_id)) as total_processes
    from silver
    group by 1
),

total_applied as (
    select count(distinct concat(candidate_id, '-', job_id)) as total
    from silver
    where stage = 'applied'
)

select
    s.stage,
    s.total_processes,
    round(s.total_processes / t.total * 100, 2) as pct_of_applied
from stage_counts s
cross join total_applied t
order by
    case s.stage
        when 'applied'   then 1
        when 'screening' then 2
        when 'interview' then 3
        when 'approved'  then 4
        when 'rejected'  then 5
    end
