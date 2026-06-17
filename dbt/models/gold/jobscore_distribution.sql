with silver as (
    select * from {{ ref('silver_candidates') }}
),

scored as (
    select
        state_normalized as state,
        case
            when jobscore between 0  and 20  then '0-20'
            when jobscore between 21 and 40  then '21-40'
            when jobscore between 41 and 60  then '41-60'
            when jobscore between 61 and 80  then '61-80'
            when jobscore between 81 and 100 then '81-100'
            else 'sem score'
        end as score_range,
        count(*) as total
    from silver
    group by 1, 2
)

select * from scored
order by state, score_range
