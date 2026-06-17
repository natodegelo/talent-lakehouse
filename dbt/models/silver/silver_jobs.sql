with source as (
    select * from {{ ref('stg_jobs') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by job_id
        order by updated_at desc
    ) = 1
),

normalized as (
    select
        job_id,
        title,
        company,
        job_type,

        state,

        case
            when upper(trim(state)) in ('SP', 'SÃO PAULO', 'SAO PAULO', 'S.P.', 'SAOPAULO') then 'SP'
            when upper(trim(state)) in ('RJ', 'RIO DE JANEIRO', 'R.J.', 'RIO') then 'RJ'
            when upper(trim(state)) in ('MG', 'MINAS GERAIS', 'M.G.', 'MINAS') then 'MG'
            when upper(trim(state)) in ('BA', 'BAHIA', 'B.A.', 'BA.') then 'BA'
            when upper(trim(state)) in ('PR', 'PARANÁ', 'PARANA', 'P.R.', 'PR.') then 'PR'
            when upper(trim(state)) in ('RS', 'RIO GRANDE DO SUL', 'R.S.', 'RS.') then 'RS'
            when upper(trim(state)) in ('PE', 'PERNAMBUCO', 'P.E.', 'PERNAMB.') then 'PE'
            when upper(trim(state)) in ('CE', 'CEARÁ', 'CEARA', 'C.E.', 'CE') then 'CE'
            when upper(trim(state)) in ('PA', 'PARÁ', 'PARA', 'P.A.', 'PA.') then 'PA'
            when upper(trim(state)) in ('GO', 'GOIÁS', 'GOIAS', 'G.O.') then 'GO'
            else 'DESCONHECIDO'
        end as state_normalized,

        city,
        salary,
        status,

        safe.parse_timestamp('%Y-%m-%dT%H:%M:%S', created_at) as created_at,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%S', updated_at) as updated_at

    from deduplicated
)

select * from normalized
