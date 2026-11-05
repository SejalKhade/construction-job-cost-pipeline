-- Applies business rules to every cost row and tags failures with a reason.
-- Valid rows flow to fct_job_cost; failed rows go to dq_job_cost_quarantine. Nothing is silently dropped.
with c as (select * from {{ ref('stg_job_cost') }})
select
    c.*,
    case
        when c.cost_code is null                 then 'missing_cost_code'
        when c.amount is null                    then 'unparseable_amount'
        when c.amount < 0                        then 'negative_amount'
        when p.project_id is null                then 'unknown_project'
        when b.cost_code is null                 then 'cost_code_not_in_budget'
        else null
    end as dq_reason
from c
left join {{ ref('stg_projects') }} p on c.project_id = p.project_id
left join {{ ref('stg_budgets') }}  b on c.project_id = b.project_id and c.cost_code = b.cost_code
