select entry_id, amount from {{ ref('fct_job_cost') }} where amount < 0
