select * from {{ ref('int_job_cost_validated') }} where dq_reason is not null
