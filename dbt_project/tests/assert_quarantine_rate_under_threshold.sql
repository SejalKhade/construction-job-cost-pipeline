-- Fails if the share of rejected cost rows goes above the agreed threshold (a sign of a broken source).
select count(*) filter (where dq_reason is not null) * 100.0 / count(*) as quarantine_rate_pct
from {{ ref('int_job_cost_validated') }}
having count(*) filter (where dq_reason is not null) * 100.0 / count(*) > {{ var('max_quarantine_rate_pct') }}
