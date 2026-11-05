-- Completeness: every distinct entry in Bronze ends up in exactly one place (fact or quarantine).
with bronze as (select count(distinct entry_id) as n from {{ source('bronze', 'job_cost') }}),
landed as (
    select (select count(*) from {{ ref('fct_job_cost') }})
         + (select count(*) from {{ ref('dq_job_cost_quarantine') }}) as n
)
select bronze.n as bronze_entries, landed.n as landed_entries
from bronze, landed
where bronze.n <> landed.n
