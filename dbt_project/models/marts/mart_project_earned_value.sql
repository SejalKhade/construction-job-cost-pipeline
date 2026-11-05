-- Earned value per project as of the latest status date:
--   PV = budget x planned % complete, EV = budget x actual % complete, AC = actual cost to date
--   CPI = EV / AC (cost efficiency), SPI = EV / PV (schedule efficiency), EAC = BAC / CPI
with as_of as (select max(status_date) as as_of_date from {{ ref('fct_schedule_progress') }}),
latest as (
    select activity_id, max_by(percent_complete, status_date) as actual_pct
    from {{ ref('fct_schedule_progress') }} group by activity_id
),
act as (
    select a.project_id, a.activity_id, b.budget_amount,
           least(1.0, greatest(0.0,
               date_diff('day', a.planned_start, o.as_of_date + 1)
               / greatest(date_diff('day', a.planned_start, a.planned_finish), 1)::double)) as planned_frac,
           coalesce(l.actual_pct, 0) / 100.0 as actual_frac
    from {{ ref('stg_activities') }} a
    join {{ ref('stg_budgets') }} b using (project_id, cost_code)
    cross join as_of o
    left join latest l using (activity_id)
),
pv_ev as (
    select project_id,
           sum(budget_amount)                as bac,
           sum(budget_amount * planned_frac) as pv,
           sum(budget_amount * actual_frac)  as ev
    from act group by project_id
),
ac as (select project_id, sum(amount) as ac from {{ ref('fct_job_cost') }} group by project_id)
select
    p.project_id, p.project_name, p.market, p.region,
    (select as_of_date from as_of)                              as as_of_date,
    round(e.bac, 2) as bac, round(e.pv, 2) as pv, round(e.ev, 2) as ev,
    round(coalesce(c.ac, 0), 2)                                 as ac,
    round(e.ev / nullif(c.ac, 0), 3)                            as cpi,
    round(e.ev / nullif(e.pv, 0), 3)                            as spi,
    round(e.bac / nullif(e.ev / nullif(c.ac, 0), 0), 2)         as eac,
    case
        when e.ev / nullif(c.ac, 0) < 0.90 or e.ev / nullif(e.pv, 0) < 0.90 then 'AT RISK'
        when e.ev / nullif(c.ac, 0) < 0.97 or e.ev / nullif(e.pv, 0) < 0.97 then 'WATCH'
        else 'ON TRACK'
    end                                                         as health_status
from {{ ref('stg_projects') }} p
join pv_ev e using (project_id)
left join ac c using (project_id)
