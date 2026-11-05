-- Business rule: reported progress on an activity should not go backwards day over day.
select activity_id, status_date, percent_complete, prev_pct
from (
    select *, lag(percent_complete) over (partition by activity_id order by status_date) as prev_pct
    from {{ ref('fct_schedule_progress') }}
)
where percent_complete < prev_pct
