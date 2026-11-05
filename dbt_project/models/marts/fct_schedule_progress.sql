-- Grain: one row per activity per status date (valid rows only).
select activity_id, project_id, status_date, percent_complete
from {{ ref('int_schedule_validated') }}
where dq_reason is null
