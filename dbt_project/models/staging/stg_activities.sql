select activity_id, project_id, cost_code,
       cast(planned_start as date) as planned_start,
       cast(planned_finish as date) as planned_finish
from {{ source('bronze', 'activities') }}
