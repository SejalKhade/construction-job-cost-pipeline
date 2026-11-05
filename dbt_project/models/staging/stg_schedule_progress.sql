select
    activity_id,
    project_id,
    cast(status_date as date)                 as status_date,
    try_cast(percent_complete as double)      as percent_complete,
    _source_file,
    _ingested_at
from {{ source('bronze', 'schedule_progress') }}
qualify row_number() over (partition by activity_id, status_date order by _ingested_at desc) = 1
