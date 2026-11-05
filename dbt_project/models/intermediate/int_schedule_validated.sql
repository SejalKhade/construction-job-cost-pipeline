-- Business rules for progress snapshots. A value that is higher than the next day's value
-- is a bad field entry (progress cannot go backwards), so it is quarantined as a spike.
with s as (
    select s.*, a.activity_id is not null as known_activity
    from {{ ref('stg_schedule_progress') }} s
    left join {{ ref('stg_activities') }} a using (activity_id)
),
next_value as (
    select *,
           lead(percent_complete) over (partition by activity_id order by status_date) as next_pct
    from s
    where percent_complete between 0 and 100
)
select s.* exclude (known_activity),
       case when s.percent_complete is null then 'unparseable_percent'
            when s.percent_complete < 0 or s.percent_complete > 100 then 'percent_out_of_range'
            when not s.known_activity then 'unknown_activity'
            when n.next_pct is not null and s.percent_complete > n.next_pct then 'progress_spike'
       end as dq_reason
from s
left join next_value n using (activity_id, status_date)
