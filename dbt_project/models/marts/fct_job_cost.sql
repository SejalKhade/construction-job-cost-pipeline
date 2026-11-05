{{
  config(
    materialized = 'incremental',
    unique_key = 'entry_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
  )
}}
-- Grain: one row per field cost entry.
-- Incremental on _ingested_at (not entry_date) so late-arriving entries for older dates are still picked up.
select * exclude (dq_reason)
from {{ ref('int_job_cost_validated') }}
where dq_reason is null
{% if is_incremental() %}
  and _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
