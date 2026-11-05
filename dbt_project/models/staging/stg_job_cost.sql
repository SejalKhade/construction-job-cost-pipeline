-- Types the raw feed and removes re-sent duplicates (latest copy of each entry_id wins).
-- SELECT * REPLACE keeps any new source columns automatically (schema evolution).
with typed as (
    select * replace (
        nullif(trim(cost_code), '')         as cost_code,
        cast(entry_date as date)            as entry_date,
        try_cast(quantity as double)        as quantity,
        try_cast(unit_cost as double)       as unit_cost,
        try_cast(amount as decimal(14, 2))  as amount
    ),
    row_number() over (partition by entry_id order by _ingested_at desc, _source_file desc) as _rn
    from {{ source('bronze', 'job_cost') }}
)
select * exclude (_rn) from typed where _rn = 1
