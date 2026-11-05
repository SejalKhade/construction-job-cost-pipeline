# Runbook: Construction Job Cost Pipeline

Guide for whoever is on call. Each section says what the alert means, what to check, and how to fix it.

## Daily run

- Job: `daily_job_cost_refresh`, runs at 05:30 America/Chicago
- Order: `source_landing` -> `bronze/*` (PySpark) -> Bronze checks -> dbt build (models + tests)
- Done when: all assets are green and `mart_project_earned_value.as_of_date` is yesterday or today

## Alerts and fixes

### `bronze/job_cost: schema_contract` failed (ERROR)
**Meaning:** a required column is missing from the source file. Downstream assets are blocked, so no bad numbers are published.
1. Open the check metadata and read `missing_required`.
2. Open the newest file in `data/landing/job_cost/` and compare its header to `contracts/job_cost.yml`.
3. Contact the source owner (Field Cost Systems). Do not edit the contract to make the check pass.
4. After the source is fixed, rerun the job. The load manifest skips files already loaded.

### `bronze/job_cost: schema_contract` warning (new column)
**Meaning:** the source added a column. The pipeline keeps working (schema evolution), but nobody has reviewed the column yet.
1. Read `new_unreviewed` in the metadata.
2. Confirm meaning, type, and owner with the source team.
3. Add it to `optional_columns` in the contract and open a pull request.

### `freshness` failed
**Meaning:** the newest business date is more than 1 day old.
1. Check whether today's file landed in `data/landing/<feed>/`.
2. File missing: upstream export did not run. Contact the source owner.
3. File present but not loaded: look at the `bronze_tables` step logs for a Spark error, fix it, rerun.

### `completeness` failed
**Meaning:** a day is missing between the first and last load.
1. Read `missing_days` in the metadata.
2. Ask the source owner to resend those files, drop them into the landing folder, and rerun. Only new files are read.

### dbt test `assert_quarantine_rate_under_threshold` failed
**Meaning:** more than 2% of cost rows were rejected. That usually means a source problem, not random bad entries.
1. `select dq_reason, count(*) from marts.dq_job_cost_quarantine group by 1`
2. One reason spiking (for example `unknown_project`) usually means reference data is out of date. Check `projects.csv` and `budgets.csv`.
3. Share the quarantine rows with the data owner. Do not delete them.

### dbt test `assert_job_cost_reconciles_to_bronze` failed
**Meaning:** some entries are in Bronze but in neither the fact table nor the quarantine. Rows are being lost.
1. Compare `count(distinct entry_id)` in Bronze with the fact and quarantine counts.
2. Usual cause: a change to the incremental filter in `fct_job_cost`. Run `dbt build --full-refresh -s fct_job_cost`.

### dbt test `assert_percent_complete_never_decreases` failed
**Meaning:** progress went backwards on an activity after validation. The spike rule did not catch it.
1. Look at the activity's history in `intermediate.int_schedule_validated`.
2. Confirm with Project Controls whether the lower value is a real correction.

### Alert: "Quarantine rate above threshold", "Job cost feed is stale", or "Daily row volume dropped"
**Meaning:** a monitoring rule in `pipeline/monitoring.py` was broken on the latest run. Alerts land in `data/alerts.jsonl` and the webhook channel.
1. Check the trend: `select * from ops.pipeline_run_metrics order by recorded_at desc limit 10`
2. Volume drop: confirm with the source owner whether the field crews actually logged less (holiday, weather) or the export was partial.
3. Stale feed or quarantine spike: follow the freshness or quarantine sections above.

### Alert: "Dagster run failed"
1. Open the run in the Dagster UI and find the first failed step.
2. Fix the cause, then re-execute from the failed step. Ingestion and dbt are idempotent, so a rerun is safe.

## Recovery

| Situation | Action |
|---|---|
| Rebuild Gold from scratch | `dbt build --full-refresh --profiles-dir .` |
| Reload one feed into Bronze | Delete `data/bronze/<feed>/` and `data/bronze/_manifests/<feed>.json`, then rerun |
| Backfill a late file | Put it in the landing folder and rerun. Incremental loads use `_ingested_at`, so older dates are picked up |

## Change process

All changes go through a pull request. CI compiles the dbt project and runs the full end-to-end test suite before merge.
