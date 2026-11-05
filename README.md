# Construction Job Cost & Schedule Pipeline

A production-style ELT pipeline for a heavy civil contractor: daily field cost entries and schedule progress land as files, get ingested with **PySpark**, modeled with **dbt** into a **star schema**, checked for **completeness, accuracy, freshness, schema changes, and business rules**, and orchestrated end to end with **Dagster**. The output is an earned value mart (CPI, SPI, EAC) that tells project controls which jobs are drifting on cost or schedule.

[![CI](https://github.com/SejalKhade/construction-job-cost-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/SejalKhade/construction-job-cost-pipeline/actions/workflows/ci.yml)

## Why this exists

Project managers need cost and schedule certainty. That depends on two feeds that are messy in real life: field cost entries (labor, equipment, material, subcontract by cost code) and percent-complete updates from scheduling. This pipeline turns those feeds into numbers people can trust every morning, and stops loudly when a source breaks instead of publishing a wrong number.

## Results (60 days, 60 projects, $1.35B total budget)

| What | Number |
|---|---|
| Raw cost rows ingested | 204,324 |
| Re-sent duplicates removed | 2,009 |
| Clean rows in `fct_job_cost` | 200,468 |
| Rows quarantined with a reason | 1,847 (0.9%): missing cost code, negative amount, unknown project |
| Bad progress updates caught | 76 (out of range or a spike that later drops) |
| Projects scored | 60: 17 on track, 36 watch, 7 at risk |
| Data-quality checks per run | 43 (37 dbt tests + 6 Dagster Bronze checks) |
| End-to-end tests | 22 (pytest, run in CI on every push) |

Every Bronze entry ends up in exactly one place, the fact table or the quarantine. A dbt test proves this on every run.

## Architecture

```
 Source systems (daily CSV drops)
   job_cost_YYYY-MM-DD.csv   schedule_progress_YYYY-MM-DD.csv   reference: projects, cost_codes, budgets, activities
                 |
                 v  PySpark (pipeline/spark_bronze.py)
 BRONZE   Parquet partitioned by business date, raw strings + lineage (_source_file, _ingested_at)
          incremental via a load manifest; files grouped by header so a new column is read correctly
                 |   Dagster asset checks: schema_contract, freshness, completeness (blocking)
                 v  dbt (dbt_project/)
 SILVER   staging views: typing, dedupe (latest copy of each entry wins)
          intermediate: every row tagged with dq_reason by business rules
                 |
                 v
 GOLD     dim_project, dim_cost_code, dim_date          (dimensions)
          fct_job_cost (incremental), fct_schedule_progress (facts)
          dq_job_cost_quarantine                         (rejected rows for the data owner)
          mart_project_earned_value                      (CPI, SPI, EAC, health status)
```

Dagster runs the whole graph on a daily schedule (05:30 Central). Bronze asset keys match the dbt source names, so the Dagster UI shows full lineage from raw file to earned value mart, and every dbt test shows up as a Dagster asset check.

## Data-quality coverage

| Type | Where | How |
|---|---|---|
| Schema changes | Dagster `schema_contract` | Bronze columns compared to `contracts/*.yml`. Missing required column = ERROR (blocks downstream). New column = flagged until reviewed |
| Freshness | Dagster `freshness`, dbt `source freshness` | Newest business date at most 1 day behind; `_ingested_at` warn 24h / error 48h |
| Completeness | Dagster `completeness`, dbt `assert_job_cost_reconciles_to_bronze` | No missing days; Bronze entries = fact + quarantine |
| Accuracy | dbt `unique`, `not_null`, `relationships`, `accepted_values` | Keys, foreign keys to every dimension, allowed values |
| Business rules | `int_*_validated`, singular tests | No negative cost, cost code must be in the project budget, progress 0 to 100 and never goes backwards, quarantine rate under 2% |

## Schema evolution and incremental loads

On 2026-09-10 the job cost source starts sending a new `crew_size` column. Nothing breaks:
1. Spark groups landing files by header, so old and new files are both read correctly.
2. Bronze readers merge Parquet schemas by column name.
3. `stg_job_cost` uses `SELECT * REPLACE (...)`, so new columns flow through without code changes.
4. `fct_job_cost` is incremental with `on_schema_change: append_new_columns`. The column is added and older rows stay null.

Incremental loads key on `_ingested_at`, not the business date, so late entries for older dates are still picked up. Reruns are idempotent: with no new files, a rerun changes nothing (tested).

## Run it

```bash
pip install -r requirements.txt          # needs Java 17 for PySpark
cd dbt_project && dbt parse --profiles-dir . && cd ..

# Option 1: Dagster UI with lineage graph, checks, and schedule
dagster dev                              # http://localhost:3000, then materialize all

# Option 2: one-off run from the command line
python -m pipeline.generate_source_data
python -m pipeline.spark_bronze
cd dbt_project && dbt build --profiles-dir . && dbt docs generate --profiles-dir .
```

## Tests

```bash
pytest -v
```

The suite runs the pipeline three times through Dagster, like three days in production: an initial 40-day load, a 20-day incremental load that brings the schema change, and a rerun with no new files. It checks that every check passes, the new column is appended, the rerun is idempotent, duplicates are removed, the numbers reconcile to Bronze, and the earned value math is correct.

## Project layout

```
pipeline/            generate_source_data.py, spark_bronze.py, quality.py, config.py
contracts/           data contracts per source feed
dbt_project/         staging, intermediate, marts models, schema tests, singular tests
orchestration/       Dagster assets, asset checks, job, schedule
tests/               end-to-end pytest suite
docs/RUNBOOK.md      on-call guide: what each failure means and how to fix it
```

## What I would add next

- Swap DuckDB for Snowflake or Databricks (dbt adapter change; models stay the same)
- Land files in Azure Data Lake Storage and run the Spark job on Databricks
- Send Dagster run and check metrics to Grafana for alerting
- Daily partitions in Dagster for targeted backfills
