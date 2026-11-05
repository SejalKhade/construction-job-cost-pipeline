"""
Dagster orchestration: source landing -> PySpark Bronze -> dbt (Silver/Gold + tests).

Asset graph (each box is visible with lineage in the Dagster UI):

  source_landing -> bronze/{job_cost, schedule_progress, projects, ...} -> dbt models -> mart_project_earned_value

* Bronze asset keys match the dbt source names, so Dagster links them to the dbt models automatically.
* dbt tests appear as Dagster asset checks; Bronze also has schema-contract, freshness,
  and completeness checks. A failed ERROR check blocks downstream assets.
* A daily schedule refreshes the whole graph at 05:30 Central.
"""
from dagster import (AssetCheckResult, AssetCheckSeverity, AssetExecutionContext, AssetKey,
                     AssetSpec, DefaultScheduleStatus, Definitions, MaterializeResult,
                     ScheduleDefinition, asset, asset_check, define_asset_job, multi_asset)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from pipeline import generate_source_data, quality, spark_bronze
from pipeline.config import DAILY_FEEDS, DBT_PROJECT_DIR, REFERENCE_TABLES

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

BRONZE_TABLES = list(DAILY_FEEDS) + REFERENCE_TABLES


@asset(group_name="ingestion", description="Daily CSV drops from the cost and scheduling systems (simulated).")
def source_landing(context: AssetExecutionContext):
    generate_source_data.main()
    return MaterializeResult(metadata={"days": generate_source_data.N_DAYS})


@multi_asset(
    specs=[AssetSpec(AssetKey(["bronze", t]), deps=[source_landing], group_name="bronze",
                     description=f"Raw {t} as Parquet (PySpark), with lineage columns")
           for t in BRONZE_TABLES],
    can_subset=False,
)
def bronze_tables(context: AssetExecutionContext):
    results = {r["feed"]: r for r in spark_bronze.main()}
    for t in BRONZE_TABLES:
        r = results[t]
        yield MaterializeResult(asset_key=AssetKey(["bronze", t]),
                                metadata={k: v for k, v in r.items() if k != "feed"})


def make_bronze_checks(feed):
    key = AssetKey(["bronze", feed])

    @asset_check(asset=key, name="schema_contract", blocking=True,
                 description="Required columns present; new columns flagged for review")
    def schema_contract():
        passed, meta = quality.check_schema_contract(feed)
        sev = AssetCheckSeverity.ERROR if meta["missing_required"] else AssetCheckSeverity.WARN
        return AssetCheckResult(passed=passed and not meta["new_unreviewed"], severity=sev, metadata=meta)

    @asset_check(asset=key, name="freshness", blocking=True,
                 description="Newest business date is no more than 1 day behind")
    def freshness():
        passed, meta = quality.check_freshness(feed)
        return AssetCheckResult(passed=passed, metadata=meta)

    @asset_check(asset=key, name="completeness", blocking=True,
                 description="No missing days between the first and last load")
    def completeness():
        passed, meta = quality.check_completeness(feed)
        return AssetCheckResult(passed=passed, metadata=meta)

    return [schema_contract, freshness, completeness]


bronze_checks = [c for feed in DAILY_FEEDS for c in make_bronze_checks(feed)]


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def jobcost_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


daily_refresh_job = define_asset_job("daily_job_cost_refresh", selection="*")

daily_schedule = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="30 5 * * *",
    execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[source_landing, bronze_tables, jobcost_dbt_models],
    asset_checks=bronze_checks,
    jobs=[daily_refresh_job],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
