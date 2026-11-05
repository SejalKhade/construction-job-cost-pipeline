"""
Dagster orchestration: source landing -> PySpark Bronze -> dbt (Silver/Gold + tests).

Asset graph (each box is visible with lineage in the Dagster UI):

  source_landing -> bronze/{job_cost, schedule_progress, projects, ...} -> dbt models -> mart_project_earned_value

* Bronze asset keys match the dbt source names, so Dagster links them to the dbt models automatically.
* dbt tests appear as Dagster asset checks; Bronze also has schema-contract, freshness,
  and completeness checks. A failed ERROR check blocks downstream assets.
* A daily schedule refreshes the whole graph at 05:30 Central.
* Monitoring: pipeline_run_metrics records each run and alerts on rule breaks;
  a run failure sensor alerts when any run fails.
"""
from dagster import (AssetCheckResult, AssetCheckSeverity, AssetExecutionContext, AssetKey,
                     AssetSpec, DefaultScheduleStatus, Definitions, MaterializeResult,
                     RunFailureSensorContext, ScheduleDefinition, asset, asset_check,
                     define_asset_job, multi_asset, run_failure_sensor)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from pipeline import generate_source_data, monitoring, quality, spark_bronze
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


@asset(group_name="monitoring", deps=[AssetKey(["marts", "mart_project_earned_value"]),
                                    AssetKey(["marts", "dq_job_cost_quarantine"])],
       description="One row per run in ops.pipeline_run_metrics; sends alerts when a rule is broken")
def pipeline_run_metrics(context: AssetExecutionContext):
    m = monitoring.record_run_metrics(context.run_id)
    for alert in m["alerts"]:
        context.log.warning(f"ALERT: {alert}")
    return MaterializeResult(metadata={k: str(v) for k, v in m.items()})


@run_failure_sensor(description="Alerts (log + optional Slack/Teams webhook) when any run fails")
def alert_on_run_failure(context: RunFailureSensorContext):
    monitoring.send_alert(f"Dagster run failed: {context.dagster_run.job_name}",
                          {"run_id": context.dagster_run.run_id,
                           "error": context.failure_event.message})


daily_refresh_job = define_asset_job("daily_job_cost_refresh", selection="*")

daily_schedule = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="30 5 * * *",
    execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[source_landing, bronze_tables, jobcost_dbt_models, pipeline_run_metrics],
    asset_checks=bronze_checks,
    jobs=[daily_refresh_job],
    schedules=[daily_schedule],
    sensors=[alert_on_run_failure],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
