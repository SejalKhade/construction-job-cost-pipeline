"""
End-to-end tests. The pipeline runs three times through Dagster, like three days in production:

  run 1: 40 days of source files                  -> initial load
  run 2: 20 more days; job_cost adds crew_size     -> incremental load + schema evolution
  run 3: no new files                              -> rerun must change nothing (idempotent)
"""
import duckdb
import pytest
from dagster import materialize

from orchestration.definitions import (bronze_checks, bronze_tables, defs, jobcost_dbt_models,
                                      pipeline_run_metrics)
from pipeline import generate_source_data, monitoring, quality, spark_bronze
from pipeline.config import WAREHOUSE_PATH
from dagster_dbt import DbtCliResource
from orchestration.definitions import dbt_project

ASSETS = [bronze_tables, jobcost_dbt_models, pipeline_run_metrics, *bronze_checks]
RESOURCES = {"dbt": DbtCliResource(project_dir=dbt_project)}


def run_pipeline():
    result = materialize(ASSETS, resources=RESOURCES, raise_on_error=False)
    checks = result.get_asset_check_evaluations()
    return result, checks


def q(sql):
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def snapshot():
    return {
        "fct": q("select count(*) from marts.fct_job_cost")[0][0],
        "quarantine": q("select count(*) from marts.dq_job_cost_quarantine")[0][0],
        "fct_cols": {r[0] for r in q("describe marts.fct_job_cost")},
        "cost": float(q("select sum(amount) from marts.fct_job_cost")[0][0]),
    }


@pytest.fixture(scope="session")
def runs():
    out = {}
    generate_source_data.main(n_days=40)
    out["r1"], out["c1"] = run_pipeline()
    out["s1"] = snapshot()
    generate_source_data.main(n_days=60)
    out["r2"], out["c2"] = run_pipeline()
    out["s2"] = snapshot()
    out["r3"], out["c3"] = run_pipeline()
    out["s3"] = snapshot()
    return out


# Orchestration
def test_definitions_have_daily_schedule():
    sched = defs.get_schedule_def("daily_job_cost_refresh_schedule")
    assert sched.cron_schedule == "30 5 * * *"
    assert sched.execution_timezone == "America/Chicago"


def test_all_runs_succeed(runs):
    assert runs["r1"].success and runs["r2"].success and runs["r3"].success


def test_every_check_passes(runs):
    for key in ("c1", "c2", "c3"):
        failed = [(c.asset_key.to_user_string(), c.check_name) for c in runs[key] if not c.passed]
        assert not failed, failed


def test_bronze_checks_ran(runs):
    names = {(c.asset_key.path[-1], c.check_name) for c in runs["c2"] if c.asset_key.path[0] == "bronze"}
    for feed in ("job_cost", "schedule_progress"):
        for check in ("schema_contract", "freshness", "completeness"):
            assert (feed, check) in names


def test_dbt_tests_surface_as_dagster_checks(runs):
    dbt_checks = [c for c in runs["c2"] if c.asset_key.path[0] != "bronze"]
    assert len(dbt_checks) >= 30


# Incremental loading and schema evolution
def test_incremental_run_adds_rows(runs):
    assert runs["s2"]["fct"] > runs["s1"]["fct"]


def test_schema_evolution_new_column_appended(runs):
    assert "crew_size" not in runs["s1"]["fct_cols"]
    assert "crew_size" in runs["s2"]["fct_cols"]


def test_old_rows_have_null_crew_size(runs):
    early, late = q("""
        select count(crew_size) filter (where entry_date < date '2026-09-10'),
               count(crew_size) filter (where entry_date >= date '2026-09-10')
        from marts.fct_job_cost""")[0]
    assert early == 0 and late > 0


def test_rerun_is_idempotent(runs):
    assert runs["s3"] == runs["s2"]


def test_bronze_loaded_each_file_once():
    # Manifest records every file received (including empty ones); no file is loaded twice
    assert len(spark_bronze.loaded_files("job_cost")) == 60
    reloaded = q(f"select count(*) from (select _source_file from {quality.bronze_relation('job_cost')} "
                 f"group by 1 having count(distinct _ingested_at) > 1)")[0][0]
    assert reloaded == 0


# Data quality and reconciliation
def test_duplicates_removed(runs):
    bronze_rows = q(f"select count(*) from {quality.bronze_relation('job_cost')}")[0][0]
    distinct = q(f"select count(distinct entry_id) from {quality.bronze_relation('job_cost')}")[0][0]
    assert bronze_rows > distinct
    assert runs["s2"]["fct"] + runs["s2"]["quarantine"] == distinct


def test_fact_has_no_duplicate_entries(runs):
    assert q("select count(*) - count(distinct entry_id) from marts.fct_job_cost")[0][0] == 0


def test_quarantine_catches_each_bad_row_type(runs):
    reasons = {r[0] for r in q("select distinct dq_reason from marts.dq_job_cost_quarantine")}
    assert {"missing_cost_code", "negative_amount", "unknown_project"} <= reasons


def test_quarantine_rate_under_two_percent(runs):
    assert runs["s2"]["quarantine"] / (runs["s2"]["fct"] + runs["s2"]["quarantine"]) < 0.02


def test_no_orphan_projects_in_fact(runs):
    assert q("""select count(*) from marts.fct_job_cost f
                left join marts.dim_project p using (project_id) where p.project_id is null""")[0][0] == 0


# Earned value business logic
def test_earned_value_math(runs):
    for ev, ac, pv, cpi, spi in q("select ev, ac, pv, cpi, spi from marts.mart_project_earned_value where ac > 0"):
        assert abs(float(ev) / float(ac) - float(cpi)) < 0.001
        assert abs(float(ev) / float(pv) - float(spi)) < 0.001


def test_every_project_scored(runs):
    n, statuses = q("select count(*), list(distinct health_status) from marts.mart_project_earned_value")[0]
    assert n == generate_source_data.N_PROJECTS
    assert set(statuses) <= {"ON TRACK", "WATCH", "AT RISK"}


def test_actual_cost_matches_fact(runs):
    mart = float(q("select sum(ac) from marts.mart_project_earned_value")[0][0])
    assert abs(mart - runs["s2"]["cost"]) < 1.0


# Quality check units
def test_schema_check_fails_on_dropped_column():
    contract = {"required_columns": ["a", "b"], "optional_columns": []}
    passed, meta = quality.compare_schema({"a"}, contract)
    assert not passed and meta["missing_required"] == ["b"]


def test_schema_check_flags_new_column():
    contract = {"required_columns": ["a"], "optional_columns": []}
    passed, meta = quality.compare_schema({"a", "new_col"}, contract)
    assert passed and meta["new_unreviewed"] == ["new_col"]


def test_completeness_finds_gaps():
    from datetime import date
    days = [date(2026, 8, 1), date(2026, 8, 2), date(2026, 8, 5)]
    assert quality.missing_days(days) == ["2026-08-03", "2026-08-04"]


def test_freshness_flags_stale_feed():
    from datetime import date
    passed, meta = quality.check_freshness("job_cost", expected_latest=date(2026, 10, 15))
    assert not passed and meta["lag_days"] > 1


# Monitoring and alerting
def test_run_metrics_recorded_each_run(runs):
    rows = q("select fact_rows, alerts from ops.pipeline_run_metrics order by recorded_at")
    assert len(rows) == 3
    assert rows[0][0] < rows[1][0] == rows[2][0]
    assert all(r[1] == 0 for r in rows)          # healthy runs raise no alerts


def test_failure_sensor_registered():
    assert defs.get_sensor_def("alert_on_run_failure") is not None


def test_alert_rules_fire():
    bad = {"quarantine_rate_pct": 5.0, "feed_lag_days": 3, "latest_day_rows": 100, "prior_7d_avg_rows": 3000}
    titles = [t for t, _ in monitoring.evaluate_rules(bad)]
    assert titles == ["Quarantine rate above threshold", "Job cost feed is stale", "Daily row volume dropped"]


def test_healthy_metrics_raise_no_alert():
    ok = {"quarantine_rate_pct": 0.9, "feed_lag_days": 0, "latest_day_rows": 3100, "prior_7d_avg_rows": 3000}
    assert monitoring.evaluate_rules(ok) == []


def test_alert_written_to_log(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "ALERTS_FILE", tmp_path / "alerts.jsonl")
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monitoring.send_alert("test alert", {"x": 1})
    assert "test alert" in (tmp_path / "alerts.jsonl").read_text()
