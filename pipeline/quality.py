"""
Bronze-layer data-quality checks, run by Dagster after every ingestion.

  schema_contract  -> did the source add, drop, or rename columns? (schema changes)
  freshness        -> is the newest business date as recent as expected? (freshness)
  completeness     -> is any daily file missing between the first and last day? (completeness)

Accuracy and business rules live in dbt tests, closer to the models they protect.
"""
from datetime import date, timedelta

import duckdb
import yaml

from pipeline.config import BRONZE_DIR, CONTRACTS_DIR, DAILY_FEEDS, LANDING_DIR

LINEAGE_COLS = {"_source_file", "_ingested_at"}


def run(sql):
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def bronze_relation(feed):
    return (f"read_parquet('{BRONZE_DIR / feed}/**/*.parquet', "
            f"hive_partitioning = true, union_by_name = true)")


def bronze_columns(feed):
    rows = run(f"DESCRIBE SELECT * FROM {bronze_relation(feed)}")
    return {r[0] for r in rows} - LINEAGE_COLS


def load_contract(feed):
    with open(CONTRACTS_DIR / f"{feed}.yml") as f:
        return yaml.safe_load(f)


def compare_schema(actual, contract):
    """Returns (passed, details). Missing required columns fail; new columns only warn."""
    required = set(contract["required_columns"])
    known = required | set(contract.get("optional_columns", []))
    missing = sorted(required - actual)
    unexpected = sorted(actual - known)
    return not missing, {"missing_required": missing, "new_unreviewed": unexpected,
                         "column_count": len(actual)}


def check_schema_contract(feed):
    return compare_schema(bronze_columns(feed), load_contract(feed))


def check_freshness(feed, expected_latest=None, max_lag_days=1):
    """Newest business date must be within max_lag_days of the expected date.
    expected_latest defaults to the newest landing file (the simulated 'today')."""
    date_col = DAILY_FEEDS[feed]
    latest = run(f"SELECT max({date_col}) FROM {bronze_relation(feed)}")[0][0]
    if expected_latest is None:
        files = sorted((LANDING_DIR / feed).glob("*.csv"))
        expected_latest = date.fromisoformat(files[-1].stem.split("_")[-1])
    lag = (expected_latest - latest).days
    return lag <= max_lag_days, {"latest_date": str(latest), "expected": str(expected_latest), "lag_days": lag}


def missing_days(days):
    if not days:
        return []
    have = set(days)
    d, end, gaps = min(days), max(days), []
    while d <= end:
        if d not in have:
            gaps.append(str(d))
        d += timedelta(days=1)
    return gaps


def check_completeness(feed):
    date_col = DAILY_FEEDS[feed]
    days = [r[0] for r in run(f"SELECT DISTINCT {date_col} FROM {bronze_relation(feed)}")]
    gaps = missing_days(days)
    return not gaps, {"days_loaded": len(days), "missing_days": gaps}
