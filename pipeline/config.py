"""Central paths and settings shared by ingestion, dbt, and Dagster."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", ROOT / "data"))
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
WAREHOUSE_PATH = DATA_DIR / "warehouse.duckdb"
CONTRACTS_DIR = ROOT / "contracts"
DBT_PROJECT_DIR = ROOT / "dbt_project"

# Daily source feeds (one file per day) and reference tables (full snapshot)
DAILY_FEEDS = {"job_cost": "entry_date", "schedule_progress": "status_date"}
REFERENCE_TABLES = ["projects", "cost_codes", "budgets", "activities"]

# dbt reads these, so absolute paths work no matter which folder dbt runs from
os.environ.setdefault("BRONZE_DIR", str(BRONZE_DIR))
os.environ.setdefault("WAREHOUSE_PATH", str(WAREHOUSE_PATH))
