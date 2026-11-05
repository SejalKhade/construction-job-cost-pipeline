"""Every test session runs the pipeline in a fresh, throwaway data folder."""
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_tmp = Path(tempfile.mkdtemp(prefix="jobcost_test_"))
os.environ["PIPELINE_DATA_DIR"] = str(_tmp)
os.environ["BRONZE_DIR"] = str(_tmp / "bronze")
os.environ["WAREHOUSE_PATH"] = str(_tmp / "warehouse.duckdb")
