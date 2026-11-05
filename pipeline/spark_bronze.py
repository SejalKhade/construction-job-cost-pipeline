"""
Bronze ingestion with PySpark.

* Daily feeds are loaded incrementally: only landing files not yet in Bronze are read,
  so reruns are idempotent (a rerun with no new files loads 0 rows).
* Files are grouped by header, so a feed that adds a column mid-stream is still read
  correctly; Parquet is written partitioned by business date and readers merge
  schemas by column name (schema evolution).
* Bronze keeps source values as strings, plus lineage columns (_source_file, _ingested_at).
  Typing and validation happen in dbt (Silver), never in Bronze.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import BRONZE_DIR, DAILY_FEEDS, LANDING_DIR, REFERENCE_TABLES


def get_spark():
    return (SparkSession.builder.master("local[2]").appName("jobcost-bronze")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def loaded_files(feed):
    manifest = BRONZE_DIR / "_manifests" / f"{feed}.json"
    return set(json.loads(manifest.read_text())) if manifest.exists() else set()


def save_manifest(feed, files):
    path = BRONZE_DIR / "_manifests" / f"{feed}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(files), indent=0))


def header_of(path):
    with open(path) as f:
        return f.readline().strip()


def ingest_daily_feed(spark, feed, date_col):
    done = loaded_files(feed)
    new = sorted(p for p in (LANDING_DIR / feed).glob("*.csv") if p.name not in done)
    if not new:
        return {"feed": feed, "new_files": 0, "rows": 0}

    groups = {}
    for p in new:
        groups.setdefault(header_of(p), []).append(str(p))

    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = 0
    for files in groups.values():
        df = (spark.read.option("header", True).option("inferSchema", False).csv(files)
              .withColumn("_source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
              .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp")))
        rows += df.count()
        df.write.mode("append").partitionBy(date_col).parquet(str(BRONZE_DIR / feed))

    save_manifest(feed, done | {p.name for p in new})
    return {"feed": feed, "new_files": len(new), "rows": rows, "header_versions": len(groups)}


def ingest_reference(spark, table):
    src = LANDING_DIR / "reference" / f"{table}.csv"
    df = (spark.read.option("header", True).option("inferSchema", False).csv(str(src))
          .withColumn("_ingested_at", F.current_timestamp()))
    df.write.mode("overwrite").parquet(str(BRONZE_DIR / table))
    return {"feed": table, "rows": df.count()}


def main():
    spark = get_spark()
    try:
        results = [ingest_reference(spark, t) for t in REFERENCE_TABLES]
        results += [ingest_daily_feed(spark, f, c) for f, c in DAILY_FEEDS.items()]
    finally:
        spark.stop()
    for r in results:
        print(r)
    return results


if __name__ == "__main__":
    main()
