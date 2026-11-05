"""
Monitoring and alerting.

After every run, record_run_metrics() appends one row to ops.pipeline_run_metrics
(row counts, quarantine rate, feed lag, at-risk projects), so trends can be charted in a
BI or observability tool (for example Grafana reading this table).

Alert rules are checked on every run. An alert is always written to data/alerts.jsonl and,
when ALERT_WEBHOOK_URL is set (a Slack or Teams incoming webhook), posted there too.
Failed Dagster runs alert through the same path (see the run failure sensor).
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import duckdb

from pipeline.config import DATA_DIR, WAREHOUSE_PATH

RULES = {
    "max_quarantine_rate_pct": 2.0,   # same threshold as the dbt test, checked as a trend too
    "max_feed_lag_days": 1,
    "max_row_drop_pct": 50.0,         # latest day's rows vs the average of the 7 days before it
}
ALERTS_FILE = DATA_DIR / "alerts.jsonl"


def send_alert(title, details):
    alert = {"at": datetime.now(timezone.utc).isoformat(), "title": title, "details": details}
    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_FILE, "a") as f:
        f.write(json.dumps(alert, default=str) + "\n")
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        body = json.dumps({"text": f":rotating_light: {title}\n{json.dumps(details, default=str)}"}).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
    return alert


def evaluate_rules(current):
    """Returns a list of (title, details) for every rule the current run breaks."""
    alerts = []
    if current["quarantine_rate_pct"] > RULES["max_quarantine_rate_pct"]:
        alerts.append(("Quarantine rate above threshold",
                       {"rate_pct": current["quarantine_rate_pct"], "limit": RULES["max_quarantine_rate_pct"]}))
    if current["feed_lag_days"] > RULES["max_feed_lag_days"]:
        alerts.append(("Job cost feed is stale", {"lag_days": current["feed_lag_days"]}))
    if current["prior_7d_avg_rows"]:
        drop = 100.0 * (1 - current["latest_day_rows"] / current["prior_7d_avg_rows"])
        if drop > RULES["max_row_drop_pct"]:
            alerts.append(("Daily row volume dropped", {"drop_pct": round(drop, 1),
                                                        "latest_day_rows": current["latest_day_rows"]}))
    return alerts


def record_run_metrics(run_id, expected_latest_date=None):
    con = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute("""
            CREATE TABLE IF NOT EXISTS ops.pipeline_run_metrics (
                run_id VARCHAR, recorded_at TIMESTAMP, fact_rows BIGINT, new_fact_rows BIGINT,
                quarantine_rows BIGINT, quarantine_rate_pct DOUBLE, latest_entry_date DATE,
                feed_lag_days INTEGER, projects_at_risk INTEGER, alerts INTEGER)""")
        fact, latest = con.execute("SELECT count(*), max(entry_date) FROM marts.fct_job_cost").fetchone()
        quarantine = con.execute("SELECT count(*) FROM marts.dq_job_cost_quarantine").fetchone()[0]
        at_risk = con.execute(
            "SELECT count(*) FROM marts.mart_project_earned_value WHERE health_status = 'AT RISK'").fetchone()[0]
        latest_rows, prior_avg = con.execute("""
            WITH d AS (SELECT entry_date, count(*) AS n FROM marts.fct_job_cost GROUP BY 1)
            SELECT max(n) FILTER (WHERE entry_date = (SELECT max(entry_date) FROM d)),
                   avg(n) FILTER (WHERE entry_date >= (SELECT max(entry_date) FROM d) - 7
                                    AND entry_date <  (SELECT max(entry_date) FROM d))
            FROM d""").fetchone()
        prev_row = con.execute("""SELECT fact_rows, new_fact_rows FROM ops.pipeline_run_metrics
                                  ORDER BY recorded_at DESC LIMIT 1""").fetchone()
        previous = {"fact_rows": prev_row[0], "new_fact_rows": prev_row[1]} if prev_row else None
        expected = expected_latest_date or latest
        current = {
            "fact_rows": fact,
            "new_fact_rows": fact - (previous["fact_rows"] if previous else 0),
            "quarantine_rows": quarantine,
            "quarantine_rate_pct": round(100.0 * quarantine / max(fact + quarantine, 1), 3),
            "latest_entry_date": latest,
            "feed_lag_days": (expected - latest).days,
            "projects_at_risk": at_risk,
            "latest_day_rows": latest_rows,
            "prior_7d_avg_rows": prior_avg,
        }
        alerts = evaluate_rules(current)
        for title, details in alerts:
            send_alert(title, {**details, "run_id": run_id})
        con.execute("INSERT INTO ops.pipeline_run_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, datetime.now(timezone.utc).replace(tzinfo=None), current["fact_rows"],
                     current["new_fact_rows"], current["quarantine_rows"], current["quarantine_rate_pct"],
                     current["latest_entry_date"], current["feed_lag_days"], current["projects_at_risk"],
                     len(alerts)])
        return {**{k: v for k, v in current.items() if k != "prior_7d_avg_rows"},
                "alerts": [a[0] for a in alerts]}
    finally:
        con.close()
