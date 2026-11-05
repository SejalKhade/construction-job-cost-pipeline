"""
Simulates the source systems of a heavy civil contractor.

Reference data (full snapshots): projects, cost_codes, budgets, activities
Daily drops (one CSV per day):  job_cost (field cost entries), schedule_progress

Real-world problems are injected on purpose so the pipeline has to handle them:
  * schema change: the job_cost feed adds a `crew_size` column partway through
  * duplicates: some entries are re-sent in the next day's file
  * bad rows: missing cost codes, negative amounts, unknown project ids, percent complete over 100
"""
import argparse
import csv
import random
from datetime import date, timedelta

import numpy as np

from pipeline.config import LANDING_DIR

SEED = 7
START = date(2026, 8, 1)
N_DAYS = 60
SCHEMA_CHANGE_DAY = 40  # day index when the job_cost feed adds crew_size
N_PROJECTS = 60

MARKETS = ["Transportation", "Water", "Power", "Building", "Mining", "Industrial"]
REGIONS = ["Midwest", "South Central", "Mountain", "Northwest", "Southeast"]
COST_CODES = [
    ("01-100", "Mobilization", "Labor"), ("02-200", "Site Clearing", "Equipment"),
    ("02-310", "Earthwork Excavation", "Equipment"), ("02-320", "Embankment Fill", "Material"),
    ("03-100", "Concrete Formwork", "Labor"), ("03-200", "Rebar", "Material"),
    ("03-300", "Cast-in-Place Concrete", "Material"), ("05-120", "Structural Steel", "Subcontract"),
    ("31-230", "Dewatering", "Equipment"), ("32-120", "Asphalt Paving", "Subcontract"),
    ("33-100", "Water Utilities", "Subcontract"), ("26-050", "Electrical", "Subcontract"),
    ("34-710", "Traffic Control", "Labor"), ("01-500", "Temporary Facilities", "Material"),
    ("01-740", "Cleanup", "Labor"), ("02-400", "Piling", "Subcontract"),
    ("03-400", "Precast Concrete", "Material"), ("31-500", "Shoring", "Equipment"),
]


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def build_reference(rng):
    projects, budgets, activities = [], [], []
    for i in range(1, N_PROJECTS + 1):
        pid = f"P{i:03d}"
        start = START + timedelta(days=0 if i == 1 else int(rng.integers(0, 12)))
        projects.append({
            "project_id": pid,
            "project_name": f"{random.choice(['I-', 'US-', 'SH-'])}{rng.integers(10, 99)} "
                            f"{random.choice(['Bridge', 'Interchange', 'Treatment Plant', 'Substation', 'Dam', 'Rail Yard'])}",
            "market": random.choice(MARKETS),
            "region": random.choice(REGIONS),
            "start_date": start.isoformat(),
            "contract_value": 0,
            # hidden performance drivers (not exported)
            "_cost_factor": float(rng.uniform(0.95, 1.12)),
            "_rate_factor": float(rng.uniform(0.88, 1.12)),
            "_delay_days": 0 if i == 1 else int(rng.integers(0, 4)),
        })
        codes = random.sample(COST_CODES, k=int(rng.integers(10, 16)))
        total = 0
        for code, _, _ in codes:
            bac = round(float(rng.uniform(0.2, 3.5)) * 1_000_000, 2)
            total += bac
            budgets.append({"project_id": pid, "cost_code": code, "budget_amount": bac})
            p_start = start + timedelta(days=int(rng.integers(0, 25)))
            dur = int(rng.integers(30, 140))
            activities.append({
                "activity_id": f"{pid}-{code}",
                "project_id": pid,
                "cost_code": code,
                "planned_start": p_start.isoformat(),
                "planned_finish": (p_start + timedelta(days=dur)).isoformat(),
                "_budget": bac,
            })
        projects[-1]["contract_value"] = round(total * 1.08, 2)
    return projects, budgets, activities


def pct_at(day, start, duration):
    if day < start:
        return 0.0
    return min(100.0, 100.0 * ((day - start).days + 1) / max(duration, 1))


def main(n_days=N_DAYS):
    random.seed(SEED)
    rng = np.random.default_rng(SEED)
    projects, budgets, activities = build_reference(rng)
    proj = {p["project_id"]: p for p in projects}

    ref = LANDING_DIR / "reference"
    write_csv(ref / "projects.csv", [{k: v for k, v in p.items() if not k.startswith("_")} for p in projects],
              ["project_id", "project_name", "market", "region", "start_date", "contract_value"])
    write_csv(ref / "cost_codes.csv",
              [{"cost_code": c, "description": d, "category": cat} for c, d, cat in COST_CODES],
              ["cost_code", "description", "category"])
    write_csv(ref / "budgets.csv", budgets, ["project_id", "cost_code", "budget_amount"])
    write_csv(ref / "activities.csv", [{k: v for k, v in a.items() if not k.startswith("_")} for a in activities],
              ["activity_id", "project_id", "cost_code", "planned_start", "planned_finish"])

    last_pct = {a["activity_id"]: 0.0 for a in activities}
    entry_seq, carry_over = 0, []
    for d in range(n_days):
        day = START + timedelta(days=d)
        cost_rows, sched_rows = list(carry_over), []
        carry_over = []
        for a in activities:
            p = proj[a["project_id"]]
            ps = date.fromisoformat(a["planned_start"])
            dur = (date.fromisoformat(a["planned_finish"]) - ps).days
            actual_start = ps + timedelta(days=p["_delay_days"])
            pct = round(pct_at(day, actual_start, dur / p["_rate_factor"]), 1)
            pct = max(pct, last_pct[a["activity_id"]])
            delta = pct - last_pct[a["activity_id"]]
            last_pct[a["activity_id"]] = pct
            if day >= ps:
                reported = pct
                if random.random() < 0.002:
                    reported = round(pct + 100, 1)  # bad entry from the field
                sched_rows.append({"activity_id": a["activity_id"], "project_id": a["project_id"],
                                   "status_date": day.isoformat(), "percent_complete": reported})
            if delta <= 0:
                continue
            day_cost = a["_budget"] * delta / 100 * p["_cost_factor"]
            n = int(rng.integers(3, 12))
            for share in rng.dirichlet(np.ones(n)):
                entry_seq += 1
                amount = round(day_cost * share, 2)
                qty = round(float(rng.uniform(1, 80)), 2)
                row = {
                    "entry_id": f"JC{entry_seq:08d}",
                    "project_id": a["project_id"],
                    "cost_code": a["cost_code"],
                    "entry_date": day.isoformat(),
                    "quantity": qty,
                    "unit_cost": round(amount / qty, 4),
                    "amount": amount,
                    "crew_or_vendor": f"CREW_{rng.integers(1, 60):02d}",
                }
                r = random.random()
                if r < 0.004:
                    row["cost_code"] = ""          # missing cost code
                elif r < 0.007:
                    row["amount"] = -amount        # negative cost
                elif r < 0.009:
                    row["project_id"] = "P999"     # unknown project
                if d >= SCHEMA_CHANGE_DAY:
                    row["crew_size"] = int(rng.integers(2, 25))
                cost_rows.append(row)
                if random.random() < 0.01 and d < n_days - 1:
                    carry_over.append(dict(row))   # re-sent in tomorrow's file
        fields = ["entry_id", "project_id", "cost_code", "entry_date", "quantity", "unit_cost",
                  "amount", "crew_or_vendor"]
        if d >= SCHEMA_CHANGE_DAY:
            fields.append("crew_size")
            for row in cost_rows:
                row.setdefault("crew_size", "")
        write_csv(LANDING_DIR / "job_cost" / f"job_cost_{day.isoformat()}.csv", cost_rows, fields)
        write_csv(LANDING_DIR / "schedule_progress" / f"schedule_progress_{day.isoformat()}.csv", sched_rows,
                  ["activity_id", "project_id", "status_date", "percent_complete"])
    print(f"Landed {n_days} daily drops for {N_PROJECTS} projects, {len(activities)} activities, "
          f"{entry_seq:,} job cost entries")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=N_DAYS)
    main(ap.parse_args().days)
