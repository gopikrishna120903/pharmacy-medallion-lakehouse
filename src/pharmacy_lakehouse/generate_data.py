"""Generate synthetic pharmacy data (no real PHI) with realistic data-quality problems.

Writes CSV files to <out>/landing/<batch>/{members,providers,fills}.csv
  day1 - initial load
  day2 - new fills plus member changes (address / plan) to exercise SCD Type 2
"""
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

FIRST = ["Ava", "Liam", "Noah", "Emma", "Mia", "Ethan", "Priya", "Arjun", "Sofia", "Lucas"]
LAST = ["Smith", "Patel", "Garcia", "Nguyen", "Brown", "Reddy", "Khan", "Lopez", "Kim", "Davis"]
PLANS = ["COMMERCIAL", "MEDICARE_D", "MEDICAID", "CASH"]
ZIPS = ["76201", "75201", "60601", "10001", "94105", "30301", "98101", "33101"]
SPECIALTIES = ["Family Medicine", "Cardiology", "Endocrinology", "Pediatrics", "Psychiatry"]
DRUGS = [
    ("0071-0155-23", "Atorvastatin 20mg"),      # 4-4-2
    ("00093-7180-56", "Metformin 500mg"),       # 5-4-2
    ("59762-322-01", "Lisinopril 10mg"),        # 5-3-2
    ("00378-1805-1", "Amlodipine 5mg"),         # 5-4-1
    ("00591040401", "Levothyroxine 50mcg"),     # 11 digits, no hyphens
    ("16729-131-01", "Sertraline 50mg"),        # 5-3-2
]


def _member(i: int, rng: random.Random) -> dict:
    dob = date(1940, 1, 1) + timedelta(days=rng.randint(0, 30000))
    return {
        "member_id": f"M{i:08d}",
        "first_name": rng.choice(FIRST),
        "last_name": rng.choice(LAST),
        "date_of_birth": dob.isoformat(),
        "zip_code": rng.choice(ZIPS),
        "plan_type": rng.choice(PLANS),
        "updated_at": "2026-01-01T00:00:00",
    }


def _write(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _fills(n: int, members: list, npis: list, day: date, start_id: int, rng: random.Random) -> list:
    rows = []
    for k in range(n):
        ndc, drug = rng.choice(DRUGS)
        qty = rng.choice([30, 60, 90])
        paid = round(rng.uniform(4, 250), 2)
        row = {
            "fill_id": f"F{start_id + k:09d}",
            "rx_number": f"RX{rng.randint(1_000_000, 9_999_999)}",
            "member_id": rng.choice(members)["member_id"],
            "prescriber_npi": rng.choice(npis),
            "ndc": ndc,
            "drug_name": drug,
            "quantity": str(qty),
            "days_supply": str(qty),
            "fill_date": (datetime.combine(day, datetime.min.time())
                          + timedelta(minutes=rng.randint(0, 1439))).isoformat(),
            "store_id": f"S{rng.randint(1, 25):04d}",
            "paid_amount": f"{paid:.2f}",
            "copay": f"{min(paid, rng.choice([0, 5, 10, 25])):.2f}",
        }
        roll = rng.random()
        if roll < 0.02:
            row["member_id"] = ""                     # missing member identifier
        elif roll < 0.04:
            row["ndc"] = "12345-67"                   # invalid NDC
        elif roll < 0.05:
            row["quantity"] = "-30"                   # negative quantity
        elif roll < 0.06:
            row["prescriber_npi"] = "12345"           # invalid NPI
        rows.append(row)
    # inject exact duplicates (re-sent records from source)
    rows += [dict(r) for r in rng.sample(rows, k=max(1, n // 50))]
    return rows


def generate(out: str, members: int = 500, fills_per_day: int = 5000, seed: int = 42) -> None:
    rng = random.Random(seed)
    base = Path(out) / "landing"
    mem = [_member(i, rng) for i in range(1, members + 1)]
    providers = [
        {"npi": str(1_000_000_000 + i), "provider_name": f"Dr. {rng.choice(LAST)}",
         "specialty": rng.choice(SPECIALTIES), "state": "TX"}
        for i in range(1, 81)
    ]
    npis = [p["npi"] for p in providers]

    _write(base / "day1" / "members.csv", mem)
    _write(base / "day1" / "providers.csv", providers)
    _write(base / "day1" / "fills.csv",
           _fills(fills_per_day, mem, npis, date(2026, 1, 1), 1, rng))

    changed = []
    for m in rng.sample(mem, k=members // 10):
        m2 = dict(m)
        m2["zip_code"] = rng.choice([z for z in ZIPS if z != m["zip_code"]])
        m2["plan_type"] = rng.choice(PLANS)
        m2["updated_at"] = "2026-01-02T06:00:00"
        changed.append(m2)
    new_members = [_member(i, rng) | {"updated_at": "2026-01-02T06:00:00"}
                   for i in range(members + 1, members + 21)]
    _write(base / "day2" / "members.csv", changed + new_members)
    _write(base / "day2" / "providers.csv", providers)
    _write(base / "day2" / "fills.csv",
           _fills(fills_per_day, mem + new_members, npis, date(2026, 1, 2), 1_000_000, rng))
    print(f"Synthetic data written to {base}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data")
    p.add_argument("--members", type=int, default=500)
    p.add_argument("--fills-per-day", type=int, default=5000)
    a = p.parse_args()
    generate(a.out, a.members, a.fills_per_day)
