"""Generate a synthetic tool-telemetry dataset with realistic PII surface.

Output is a CSV with the columns the rubric covers — direct identifiers,
quasi-identifiers, sensitive attributes, and behavioral data. Values are
fully synthetic; no real people, no real serials.

Usage:
    python data/generate_synthetic.py --rows 1000 --out data/tool_telemetry.csv
"""

from __future__ import annotations

import argparse
import csv
import random
import string
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

TOOL_MODELS = [
    ("DCD800", "20V MAX Drill"),
    ("DCF887", "20V MAX Impact Driver"),
    ("DCF899", "20V MAX Impact Wrench"),
    ("DCS577", "FLEXVOLT Worm Drive Saw"),
    ("DCG416", "20V MAX Grinder"),
]
FIRMWARE = ["2.4.1", "2.5.0", "2.5.1", "2.6.0-rc2"]
JOB_SITES = [
    ("JS-001", "1200 W Burnside St, Portland, OR 97209", 45.5234, -122.6845),
    ("JS-002", "405 Lexington Ave, New York, NY 10174",   40.7516, -73.9755),
    ("JS-003", "233 S Wacker Dr, Chicago, IL 60606",      41.8789, -87.6359),
    ("JS-004", "601 Massachusetts Ave NW, Washington, DC", 38.9006, -77.0218),
    ("JS-005", "555 California St, San Francisco, CA",    37.7929, -122.4039),
]
FIRST_NAMES = ["Alex","Jordan","Sam","Casey","Morgan","Riley","Avery","Quinn","Reese","Drew","Emery","Hayden"]
LAST_NAMES  = ["Nguyen","Patel","Garcia","Smith","Johnson","Williams","Brown","Davis","Miller","Wilson","Anderson","Thomas"]
ERROR_CODES = ["", "", "", "", "E001_LOW_BATT", "E014_OVERTORQUE", "E022_TEMP_HIGH"]


def make_operator(i: int) -> tuple[str, str, str]:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"
    op_id = f"OP-{10000 + i:05d}"
    email = f"{first.lower()}.{last.lower()}@example.com"
    return op_id, name, email


def make_serial() -> str:
    return "DW" + "".join(random.choices(string.digits, k=10))


def gen(rows: int, out_path: Path) -> None:
    operators = [make_operator(i) for i in range(max(50, rows // 5))]
    serials = [make_serial() for _ in range(max(30, rows // 8))]

    base_ts = datetime.now(timezone.utc) - timedelta(days=30)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "event_id", "event_ts",
            "tool_serial", "tool_model", "tool_model_name", "firmware_version",
            "operator_id", "operator_email", "operator_name",
            "job_site_id", "job_site_address", "gps_lat", "gps_lon",
            "battery_pct", "torque_nm", "usage_minutes", "error_code",
        ])
        for _ in range(rows):
            op_id, name, email = random.choice(operators)
            serial = random.choice(serials)
            model_code, model_name = random.choice(TOOL_MODELS)
            site_id, site_addr, lat, lon = random.choice(JOB_SITES)
            ts = base_ts + timedelta(seconds=random.randint(0, 30 * 24 * 3600))
            # GPS jitter — within ~500m of the site centroid
            lat_j = round(lat + random.uniform(-0.005, 0.005), 6)
            lon_j = round(lon + random.uniform(-0.005, 0.005), 6)
            w.writerow([
                str(uuid.uuid4()), ts.isoformat(),
                serial, model_code, model_name, random.choice(FIRMWARE),
                op_id, email, name,
                site_id, site_addr, lat_j, lon_j,
                random.randint(5, 100),
                round(random.uniform(2.0, 95.0), 1),
                random.randint(1, 240),
                random.choice(ERROR_CODES),
            ])

    print(f"Wrote {rows} rows → {out_path}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rows", type=int, default=200, help="Number of rows to generate")
    p.add_argument("--out", type=Path, default=Path("data/tool_telemetry.csv"))
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = p.parse_args()
    random.seed(args.seed)
    gen(args.rows, args.out)


if __name__ == "__main__":
    main()
