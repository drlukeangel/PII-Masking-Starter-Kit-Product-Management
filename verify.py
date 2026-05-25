"""Post-mask invariants check.

Reads a masked CSV and asserts every column matches the treatment its
bucket calls for in `rubric.md`. Exits 0 on pass, 1 on fail with a
human-readable diff so the failure tells you exactly which column drifted.

Wire into CI on the data-pipeline repo. The rubric is now executable.

Usage:
    python verify.py --input data/tool_telemetry_masked.csv --rubric rubric.md
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Callable

# Expected rules per column. Each rule returns (ok: bool, reason: str)
RULES: dict[str, Callable[[str], tuple[bool, str]]] = {}


def expect_hex_digest(length: int):
    pattern = re.compile(rf"^[0-9a-f]{{{length}}}$")
    def check(v: str) -> tuple[bool, str]:
        if not v:
            return True, "empty"
        return bool(pattern.match(v)), f"expected hex digest of length {length}, got {v!r}"
    return check


def expect_token(namespace_prefix: str):
    pattern = re.compile(rf"^{re.escape(namespace_prefix)}_[0-9a-f]{{8}}$")
    def check(v: str) -> tuple[bool, str]:
        if not v:
            return True, "empty"
        return bool(pattern.match(v)), f"expected token like {namespace_prefix}_xxxxxxxx, got {v!r}"
    return check


def expect_gps_snapped(grid: float = 0.01):
    def check(v: str) -> tuple[bool, str]:
        if not v:
            return True, "empty"
        try:
            f = float(v)
        except ValueError:
            return False, f"not a number: {v!r}"
        # Snapped value should be an integer multiple of `grid` within float tolerance
        ratio = f / grid
        delta = abs(ratio - round(ratio))
        if delta < 1e-6:
            return True, ""
        return False, f"not snapped to {grid}° grid (got {v}, ratio {ratio:.6f})"
    return check


def expect_no_street_number():
    pattern = re.compile(r"^\s*\d+\s")
    def check(v: str) -> tuple[bool, str]:
        if not v:
            return True, "empty"
        if pattern.match(v):
            return False, f"still contains a street number prefix: {v!r}"
        return True, ""
    return check


def expect_hour_truncated():
    # Accept either "YYYY-MM-DD HH:00:00" or ISO with :00:00 minutes/seconds
    pattern = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:00:00")
    def check(v: str) -> tuple[bool, str]:
        if not v:
            return True, "empty"
        return bool(pattern.match(v)), f"expected timestamp truncated to the hour, got {v!r}"
    return check


def expect_no_email():
    pattern = re.compile(r"@.+\.")
    def check(v: str) -> tuple[bool, str]:
        if not v:
            return True, "empty"
        if pattern.search(v):
            return False, f"raw email leaked: {v!r}"
        return True, ""
    return check


# Map column → rule. Mirrors `rubric.md` and the glue job.
RULES.update({
    "tool_serial":      expect_hex_digest(64),
    "operator_email":   expect_hex_digest(64),
    "operator_id":      expect_token("op"),
    "operator_name":    expect_hex_digest(64),   # now hashed as a direct identifier
    "job_site_id":      expect_token("site"),
    "job_site_address": expect_no_street_number(),
    "gps_lat":          expect_gps_snapped(),
    "gps_lon":          expect_gps_snapped(),
    "event_ts":         expect_hour_truncated(),
})

# Also a tripwire — these columns should NEVER appear raw in any row.
RAW_EMAIL_TRIPWIRE = expect_no_email()


def verify(csv_path: Path) -> tuple[int, list[str]]:
    failures: list[str] = []
    rows_checked = 0

    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            rows_checked += 1
            for col, rule in RULES.items():
                if col not in row:
                    failures.append(f"row {i}: column '{col}' missing from output")
                    continue
                ok, reason = rule(row[col])
                if not ok:
                    failures.append(f"row {i}, column '{col}': {reason}")

            # Tripwire — scan every value for stray emails (in case a free-text
            # column got added without being added to the rubric).
            for col, val in row.items():
                ok, reason = RAW_EMAIL_TRIPWIRE(val)
                if not ok:
                    failures.append(f"row {i}, column '{col}' tripwire: {reason}")

    return rows_checked, failures


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, required=True, help="Masked CSV to verify")
    p.add_argument("--rubric", type=Path, default=Path("rubric.md"), help="Rubric file (informational)")
    p.add_argument("--max-show", type=int, default=10, help="Max failure lines to print before truncating")
    args = p.parse_args()

    if not args.input.exists():
        print(f"FAIL — input not found: {args.input}", file=sys.stderr)
        return 2

    rows, failures = verify(args.input)
    print(f"Verified {rows} rows against {args.rubric}")

    if not failures:
        print("OK — masking matches the rubric.")
        return 0

    print(f"FAIL — {len(failures)} rubric violations:")
    for line in failures[: args.max_show]:
        print(f"  · {line}")
    if len(failures) > args.max_show:
        print(f"  · ... and {len(failures) - args.max_show} more")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
