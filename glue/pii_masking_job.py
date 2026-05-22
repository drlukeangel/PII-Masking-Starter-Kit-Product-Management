"""PII masking — PySpark job, production path.

Runs as a local PySpark job (`spark-submit`) or as an AWS Glue job.
Encodes the rubric in `rubric.md` directly: direct identifiers → hashed,
quasi-identifiers → tokenized, sensitive attributes → generalized,
behavioral → kept.

Usage (local):
    spark-submit glue/pii_masking_job.py \\
        --input  data/tool_telemetry.csv \\
        --output data/tool_telemetry_masked.csv \\
        --salt   "$(openssl rand -hex 32)"

Usage (Glue):
    Set the job arguments --input, --output, --salt in the Glue Job
    definition. The salt should come from Secrets Manager, not from
    the CLI.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import sys
from typing import Iterable

try:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import StringType, DoubleType
    HAVE_SPARK = True
except ImportError:
    HAVE_SPARK = False

# Columns by rubric bucket. Single source of truth — verify.py reads this too.
DIRECT_IDENTIFIERS = ["tool_serial", "operator_email"]
QUASI_IDENTIFIERS  = ["operator_id", "operator_name", "job_site_id"]
SENSITIVE_LOCATION = [("gps_lat", "gps_lon")]   # paired
SENSITIVE_TIMESTAMPS = ["event_ts"]
SENSITIVE_FREETEXT_DROP_FROM = "job_site_address"   # drop street; keep city + state
BEHAVIORAL_KEEP = [
    "event_id", "tool_model", "tool_model_name", "firmware_version",
    "battery_pct", "torque_nm", "usage_minutes", "error_code",
]

# Generalization parameters — pulled into constants so they're easy to find.
GPS_GRID_DEGREES = 0.01            # ≈ 1.1 km
TIMESTAMP_TRUNC_TO = "hour"        # PySpark date_trunc unit


# --------------------------------------------------------------------------- #
# Treatment functions (also used by verify.py)
# --------------------------------------------------------------------------- #

def hash_direct(value: str, salt: str) -> str:
    """Direct identifier → HMAC-SHA256 with rotating salt, hex digest."""
    if value is None or value == "":
        return ""
    return hmac.new(salt.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def tokenize_quasi(value: str, namespace: str) -> str:
    """Quasi-identifier → stable random-looking token. Same value always
    maps to the same token within a given namespace; different namespaces
    keep different mappings so cross-table joins on quasi columns break."""
    if value is None or value == "":
        return ""
    digest = hashlib.sha256(f"{namespace}::{value}".encode("utf-8")).hexdigest()
    return f"{namespace[:3]}_{digest[:8]}"


def snap_to_grid(coord: float, grid: float = GPS_GRID_DEGREES) -> float:
    """GPS coordinate → grid centroid. ~1.1 km buckets at 0.01°."""
    if coord is None:
        return None
    return round(round(coord / grid) * grid, 4)


def drop_street(address: str) -> str:
    """Address → city + state only. Tool-telemetry job-site addresses are
    of the form 'NUMBER STREET, CITY, STATE ZIP'. Keep the last two
    comma-separated pieces."""
    if not address:
        return ""
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 3:
        return ", ".join(parts[-2:])
    return ", ".join(parts)


# --------------------------------------------------------------------------- #
# Spark UDFs
# --------------------------------------------------------------------------- #

def _register_udfs(spark, salt: str):
    spark.udf.register("hash_direct", lambda v: hash_direct(v, salt), StringType())
    spark.udf.register("tokenize_op",   lambda v: tokenize_quasi(v, "op"),   StringType())
    spark.udf.register("tokenize_name", lambda v: tokenize_quasi(v, "name"), StringType())
    spark.udf.register("tokenize_site", lambda v: tokenize_quasi(v, "site"), StringType())
    spark.udf.register("snap_grid",     lambda c: snap_to_grid(c),           DoubleType())
    spark.udf.register("drop_street",   drop_street,                          StringType())


# --------------------------------------------------------------------------- #
# Main pipeline
# --------------------------------------------------------------------------- #

def mask(spark, input_path: str, output_path: str, salt: str) -> None:
    df = spark.read.option("header", True).csv(input_path)

    _register_udfs(spark, salt)

    masked = (
        df
        # Direct identifiers
        .withColumn("tool_serial",    F.expr("hash_direct(tool_serial)"))
        .withColumn("operator_email", F.expr("hash_direct(operator_email)"))
        # Quasi-identifiers
        .withColumn("operator_id",    F.expr("tokenize_op(operator_id)"))
        .withColumn("operator_name",  F.expr("tokenize_name(operator_name)"))
        .withColumn("job_site_id",    F.expr("tokenize_site(job_site_id)"))
        # Sensitive — location: snap GPS to grid
        .withColumn("gps_lat",        F.expr("snap_grid(cast(gps_lat as double))"))
        .withColumn("gps_lon",        F.expr("snap_grid(cast(gps_lon as double))"))
        # Sensitive — address: keep city + state
        .withColumn("job_site_address", F.expr(f"drop_street({SENSITIVE_FREETEXT_DROP_FROM})"))
        # Sensitive — timestamp: truncate to hour
        .withColumn("event_ts",       F.date_trunc(TIMESTAMP_TRUNC_TO, F.to_timestamp(F.col("event_ts"))))
    )

    # Write back as CSV (Glue would write Parquet to S3 in production).
    masked.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Masked {masked.count()} rows → {output_path}")


def parse_glue_args(argv: Iterable[str]) -> argparse.Namespace:
    """Accept both `--input X` and `--input=X` forms. Glue passes the latter."""
    p = argparse.ArgumentParser()
    p.add_argument("--input",  required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--salt",   required=True)
    p.add_argument("--JOB_NAME", default=None, help="Glue sets this automatically")
    return p.parse_args(list(argv))


def main() -> int:
    if not HAVE_SPARK:
        print("PySpark not installed. Install with: pip install pyspark", file=sys.stderr)
        return 2
    args = parse_glue_args(sys.argv[1:])
    spark = SparkSession.builder.appName("pii-masking").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        mask(spark, args.input, args.output, args.salt)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
