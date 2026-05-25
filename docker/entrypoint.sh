#!/usr/bin/env bash
# Mask the sample dataset with the production Glue job, then verify the
# output against the rubric. Overridable via env vars:
#   INPUT  — raw CSV to mask        (default: the bundled sample)
#   OUTDIR — Spark output directory (default: /tmp/masked)
#   SALT   — hashing salt           (default: a fresh random 32-byte hex string)
set -euo pipefail

INPUT="${INPUT:-data/sample_tool_telemetry.csv}"
OUTDIR="${OUTDIR:-/tmp/masked}"
SALT="${SALT:-$(python3 -c 'import secrets; print(secrets.token_hex(32))')}"

echo "==> Masking ${INPUT}  (salt: ${#SALT} hex chars, rotating)"
rm -rf "${OUTDIR}"
spark-submit glue/pii_masking_job.py \
  --input  "${INPUT}" \
  --output "${OUTDIR}" \
  --salt   "${SALT}"

# coalesce(1).write.csv() emits a directory of part files; grab the CSV.
MASKED="$(find "${OUTDIR}" -name 'part-*.csv' | head -1)"
echo "==> Verifying ${MASKED} against rubric.md"
python3 verify.py --input "${MASKED}" --rubric rubric.md
