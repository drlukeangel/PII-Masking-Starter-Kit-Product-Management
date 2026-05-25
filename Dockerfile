# PII Masking Starter Kit — containerized pipeline.
#
# Bundles Java + Python + PySpark so the production `spark-submit` path runs
# anywhere a container does — no local JVM or Spark install required. The
# default run masks the sample dataset and verifies the output against
# rubric.md. Exit 0 means the rubric is honored.
#
#   docker build -t pii-kit .      (or: podman build -t pii-kit .)
#   docker run --rm pii-kit
# Pinned to bookworm: trixie (the current `slim` default) no longer packages
# openjdk-17, and Java 17 is the version supported across every Spark release
# `pyspark>=3.5` resolves to (3.5.x through 4.1.x).
FROM python:3.11-slim-bookworm

# PySpark 3.5 needs a JVM (Java 8/11/17). `procps` provides `ps`, which some
# Spark launch scripts shell out to.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

# Let Spark find Java via PATH (arch-agnostic — works on amd64 and arm64).
ENV PYSPARK_PYTHON=python3

WORKDIR /app

# Install deps first so the layer caches across source edits.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Call via bash so a missing exec bit (e.g. checked out on Windows) doesn't break the run.
ENTRYPOINT ["bash", "/app/docker/entrypoint.sh"]
