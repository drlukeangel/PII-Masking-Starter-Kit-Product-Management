# Aurora DSQL Kafka Connect Connector Toolkit

This repository contains ready-to-use Kafka Connect connector definitions and helper scripts for running a **Debezium v2 CDC pipeline** with Aurora DSQL.

If you are new to Kafka Connect, start here: this README is written for junior developers and explains the *what*, *why*, and *how*.

> **Looking for the README in GitHub?** It is at the repository root as `README.md`.

## Quick start (5 minutes)

```bash
# 1) Validate JSON structure
python3 scripts/manage_connectors.py validate connectors/source-postgres-cdc.json

# 2) Optional: resolve env vars and preview final config
python3 scripts/manage_connectors.py render connectors/source-postgres-cdc.json --render-env

# 3) Check required plugin(s) on your Connect cluster
python3 scripts/manage_connectors.py preflight --connect-url http://localhost:8083 connectors/source-postgres-cdc.json

# 4) Deploy and wait until RUNNING
python3 scripts/manage_connectors.py deploy --connect-url http://localhost:8083 --preflight --wait connectors/source-postgres-cdc.json
```

---

## What this repo includes

- `connectors/source-postgres-cdc.json`
  - Debezium PostgreSQL source connector configuration (captures Aurora DSQL changes).
- `connectors/sink-jdbc-upsert.json`
  - JDBC sink connector configuration (writes records to a target database with upsert semantics).
- `scripts/manage_connectors.py`
  - CLI for validating, rendering, preflight-checking, and deploying connectors.
- `scripts/deploy_connectors.sh`
  - Convenience shell wrapper for deployment workflows.
- `scripts/setup_cdc.sql`
  - SQL helpers for CDC setup tasks.
- `aurora-dsql-debezium-v2-configs.md`
  - Additional config notes and reference material.

---

## Concepts (quick primer)

Before using the scripts, understand three terms:

1. **Connector**: a Kafka Connect plugin instance.
   - *Source connector*: reads from a system and writes to Kafka topics.
   - *Sink connector*: reads from Kafka topics and writes to a destination.
2. **Kafka Connect worker**: the running service that hosts plugins and executes connectors.
3. **Preflight**: validation step that confirms the required connector plugins are installed in the worker before deployment.

---

## Prerequisites

- Python 3.10+ (or compatible modern Python 3).
- A reachable Kafka Connect REST endpoint (default: `http://localhost:8083`).
- Connector plugins installed on your Connect workers:
  - `io.debezium.connector.postgresql.PostgresConnector`
  - `io.debezium.connector.jdbc.JdbcSinkConnector`
- Environment variables required by your connector JSON (if you use `${ENV_VAR}` placeholders).

---

## File format expected by the CLI

Each connector JSON file must include:

- top-level `name`
- top-level `config` object
- `config.connector.class`

If any of these are missing, validation or deployment will fail with an explicit error.

---

## CLI: `scripts/manage_connectors.py`

### 1) Validate connector files

```bash
python3 scripts/manage_connectors.py validate connectors/source-postgres-cdc.json connectors/sink-jdbc-upsert.json
```

Validate and also resolve `${ENV_VAR}` placeholders:

```bash
python3 scripts/manage_connectors.py validate --render-env connectors/source-postgres-cdc.json
```

### 2) Render a connector file (preview final JSON)

```bash
python3 scripts/manage_connectors.py render connectors/source-postgres-cdc.json --render-env
```

Useful for debugging env substitutions before deployment.

### 3) Run plugin preflight checks

Check required plugins for specific files:

```bash
python3 scripts/manage_connectors.py preflight --connect-url http://localhost:8083 connectors/source-postgres-cdc.json
```

> Important: when files are supplied, preflight only checks plugin classes used by those files. This allows source-only or sink-only clusters to pass preflight for relevant connectors.

Run baseline/global preflight (no files provided):

```bash
python3 scripts/manage_connectors.py preflight --connect-url http://localhost:8083
```

### 4) Deploy connectors

Dry run (no HTTP writes):

```bash
python3 scripts/manage_connectors.py deploy --dry-run --render-env connectors/source-postgres-cdc.json
```

Deploy with preflight and wait for RUNNING state:

```bash
python3 scripts/manage_connectors.py deploy \
  --connect-url http://localhost:8083 \
  --render-env \
  --preflight \
  --wait \
  connectors/source-postgres-cdc.json connectors/sink-jdbc-upsert.json
```

---

## Typical junior-friendly workflow

1. Copy/update connector JSON in `connectors/`.
2. Run `validate` first.
3. Run `render --render-env` to verify substitutions and final values.
4. Run `preflight` against the target Connect cluster.
5. Deploy with `--dry-run` once.
6. Deploy for real with `--preflight --wait`.
7. Confirm connector/task states are `RUNNING`.

---

## Troubleshooting

### `ERROR: Missing required env var: ...`
A `${VAR_NAME}` placeholder exists in your JSON, but the env var is not exported in your shell/session.

### `Missing connector plugins:`
The Connect worker does not have the listed connector class installed. Install plugin jars and restart workers (or update plugin path/image).

### `Unable to reach Kafka Connect...`
- Verify URL/port.
- Confirm network routing and service availability.
- Check if the Connect API is exposed from your environment.

### Connector did not become `RUNNING` during `--wait`
Inspect connector status and task errors from Kafka Connect REST API or logs. The script surfaces state, but root cause usually appears in worker logs.

---

## Safety notes

- Use `--dry-run` before real deployments, especially in shared environments.
- Prefer least-privilege credentials in connector configs.
- Avoid committing secrets; reference environment variables instead.

---

## Quick command reference

```bash
# Validate
python3 scripts/manage_connectors.py validate <file...>

# Validate with env substitution
python3 scripts/manage_connectors.py validate --render-env <file...>

# Render one file
python3 scripts/manage_connectors.py render <file> [--render-env]

# Preflight (optional file list)
python3 scripts/manage_connectors.py preflight [--connect-url URL] [file...]

# Deploy
python3 scripts/manage_connectors.py deploy [options] <file...>
```

