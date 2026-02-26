#!/usr/bin/env bash
set -euo pipefail

# Thin wrapper around the Python CLI.
# Usage:
#   CONNECT_URL=http://localhost:8083 ./scripts/deploy_connectors.sh

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SOURCE_CONFIG="${SOURCE_CONFIG:-connectors/source-postgres-cdc.json}"
SINK_CONFIG="${SINK_CONFIG:-connectors/sink-jdbc-upsert.json}"

python3 scripts/manage_connectors.py validate --render-env "$SOURCE_CONFIG" "$SINK_CONFIG"
python3 scripts/manage_connectors.py preflight --connect-url "$CONNECT_URL" "$SOURCE_CONFIG" "$SINK_CONFIG"
python3 scripts/manage_connectors.py deploy --render-env --preflight --wait --timeout-s 120 --connect-url "$CONNECT_URL" "$SOURCE_CONFIG" "$SINK_CONFIG"
