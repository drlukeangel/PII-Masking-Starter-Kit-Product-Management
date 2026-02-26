# Aurora DSQL + Kafka Connect (Debezium v2)

Short answer: **the JSON itself is simple, production operation is not**.

This repo now includes executable code that handles real operational complexity: validation, env templating, plugin preflight checks, connector upsert, and RUNNING-state waits.

## What this includes

- Connector templates:
  - `connectors/source-postgres-cdc.json`
  - `connectors/sink-jdbc-upsert.json`
- Management CLI:
  - `scripts/manage_connectors.py`
- Wrapper script:
  - `scripts/deploy_connectors.sh`
- Source DB setup SQL:
  - `scripts/setup_cdc.sql`

## Why it is more than “just config”

Real CDC ingestion needs:
- correct connector plugins loaded in Kafka Connect,
- robust secret/env substitution,
- idempotent deploy semantics (upsert connector configs),
- post-deploy health checks (connector/task RUNNING),
- source-side publication/replication setup.

All of that is automated by the CLI.

## Connector design

### Source: Debezium PostgreSQL

- `connector.class=io.debezium.connector.postgresql.PostgresConnector`
- Debezium v2 namespace key: `topic.prefix` (not v1 `database.server.name`)
- Logical decoding plugin: `plugin.name=pgoutput`
- Replication controls: `slot.name`, `publication.name`

### Sink: Debezium JDBC

- `connector.class=io.debezium.connector.jdbc.JdbcSinkConnector`
- PostgreSQL dialect: `dialect.name=PostgreSqlDatabaseDialect`
- Topic routing: `topics.regex`
- Idempotent writes: `insert.mode=upsert` + record-key PK mapping

## End-to-end run

1) Prepare source DB publication:

```bash
psql "host=$AURORA_DSQL_HOST port=5432 dbname=$AURORA_DSQL_DB user=$SOURCE_DB_USER sslmode=require" -f scripts/setup_cdc.sql
```

2) Export variables used by templates:

```bash
export AURORA_DSQL_HOST='your-aurora-dsql-writer.cluster-xxxxxxxxxxxx.us-east-1.rds.amazonaws.com'
export AURORA_DSQL_DB='appdb'
export SOURCE_DB_USER='dbz_user'
export SINK_DB_USER='sink_user'
export AURORA_DSQL_PASSWORD='...'
```

3) Validate + preflight + deploy + wait:

```bash
./scripts/deploy_connectors.sh
```

4) Optional dry-run rendering (no HTTP calls):

```bash
python3 scripts/manage_connectors.py deploy --dry-run --render-env connectors/source-postgres-cdc.json connectors/sink-jdbc-upsert.json
```


## Do we need a new connector plugin for Aurora DSQL?

**Usually no.** Aurora DSQL is PostgreSQL-compatible, so you should use existing Debezium/Kafka Connect plugins:

- Source CDC: `io.debezium.connector.postgresql.PostgresConnector` (Debezium v2)
- Sink ingest: `io.debezium.connector.jdbc.JdbcSinkConnector`

You only need a custom connector plugin if Aurora DSQL exposes behavior that is incompatible with the PostgreSQL protocol/features your deployment depends on. In most setups, the standard Debezium v2 PostgreSQL source + Debezium JDBC sink is the right path.

You can verify plugin availability in your Connect cluster with:

```bash
python3 scripts/manage_connectors.py preflight --connect-url http://localhost:8083 connectors/source-postgres-cdc.json connectors/sink-jdbc-upsert.json
```

## Aurora DSQL JDBC / IAM notes

- Keep TLS on (`sslmode=require`; stricter envs: `verify-full`).
- For IAM auth, use generated short-lived token as password and rotate before expiration.
- Externalize secrets using Kafka Connect Config Providers or your secret manager.
