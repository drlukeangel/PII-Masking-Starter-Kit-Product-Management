#!/usr/bin/env python3
"""Manage Kafka Connect Debezium connectors for Aurora DSQL.

Features:
- Validate connector JSON files.
- Render connector JSON from environment variables.
- Preflight Kafka Connect plugin availability.
- Upsert connectors through Kafka Connect REST API.
- Wait for healthy connector/task states.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
REQUIRED_CONNECTOR_CLASSES = {
    "io.debezium.connector.postgresql.PostgresConnector",
    "io.debezium.connector.jdbc.JdbcSinkConnector",
}


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if "name" not in data or "config" not in data:
        raise ValueError(f"{path} must contain top-level keys: name, config")
    if not isinstance(data["config"], dict):
        raise ValueError(f"{path} key 'config' must be an object")
    if "connector.class" not in data["config"]:
        raise ValueError(f"{path} config must contain connector.class")
    return data


def substitute_env(value: Any) -> Any:
    if isinstance(value, str):

        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in os.environ:
                raise KeyError(f"Missing required env var: {key}")
            return os.environ[key]

        return ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: substitute_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute_env(v) for v in value]
    return value


def render_config(data: dict[str, Any]) -> dict[str, Any]:
    return substitute_env(data)


def http_json(method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} for {url}: {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Unable to reach Kafka Connect at {url}: {e}") from e


def connector_status(connect_url: str, name: str) -> dict[str, Any]:
    status_url = f"{connect_url.rstrip('/')}/connectors/{urllib.parse.quote(name)}/status"
    resp = http_json("GET", status_url)
    if not isinstance(resp, dict):
        raise RuntimeError(f"Unexpected status payload for {name}: {resp}")
    return resp


def upsert_connector(connect_url: str, data: dict[str, Any]) -> dict[str, Any]:
    name = data["name"]
    url = f"{connect_url.rstrip('/')}/connectors/{urllib.parse.quote(name)}/config"
    http_json("PUT", url, data["config"])
    return connector_status(connect_url, name)


def wait_until_running(connect_url: str, name: str, timeout_s: int, poll_s: int) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    while True:
        status = connector_status(connect_url, name)
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        task_states = [t.get("state", "UNKNOWN") for t in status.get("tasks", [])]
        if connector_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
            return status
        if time.time() >= deadline:
            raise RuntimeError(
                f"Connector {name} not RUNNING within timeout. connector={connector_state}, tasks={task_states}"
            )
        time.sleep(poll_s)


def get_available_plugins(connect_url: str) -> dict[str, str]:
    url = f"{connect_url.rstrip('/')}/connector-plugins"
    resp = http_json("GET", url)
    if not isinstance(resp, list):
        raise RuntimeError(f"Unexpected plugin payload: {resp}")
    plugins: dict[str, str] = {}
    for item in resp:
        if isinstance(item, dict) and "class" in item:
            cls = str(item["class"])
            version = str(item.get("version", "unknown"))
            plugins[cls] = version
    return plugins


def cmd_validate(args: argparse.Namespace) -> int:
    for fp in args.files:
        data = read_json(Path(fp))
        if args.render_env:
            _ = render_config(data)
        print(f"OK: {fp}")
    return 0


def cmd_render(args: argparse.Namespace) -> int:
    data = read_json(Path(args.file))
    rendered = render_config(data) if args.render_env else data
    print(json.dumps(rendered, indent=2, sort_keys=False))
    return 0


def cmd_preflight(args: argparse.Namespace) -> int:
    plugins = get_available_plugins(args.connect_url)
    available_classes = set(plugins.keys())
    expected: set[str] = set()

    # Scope plugin requirements to the connector files being operated on when
    # they are supplied. This keeps `deploy --preflight <single-file>` usable on
    # clusters that intentionally install only one plugin type (source-only or
    # sink-only workers).
    if args.files:
        for fp in args.files:
            cfg = read_json(Path(fp))
            expected.add(str(cfg["config"]["connector.class"]))
    else:
        # Backward-compatible fallback for standalone preflight calls where no
        # connector files were provided: validate the baseline plugin set.
        expected = set(REQUIRED_CONNECTOR_CLASSES)

    missing = sorted(expected - available_classes)
    if missing:
        print("Missing connector plugins:")
        for m in missing:
            print(f"  - {m}")
        return 2

    print("Kafka Connect preflight passed.")
    for cls in sorted(expected):
        print(f"  - found: {cls} (version={plugins.get(cls, 'unknown')})")
    return 0


def cmd_deploy(args: argparse.Namespace) -> int:
    if args.preflight:
        preflight_rc = cmd_preflight(args)
        if preflight_rc != 0:
            return preflight_rc

    for fp in args.files:
        data = read_json(Path(fp))
        if args.render_env:
            data = render_config(data)
        if args.dry_run:
            print(f"DRY-RUN: {data['name']}")
            print(json.dumps(data["config"], indent=2, sort_keys=False))
            continue

        status = upsert_connector(args.connect_url, data)
        tasks = [t.get("state", "UNKNOWN") for t in status.get("tasks", [])]
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        print(f"APPLIED: {data['name']} | connector={connector_state} | tasks={tasks}")

        if args.wait:
            status = wait_until_running(args.connect_url, data["name"], args.timeout_s, args.poll_s)
            tasks = [t.get("state", "UNKNOWN") for t in status.get("tasks", [])]
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            print(f"RUNNING: {data['name']} | connector={connector_state} | tasks={tasks}")

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Manage Aurora DSQL Debezium connectors")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_validate = sub.add_parser("validate", help="Validate connector JSON files")
    p_validate.add_argument("files", nargs="+", help="Connector JSON files")
    p_validate.add_argument("--render-env", action="store_true", help="Resolve ${ENV_VAR} placeholders")
    p_validate.set_defaults(func=cmd_validate)

    p_render = sub.add_parser("render", help="Render a connector JSON file")
    p_render.add_argument("file", help="Connector JSON file")
    p_render.add_argument("--render-env", action="store_true", help="Resolve ${ENV_VAR} placeholders")
    p_render.set_defaults(func=cmd_render)

    p_preflight = sub.add_parser("preflight", help="Check Kafka Connect plugin availability")
    p_preflight.add_argument("--connect-url", default="http://localhost:8083", help="Kafka Connect base URL")
    p_preflight.add_argument("files", nargs="*", help="Optional connector JSON files")
    p_preflight.set_defaults(func=cmd_preflight)

    p_deploy = sub.add_parser("deploy", help="Upsert connectors into Kafka Connect")
    p_deploy.add_argument("files", nargs="+", help="Connector JSON files")
    p_deploy.add_argument("--connect-url", default="http://localhost:8083", help="Kafka Connect base URL")
    p_deploy.add_argument("--render-env", action="store_true", help="Resolve ${ENV_VAR} placeholders before deploy")
    p_deploy.add_argument("--dry-run", action="store_true", help="Render and print final connector configs without sending HTTP calls")
    p_deploy.add_argument("--preflight", action="store_true", help="Check plugin availability before deploy")
    p_deploy.add_argument("--wait", action="store_true", help="Wait for connector and task states to become RUNNING")
    p_deploy.add_argument("--timeout-s", type=int, default=60, help="Timeout when --wait is enabled")
    p_deploy.add_argument("--poll-s", type=int, default=5, help="Polling interval when --wait is enabled")
    p_deploy.set_defaults(func=cmd_deploy)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except (ValueError, KeyError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
