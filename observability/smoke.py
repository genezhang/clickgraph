#!/usr/bin/env python3
"""Smoke-check the local ClickGraph observability stack."""

import json
import os
import sys
import urllib.parse
import urllib.request

PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
TEMPO = os.environ.get("TEMPO_URL", "http://localhost:3200")
GRAFANA = os.environ.get(
    "GRAFANA_URL",
    f"http://localhost:{os.environ.get('GRAFANA_PORT', '3000')}",
)
CLICKGRAPH = os.environ.get("CLICKGRAPH_URL", "http://localhost:7475")


def fetch(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def fetch_json(url, timeout=5):
    return json.loads(fetch(url, timeout))


def check(name, fn):
    try:
        fn()
    except Exception as exc:
        print(f"FAIL {name}: {exc}", file=sys.stderr)
        raise SystemExit(1)
    print(f"ok   {name}")


def prometheus_target_up():
    data = fetch_json(f"{PROMETHEUS}/api/v1/targets?state=active")
    targets = [
        target
        for target in data["data"]["activeTargets"]
        if target["labels"].get("job") == "clickgraph"
    ]
    if not targets:
        raise RuntimeError("no active target with job=clickgraph")
    if targets[0]["health"] != "up":
        raise RuntimeError(f"target health is {targets[0]['health']!r}")


def prometheus_metric_present():
    query = urllib.parse.urlencode({"query": "clickgraph_queries_total"})
    data = fetch_json(f"{PROMETHEUS}/api/v1/query?{query}")
    if not data["data"]["result"]:
        raise RuntimeError("clickgraph_queries_total has no samples")


def tempo_has_traces():
    query = urllib.parse.urlencode({"q": '{ name = "cypher.query" }'})
    data = fetch_json(f"{TEMPO}/api/search?{query}")
    if not data.get("traces"):
        raise RuntimeError("no cypher.query spans; send a query first")


def grafana_healthy():
    if fetch_json(f"{GRAFANA}/api/health")["database"] != "ok":
        raise RuntimeError("Grafana database is not ok")


def grafana_dashboard():
    data = fetch_json(f"{GRAFANA}/api/dashboards/uid/clickgraph-overview")
    title = data["dashboard"]["title"]
    if title != "ClickGraph Overview":
        raise RuntimeError(f"unexpected dashboard title {title!r}")


def main():
    check("ClickGraph /health", lambda: fetch(f"{CLICKGRAPH}/health"))
    check("Prometheus ready", lambda: fetch(f"{PROMETHEUS}/-/ready"))
    check("Prometheus scrapes ClickGraph", prometheus_target_up)
    check("Prometheus has clickgraph_queries_total", prometheus_metric_present)
    check("Tempo ready", lambda: fetch(f"{TEMPO}/ready"))
    check("Tempo has cypher.query traces", tempo_has_traces)
    check("Grafana healthy", grafana_healthy)
    check("Grafana has ClickGraph Overview", grafana_dashboard)
    print("All checks passed.")


if __name__ == "__main__":
    main()
