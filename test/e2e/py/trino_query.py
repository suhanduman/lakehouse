#!/usr/bin/env python3
"""Minimal Trino REST client for the kind e2e — STDLIB ONLY (runs in the
python:3.11-slim tools pod before any pip install happens; the stage-1
`SELECT 1` probe must not depend on pip/network beyond Trino itself).

Runs one SQL statement via POST /v1/statement (no-auth dev mode — the e2e
overlay sets `auth.oidc.enabled: false`), follows `nextUri` to completion
and prints the single scalar result. Retries the WHOLE query on any failure
(connection refused while the coordinator finishes booting, "no worker
nodes" while the worker registers, catalog warm-up) until --timeout.

Usage:
  trino_query.py --query "SELECT 1" [--expect 1] [--timeout 300]
                 [--url http://trino-coordinator:8080]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request


def _fetch(url: str, data: bytes | None = None) -> dict:
    req = urllib.request.Request(
        url,
        data=data,
        headers={"X-Trino-User": "e2e"},
        method="POST" if data is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def run_query(base_url: str, sql: str) -> list:
    body = _fetch(base_url.rstrip("/") + "/v1/statement", sql.encode())
    rows: list = []
    while True:
        if body.get("error"):
            raise RuntimeError(body["error"].get("message", str(body["error"])))
        rows.extend(body.get("data") or [])
        next_uri = body.get("nextUri")
        if not next_uri:
            return rows
        time.sleep(0.2)
        body = _fetch(next_uri)


def main() -> int:
    p = argparse.ArgumentParser(description="Run one Trino query, print the scalar result.")
    p.add_argument("--url", default="http://trino-coordinator:8080")
    p.add_argument("--query", required=True)
    p.add_argument("--expect", default=None,
                   help="fail unless the single scalar result equals this (string compare)")
    p.add_argument("--timeout", type=int, default=300,
                   help="total seconds to keep retrying failed attempts")
    args = p.parse_args()

    deadline = time.monotonic() + args.timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            rows = run_query(args.url, args.query)
            value = rows[0][0] if rows and rows[0] else None
            print(f"trino_query: {args.query!r} -> {value!r}")
            if args.expect is not None and str(value) != str(args.expect):
                print(f"FATAL: expected {args.expect!r}, got {value!r}", file=sys.stderr)
                return 1
            return 0
        except Exception as e:  # noqa: BLE001 — every failure mode retries until the deadline
            last_err = e
            print(f"trino_query: retrying after: {e}", file=sys.stderr)
            time.sleep(10)
    print(f"FATAL: query never succeeded within {args.timeout}s: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
