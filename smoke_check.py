#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Checks that a running coordinator plans with the sample statistics provider.

Runs EXPLAIN on a filter the planner cannot estimate from table statistics (TPC-H Q9's `p_name LIKE '%green%'`) and
exits non-zero unless the filter's estimate comes from the provider (`HistoryBasedSourceInfo`). Run it after the
cluster starts: a plugin that loads but no longer answers, for example after a Presto upgrade, otherwise goes
unnoticed, because the planner quietly falls back to its own estimates.

usage: smoke_check.py [--server http://localhost:8080] [--catalog hive] --schema SCHEMA
"""
import argparse
import json
import sys
import urllib.request

QUERY = "EXPLAIN SELECT count(*) FROM part WHERE p_name LIKE '%green%'"


def run(server, catalog, schema, sql):
    headers = {"X-Presto-User": "smoke_check", "X-Presto-Catalog": catalog, "X-Presto-Schema": schema}
    request = urllib.request.Request(server.rstrip("/") + "/v1/statement", data=sql.encode(), headers=headers)
    rows = []
    while request is not None:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.load(response)
        if "error" in result:
            raise RuntimeError(result["error"].get("message", "query failed"))
        rows.extend(result.get("data", []))
        next_uri = result.get("nextUri")
        request = urllib.request.Request(next_uri, headers=headers) if next_uri else None
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--server", default="http://localhost:8080")
    parser.add_argument("--catalog", default="hive")
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()
    plan = "\n".join(row[0] for row in run(args.server, args.catalog, args.schema, QUERY))
    filter_lines = [line for line in plan.split("\n") if "Estimates:" in line and "HistoryBasedSourceInfo" in line]
    if filter_lines:
        print("OK: the sample statistics provider answered:", filter_lines[0].strip())
        return 0
    print("FAIL: no estimate from the sample statistics provider; check the coordinator log for", file=sys.stderr)
    print("'No sample estimate' warnings and that history-based planning is enabled. Plan:", file=sys.stderr)
    print(plan, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
