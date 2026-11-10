#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Writes the per-table samples the sample statistics provider estimates from (see README.md).

Run it after ANALYZE, against a running cluster that serves the schema. For each table:
  - row count and size: the file metastore's numRows / totalSize when --metastore-dir is given (what the planner
    reads, which the provider's estimates are matched against), otherwise SHOW STATS's row count and an unknown size;
  - tables of at most 2 x --rows-per-table rows are kept whole;
  - larger tables are sampled by key with one full scan on the cluster: every row whose key column is a multiple of
    a prime m (m = the smallest prime >= rows / --rows-per-table). Keys spread evenly whatever the file layout, and a
    sampled key keeps all of its rows, so groups on the key are whole (e.g. whole orders of lineitem, which a HAVING
    on the order key needs). The key column is --key TABLE=COLUMN, else the table's first integer column.
Writes OUTPUT_DIR/<table>.parquet, OUTPUT_DIR/tables.properties (the provider's table list) and
OUTPUT_DIR/sample-stats.properties (provider settings, if absent).

Requires pyarrow.
"""

import argparse
import datetime
import decimal
import json
import os
import re
import sys
import time
import urllib.request

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("sample_tables.py needs pyarrow (pip install pyarrow)")

INTEGER_TYPES = ("bigint", "integer", "smallint", "tinyint")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-s", "--schema", required=True, help="schema to sample")
    parser.add_argument("-o", "--output-dir", required=True, help="directory for the samples")
    parser.add_argument("-c", "--catalog", default="hive")
    parser.add_argument("-H", "--host", default=os.environ.get("PRESTO_COORDINATOR_HOST", "localhost"))
    parser.add_argument("-p", "--port", default=os.environ.get("PRESTO_COORDINATOR_PORT", "8080"))
    parser.add_argument("-u", "--user", default="test_user")
    parser.add_argument(
        "-r", "--rows-per-table", type=int, default=100_000, help="target sample rows per table (default 100000)"
    )
    parser.add_argument(
        "-k",
        "--key",
        action="append",
        default=[],
        metavar="TABLE=COLUMN",
        help="sample TABLE by COLUMN (default: its first integer column)",
    )
    parser.add_argument(
        "-m",
        "--metastore-dir",
        help="file metastore directory (e.g. presto/docker/.hive_metastore) for numRows / totalSize",
    )
    parser.add_argument("--retries", type=int, default=3, help="attempts per table scan (default 3)")
    return parser.parse_args()


class Presto:
    def __init__(self, args):
        self.url = f"http://{args.host}:{args.port}/v1/statement"
        self.headers = {"X-Presto-User": args.user, "X-Presto-Catalog": args.catalog, "X-Presto-Schema": args.schema}

    def query(self, sql):
        """Columns ([{name, type}]) and rows of a query."""
        request = urllib.request.Request(self.url, data=sql.encode(), method="POST", headers=self.headers)
        response = json.load(urllib.request.urlopen(request))
        columns, rows = None, []
        while True:
            columns = columns or response.get("columns")
            rows.extend(response.get("data", []))
            if "error" in response:
                raise RuntimeError(response["error"]["message"])
            if "nextUri" not in response:
                return columns or [], rows
            response = json.load(urllib.request.urlopen(response["nextUri"]))


def next_prime(n):
    n = max(n, 2)
    while any(n % d == 0 for d in range(2, int(n**0.5) + 1)):
        n += 1
    return n


def arrow_column(type_name, values):
    """Presto JSON values of one column as an Arrow array; ValueError for types not handled."""
    base = type_name.split("(")[0]
    if base in ("bigint", "integer", "smallint", "tinyint"):
        return pa.array(
            values, {"bigint": pa.int64(), "integer": pa.int32(), "smallint": pa.int16(), "tinyint": pa.int8()}[base]
        )
    if base in ("double", "real"):
        return pa.array(
            [None if v is None else float(v) for v in values], pa.float64() if base == "double" else pa.float32()
        )
    if base == "boolean":
        return pa.array(values, pa.bool_())
    if base == "date":
        return pa.array([None if v is None else datetime.date.fromisoformat(v) for v in values], pa.date32())
    if base == "timestamp":
        return pa.array([None if v is None else datetime.datetime.fromisoformat(v) for v in values], pa.timestamp("us"))
    if base == "decimal":
        precision, scale = (int(x) for x in re.findall(r"\d+", type_name))
        return pa.array([None if v is None else decimal.Decimal(v) for v in values], pa.decimal128(precision, scale))
    if base in ("varchar", "char"):
        return pa.array(values, pa.string())
    raise ValueError(f"column type {type_name} is not supported")


def metastore_parameters(args, table):
    if not args.metastore_dir:
        return {}
    path = os.path.join(args.metastore_dir, args.schema, table, ".prestoSchema")
    if not os.path.isfile(path):
        return {}
    return json.load(open(path)).get("parameters", {})


def table_size(presto, args, table):
    """(rows, size in bytes or NaN) as the planner reads them."""
    parameters = metastore_parameters(args, table)
    if "numRows" in parameters:
        size = float(parameters["totalSize"]) if "totalSize" in parameters else float("nan")
        return int(parameters["numRows"]), size
    columns, rows = presto.query(f'SHOW STATS FOR "{table}"')
    names = [c["name"] for c in columns]
    summary = next(r for r in rows if r[names.index("column_name")] is None)
    return int(summary[names.index("row_count")]), float("nan")


def main():
    args = parse_args()
    presto = Presto(args)
    keys = dict(spec.split("=", 1) for spec in args.key)
    os.makedirs(args.output_dir, exist_ok=True)
    _, table_rows = presto.query(f"SHOW TABLES FROM {args.catalog}.{args.schema}")
    tables = {}
    for (table,) in sorted(table_rows):
        t0 = time.time()
        columns = [(name, type_name) for name, type_name, *_ in presto.query(f'DESCRIBE "{table}"')[1]]
        rows, size = table_size(presto, args, table)
        key = keys.get(table) or next((name for name, type_name in columns if type_name in INTEGER_TYPES), None)
        if rows <= 2 * args.rows_per_table:
            sql, sampling = f'SELECT * FROM "{table}"', "whole table"
        elif key is None:
            print(f"{table}: {rows} rows and no integer key column (use --key {table}=COLUMN): not sampled", flush=True)
            continue
        else:
            modulus = next_prime(rows // args.rows_per_table)
            sql, sampling = f'SELECT * FROM "{table}" WHERE "{key}" % {modulus} = 0', f"{key} % {modulus} = 0"
        for attempt in range(1, args.retries + 1):
            try:
                result_columns, result_rows = presto.query(sql)
                break
            except RuntimeError as e:
                if attempt == args.retries:
                    raise
                print(f"{table}: attempt {attempt} failed ({str(e)[:160]}), retrying", flush=True)
                time.sleep(15)
        try:
            data = pa.table(
                {c["name"]: arrow_column(c["type"], [r[i] for r in result_rows]) for i, c in enumerate(result_columns)}
            )
        except ValueError as e:
            print(f"{table}: {e}: not sampled", flush=True)
            continue
        pq.write_table(data, os.path.join(args.output_dir, f"{table}.parquet"))
        tables[table] = {
            "rows": rows,
            "sample_rows": data.num_rows,
            "scale": rows / max(data.num_rows, 1),
            "key": key or "",
            "input_rows": float(rows),
            "input_size": size,
            "sampling": sampling,
        }
        print(f"{table}: {data.num_rows} of {rows} rows ({sampling}), {time.time() - t0:.1f} s", flush=True)
    with open(os.path.join(args.output_dir, "tables.properties"), "w") as f:
        f.write(f"schema={args.schema}\n")
        for table, t in tables.items():
            for name, value in t.items():
                f.write(f"{table}.{name}={value}\n")
    settings = os.path.join(args.output_dir, "sample-stats.properties")
    if not os.path.exists(settings):
        with open(settings, "w") as f:
            f.write(
                "# Sample statistics provider settings (README.md). sample.dir defaults to this directory.\n"
                "node.types=FilterNode,ProjectNode\n"
            )
    print(f"wrote {len(tables)} tables to {args.output_dir}")


if __name__ == "__main__":
    main()
