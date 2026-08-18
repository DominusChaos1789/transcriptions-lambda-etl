"""Writes rows to S3 as Hive-partitioned parquet, via DuckDB.

DuckDB (not pandas/pyarrow/fastparquet) does the ingestion, dedup, and
partitioned parquet write in a single COPY statement. DuckDB's
read_json_auto needs a real file, not an in-memory list, so the mapped
rows are staged to a local temp NDJSON file first; the resulting parquet
file(s) are then walked and uploaded to S3.

Note: partition columns (e.g. cliente_prefijo, operacion_prefijo) end up
both in the S3 key path AND as regular columns inside each written file --
DuckDB's PARTITION_BY requires the column to be present in the query to
partition by it, and doesn't offer a way to drop it afterwards without a
second read/write pass per file. This is a valid, common pattern (Athena/
Glue/Spark all handle it fine); it's just slightly redundant storage.
"""

import json
import os
import tempfile
import uuid
from typing import Optional

import duckdb


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_timestamp_cast_sql(source_sql: str, timestamp_columns: list[str]) -> str:
    """`source_sql` is a bare table reference (e.g. a table function call
    like `read_json_auto(...)`), not a full SELECT -- it can't be wrapped
    in parens as a subquery. This always returns a complete SELECT
    statement so downstream callers can safely parenthesize it."""
    if not timestamp_columns:
        return f"SELECT * FROM {source_sql}"
    exclude_list = ", ".join(_quote_ident(c) for c in timestamp_columns)
    cast_list = ", ".join(
        f"CAST({_quote_ident(c)} AS TIMESTAMP) AS {_quote_ident(c)}" for c in timestamp_columns
    )
    return f"SELECT * EXCLUDE ({exclude_list}), {cast_list} FROM {source_sql}"


def _build_dedup_sql(source_sql: str, dedup: Optional[dict]) -> str:
    if not dedup or not dedup.get("enabled"):
        return source_sql

    record_key = dedup.get("record_key", [])
    if not record_key:
        return source_sql

    partition_clause = ", ".join(_quote_ident(c) for c in record_key)

    order_by = dedup.get("order_by", [])
    order_type = dedup.get("order_type", [])
    order_terms = []
    for i, col in enumerate(order_by):
        direction = "DESC" if i < len(order_type) and order_type[i].lower() == "desc" else "ASC"
        order_terms.append(f"{_quote_ident(col)} {direction}")
    order_clause = ", ".join(order_terms) if order_terms else "1"

    return (
        f"SELECT * FROM ({source_sql}) "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY {order_clause}) = 1"
    )


def write_hive_parquet(
    s3_client,
    rows: list[dict],
    bucket: str,
    prefix: str,
    partition_cols: Optional[list[str]] = None,
    timestamp_columns: Optional[list[str]] = None,
    dedup: Optional[dict] = None,
) -> list[str]:
    """Returns the list of object keys written."""
    if not rows:
        return []

    partition_cols = [c for c in (partition_cols or []) if c in rows[0]]
    prefix = prefix.rstrip("/")

    with tempfile.TemporaryDirectory() as tmp_dir:
        ndjson_path = os.path.join(tmp_dir, "rows.jsonl")
        with open(ndjson_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        source_sql = f"read_json_auto({_quote_literal(ndjson_path)})"
        source_sql = _build_timestamp_cast_sql(source_sql, timestamp_columns or [])
        source_sql = _build_dedup_sql(source_sql, dedup)

        if partition_cols:
            output_dir = os.path.join(tmp_dir, "output")
            partition_clause = f", PARTITION_BY ({', '.join(_quote_ident(c) for c in partition_cols)})"
            copy_sql = (
                f"COPY ({source_sql}) TO {_quote_literal(output_dir)} (FORMAT PARQUET{partition_clause})"
            )

            with duckdb.connect() as con:
                con.execute(copy_sql)

            written_keys = []
            for root, _dirs, files in os.walk(output_dir):
                for filename in sorted(files):
                    if not filename.endswith(".parquet"):
                        continue
                    local_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(local_path, output_dir).replace(os.sep, "/")
                    key = f"{prefix}/{relative_path}"
                    with open(local_path, "rb") as fh:
                        s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())
                    written_keys.append(key)
            return written_keys

        local_path = os.path.join(tmp_dir, f"part-{uuid.uuid4().hex}.parquet")
        copy_sql = f"COPY ({source_sql}) TO {_quote_literal(local_path)} (FORMAT PARQUET)"
        with duckdb.connect() as con:
            con.execute(copy_sql)

        key = f"{prefix}/{os.path.basename(local_path)}"
        with open(local_path, "rb") as fh:
            s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())
        return [key]
