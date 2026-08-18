"""Writes rows to S3 as Hive-partitioned parquet, via DuckDB.

Output layout is:

    <prefix>/<col_1>=<value_1>/<col_2>=<value_2>/.../
        year=YYYY/month=MM/day=DD/transcripciones_<timestamp>.parquet

e.g. .../cliente_prefijo=BDO/operacion_prefijo=SAC/year=2026/month=08/day=18/
transcripciones_20260818T153045Z.parquet. `partition_cols` (from the
contract) and year/month/day are all real Hive-style key=value segments.
year/month/day are based on the processing date (UTC, when this function
runs), not any field in the data -- constant for the whole batch, not
contract-configurable.

Partition columns are excluded from each file's own schema (they're
already encoded in the S3 key path) -- DuckDB's native PARTITION_BY
doesn't offer that, so this groups the rows by distinct partition-column
combinations manually (SELECT DISTINCT, then one COPY ... WHERE ... per
group) instead of a single PARTITION_BY COPY.

DuckDB (not pandas/pyarrow/fastparquet) does the ingestion, dedup, and
parquet write. DuckDB's read_json_auto needs a real file, not an
in-memory list, so the mapped rows are staged to a local temp NDJSON file
first; the resulting parquet file(s) are then uploaded to S3.
"""

import json
import os
import tempfile
from datetime import datetime, timezone
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

    now = datetime.now(timezone.utc)
    date_path = f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"
    filename = f"transcripciones_{now.strftime('%Y%m%dT%H%M%SZ')}.parquet"

    with tempfile.TemporaryDirectory() as tmp_dir:
        ndjson_path = os.path.join(tmp_dir, "rows.jsonl")
        with open(ndjson_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        source_sql = f"read_json_auto({_quote_literal(ndjson_path)})"
        source_sql = _build_timestamp_cast_sql(source_sql, timestamp_columns or [])
        source_sql = _build_dedup_sql(source_sql, dedup)

        written_keys = []
        with duckdb.connect() as con:
            con.execute(f"CREATE TEMP TABLE deduped AS {source_sql}")

            if partition_cols:
                ident_list = ", ".join(_quote_ident(c) for c in partition_cols)
                combos = con.execute(f"SELECT DISTINCT {ident_list} FROM deduped").fetchall()
                exclude_clause = f" EXCLUDE ({ident_list})"
            else:
                combos = [()]
                exclude_clause = ""

            for i, combo in enumerate(combos):
                where_terms = []
                params = []
                for col, value in zip(partition_cols, combo):
                    if value is None:
                        where_terms.append(f"{_quote_ident(col)} IS NULL")
                    else:
                        where_terms.append(f"{_quote_ident(col)} = ?")
                        params.append(value)
                where_clause = f" WHERE {' AND '.join(where_terms)}" if where_terms else ""

                partition_path = "/".join(f"{col}={value}" for col, value in zip(partition_cols, combo))
                key_prefix = (
                    f"{prefix}/{partition_path}/{date_path}" if partition_path else f"{prefix}/{date_path}"
                )

                local_path = os.path.join(tmp_dir, f"part_{i}.parquet")
                copy_sql = (
                    f"COPY (SELECT *{exclude_clause} FROM deduped{where_clause}) "
                    f"TO {_quote_literal(local_path)} (FORMAT PARQUET)"
                )
                con.execute(copy_sql, params)

                key = f"{key_prefix}/{filename}"
                with open(local_path, "rb") as fh:
                    s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())
                written_keys.append(key)

        return written_keys
