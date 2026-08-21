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

DuckDB (not pandas/pyarrow/fastparquet) does the ingestion and parquet
write. DuckDB's read_json_auto needs a real file, not an in-memory list,
so the mapped rows are staged to a local temp NDJSON file first; the
resulting parquet file(s) are then uploaded to S3. Dedup is NOT done here
-- it happens once on the business rows in transform.py, before they're
split into output_core/output_atts, since output_atts rows don't carry
the dedup key column at all.
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


# Maps the contract's column "type" values to DuckDB SQL types. Every
# declared column gets cast explicitly rather than trusting DuckDB's
# read_json_auto type inference -- that inference over-eagerly promotes
# UUID-shaped strings (e.g. conversacion_id) to DuckDB's native UUID type,
# which most Parquet readers other than DuckDB itself don't understand
# (AWS S3 Select fails with "Unsupported Parquet type UUID" on it).
_SQL_TYPE_BY_CONTRACT_TYPE = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "timestamp": "TIMESTAMP",
    "json": "VARCHAR",
}


def _build_type_cast_sql(source_sql: str, column_types: dict[str, str]) -> str:
    """`source_sql` is a bare table reference (e.g. a table function call
    like `read_json_auto(...)`), not a full SELECT -- it can't be wrapped
    in parens as a subquery. This always returns a complete SELECT
    statement so downstream callers can safely parenthesize it."""
    castable = {
        name: _SQL_TYPE_BY_CONTRACT_TYPE[contract_type]
        for name, contract_type in column_types.items()
        if contract_type in _SQL_TYPE_BY_CONTRACT_TYPE
    }
    if not castable:
        return f"SELECT * FROM {source_sql}"
    exclude_list = ", ".join(_quote_ident(c) for c in castable)
    cast_list = ", ".join(
        f"CAST({_quote_ident(c)} AS {sql_type}) AS {_quote_ident(c)}" for c, sql_type in castable.items()
    )
    return f"SELECT * EXCLUDE ({exclude_list}), {cast_list} FROM {source_sql}"


def write_hive_parquet(
    s3_client,
    rows: list[dict],
    bucket: str,
    prefix: str,
    partition_cols: Optional[list[str]] = None,
    column_types: Optional[dict[str, str]] = None,
    filename_prefix: str = "part",
) -> list[str]:
    """Returns the list of object keys written."""
    if not rows:
        return []

    partition_cols = [c for c in (partition_cols or []) if c in rows[0]]
    # Callers may pass a broader column_types mapping than what these
    # specific rows actually carry (e.g. the contract's full set of
    # technical column types, reused across two different output shapes)
    # -- only cast columns that actually exist here.
    column_types = {k: v for k, v in (column_types or {}).items() if k in rows[0]}
    prefix = prefix.rstrip("/")

    now = datetime.now(timezone.utc)
    date_path = f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"
    filename = f"{filename_prefix}_{now.strftime('%Y%m%dT%H%M%SZ')}.parquet"

    with tempfile.TemporaryDirectory() as tmp_dir:
        ndjson_path = os.path.join(tmp_dir, "rows.jsonl")
        with open(ndjson_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        source_sql = f"read_json_auto({_quote_literal(ndjson_path)})"
        source_sql = _build_type_cast_sql(source_sql, column_types)

        written_keys = []
        with duckdb.connect() as con:
            con.execute(f"CREATE TEMP TABLE staged AS {source_sql}")

            if partition_cols:
                ident_list = ", ".join(_quote_ident(c) for c in partition_cols)
                combos = con.execute(f"SELECT DISTINCT {ident_list} FROM staged").fetchall()
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
                    f"COPY (SELECT *{exclude_clause} FROM staged{where_clause}) "
                    f"TO {_quote_literal(local_path)} (FORMAT PARQUET)"
                )
                con.execute(copy_sql, params)

                key = f"{key_prefix}/{filename}"
                with open(local_path, "rb") as fh:
                    s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())
                written_keys.append(key)

        return written_keys
