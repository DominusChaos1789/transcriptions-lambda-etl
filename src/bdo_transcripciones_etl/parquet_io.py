"""Writes a DataFrame to S3 as a Hive-partitioned parquet dataset, e.g.:

    s3://bucket/prefix/cliente_prefijo=BDO/operacion_prefijo=SAC/part-<uuid>.parquet

Implemented directly with pyarrow + boto3 (no s3fs/awswrangler dependency)
so it works the same against moto in tests as it does against real S3.
"""
import io
import uuid
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_hive_parquet(
    s3_client,
    df: pd.DataFrame,
    bucket: str,
    prefix: str,
    partition_cols: Optional[list[str]] = None,
) -> list[str]:
    """Returns the list of object keys written."""
    partition_cols = [c for c in (partition_cols or []) if c in df.columns]
    prefix = prefix.rstrip("/")
    written_keys: list[str] = []

    if not partition_cols:
        return [_write_partition(s3_client, df, bucket, prefix)]

    for group_values, group_df in df.groupby(partition_cols, dropna=False):
        if not isinstance(group_values, tuple):
            group_values = (group_values,)
        partition_path = "/".join(
            f"{col}={value}" for col, value in zip(partition_cols, group_values)
        )
        data_df = group_df.drop(columns=partition_cols)
        key = _write_partition(s3_client, data_df, bucket, f"{prefix}/{partition_path}")
        written_keys.append(key)

    return written_keys


def _write_partition(s3_client, df: pd.DataFrame, bucket: str, prefix: str) -> str:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    key = f"{prefix}/part-{uuid.uuid4().hex}.parquet"
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return key
