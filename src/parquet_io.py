"""Writes a DataFrame to S3 as a Hive-partitioned parquet dataset, e.g.:

    s3://bucket/prefix/cliente_prefijo=BDO/operacion_prefijo=SAC/part-<uuid>.parquet

Uses fastparquet (not pyarrow) -- fastparquet has no dependency on the
Apache Arrow C++ library, which was failing to build from source in the
Lambda CI image. fastparquet writes to a real file path rather than an
in-memory buffer, so each partition is staged in a temp dir and uploaded
to S3 afterwards.
"""

import os
import tempfile
import uuid
from typing import Optional

import fastparquet
import pandas as pd


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
        partition_path = "/".join(f"{col}={value}" for col, value in zip(partition_cols, group_values))
        data_df = group_df.drop(columns=partition_cols)
        key = _write_partition(s3_client, data_df, bucket, f"{prefix}/{partition_path}")
        written_keys.append(key)

    return written_keys


def _write_partition(s3_client, df: pd.DataFrame, bucket: str, prefix: str) -> str:
    key = f"{prefix}/part-{uuid.uuid4().hex}.parquet"
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "part.parquet")
        fastparquet.write(local_path, df, compression="SNAPPY", write_index=False)
        with open(local_path, "rb") as f:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f.read())
    return key
