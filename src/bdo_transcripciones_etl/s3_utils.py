"""Thin S3 helpers: list/read/delete JSON objects, write a Hive-partitioned
parquet dataset. Kept separate from business logic so they're easy to mock
in unit tests."""
import json
import logging
from typing import Any, Iterable

import boto3

logger = logging.getLogger(__name__)

JSON_SUFFIX = ".json"


def list_json_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    """Return every key under `prefix` whose name ends in .json, across
    however many pages the listing needs."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(JSON_SUFFIX):
                keys.append(key)
    return keys


def read_json(s3_client, bucket: str, key: str) -> Any:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return json.loads(body)


def write_json(s3_client, bucket: str, key: str, payload: Any) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def delete_objects(s3_client, bucket: str, keys: Iterable[str]) -> list[str]:
    """Delete keys in batches of 1000 (the S3 delete_objects limit).
    Returns the keys that were actually deleted."""
    keys = list(keys)
    deleted: list[str] = []
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        if not batch:
            continue
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": False},
        )
        for obj in response.get("Deleted", []):
            deleted.append(obj["Key"])
        for err in response.get("Errors", []):
            logger.error("Failed to delete s3://%s/%s: %s", bucket, err["Key"], err.get("Message"))
    return deleted


def build_s3_client():
    return boto3.client("s3")
