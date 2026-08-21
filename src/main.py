"""Lambda entry point.

1. Loads the contract (catalog + column mapping + technical-column
   recipes + output locations).
2. Lists+reads every source JSON file under the contract's source prefix.
3. Renames/casts columns per the contract, then deduplicates per
   `deduplication` (record_key/order_by/order_type).
4. Computes the contract's technical_columns for each surviving row, and
   splits into two output row-sets: output_core (business + core audit
   columns) and output_atts (one EAV row per business column: name/type/
   position/value, plus a leaner set of audit columns).
5. Writes both as Hive-partitioned parquet.
6. Deletes the source objects once both writes succeed.
7. Resolves the Genesys "conversation surveys" unitary endpoint and builds
   the per-conversation payload a Step Function uses to call it.
"""

import logging
import uuid
from datetime import datetime, timezone

import boto3

import src.s3_utils as s3_utils
from src.config import load_settings
from src.contract import load_contract, load_unitary_endpoint
from src.parquet_io import write_hive_parquet
from src.step_function import build_step_function_payload
from src.technical_columns import build_output_rows
from src.transform import build_rows, dedup_rows

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    settings = load_settings()
    s3_client = boto3.client("s3")

    contract = load_contract(s3_client, settings)

    source_bucket = settings.resolve_bucket(contract.source_bucket_logical)
    source_keys = s3_utils.list_json_keys(s3_client, source_bucket, contract.source_prefix)
    logger.info(
        "Found %d json files under s3://%s/%s", len(source_keys), source_bucket, contract.source_prefix
    )

    if not source_keys:
        return {
            "processed_files": 0,
            "skipped_files": [],
            "conversation_ids": [],
            "conversations": [],
        }

    records, successful_keys, skipped_keys = s3_utils.read_json_files(s3_client, source_bucket, source_keys)
    if skipped_keys:
        logger.warning(
            "Skipped %d unreadable source file(s) under s3://%s/%s",
            len(skipped_keys),
            source_bucket,
            contract.source_prefix,
        )

    rows, conversation_ids = build_rows(records, contract)
    for row, key in zip(rows, successful_keys):
        row["_source_key"] = key
    rows = dedup_rows(rows, contract.deduplication)

    execution_id = getattr(context, "aws_request_id", None) or str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    core_rows, atts_rows = build_output_rows(rows, contract, now, execution_id)

    core_bucket = settings.resolve_bucket(contract.output_core_bucket_logical)
    core_keys = write_hive_parquet(
        s3_client,
        core_rows,
        core_bucket,
        contract.output_core_prefix,
        partition_cols=contract.partition_by,
        column_types={**contract.column_types, **contract.technical_column_types},
        filename_prefix=contract.output_core_filename,
    )
    logger.info(
        "Wrote %d output_core parquet object(s) to s3://%s/%s",
        len(core_keys),
        core_bucket,
        contract.output_core_prefix,
    )

    atts_bucket = settings.resolve_bucket(contract.output_atts_bucket_logical)
    atts_keys = write_hive_parquet(
        s3_client,
        atts_rows,
        atts_bucket,
        contract.output_atts_prefix,
        partition_cols=contract.partition_by,
        column_types=contract.technical_column_types,
        filename_prefix=contract.output_atts_filename,
    )
    logger.info(
        "Wrote %d output_atts parquet object(s) to s3://%s/%s",
        len(atts_keys),
        atts_bucket,
        contract.output_atts_prefix,
    )

    # Only delete files that were actually read and processed -- unreadable
    # ones stay in the landing bucket for investigation.
    deleted_keys = s3_utils.delete_objects(s3_client, source_bucket, successful_keys)
    logger.info("Deleted %d source object(s) from s3://%s", len(deleted_keys), source_bucket)

    unitary_config = load_unitary_endpoint(s3_client, settings)
    step_function_payload = build_step_function_payload(unitary_config, conversation_ids)

    return {
        "processed_files": len(successful_keys),
        "skipped_files": skipped_keys,
        "output_core_bucket": core_bucket,
        "output_core_keys": core_keys,
        "output_atts_bucket": atts_bucket,
        "output_atts_keys": atts_keys,
        "deleted_source_files": len(deleted_keys),
        **step_function_payload,
    }
