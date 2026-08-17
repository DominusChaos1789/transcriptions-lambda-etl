"""Lambda entry point.

1. Loads the BDO SAC contract (catalog + column mapping + output location).
2. Lists+reads every *.json transcription under the contract's source
   prefix in the landing bucket.
3. Renames/casts/hashes/encrypts columns per the contract, producing a
   Hive-partitioned parquet dataset in the refined bucket.
4. Deletes the source objects once the parquet write has succeeded.
5. Resolves the Genesys "conversation surveys" unitary endpoint and builds
   the per-conversation payload a Step Function uses to call it.
"""
import logging

import boto3

import s3_utils
from config import Settings, load_settings
from contract import load_contract, load_unitary_endpoint
from parquet_io import write_hive_parquet
from security import NoOpEncryptionService
from step_function import build_step_function_payload
from transform import build_dataframe

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _build_encryption_service(contract, settings: Settings):
    # Encryption is skipped for now -- sensitive columns are written as
    # plaintext (hashing still applies). Set ENABLE_ENCRYPTION=true once
    # KMS/cryptography wiring is ready; only then does `security.py`'s
    # cryptography-based EncryptionService get imported and used.
    if not settings.enable_encryption or not contract.encryption.get("enabled"):
        return NoOpEncryptionService()

    from security import EncryptionService

    kms_client = boto3.client("kms")
    return EncryptionService(kms_client, contract.encryption["kms_key_alias"])


def handler(event, context):
    settings = load_settings()
    s3_client = boto3.client("s3")

    contract = load_contract(s3_client, settings)

    source_bucket = settings.resolve_bucket(contract.source_bucket_logical)
    source_keys = s3_utils.list_json_keys(s3_client, source_bucket, contract.source_prefix)
    logger.info("Found %d json files under s3://%s/%s", len(source_keys), source_bucket, contract.source_prefix)

    if not source_keys:
        return {
            "processed_files": 0,
            "conversation_ids": [],
            "conversations": [],
        }

    records = [s3_utils.read_json(s3_client, source_bucket, key) for key in source_keys]

    encryption_service = _build_encryption_service(contract, settings)
    df, conversation_ids = build_dataframe(records, contract, encryption_service)

    output_bucket = settings.resolve_bucket(contract.output_bucket_logical)
    written_keys = write_hive_parquet(
        s3_client,
        df,
        output_bucket,
        contract.output_prefix,
        partition_cols=contract.partition_by,
    )
    logger.info("Wrote %d parquet object(s) to s3://%s/%s", len(written_keys), output_bucket, contract.output_prefix)

    deleted_keys = s3_utils.delete_objects(s3_client, source_bucket, source_keys)
    logger.info("Deleted %d source object(s) from s3://%s", len(deleted_keys), source_bucket)

    unitary_config = load_unitary_endpoint(s3_client, settings)
    step_function_payload = build_step_function_payload(unitary_config, conversation_ids)

    return {
        "processed_files": len(source_keys),
        "output_bucket": output_bucket,
        "output_keys": written_keys,
        "deleted_source_files": len(deleted_keys),
        **step_function_payload,
    }
