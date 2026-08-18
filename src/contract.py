"""Loads the data contract (bdo_sac_structure.json) that drives the whole
ETL: which bucket/prefix to read from, how to rename columns, and where the
resulting parquet table gets written."""

from dataclasses import dataclass
from typing import Any

import s3_utils
from config import Settings


@dataclass(frozen=True)
class Contract:
    raw: dict

    @property
    def source_bucket_logical(self) -> str:
        return self.raw["source"]["bucket_name"]

    @property
    def source_prefix(self) -> str:
        return self.raw["source"]["prefix_pattern"]

    @property
    def output_bucket_logical(self) -> str:
        return self.raw["output_append"]["bucket_name"]

    @property
    def output_prefix(self) -> str:
        return self.raw["output_append"]["prefix_pattern"]

    @property
    def columns(self) -> list[dict]:
        return self.raw["columns"]

    @property
    def transformations(self) -> list[dict]:
        return self.raw.get("transformations", [])

    @property
    def partition_by(self) -> list[str]:
        return self.raw.get("output", {}).get("iceberg", {}).get("partition_by", [])

    @property
    def deduplication(self) -> dict:
        return self.raw.get("deduplication", {})


def load_contract(s3_client, settings: Settings) -> Contract:
    raw = s3_utils.read_json(s3_client, settings.resources_bucket, settings.contract_key)
    return Contract(raw=raw)


def load_unitary_endpoint(s3_client, settings: Settings) -> dict[str, Any]:
    """Resolves params/genesys/api/core.json -> the "unitary" entry ->
    fetches that unitary.json file and returns its contents."""
    core = s3_utils.read_json(s3_client, settings.resources_bucket, settings.core_config_key)
    unitary_paths = core.get("unitary", [])
    if not unitary_paths:
        raise ValueError(f"core config at {settings.core_config_key} has no 'unitary' entry")
    unitary_key = unitary_paths[0]
    return s3_utils.read_json(s3_client, settings.resources_bucket, unitary_key)
