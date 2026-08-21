"""Loads the data contract (transcripcion.json) that drives the whole ETL:
which bucket/prefix to read from, how to rename columns, how to compute
technical/audit columns, and where the two resulting parquet outputs
(output_core, output_atts) get written."""

from dataclasses import dataclass
from typing import Any

import src.s3_utils as s3_utils
from src.config import Settings

# technical_columns entries in the contract JSON only carry calculus_type/
# description/columns/delimiter -- no "type" field. This is the Lambda's
# own fixed knowledge of what SQL type each one should be cast to on
# write; anything not listed here defaults to "string".
_TECHNICAL_COLUMN_TYPES = {
    "ejecucion_id": "string",
    "registro_id": "string",
    "registro_hash": "string",
    "cliente_prefijo": "string",
    "operacion_prefijo": "string",
    "operacion_segmento": "string",
    "operacion_proceso": "string",
    "base_id": "string",
    "base_grupo": "string",
    "base_fecha_id": "string",
    "base_proceso": "string",
    "base_nombre": "string",
    "cargue_id": "string",
    "cargue_fecha": "timestamp",
    "cargue_fecha_id": "string",
    "archivo_fecha": "timestamp",
    "archivo_fecha_id": "string",
    "parquet_nombre": "string",
    "canal_id": "string",
    "canal_proveedor": "string",
    "atributo_nombre": "string",
    "atributo_tipo": "string",
    "atributo_posicion": "integer",
    "atributo_valor": "string",
}


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
    def columns(self) -> list[dict]:
        return self.raw["columns"]

    @property
    def transformations(self) -> list[dict]:
        return self.raw.get("transformations", [])

    @property
    def partition_by(self) -> list[str]:
        return self.raw.get("output", {}).get("iceberg", {}).get("partition_by", [])

    @property
    def timestamp_columns(self) -> list[str]:
        return [c["name"] for c in self.columns if c["type"] == "timestamp"]

    @property
    def column_types(self) -> dict[str, str]:
        return {c["name"]: c["type"] for c in self.columns}

    @property
    def deduplication(self) -> dict:
        return self.raw.get("deduplication", {})

    @property
    def technical_columns(self) -> dict:
        return self.raw.get("technical_columns", {})

    @property
    def technical_column_types(self) -> dict[str, str]:
        # Union of every technical column name actually referenced by
        # either output, not just the ones with a calculus_type defined --
        # a name can show up in technical_col_keep without a matching
        # technical_columns entry (a gap in the contract itself), and it
        # still needs a real SQL type instead of falling back to whatever
        # DuckDB infers for an all-NULL column (typically JSON).
        names = (
            set(self.technical_columns)
            | set(self.output_core_technical_columns)
            | set(self.output_atts_technical_columns)
        )
        return {name: _TECHNICAL_COLUMN_TYPES.get(name, "string") for name in names}

    def _output(self, key: str) -> dict:
        return self.raw[key]

    @property
    def output_core_bucket_logical(self) -> str:
        return self._output("output_core")["bucket_name"]

    @property
    def output_core_prefix(self) -> str:
        return self._output("output_core")["prefix_pattern"]

    @property
    def output_core_filename(self) -> str:
        return self._output("output_core")["output_name_file"]

    @property
    def output_core_technical_columns(self) -> list[str]:
        return self._output("output_core").get("technical_col_keep", [])

    @property
    def output_atts_bucket_logical(self) -> str:
        return self._output("output_atts")["bucket_name"]

    @property
    def output_atts_prefix(self) -> str:
        return self._output("output_atts")["prefix_pattern"]

    @property
    def output_atts_filename(self) -> str:
        return self._output("output_atts")["output_name_file"]

    @property
    def output_atts_technical_columns(self) -> list[str]:
        return self._output("output_atts").get("technical_col_keep", [])


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
