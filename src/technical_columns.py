"""Computes the contract's "technical_columns" (audit/EAV metadata columns)
for each business row, and builds the two output row-sets (output_core,
output_atts) the contract defines.

Each technical column has a "calculus_type" describing how to derive its
value:

  execution_id      -> the Lambda's own AWS request id
  uniqe             -> sha256 of a fresh random UUID (surrogate key, unique
                        per record, not reproducible across reruns)
  concatetation     -> sha256(delimiter.join(resolved column values))
  field_mapping     -> first non-null value resolved from "columns", each
                        of which may be: a business row field, a contract
                        top-level field (e.g. client_prefix), a bracketed
                        nested contract reference
                        (output_core[output_name_file]), another
                        (already-computed) technical column, or the
                        sentinel "No apply" (always skipped -> null)
  file_name         -> the source S3 object's filename
  file_landing_date -> the processing date (UTC, when this Lambda runs);
                        columns whose name ends in "_id" get the date-only
                        (yyyy-mm-dd) form, others get the full ISO datetime

The four remaining calculus_types (column_name/column_type_data/
column_value/column_index_position_map) aren't computed here -- they're
per-attribute EAV fields, populated directly in build_output_rows() while
pivoting each business column into its own output_atts row.
"""

import hashlib
import os
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

from src.contract import Contract

_ATTRIBUTE_CALCULUS_TYPES = {
    "column_name",
    "column_type_data",
    "column_value",
    "column_index_position_map",
}
_EAV_FIELDS = {"atributo_nombre", "atributo_tipo", "atributo_posicion", "atributo_valor"}
_NO_APPLY = "No apply"


def _resolve_bracket_reference(contract_raw: dict, name: str) -> Optional[Any]:
    """e.g. "output_core[output_name_file]" -> contract_raw["output_core"]["output_name_file"]."""
    if not (name.endswith("]") and "[" in name):
        return None
    base, _, rest = name[:-1].partition("[")
    return contract_raw.get(base, {}).get(rest)


def _compute_one(
    name: str,
    spec: dict,
    resolve: Callable[[str], Any],
    source_filename: str,
    now: datetime,
    execution_id: str,
) -> Any:
    calculus_type = spec["calculus_type"]

    if calculus_type == "execution_id":
        return execution_id
    if calculus_type == "uniqe":
        return hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    if calculus_type == "concatetation":
        delimiter = spec.get("delimiter", "")
        parts = [str(resolve(c)) if resolve(c) is not None else "" for c in spec.get("columns", [])]
        return hashlib.sha256(delimiter.join(parts).encode("utf-8")).hexdigest()
    if calculus_type == "field_mapping":
        for candidate in spec.get("columns", []):
            if candidate == _NO_APPLY:
                continue
            value = resolve(candidate)
            if value is not None:
                return value
        return None
    if calculus_type == "file_name":
        return source_filename
    if calculus_type == "file_landing_date":
        if name.endswith("_id"):
            return now.strftime("%Y-%m-%d")
        return now.isoformat()
    if calculus_type in _ATTRIBUTE_CALCULUS_TYPES:
        return None  # computed per-attribute in build_output_rows, not here
    raise ValueError(f"Unknown calculus_type {calculus_type!r} for technical column {name!r}")


def compute_technical_columns(
    technical_columns_spec: dict,
    contract_raw: dict,
    row: dict,
    source_filename: str,
    now: datetime,
    execution_id: str,
) -> dict:
    """Computes every non-attribute technical column for one row, resolving
    cross-references between them lazily (a column's "columns" list can
    name another technical column, regardless of dict order)."""
    computed: dict[str, Any] = {}
    in_progress: set[str] = set()

    def resolve(name: str) -> Any:
        if name in computed:
            return computed[name]
        if name in row:
            return row[name]
        bracket_value = _resolve_bracket_reference(contract_raw, name)
        if bracket_value is not None:
            return bracket_value
        if name in contract_raw:
            return contract_raw[name]
        if name in technical_columns_spec:
            return render(name)
        return None

    def render(name: str) -> Any:
        if name in computed:
            return computed[name]
        if name in in_progress:
            raise ValueError(f"Circular technical_columns dependency involving {name!r}")
        in_progress.add(name)
        value = _compute_one(name, technical_columns_spec[name], resolve, source_filename, now, execution_id)
        in_progress.discard(name)
        computed[name] = value
        return value

    for name, spec in technical_columns_spec.items():
        if spec["calculus_type"] in _ATTRIBUTE_CALCULUS_TYPES:
            continue
        render(name)

    return computed


def build_output_rows(
    rows: list[dict],
    contract: Contract,
    now: datetime,
    execution_id: str,
) -> tuple[list[dict], list[dict]]:
    """Returns (core_rows, atts_rows). Each business row becomes exactly
    one core_row (business columns + output_core's kept technical columns)
    and len(contract.columns) atts_rows -- one EAV row per business
    column, carrying output_atts's kept technical columns plus
    atributo_nombre/tipo/posicion/valor. Consumes each row's "_source_key"
    bookkeeping field (main.py attaches it after build_rows) for the
    file_name calculus_type; it's popped off before appearing in output."""
    core_keep = contract.output_core_technical_columns
    atts_keep = [c for c in contract.output_atts_technical_columns if c not in _EAV_FIELDS]

    core_rows: list[dict] = []
    atts_rows: list[dict] = []

    for row in rows:
        source_key = row.pop("_source_key", "")
        source_filename = os.path.basename(source_key) if source_key else ""

        technical = compute_technical_columns(
            contract.technical_columns, contract.raw, row, source_filename, now, execution_id
        )

        core_row = dict(row)
        for name in core_keep:
            core_row[name] = technical.get(name)
        core_rows.append(core_row)

        shared_atts_fields = {name: technical.get(name) for name in atts_keep}
        for col_spec in contract.columns:
            col_name = col_spec["name"]
            value = row.get(col_name)
            atts_row = dict(shared_atts_fields)
            atts_row["atributo_nombre"] = col_name
            atts_row["atributo_tipo"] = col_spec["type"]
            atts_row["atributo_posicion"] = col_spec["position"]
            atts_row["atributo_valor"] = None if value is None else str(value)
            atts_rows.append(atts_row)

    return core_rows, atts_rows
