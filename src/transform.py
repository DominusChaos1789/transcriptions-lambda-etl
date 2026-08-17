"""Turns raw landing-bucket transcription JSON records into rows matching
the contract's target schema: rename via `columns`, stamp constants, apply
`transformations`, hash/encrypt sensitive fields, and hand back the parquet-
ready rows plus the list of conversation ids seen in this batch."""

import json
import unicodedata
from typing import Any

import pandas as pd

from contract import Contract
from security import hash_value

CONVERSATION_ID_COLUMN = "conversacion_id"


def _cast_value(raw_value: Any, col_type: str) -> Any:
    if raw_value is None:
        return None
    if col_type == "string":
        return str(raw_value)
    if col_type == "integer":
        return int(raw_value)
    if col_type == "timestamp":
        return pd.to_datetime(raw_value, utc=True)
    if col_type == "json":
        return raw_value
    return raw_value


def _remove_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def _map_record(record: dict, contract: Contract) -> dict:
    row: dict[str, Any] = {}
    for col_spec in contract.columns:
        name = col_spec["name"]
        if "source_column" in col_spec:
            raw_value = record.get(col_spec["source_column"])
            row[name] = _cast_value(raw_value, col_spec["type"])
        else:
            row[name] = col_spec.get("default_value")
    return row


def _apply_transformations(row: dict, contract: Contract) -> dict:
    for rule in contract.transformations:
        rule_type = rule["type"]
        if rule.get("selector") == "all-string":
            target_columns = [k for k, v in row.items() if isinstance(v, str)]
        else:
            target_columns = rule.get("columns", [])

        for col in target_columns:
            value = row.get(col)
            if not isinstance(value, str):
                continue
            if rule_type == "trim":
                row[col] = value.strip()
            elif rule_type == "uppercase":
                row[col] = value.upper()
            elif rule_type == "remove_accents":
                row[col] = _remove_accents(value)
    return row


def _build_aad(row: dict, contract: Contract) -> str:
    aad_columns = contract.encryption.get("aad_columns", [])
    parts = []
    for col in aad_columns:
        if col in row:
            parts.append(str(row[col]))
        else:
            parts.append(str(contract.raw.get(col, "")))
    return "|".join(parts)


def _apply_security(row: dict, contract: Contract, encryption_service) -> dict:
    columns_by_name = {c["name"]: c for c in contract.columns}
    encrypted_columns = set(contract.encryption.get("columns", []))

    for name, col_spec in columns_by_name.items():
        value = row.get(name)
        if value is None:
            continue

        if col_spec.get("hash"):
            row[f"{name}_hash"] = hash_value(str(value))

        if col_spec.get("encrypt") and name in encrypted_columns:
            plaintext = json.dumps(value, ensure_ascii=False) if col_spec["type"] == "json" else str(value)
            aad = _build_aad(row, contract)
            envelope = encryption_service.encrypt(plaintext, aad)
            row[name] = envelope["ciphertext"]
            row[f"{name}_edk"] = envelope.get("encrypted_data_key")
        elif col_spec["type"] == "json":
            row[name] = json.dumps(value, ensure_ascii=False)

    return row


def _deduplicate(df: pd.DataFrame, contract: Contract) -> pd.DataFrame:
    dedup = contract.deduplication
    if not dedup.get("enabled"):
        return df

    order_by = dedup.get("order_by", [])
    order_type = dedup.get("order_type", [])
    ascending = [ot.lower() != "desc" for ot in order_type] or True
    if order_by:
        df = df.sort_values(by=order_by, ascending=ascending)

    record_key = dedup.get("record_key", [])
    if record_key:
        df = df.drop_duplicates(subset=record_key, keep="first")
    return df


def build_dataframe(
    records: list[dict], contract: Contract, encryption_service
) -> tuple[pd.DataFrame, list[str]]:
    """Maps+cleans+secures every record, returning the resulting DataFrame
    and the deduplicated list of conversacion_id values seen."""
    rows = []
    conversation_ids: list[str] = []

    for record in records:
        row = _map_record(record, contract)
        row = _apply_transformations(row, contract)
        conversation_id = row.get(CONVERSATION_ID_COLUMN)
        if conversation_id:
            conversation_ids.append(conversation_id)
        row = _apply_security(row, contract, encryption_service)
        rows.append(row)

    df = pd.DataFrame(rows)
    df = _deduplicate(df, contract)

    unique_conversation_ids = sorted(set(conversation_ids))
    return df, unique_conversation_ids
