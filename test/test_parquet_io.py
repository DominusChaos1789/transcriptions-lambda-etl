import json
import os
import tempfile
from datetime import datetime, timezone

import duckdb
import pytest

from src.contract import Contract
from src.parquet_io import write_hive_parquet
from src.transform import build_rows
from test.conftest import load_fixture


def _read_parquet_rows(s3_client, bucket: str, key: str) -> list[dict]:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "part.parquet")
        with open(local_path, "wb") as f:
            f.write(body)
        relation = duckdb.sql(f"SELECT * FROM read_parquet('{local_path}')")
        columns = [d[0] for d in relation.description]
        return [dict(zip(columns, row)) for row in relation.fetchall()]


def _expected_date_path() -> str:
    now = datetime.now(timezone.utc)
    return f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"


@pytest.fixture
def contract():
    raw = load_fixture("bdo_sac_structure.json")
    return Contract(raw=raw)


def test_write_hive_parquet_partitions_by_configured_columns(aws, contract):
    s3 = aws["s3"]
    records = [load_fixture("sample_transcription_1.json"), load_fixture("sample_transcription_2.json")]
    rows, _ = build_rows(records, contract)

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "transacciones/empatia/transcripciones",
        partition_cols=contract.partition_by,
        timestamp_columns=contract.timestamp_columns,
        dedup=contract.deduplication,
    )

    assert len(keys) >= 1
    expected_prefix = f"transacciones/empatia/transcripciones/BDO/SAC/{_expected_date_path()}/"
    for key in keys:
        assert key.startswith(expected_prefix)
        filename = key.rsplit("/", 1)[-1]
        assert filename.startswith("transcripciones_")
        assert filename.endswith(".parquet")

    total_rows = sum(len(_read_parquet_rows(s3, "augusta-nexa-dev-refined", key)) for key in keys)
    assert total_rows == 2

    # Partition columns are encoded in the path, not duplicated in the file.
    written = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])[0]
    assert "cliente_prefijo" not in written
    assert "operacion_prefijo" not in written


def test_write_hive_parquet_casts_timestamp_columns(aws, contract):
    s3 = aws["s3"]
    records = [load_fixture("sample_transcription_1.json")]
    rows, _ = build_rows(records, contract)

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "p",
        partition_cols=contract.partition_by,
        timestamp_columns=contract.timestamp_columns,
        dedup=contract.deduplication,
    )

    written = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])[0]
    import datetime

    assert isinstance(written["interaccion_fecha_inicio"], datetime.datetime)


def test_write_hive_parquet_dedup_keeps_latest_fecha_fin(aws, contract):
    s3 = aws["s3"]
    older = load_fixture("sample_transcription_1.json")
    newer = dict(older)
    newer["exported_at"] = "2026-08-16T20:00:00-05:00"
    newer["interaction_result"] = "actualizado"

    rows, _ = build_rows([older, newer], contract)
    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "dedup",
        partition_cols=contract.partition_by,
        timestamp_columns=contract.timestamp_columns,
        dedup=contract.deduplication,
    )

    written_rows = []
    for key in keys:
        written_rows.extend(_read_parquet_rows(s3, "augusta-nexa-dev-refined", key))

    assert len(written_rows) == 1
    assert written_rows[0]["interaccion_resultado"] == "actualizado"


def test_write_hive_parquet_empty_rows_writes_nothing(aws, contract):
    s3 = aws["s3"]
    keys = write_hive_parquet(
        s3,
        [],
        "augusta-nexa-dev-refined",
        "p",
        partition_cols=contract.partition_by,
    )
    assert keys == []


def test_write_hive_parquet_without_partition_columns_writes_a_single_file(aws):
    s3 = aws["s3"]
    rows = [{"a": "x", "b": 1}, {"a": "y", "b": 2}]

    keys = write_hive_parquet(s3, rows, "augusta-nexa-dev-refined", "flat")

    assert len(keys) == 1
    assert keys[0].startswith(f"flat/{_expected_date_path()}/transcripciones_")
    assert keys[0].endswith(".parquet")
    written_rows = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])
    assert len(written_rows) == 2


def test_write_hive_parquet_json_column_round_trips(aws, contract):
    s3 = aws["s3"]
    records = [load_fixture("sample_transcription_1.json")]
    rows, _ = build_rows(records, contract)

    keys = write_hive_parquet(
        s3,
        rows,
        "augusta-nexa-dev-refined",
        "p",
        partition_cols=contract.partition_by,
        timestamp_columns=contract.timestamp_columns,
        dedup=contract.deduplication,
    )

    written = _read_parquet_rows(s3, "augusta-nexa-dev-refined", keys[0])[0]
    messages = json.loads(written["mensajes"])
    assert messages[0]["role"] == "assistant"
    assert len(messages) == 4
