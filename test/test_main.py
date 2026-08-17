import io
import json

import pyarrow.parquet as pq

import s3_utils
from main import handler
from test.conftest import LANDING_BUCKET, REFINED_BUCKET, SOURCE_PREFIX


def test_lambda_handler_end_to_end(aws, seeded_source_files):
    s3 = aws["s3"]

    result = handler({}, None)

    assert result["processed_files"] == 2
    assert result["deleted_source_files"] == 2
    assert result["output_bucket"] == REFINED_BUCKET

    # Source objects are cleaned up after a successful load.
    remaining = s3_utils.list_json_keys(s3, LANDING_BUCKET, SOURCE_PREFIX)
    assert remaining == []

    # Parquet was written Hive-partitioned by cliente_prefijo/operacion_prefijo.
    assert len(result["output_keys"]) >= 1
    for key in result["output_keys"]:
        assert key.startswith(
            "transacciones/empatia/transcripciones/cliente_prefijo=BDO/operacion_prefijo=SAC/"
        )
        assert key.endswith(".parquet")

    total_rows = 0
    for key in result["output_keys"]:
        body = s3.get_object(Bucket=REFINED_BUCKET, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        total_rows += table.num_rows
        columns = table.column_names
        # Partition columns are encoded in the path, not duplicated in the file.
        assert "cliente_prefijo" not in columns
        assert "operacion_prefijo" not in columns
        assert "consumidor_id_hash" in columns
        assert "conversacion_id" in columns

        # Encryption is skipped by default (ENABLE_ENCRYPTION unset): sensitive
        # columns are written as plaintext, hashing still applies.
        table_dict = table.to_pylist()
        for row in table_dict:
            if row["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1":
                assert row["consumidor_id"] == "14984986"
                assert json.loads(row["mensajes"])[0]["role"] == "assistant"
    assert total_rows == 2

    # Step function payload covers both conversations found in this batch.
    assert sorted(result["conversation_ids"]) == sorted(
        ["2a0db425-2370-4786-be61-1a9ee8e89855", "7b1fa3c2-91aa-4e2b-9d3a-0b2f6a7c1234"]
    )
    assert len(result["conversations"]) == 2
    for conversation in result["conversations"]:
        assert conversation["conversation_id"] in result["conversation_ids"]
        assert conversation["request_context"]["url"].endswith("/surveys")
        assert "{conversationId}" not in conversation["request_context"]["url"]


def test_lambda_handler_no_source_files_is_a_noop(aws):
    result = handler({}, None)

    assert result["processed_files"] == 0
    assert result["conversation_ids"] == []
    assert result["conversations"] == []


def test_lambda_handler_encrypts_when_explicitly_enabled(aws, seeded_source_files, monkeypatch):
    monkeypatch.setenv("ENABLE_ENCRYPTION", "true")
    s3 = aws["s3"]

    result = handler({}, None)

    body = s3.get_object(Bucket=REFINED_BUCKET, Key=result["output_keys"][0])["Body"].read()
    table = pq.read_table(io.BytesIO(body)).to_pylist()
    row = next(r for r in table if r["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1")

    # With the flag on, sensitive columns are no longer plaintext, and each
    # row carries the wrapped data key needed to decrypt it later.
    assert row["consumidor_id"] != "14984986"
    assert row["consumidor_id_edk"] is not None
