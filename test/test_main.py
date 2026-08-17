import json
import os
import tempfile

import pandas as pd

import s3_utils
from main import handler
from test.conftest import LANDING_BUCKET, REFINED_BUCKET, SOURCE_PREFIX


def _read_parquet_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "part.parquet")
        with open(local_path, "wb") as f:
            f.write(body)
        return pd.read_parquet(local_path, engine="fastparquet")


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
        df = _read_parquet_from_s3(s3, REFINED_BUCKET, key)
        total_rows += len(df)
        columns = df.columns.tolist()
        # Partition columns are encoded in the path, not duplicated in the file.
        assert "cliente_prefijo" not in columns
        assert "operacion_prefijo" not in columns
        assert "consumidor_id_hash" in columns
        assert "conversacion_id" in columns

        # Encryption is skipped by default (ENABLE_ENCRYPTION unset): sensitive
        # columns are written as plaintext, hashing still applies.
        for row in df.to_dict("records"):
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

    df = _read_parquet_from_s3(s3, REFINED_BUCKET, result["output_keys"][0])
    row = next(r for r in df.to_dict("records") if r["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1")

    # With the flag on, sensitive columns are no longer plaintext, and each
    # row carries the wrapped data key needed to decrypt it later.
    assert row["consumidor_id"] != "14984986"
    assert row["consumidor_id_edk"] is not None
