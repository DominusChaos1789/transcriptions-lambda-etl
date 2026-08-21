import json
import os
import tempfile
from datetime import datetime, timezone

import duckdb

import src.s3_utils as s3_utils
from src.main import handler
from test.conftest import LANDING_BUCKET, REFINED_BUCKET, SOURCE_PREFIX


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


def test_lambda_handler_end_to_end(aws, seeded_source_files):
    s3 = aws["s3"]

    result = handler({}, None)

    assert result["processed_files"] == 2
    assert result["deleted_source_files"] == 2
    assert result["output_core_bucket"] == REFINED_BUCKET
    assert result["output_atts_bucket"] == REFINED_BUCKET

    # Source objects are cleaned up after a successful load.
    remaining = s3_utils.list_json_keys(s3, LANDING_BUCKET, SOURCE_PREFIX)
    assert remaining == []

    # output_core: Hive-partitioned by cliente_prefijo/operacion_prefijo
    # (from the contract) then year=/month=/day= (processing date).
    assert len(result["output_core_keys"]) >= 1
    core_prefix = (
        f"transacciones/parquet/transcripciones_core/cliente_prefijo=BDO/"
        f"operacion_prefijo=SAC/{_expected_date_path()}/"
    )
    for key in result["output_core_keys"]:
        assert key.startswith(core_prefix)
        assert key.rsplit("/", 1)[-1].startswith("transcripciones_")

    total_core_rows = 0
    for key in result["output_core_keys"]:
        written_rows = _read_parquet_rows(s3, REFINED_BUCKET, key)
        total_core_rows += len(written_rows)
        for row in written_rows:
            assert "conversacion_id" in row
            # technical columns are present alongside the business ones
            assert "ejecucion_id" in row
            assert "registro_hash" in row
            if row["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1":
                assert row["consumidor_id"] == "14984986"
                assert json.loads(row["mensajes"])[0]["role"] == "assistant"
    assert total_core_rows == 2

    # output_atts: one EAV row per business column per source row
    # (18 columns * 2 rows).
    assert len(result["output_atts_keys"]) >= 1
    atts_prefix = (
        f"transacciones/parquet/transcripciones_atts/cliente_prefijo=BDO/"
        f"operacion_prefijo=SAC/{_expected_date_path()}/"
    )
    total_atts_rows = 0
    for key in result["output_atts_keys"]:
        assert key.startswith(atts_prefix)
        written_rows = _read_parquet_rows(s3, REFINED_BUCKET, key)
        total_atts_rows += len(written_rows)
        for row in written_rows:
            assert row["atributo_nombre"]
            assert row["atributo_tipo"]
            assert row["atributo_posicion"] is not None
    assert total_atts_rows == 18 * 2

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
    assert result["skipped_files"] == []
    assert result["conversation_ids"] == []
    assert result["conversations"] == []


def test_lambda_handler_skips_bad_file_and_processes_the_rest(aws, seeded_source_files):
    s3 = aws["s3"]
    bad_key = f"{SOURCE_PREFIX}/corrupted.json"
    s3.put_object(Bucket=LANDING_BUCKET, Key=bad_key, Body=b"")

    result = handler({}, None)

    assert result["processed_files"] == 2
    assert result["skipped_files"] == [bad_key]
    assert result["deleted_source_files"] == 2

    # The bad file is left in place for investigation; only the good ones
    # (and the source files that succeeded) are gone.
    remaining = s3_utils.list_json_keys(s3, LANDING_BUCKET, SOURCE_PREFIX)
    assert remaining == [bad_key]
