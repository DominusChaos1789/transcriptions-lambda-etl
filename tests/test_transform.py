import json
import os

import pytest

from bdo_transcripciones_etl.contract import Contract
from bdo_transcripciones_etl.security import EncryptionService, hash_value
from bdo_transcripciones_etl.transform import build_dataframe
from tests.conftest import load_fixture


class FakeKmsClient:
    """Stands in for boto3's KMS client without needing moto: generates a
    real random AES key locally and 'wraps' it as a base64 marker so
    encrypt/decrypt round-trips exactly like the real service would."""

    def __init__(self):
        self._keys_by_wrapper = {}

    def generate_data_key(self, KeyId, KeySpec):
        plaintext = os.urandom(32)
        wrapper = os.urandom(16)
        self._keys_by_wrapper[wrapper] = plaintext
        return {"Plaintext": plaintext, "CiphertextBlob": wrapper}

    def decrypt(self, CiphertextBlob):
        return {"Plaintext": self._keys_by_wrapper[CiphertextBlob]}


@pytest.fixture
def contract():
    raw = load_fixture("bdo_sac_structure.json")
    return Contract(raw=raw)


@pytest.fixture
def encryption_service():
    return EncryptionService(FakeKmsClient(), "alias/data-sensitive")


@pytest.fixture
def records():
    return [
        load_fixture("sample_transcription_1.json"),
        load_fixture("sample_transcription_2.json"),
    ]


def test_build_dataframe_returns_one_row_per_record(records, contract, encryption_service):
    df, _ = build_dataframe(records, contract, encryption_service)
    assert len(df) == 2


def test_conversation_ids_are_collected_from_genesys_cloud_id(records, contract, encryption_service):
    _, conversation_ids = build_dataframe(records, contract, encryption_service)
    assert conversation_ids == sorted(
        ["2a0db425-2370-4786-be61-1a9ee8e89855", "7b1fa3c2-91aa-4e2b-9d3a-0b2f6a7c1234"]
    )


def test_default_value_columns_are_stamped(records, contract, encryption_service):
    df, _ = build_dataframe(records, contract, encryption_service)
    assert (df["cliente_prefijo"] == "BDO").all()
    assert (df["operacion_prefijo"] == "SAC").all()
    assert (df["caso_uso_id"] == "temporal_id").all()
    assert (df["canal"] == "VOICEBOT").all()
    assert (df["origen"] == "GENESYS_CLOUD").all()


def test_trim_uppercase_and_remove_accents_applied_in_order(records, contract, encryption_service):
    df, _ = build_dataframe(records, contract, encryption_service)
    row2 = df[df["interaccion_id"] == "aa11bb22cc33dd44ee55ff6677889900"].iloc[0]

    # "  Andres  " -> trim -> "Andres" -> uppercase -> "ANDRES" -> no accents to strip.
    # consumidor_nombre is encrypted, so assert on the decrypted plaintext.
    decrypted_name = encryption_service.decrypt(
        {"ciphertext": row2["consumidor_nombre"], "encrypted_data_key": row2["consumidor_nombre_edk"]},
        aad=_expected_aad(row2, contract),
    )
    assert decrypted_name == "ANDRES"

    decrypted_lastname = encryption_service.decrypt(
        {"ciphertext": row2["consumidor_apellido"], "encrypted_data_key": row2["consumidor_apellido_edk"]},
        aad=_expected_aad(row2, contract),
    )
    # "Niño" -> trim (no-op) -> uppercase -> "NIÑO" -> remove_accents -> "NINO"
    assert decrypted_lastname == "NINO"


def test_hash_columns_store_sha256_of_final_value(records, contract, encryption_service):
    df, _ = build_dataframe(records, contract, encryption_service)
    row1 = df[df["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1"].iloc[0]

    assert row1["consumidor_id_hash"] == hash_value("14984986")


def test_encrypted_columns_do_not_store_cleartext_pii(records, contract, encryption_service):
    df, _ = build_dataframe(records, contract, encryption_service)
    row1 = df[df["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1"].iloc[0]

    assert row1["consumidor_id"] != "14984986"
    assert row1["consumidor_nombre"] != "VIMARASIO"


def test_json_column_mensajes_is_encrypted_and_round_trips(records, contract, encryption_service):
    df, _ = build_dataframe(records, contract, encryption_service)
    row1 = df[df["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1"].iloc[0]

    decrypted = encryption_service.decrypt(
        {"ciphertext": row1["mensajes"], "encrypted_data_key": row1["mensajes_edk"]},
        aad=_expected_aad(row1, contract),
    )
    messages = json.loads(decrypted)
    assert messages[0]["role"] == "assistant"
    assert len(messages) == 4


def test_non_required_missing_field_is_none(contract, encryption_service):
    record = load_fixture("sample_transcription_1.json")
    record.pop("interaction_result")
    df, _ = build_dataframe([record], contract, encryption_service)
    assert df.iloc[0]["interaccion_resultado"] in (None, "")


def test_deduplication_keeps_row_with_latest_fecha_fin(contract, encryption_service):
    older = load_fixture("sample_transcription_1.json")
    newer = dict(older)
    newer["exported_at"] = "2026-08-16T20:00:00-05:00"
    newer["interaction_result"] = "actualizado"

    df, _ = build_dataframe([older, newer], contract, encryption_service)

    assert len(df) == 1
    # "interaccion_resultado" isn't in the uppercase transformation's column
    # list, so only trim/remove_accents apply to it -- it stays lowercase.
    assert df.iloc[0]["interaccion_resultado"] == "actualizado"


def _expected_aad(row, contract) -> str:
    aad_columns = contract.encryption.get("aad_columns", [])
    parts = []
    for col in aad_columns:
        if col in row.index:
            parts.append(str(row[col]))
        else:
            parts.append(str(contract.raw.get(col, "")))
    return "|".join(parts)
