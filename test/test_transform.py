import json

import pytest

from contract import Contract
from transform import build_dataframe
from test.conftest import load_fixture


@pytest.fixture
def contract():
    raw = load_fixture("bdo_sac_structure.json")
    return Contract(raw=raw)


@pytest.fixture
def records():
    return [
        load_fixture("sample_transcription_1.json"),
        load_fixture("sample_transcription_2.json"),
    ]


def test_build_dataframe_returns_one_row_per_record(records, contract):
    df, _ = build_dataframe(records, contract)
    assert len(df) == 2


def test_conversation_ids_are_collected_from_genesys_cloud_id(records, contract):
    _, conversation_ids = build_dataframe(records, contract)
    assert conversation_ids == sorted(
        ["2a0db425-2370-4786-be61-1a9ee8e89855", "7b1fa3c2-91aa-4e2b-9d3a-0b2f6a7c1234"]
    )


def test_default_value_columns_are_stamped(records, contract):
    df, _ = build_dataframe(records, contract)
    assert (df["cliente_prefijo"] == "BDO").all()
    assert (df["operacion_prefijo"] == "SAC").all()
    assert (df["caso_uso_id"] == "temporal_id").all()
    assert (df["canal"] == "VOICEBOT").all()
    assert (df["origen"] == "GENESYS_CLOUD").all()


def test_trim_uppercase_and_remove_accents_applied_in_order(records, contract):
    df, _ = build_dataframe(records, contract)
    row2 = df[df["interaccion_id"] == "aa11bb22cc33dd44ee55ff6677889900"].iloc[0]

    # "  Andres  " -> trim -> "Andres" -> uppercase -> "ANDRES" -> no accents to strip.
    assert row2["consumidor_nombre"] == "ANDRES"
    # "Niño" -> trim (no-op) -> uppercase -> "NIÑO" -> remove_accents -> "NINO"
    assert row2["consumidor_apellido"] == "NINO"


def test_json_column_mensajes_is_serialized_to_a_string(records, contract):
    df, _ = build_dataframe(records, contract)
    row1 = df[df["interaccion_id"] == "5624561b59ae99a0fae1e65cc206e4a1"].iloc[0]

    messages = json.loads(row1["mensajes"])
    assert messages[0]["role"] == "assistant"
    assert len(messages) == 4


def test_non_required_missing_field_is_none(contract):
    record = load_fixture("sample_transcription_1.json")
    record.pop("interaction_result")
    df, _ = build_dataframe([record], contract)
    assert df.iloc[0]["interaccion_resultado"] in (None, "")


def test_deduplication_keeps_row_with_latest_fecha_fin(contract):
    older = load_fixture("sample_transcription_1.json")
    newer = dict(older)
    newer["exported_at"] = "2026-08-16T20:00:00-05:00"
    newer["interaction_result"] = "actualizado"

    df, _ = build_dataframe([older, newer], contract)

    assert len(df) == 1
    # "interaccion_resultado" isn't in the uppercase transformation's column
    # list, so only trim/remove_accents apply to it -- it stays lowercase.
    assert df.iloc[0]["interaccion_resultado"] == "actualizado"
