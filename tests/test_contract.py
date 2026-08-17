from bdo_transcripciones_etl.config import load_settings
from bdo_transcripciones_etl.contract import load_contract, load_unitary_endpoint


def test_resolve_bucket_prefixes_logical_name():
    settings = load_settings()
    assert settings.resolve_bucket("refined") == "augusta-nexa-dev-refined"
    assert settings.resolve_bucket("providers-landing") == "augusta-nexa-dev-providers-landing"


def test_resolve_bucket_is_idempotent_if_already_prefixed():
    settings = load_settings()
    assert settings.resolve_bucket("augusta-nexa-dev-refined") == "augusta-nexa-dev-refined"


def test_load_contract_exposes_source_and_output(aws):
    settings = load_settings()
    contract = load_contract(aws["s3"], settings)

    assert contract.source_bucket_logical == "providers-landing"
    assert contract.source_prefix == "external/datanexa/transacciones/empatia/transcripciones/BDO"
    assert contract.output_bucket_logical == "refined"
    assert contract.output_prefix == "transacciones/empatia/transcripciones"
    assert contract.partition_by == ["cliente_prefijo", "operacion_prefijo"]
    assert {c["name"] for c in contract.columns} >= {"conversacion_id", "consumidor_id", "mensajes"}


def test_load_unitary_endpoint_follows_core_json(aws):
    settings = load_settings()
    unitary = load_unitary_endpoint(aws["s3"], settings)

    assert "conversations_surveys" in unitary
    assert unitary["conversations_surveys"]["url"] == "/api/v2/quality/conversations/{conversationId}/surveys"
    assert unitary["conversations_surveys"]["method"] == "GET"
