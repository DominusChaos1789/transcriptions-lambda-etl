from src.config import load_settings
from src.contract import load_contract, load_unitary_endpoint


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
    assert contract.partition_by == ["cliente_prefijo", "operacion_prefijo"]
    assert {c["name"] for c in contract.columns} >= {"conversacion_id", "consumidor_id", "mensajes"}
    assert set(contract.timestamp_columns) == {"interaccion_fecha_inicio", "interaccion_fecha_fin"}

    assert contract.output_core_bucket_logical == "refined"
    assert contract.output_core_prefix == "transacciones/parquet/transcripciones_core"
    assert contract.output_core_filename == "transcripciones"
    assert "registro_hash" in contract.output_core_technical_columns

    assert contract.output_atts_bucket_logical == "refined"
    assert contract.output_atts_prefix == "transacciones/parquet/transcripciones_atts"
    assert contract.output_atts_filename == "transcripciones"
    assert "atributo_valor" in contract.output_atts_technical_columns


def test_technical_columns_are_exposed(aws):
    settings = load_settings()
    contract = load_contract(aws["s3"], settings)

    assert contract.technical_columns["registro_hash"]["calculus_type"] == "concatetation"
    # gestion_tipo/gestion_canal are referenced by output_core.technical_col_keep
    # but have no technical_columns entry -- a real gap in the contract --
    # they should still get a safe default type rather than crashing.
    assert contract.technical_column_types["gestion_tipo"] == "string"
    assert contract.technical_column_types["registro_hash"] == "string"
    assert contract.technical_column_types["atributo_posicion"] == "integer"
    assert contract.technical_column_types["cargue_fecha"] == "timestamp"


def test_load_unitary_endpoint_follows_core_json(aws):
    settings = load_settings()
    unitary = load_unitary_endpoint(aws["s3"], settings)

    assert "conversations_surveys" in unitary
    assert unitary["conversations_surveys"]["url"] == "/api/v2/quality/conversations/{conversationId}/surveys"
    assert unitary["conversations_surveys"]["method"] == "GET"
