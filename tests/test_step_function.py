from step_function import build_step_function_payload
from tests.conftest import load_fixture


def test_build_step_function_payload_replaces_conversation_id():
    unitary_config = load_fixture("unitary.json")
    conversation_ids = ["2a0db425-2370-4786-be61-1a9ee8e89855", "7b1fa3c2-91aa-4e2b-9d3a-0b2f6a7c1234"]

    payload = build_step_function_payload(unitary_config, conversation_ids)

    assert payload["conversation_ids"] == conversation_ids
    assert len(payload["conversations"]) == 2

    first = payload["conversations"][0]
    assert first["conversation_id"] == conversation_ids[0]
    assert first["endpoint"] == "conversations_surveys"
    assert (
        first["request_context"]["url"]
        == f"/api/v2/quality/conversations/{conversation_ids[0]}/surveys"
    )
    assert first["request_context"]["method"] == "GET"
    assert first["request_context"]["result_data"] == "state"
    assert "{conversationId}" not in first["request_context"]["url"]


def test_build_step_function_payload_empty_conversations_yields_empty_list():
    unitary_config = load_fixture("unitary.json")

    payload = build_step_function_payload(unitary_config, [])

    assert payload["conversation_ids"] == []
    assert payload["conversations"] == []
