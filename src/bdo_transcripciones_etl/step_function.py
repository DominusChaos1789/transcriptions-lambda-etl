"""Builds the payload the Step Function consumes to call the Genesys
"conversation surveys" API once per conversation id found in this batch.

Resolution path: params/genesys/api/core.json -> "unitary" entry -> that
file's own content (e.g. params/genesys/api/unitary.json), which holds the
"{conversationId}" URL template this module fills in per conversation.
"""
from typing import Any


def build_step_function_payload(
    unitary_config: dict[str, Any], conversation_ids: list[str]
) -> dict[str, Any]:
    conversations = []
    for endpoint_name, endpoint_spec in unitary_config.items():
        url_template = endpoint_spec["url"]
        for conversation_id in conversation_ids:
            conversations.append(
                {
                    "conversation_id": conversation_id,
                    "endpoint": endpoint_name,
                    "request_context": {
                        "url": url_template.replace("{conversationId}", conversation_id),
                        "method": endpoint_spec.get("method"),
                        "type": endpoint_spec.get("type"),
                        "path": endpoint_spec.get("path"),
                        "result_data": endpoint_spec.get("result_data"),
                    },
                }
            )

    return {
        "conversation_ids": conversation_ids,
        "conversations": conversations,
    }
