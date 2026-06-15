"""Utilities for serializing/deserializing A2A protobuf events for Redis queues.

The v1.1 SDK exposes ``Event = Message | Task | TaskStatusUpdateEvent |
TaskArtifactUpdateEvent`` as protobuf-generated message classes (no longer
Pydantic models). v1.0 ProtoJSON switched enum encoding to SCREAMING_SNAKE_CASE;
``google.protobuf.json_format.MessageToDict``/``ParseDict`` honor that change
natively, so we use them directly and avoid any custom enum lowercasing.
"""

import json
from typing import Any, Union, cast

from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message as ProtoMessage

from a2a.types import (
    Message,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)


# Map of event type names -> proto Message class. Restricted to the v1.1 Event
# union; the v0.2 PushNotificationConfig alias is intentionally absent.
_EVENT_TYPES: dict[str, type[ProtoMessage]] = {
    "Message": Message,
    "Task": Task,
    "TaskStatusUpdateEvent": TaskStatusUpdateEvent,
    "TaskArtifactUpdateEvent": TaskArtifactUpdateEvent,
}


def serialize_event(event: Any) -> dict[str, Any]:
    """Serialize an event to a {event_type, event_data} dictionary.

    Protobuf messages are converted with ``MessageToDict`` which preserves the
    v1.0 ProtoJSON SCREAMING_SNAKE_CASE enum encoding. Non-protobuf inputs
    (legacy dicts, plain values used in tests) are passed through, falling
    back to ``model_dump`` when present for backwards compatibility.
    """
    if isinstance(event, ProtoMessage):
        event_data: Any = MessageToDict(event)
    elif hasattr(event, "model_dump"):
        event_data = event.model_dump()
    else:
        event_data = event

    return {"event_type": type(event).__name__, "event_data": event_data}


def deserialize_event(event_structure: Any) -> Any:
    """Reconstruct a protobuf event from a serialized structure.

    Returns the original ``event_data`` (or pass-through value) when the
    type is unknown or reconstruction fails.
    """
    if not isinstance(event_structure, dict) or "event_data" not in event_structure:
        return cast(Any, event_structure)

    typed_structure: dict[str, Any] = cast(dict[str, Any], event_structure)
    event_data: Any = typed_structure["event_data"]
    event_type: str | None = typed_structure.get("event_type")

    if event_type and event_type in _EVENT_TYPES:
        proto_cls = _EVENT_TYPES[event_type]
        try:
            instance = proto_cls()
            ParseDict(event_data, instance)
            return instance
        except Exception:
            return event_data

    return event_data


def serialize_to_json(data: dict[str, Any], **json_kwargs: Any) -> str:
    """Serialize dictionary to JSON string with default str conversion."""
    return json.dumps(data, default=str, **json_kwargs)


def deserialize_from_json(json_str: Union[str, bytes]) -> dict[str, Any]:
    """Deserialize JSON string/bytes to dictionary."""
    if isinstance(json_str, bytes):
        json_str = json_str.decode()
    result: dict[str, Any] = json.loads(json_str)
    return result
