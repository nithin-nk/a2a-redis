"""Tests for protobuf event serialization/deserialization in model_utils."""

import json

import pytest
from unittest.mock import AsyncMock

from a2a.types import (
    Message,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from a2a_redis.model_utils import deserialize_event, serialize_event
from a2a_redis import RedisStreamsEventQueue


def _build_message() -> Message:
    msg = Message()
    msg.message_id = "test-123"
    return msg


def _build_task() -> Task:
    t = Task()
    t.id = "task-1"
    t.context_id = "ctx-1"
    return t


def _build_status_update() -> TaskStatusUpdateEvent:
    e = TaskStatusUpdateEvent()
    e.task_id = "task-1"
    return e


def _build_artifact_update() -> TaskArtifactUpdateEvent:
    e = TaskArtifactUpdateEvent()
    e.task_id = "task-1"
    return e


@pytest.mark.parametrize(
    "build, expected_type, expected_name",
    [
        (_build_message, Message, "Message"),
        (_build_task, Task, "Task"),
        (_build_status_update, TaskStatusUpdateEvent, "TaskStatusUpdateEvent"),
        (_build_artifact_update, TaskArtifactUpdateEvent, "TaskArtifactUpdateEvent"),
    ],
)
def test_event_serialization_roundtrip(build, expected_type, expected_name):
    """Every Event union member survives serialize -> deserialize as the same proto type."""
    original = build()

    serialized = serialize_event(original)
    assert serialized["event_type"] == expected_name
    assert isinstance(serialized["event_data"], dict)

    reconstructed = deserialize_event(serialized)
    assert isinstance(reconstructed, expected_type)
    assert reconstructed == original


@pytest.mark.asyncio
async def test_streams_queue_protobuf_model_preservation():
    """Streams dequeue rehydrates Message-tagged JSON back into a Message proto."""
    original = _build_message()
    # The JSON stored in Redis is whatever MessageToDict produced at enqueue time.
    from google.protobuf.json_format import MessageToDict

    payload = json.dumps(MessageToDict(original)).encode()

    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock()
    redis_mock.xreadgroup = AsyncMock(
        return_value=[
            (
                b"stream:test",
                [
                    (
                        b"123-0",
                        {
                            b"event_type": b"Message",
                            b"event_data": payload,
                        },
                    )
                ],
            )
        ]
    )
    redis_mock.xack = AsyncMock()
    redis_mock.xgroup_create = AsyncMock()

    queue = RedisStreamsEventQueue(redis_mock, "test")
    result = await queue.dequeue_event()

    assert isinstance(result, Message)
    assert result.message_id == "test-123"


def test_deserialize_event_non_dict_passthrough():
    """deserialize_event returns non-dict values as-is."""
    assert deserialize_event("just a string") == "just a string"
    assert deserialize_event([1, 2, 3]) == [1, 2, 3]
    assert deserialize_event(None) is None


def test_deserialize_event_dict_without_event_data():
    """deserialize_event returns dict without event_data as-is."""
    data = {"some_key": "some_value"}
    assert deserialize_event(data) == data


def test_deserialize_event_unknown_type():
    """deserialize_event returns event_data when type is unknown."""
    event_structure = {"event_type": "UnknownType", "event_data": {"field": "value"}}
    assert deserialize_event(event_structure) == {"field": "value"}


def test_deserialize_event_model_reconstruction_failure():
    """deserialize_event falls back to event_data when proto parse fails."""
    event_structure = {
        "event_type": "Message",
        "event_data": {"definitely_not_a_proto_field": "data"},
    }
    result = deserialize_event(event_structure)
    # ParseDict raises on unknown fields by default, so we fall back to event_data.
    assert result == {"definitely_not_a_proto_field": "data"}
