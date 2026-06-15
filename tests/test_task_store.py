"""Tests for RedisTaskStore and RedisJSONTaskStore."""

import json
import pytest
from unittest.mock import MagicMock

from a2a_redis.task_store import RedisTaskStore, RedisJSONTaskStore

from tests.conftest import TEST_CONTEXT, TEST_CONTEXT_OTHER


def _build_task(task_id: str = "task_123", context_id: str = "context_456"):
    """Build a protobuf Task for tests."""
    from a2a.types.a2a_pb2 import Task, TaskStatus
    from a2a.types.a2a_pb2 import TASK_STATE_SUBMITTED

    task = Task(id=task_id, context_id=context_id)
    task.status.CopyFrom(TaskStatus(state=TASK_STATE_SUBMITTED))
    return task


class TestRedisTaskStore:
    """Unit-ish tests against the real Redis fixture for RedisTaskStore."""

    def test_init(self, redis_client):
        """Test RedisTaskStore initialization."""
        store = RedisTaskStore(redis_client, prefix="test:")
        assert store.redis is redis_client
        assert store.prefix == "test:"

    def test_task_key_generation(self, redis_client):
        """Test owner-scoped task key generation."""
        store = RedisTaskStore(redis_client, prefix="task:")
        assert store._task_key("alice", "123") == "task:alice:123"

    @pytest.mark.asyncio
    async def test_save_then_get_round_trip(self, task_store):
        """Save under TEST_CONTEXT and get back an equivalent Task."""
        task = _build_task(task_id="rt_task")

        await task_store.save(task, TEST_CONTEXT)
        loaded = await task_store.get("rt_task", TEST_CONTEXT)

        assert loaded is not None
        assert loaded.id == task.id
        assert loaded.context_id == task.context_id
        assert loaded.status.state == task.status.state

    @pytest.mark.asyncio
    async def test_get_returns_none_for_other_owner(self, task_store):
        """Owner isolation: another owner cannot see this owner's task."""
        task = _build_task(task_id="iso_task")

        await task_store.save(task, TEST_CONTEXT)
        loaded = await task_store.get("iso_task", TEST_CONTEXT_OTHER)

        assert loaded is None

    @pytest.mark.asyncio
    async def test_delete_under_wrong_owner_is_noop(self, task_store):
        """Delete invoked by a non-owner must leave the original task intact."""
        task = _build_task(task_id="del_iso_task")

        await task_store.save(task, TEST_CONTEXT)
        await task_store.delete("del_iso_task", TEST_CONTEXT_OTHER)

        loaded = await task_store.get("del_iso_task", TEST_CONTEXT)
        assert loaded is not None
        assert loaded.id == "del_iso_task"

    @pytest.mark.asyncio
    async def test_delete_under_correct_owner(self, task_store):
        """Owner-correct delete removes the task."""
        task = _build_task(task_id="del_task")

        await task_store.save(task, TEST_CONTEXT)
        await task_store.delete("del_task", TEST_CONTEXT)

        loaded = await task_store.get("del_task", TEST_CONTEXT)
        assert loaded is None

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self, task_store):
        """Getting a nonexistent task_id returns None."""
        loaded = await task_store.get("never_saved", TEST_CONTEXT)
        assert loaded is None

    @pytest.mark.asyncio
    async def test_protocol_version_preserved(self, task_store, redis_client):
        """protocol_version metadata is persisted on the stored hash."""
        task = _build_task(task_id="pv_task")
        await task_store.save(task, TEST_CONTEXT)

        # task_store fixture uses prefix="test_task:" and TEST_CONTEXT user is 'test_user'
        key = "test_task:test_user:pv_task"
        stored = await redis_client.hgetall(key)
        # Keys come back as bytes from real Redis (decode_responses=False).
        assert stored.get(b"protocol_version") == b"1.0"
        # task_payload should be valid JSON we can round-trip back to a Task.
        payload = json.loads(stored[b"task_payload"].decode())
        assert payload["id"] == "pv_task"

    @pytest.mark.asyncio
    async def test_list_raises_not_implemented(self, task_store):
        """list() is intentionally deferred to Slice 2."""
        from a2a.types.a2a_pb2 import ListTasksRequest

        with pytest.raises(NotImplementedError):
            await task_store.list(ListTasksRequest(), TEST_CONTEXT)


class TestRedisJSONTaskStore:
    """Tests for RedisJSONTaskStore."""

    def test_init(self, mock_redis):
        """Test RedisJSONTaskStore initialization."""
        store = RedisJSONTaskStore(mock_redis, prefix="json:")
        assert store.redis == mock_redis
        assert store.prefix == "json:"

    @pytest.mark.asyncio
    async def test_save_task(self, mock_redis, sample_task_data):
        """Test task saving with JSON."""
        from a2a.types import Task

        # Create Task object from sample data
        task = Task(**sample_task_data)

        store = RedisJSONTaskStore(mock_redis)
        await store.save(task)

        mock_redis.json.assert_called_once()
        # The save method serializes the task using model_dump()
        expected_data = task.model_dump()
        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.set.assert_called_once_with("task:task_123", "$", expected_data)

    @pytest.mark.asyncio
    async def test_save_task_with_context(self, mock_redis, sample_task_data):
        """Test that save() accepts context parameter (SDK v0.3.x compatibility)."""
        from a2a.types import Task
        from a2a.server.context import ServerCallContext

        task = Task(**sample_task_data)
        context = MagicMock(spec=ServerCallContext)

        store = RedisJSONTaskStore(mock_redis)
        # This should not raise TypeError
        await store.save(task, context)

        mock_redis.json.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_task_with_context(self, mock_redis):
        """Test that get() accepts context parameter (SDK v0.3.x compatibility)."""
        from a2a.server.context import ServerCallContext

        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = None
        context = MagicMock(spec=ServerCallContext)

        store = RedisJSONTaskStore(mock_redis)
        # This should not raise TypeError
        await store.get("task_123", context)

        mock_json.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_task_with_context(self, mock_redis):
        """Test that delete() accepts context parameter (SDK v0.3.x compatibility)."""
        from a2a.server.context import ServerCallContext

        context = MagicMock(spec=ServerCallContext)

        store = RedisJSONTaskStore(mock_redis)
        # This should not raise TypeError
        await store.delete("task_123", context)

        mock_redis.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_task_exists(self, mock_redis, sample_task_data):
        """Test retrieving an existing task with JSON."""
        from a2a.types import Task

        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = sample_task_data

        store = RedisJSONTaskStore(mock_redis)
        result = await store.get("task_123")

        assert isinstance(result, Task)
        assert result.id == "task_123"
        assert result.context_id == "context_456"
        mock_json.get.assert_called_once_with("task:task_123")

    @pytest.mark.asyncio
    async def test_get_task_redis_error(self, mock_redis):
        """Test retrieving task when Redis JSON operation fails."""
        mock_json = MagicMock()
        mock_json.get.side_effect = Exception("Redis error")
        mock_redis.json.return_value = mock_json

        store = RedisJSONTaskStore(mock_redis)
        result = await store.get("task_123")

        assert result is None

    @pytest.mark.asyncio
    async def test_delete_task(self, mock_redis):
        """Test task deletion with JSON."""
        mock_redis.delete.return_value = 1

        store = RedisJSONTaskStore(mock_redis)
        await store.delete("task_123")
        mock_redis.delete.assert_called_once_with("task:task_123")

    @pytest.mark.asyncio
    async def test_update_task_exists(self, mock_redis, sample_task_data):
        """Test updating an existing task with JSON."""
        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = sample_task_data

        from a2a.types import TaskStatus, TaskState

        store = RedisJSONTaskStore(mock_redis)
        updates = {"status": TaskStatus(state=TaskState.completed)}
        result = await store.update_task("task_123", updates)

        assert result is True
        # Should fetch, update, and save
        mock_json.get.assert_called_once_with("task:task_123")
        mock_json.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_task_not_exists(self, mock_redis):
        """Test updating a non-existent task with JSON."""
        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = None

        store = RedisJSONTaskStore(mock_redis)
        result = await store.update_task("nonexistent", {"status": "completed"})

        assert result is False

    @pytest.mark.asyncio
    async def test_list_task_ids(self, mock_redis):
        """Test listing task IDs."""
        mock_redis.keys.return_value = [b"task:123", b"task:456"]

        store = RedisJSONTaskStore(mock_redis)
        result = await store.list_task_ids()

        assert result == ["123", "456"]
        mock_redis.keys.assert_called_once_with("task:*")

    @pytest.mark.asyncio
    async def test_task_exists(self, mock_redis):
        """Test checking if task exists."""
        mock_redis.exists.return_value = True

        store = RedisJSONTaskStore(mock_redis)
        result = await store.task_exists("task_123")

        assert result is True
        mock_redis.exists.assert_called_once_with("task:task_123")

    @pytest.mark.asyncio
    async def test_get_task_returns_list(self, mock_redis, sample_task_data):
        """Test retrieving task when JSON.GET returns a list (JSONPath result)."""
        from a2a.types import Task

        mock_json = mock_redis.json.return_value
        # Simulate JSONPath returning a list
        mock_json.get.return_value = [sample_task_data]

        store = RedisJSONTaskStore(mock_redis)
        result = await store.get("task_123")

        assert isinstance(result, Task)
        assert result.id == "task_123"

    @pytest.mark.asyncio
    async def test_get_task_returns_unexpected_type(self, mock_redis):
        """Test retrieving task when JSON.GET returns unexpected type."""
        mock_json = mock_redis.json.return_value
        # Simulate JSONPath returning something unexpected (e.g., a string or number)
        mock_json.get.return_value = "unexpected"

        store = RedisJSONTaskStore(mock_redis)
        result = await store.get("task_123")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_task_returns_empty_list(self, mock_redis):
        """Test retrieving task when JSON.GET returns empty list."""
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = []

        store = RedisJSONTaskStore(mock_redis)
        result = await store.get("task_123")

        # Empty list should be treated as no result
        assert result is None

    @pytest.mark.asyncio
    async def test_update_task_exception(self, mock_redis, sample_task_data):
        """Test update_task returns False when exception occurs during update."""
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = sample_task_data
        # Simulate error during save
        mock_json.set.side_effect = Exception("Connection error")

        store = RedisJSONTaskStore(mock_redis)
        result = await store.update_task("task_123", {"status": "completed"})

        assert result is False
