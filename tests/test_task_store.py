"""Tests for RedisTaskStore and RedisJSONTaskStore."""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from google.protobuf.timestamp_pb2 import Timestamp

from a2a_redis.task_store import RedisTaskStore, RedisJSONTaskStore

from tests.conftest import TEST_CONTEXT, TEST_CONTEXT_OTHER


def _build_task(
    task_id: str = "task_123",
    context_id: str = "context_456",
    state=None,
    timestamp: datetime | None = None,
):
    """Build a protobuf Task for tests, with optional state + timestamp."""
    from a2a.types.a2a_pb2 import Task, TaskStatus
    from a2a.types.a2a_pb2 import TASK_STATE_SUBMITTED

    if state is None:
        state = TASK_STATE_SUBMITTED
    status = TaskStatus(state=state)
    if timestamp is not None:
        ts = Timestamp()
        ts.FromDatetime(timestamp)
        status.timestamp.CopyFrom(ts)
    task = Task(id=task_id, context_id=context_id)
    task.status.CopyFrom(status)
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


class TestRedisTaskStoreList:
    """Integration tests for RedisTaskStore.list() filters + pagination."""

    @staticmethod
    def _ts(seconds: int) -> datetime:
        """Build a UTC datetime offset by ``seconds`` from a fixed epoch."""
        return datetime(2024, 1, 1, tzinfo=timezone.utc).replace(
            second=seconds % 60, minute=(seconds // 60) % 60
        )

    @pytest.mark.asyncio
    async def test_list_returns_tasks_in_last_updated_desc(self, task_store):
        """Most recently-updated task comes back first."""
        from a2a.types.a2a_pb2 import ListTasksRequest

        t1 = _build_task(task_id="a", timestamp=self._ts(10))
        t2 = _build_task(task_id="b", timestamp=self._ts(20))
        t3 = _build_task(task_id="c", timestamp=self._ts(30))
        for t in (t1, t2, t3):
            await task_store.save(t, TEST_CONTEXT)

        resp = await task_store.list(ListTasksRequest(), TEST_CONTEXT)

        ids = [t.id for t in resp.tasks]
        assert ids == ["c", "b", "a"]
        assert resp.total_size == 3
        assert resp.next_page_token == ""

    @pytest.mark.asyncio
    async def test_list_paginates_across_three_pages(self, task_store):
        """page_size=3 over 7 tasks yields 3+3+1 in order, last token empty."""
        from a2a.types.a2a_pb2 import ListTasksRequest

        # Save 7 tasks with strictly increasing timestamps -> newest is t6.
        all_ids = [f"task_{i}" for i in range(7)]
        for i, tid in enumerate(all_ids):
            await task_store.save(
                _build_task(task_id=tid, timestamp=self._ts(i + 1)),
                TEST_CONTEXT,
            )
        # Expected newest-first order: 6, 5, 4, 3, 2, 1, 0.
        expected_order = [f"task_{i}" for i in reversed(range(7))]

        page1 = await task_store.list(
            ListTasksRequest(page_size=3), TEST_CONTEXT
        )
        assert [t.id for t in page1.tasks] == expected_order[0:3]
        assert page1.total_size == 7
        assert page1.page_size == 3
        assert page1.next_page_token  # non-empty

        page2 = await task_store.list(
            ListTasksRequest(page_size=3, page_token=page1.next_page_token),
            TEST_CONTEXT,
        )
        assert [t.id for t in page2.tasks] == expected_order[3:6]
        assert page2.next_page_token  # non-empty
        assert page2.next_page_token != page1.next_page_token

        page3 = await task_store.list(
            ListTasksRequest(page_size=3, page_token=page2.next_page_token),
            TEST_CONTEXT,
        )
        assert [t.id for t in page3.tasks] == expected_order[6:7]
        assert page3.next_page_token == ""

    @pytest.mark.asyncio
    async def test_list_filters_by_context_id(self, task_store):
        """Filter restricts results to the requested context_id only."""
        from a2a.types.a2a_pb2 import ListTasksRequest

        await task_store.save(
            _build_task(task_id="x1", context_id="ctx_A", timestamp=self._ts(1)),
            TEST_CONTEXT,
        )
        await task_store.save(
            _build_task(task_id="x2", context_id="ctx_B", timestamp=self._ts(2)),
            TEST_CONTEXT,
        )
        await task_store.save(
            _build_task(task_id="x3", context_id="ctx_A", timestamp=self._ts(3)),
            TEST_CONTEXT,
        )

        resp = await task_store.list(
            ListTasksRequest(context_id="ctx_A"), TEST_CONTEXT
        )
        ids = sorted(t.id for t in resp.tasks)
        assert ids == ["x1", "x3"]
        # total_size reports the underlying index size (all 3) — page is filtered.
        assert resp.total_size == 3

    @pytest.mark.asyncio
    async def test_list_filters_by_status_state(self, task_store):
        """Filter by status state returns only matching tasks."""
        from a2a.types.a2a_pb2 import (
            ListTasksRequest,
            TASK_STATE_COMPLETED,
            TASK_STATE_SUBMITTED,
        )

        await task_store.save(
            _build_task(
                task_id="s1",
                state=TASK_STATE_SUBMITTED,
                timestamp=self._ts(1),
            ),
            TEST_CONTEXT,
        )
        await task_store.save(
            _build_task(
                task_id="s2",
                state=TASK_STATE_COMPLETED,
                timestamp=self._ts(2),
            ),
            TEST_CONTEXT,
        )
        await task_store.save(
            _build_task(
                task_id="s3",
                state=TASK_STATE_COMPLETED,
                timestamp=self._ts(3),
            ),
            TEST_CONTEXT,
        )

        resp = await task_store.list(
            ListTasksRequest(status=TASK_STATE_COMPLETED), TEST_CONTEXT
        )
        ids = sorted(t.id for t in resp.tasks)
        assert ids == ["s2", "s3"]

    @pytest.mark.asyncio
    async def test_list_filters_by_status_timestamp_after(self, task_store):
        """status_timestamp_after acts as an inclusive lower bound."""
        from a2a.types.a2a_pb2 import ListTasksRequest

        t1_dt = self._ts(10)
        t2_dt = self._ts(20)
        t3_dt = self._ts(30)

        await task_store.save(
            _build_task(task_id="ts1", timestamp=t1_dt), TEST_CONTEXT
        )
        await task_store.save(
            _build_task(task_id="ts2", timestamp=t2_dt), TEST_CONTEXT
        )
        await task_store.save(
            _build_task(task_id="ts3", timestamp=t3_dt), TEST_CONTEXT
        )

        # Lower bound strictly above T1 -> only T2 and T3 should appear.
        bound = Timestamp()
        bound.FromDatetime(self._ts(15))
        req = ListTasksRequest()
        req.status_timestamp_after.CopyFrom(bound)

        resp = await task_store.list(req, TEST_CONTEXT)
        ids = sorted(t.id for t in resp.tasks)
        assert ids == ["ts2", "ts3"]

    @pytest.mark.asyncio
    async def test_list_owner_isolation(self, task_store):
        """Each owner's list() only sees their own tasks."""
        from a2a.types.a2a_pb2 import ListTasksRequest

        await task_store.save(
            _build_task(task_id="own_a", timestamp=self._ts(1)),
            TEST_CONTEXT,
        )
        await task_store.save(
            _build_task(task_id="own_b", timestamp=self._ts(2)),
            TEST_CONTEXT_OTHER,
        )

        resp_self = await task_store.list(ListTasksRequest(), TEST_CONTEXT)
        resp_other = await task_store.list(
            ListTasksRequest(), TEST_CONTEXT_OTHER
        )

        assert [t.id for t in resp_self.tasks] == ["own_a"]
        assert resp_self.total_size == 1
        assert [t.id for t in resp_other.tasks] == ["own_b"]
        assert resp_other.total_size == 1

    @pytest.mark.asyncio
    async def test_list_malformed_page_token_raises(self, task_store):
        """A garbage page_token raises InvalidParamsError (matching upstream)."""
        from a2a.types.a2a_pb2 import ListTasksRequest
        from a2a.utils.errors import InvalidParamsError

        with pytest.raises(InvalidParamsError):
            await task_store.list(
                ListTasksRequest(page_token="!!!not-base64!!!"),
                TEST_CONTEXT,
            )

    @pytest.mark.asyncio
    async def test_list_page_size_clamped(self, task_store):
        """page_size=0 falls back to DEFAULT_LIST_TASKS_PAGE_SIZE (50)."""
        from a2a.types.a2a_pb2 import ListTasksRequest
        from a2a.utils.constants import (
            DEFAULT_LIST_TASKS_PAGE_SIZE,
            MAX_LIST_TASKS_PAGE_SIZE,
        )

        # Save a single task so we can confirm the response uses the default.
        await task_store.save(
            _build_task(task_id="ps_one", timestamp=self._ts(1)),
            TEST_CONTEXT,
        )

        resp_default = await task_store.list(
            ListTasksRequest(page_size=0), TEST_CONTEXT
        )
        assert resp_default.page_size == DEFAULT_LIST_TASKS_PAGE_SIZE

        # Oversized requests are clamped to MAX_LIST_TASKS_PAGE_SIZE.
        resp_max = await task_store.list(
            ListTasksRequest(page_size=10_000), TEST_CONTEXT
        )
        assert resp_max.page_size == MAX_LIST_TASKS_PAGE_SIZE


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
