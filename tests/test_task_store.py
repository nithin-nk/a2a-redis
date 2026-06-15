"""Tests for RedisTaskStore and RedisJSONTaskStore."""

import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from a2a_redis.task_store import RedisTaskStore, RedisJSONTaskStore

from tests.conftest import TEST_CONTEXT, TEST_CONTEXT_OTHER


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


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


async def _redis_json_available(redis_client) -> bool:
    """Return True iff the connected Redis has the RedisJSON module loaded."""
    try:
        modules = await redis_client.execute_command("MODULE", "LIST")
    except Exception:
        return False
    for entry in modules or []:
        # MODULE LIST returns a list of arrays; the second element is the name.
        if isinstance(entry, (list, tuple)):
            for i, item in enumerate(entry):
                if isinstance(item, bytes):
                    item = item.decode()
                if isinstance(item, str) and item.lower() == "name":
                    name = entry[i + 1] if i + 1 < len(entry) else None
                    if isinstance(name, bytes):
                        name = name.decode()
                    if name and "json" in name.lower():
                        return True
        elif isinstance(entry, dict):
            name = entry.get(b"name") or entry.get("name")
            if isinstance(name, bytes):
                name = name.decode()
            if name and "json" in name.lower():
                return True
    return False


# ---------------------------------------------------------------------------
# Parametrized store fixture: runs each behavioural test under both backends.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(params=["hash", "json"])
async def task_store(request, redis_client):
    """Yield a TaskStore wired against the shared redis_client.

    Parametrized over both backends so contract-level tests run once per
    implementation. Skips the ``json`` parameter when the connected Redis does
    not have the RedisJSON module loaded.
    """
    backend = request.param
    if backend == "json":
        if not await _redis_json_available(redis_client):
            pytest.skip("RedisJSON module not loaded")
        yield RedisJSONTaskStore(redis_client, prefix="test_task:")
        return
    yield RedisTaskStore(redis_client, prefix="test_task:")


# ---------------------------------------------------------------------------
# Backend-specific tests (init / key layout). Not parametrized.
# ---------------------------------------------------------------------------


class TestRedisTaskStoreHashSpecifics:
    """Hash-backend-only tests: init defaults and on-disk hash layout."""

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
    async def test_protocol_version_preserved(self, redis_client):
        """protocol_version metadata is persisted on the stored hash."""
        store = RedisTaskStore(redis_client, prefix="test_task:")
        task = _build_task(task_id="pv_task")
        await store.save(task, TEST_CONTEXT)

        # TEST_CONTEXT user is 'test_user'
        key = "test_task:test_user:pv_task"
        stored = await redis_client.hgetall(key)
        # Keys come back as bytes from real Redis (decode_responses=False).
        assert stored.get(b"protocol_version") == b"1.0"
        # task_payload should be valid JSON we can round-trip back to a Task.
        payload = json.loads(stored[b"task_payload"].decode())
        assert payload["id"] == "pv_task"


class TestRedisJSONTaskStoreSpecifics:
    """JSON-backend-only tests: init defaults and on-disk JSON layout."""

    def test_init(self, redis_client):
        """Test RedisJSONTaskStore initialization."""
        store = RedisJSONTaskStore(redis_client, prefix="test:")
        assert store.redis is redis_client
        assert store.prefix == "test:"

    def test_task_key_generation(self, redis_client):
        """Test owner-scoped task key generation."""
        store = RedisJSONTaskStore(redis_client, prefix="task:")
        assert store._task_key("alice", "123") == "task:alice:123"

    @pytest.mark.asyncio
    async def test_payload_stored_as_json_document(self, redis_client):
        """Task payload is stored as a native RedisJSON document at $."""
        if not await _redis_json_available(redis_client):
            pytest.skip("RedisJSON module not loaded")

        store = RedisJSONTaskStore(redis_client, prefix="test_task:")
        task = _build_task(task_id="jdoc_task")
        await store.save(task, TEST_CONTEXT)

        key = "test_task:test_user:jdoc_task"
        # The key should report type "ReJSON-RL" (RedisJSON's stored type).
        key_type = await redis_client.type(key)
        if isinstance(key_type, bytes):
            key_type = key_type.decode()
        assert "json" in key_type.lower() or key_type == "ReJSON-RL"

        # And the document should round-trip via JSON.GET.
        raw = await redis_client.execute_command("JSON.GET", key)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, list):
            raw = raw[0]
        assert raw["id"] == "jdoc_task"


# ---------------------------------------------------------------------------
# Contract tests — run against both backends via the parametrized fixture.
# ---------------------------------------------------------------------------


class TestTaskStoreContract:
    """Behavioural contract tests; run under both hash and json backends."""

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


class TestTaskStoreListContract:
    """Contract tests for ``list()`` filters + pagination, run under both backends."""

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

        page1 = await task_store.list(ListTasksRequest(page_size=3), TEST_CONTEXT)
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

        resp = await task_store.list(ListTasksRequest(context_id="ctx_A"), TEST_CONTEXT)
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

        await task_store.save(_build_task(task_id="ts1", timestamp=t1_dt), TEST_CONTEXT)
        await task_store.save(_build_task(task_id="ts2", timestamp=t2_dt), TEST_CONTEXT)
        await task_store.save(_build_task(task_id="ts3", timestamp=t3_dt), TEST_CONTEXT)

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
        resp_other = await task_store.list(ListTasksRequest(), TEST_CONTEXT_OTHER)

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
