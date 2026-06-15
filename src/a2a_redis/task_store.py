"""Redis-backed task store implementations for the A2A Python SDK."""

import base64
import binascii
import json
from datetime import timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as redis
from google.protobuf.json_format import MessageToDict, ParseDict

from a2a.server.context import ServerCallContext
from a2a.server.owner_resolver import OwnerResolver, resolve_user_scope
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import ListTasksRequest, ListTasksResponse, Task
from a2a.utils.constants import (
    DEFAULT_LIST_TASKS_PAGE_SIZE,
    MAX_LIST_TASKS_PAGE_SIZE,
)
from a2a.utils.errors import InvalidParamsError


class RedisTaskStore(TaskStore):
    """Redis hash-backed TaskStore with owner-scoped keys (v1.1 contract)."""

    # Over-fetch multiplier when filters might reduce the in-page hit count.
    _OVER_FETCH = 4

    def __init__(
        self,
        redis_client: redis.Redis,
        prefix: str = "task:",
        owner_resolver: OwnerResolver = resolve_user_scope,
    ):
        """Initialize the Redis task store.

        Args:
            redis_client: Redis client instance.
            prefix: Key prefix for task storage.
            owner_resolver: Callable mapping ServerCallContext -> owner string.
        """
        self.redis = redis_client
        self.prefix = prefix
        self._owner_resolver = owner_resolver

    def _task_key(self, owner: str, task_id: str) -> str:
        """Generate the owner-scoped Redis key for a task."""
        return f"{self.prefix}{owner}:{task_id}"

    def _index_key(self, owner: str) -> str:
        """Sorted-set index of (score=-micros, member=micros:task_id)."""
        return f"{self.prefix}idx:{owner}"

    def _index_score_key(self, owner: str) -> str:
        """Hash mapping task_id -> current score in the sorted set."""
        return f"{self.prefix}idxscore:{owner}"

    @staticmethod
    def _now_micros(task: Task) -> int:
        """Return last_updated as microseconds-since-epoch, 0 if unset."""
        if task.HasField("status") and task.status.HasField("timestamp"):
            dt = task.status.timestamp.ToDatetime(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        return 0

    @staticmethod
    def _index_member(micros: int, task_id: str) -> str:
        """Build the sorted-set member: zero-padded micros + task_id."""
        # 20 digits accommodates any positive int64 micros value.
        return f"{micros:020d}:{task_id}"

    async def save(self, task: Task, context: ServerCallContext) -> None:
        """Save a task to Redis under the resolved owner scope."""
        owner = self._owner_resolver(context)
        task_dict = MessageToDict(task)
        micros = self._now_micros(task)
        last_updated = ""
        if task.status.HasField("timestamp"):
            last_updated = task.status.timestamp.ToDatetime().isoformat()
        mapping: Dict[str, str] = {
            "task_payload": json.dumps(task_dict),
            "owner": owner,
            "context_id": task.context_id,
            "last_updated": last_updated,
            "protocol_version": "1.0",
        }

        task_key = self._task_key(owner, task.id)
        index_key = self._index_key(owner)
        score_key = self._index_score_key(owner)

        # Look up prior score so we can remove the stale index member.
        prior_score_raw = await self.redis.hget(score_key, task.id)
        prior_score: Optional[int] = None
        if prior_score_raw is not None:
            if isinstance(prior_score_raw, bytes):
                prior_score_raw = prior_score_raw.decode()
            try:
                prior_score = int(prior_score_raw)
            except (TypeError, ValueError):
                prior_score = None

        new_score = -micros
        new_member = self._index_member(micros, task.id)

        pipe = self.redis.pipeline(transaction=True)
        if prior_score is not None:
            prior_micros = -prior_score
            prior_member = self._index_member(prior_micros, task.id)
            pipe.zrem(index_key, prior_member)
        pipe.zadd(index_key, {new_member: new_score})
        pipe.hset(score_key, task.id, str(new_score))
        pipe.hset(task_key, mapping=mapping)
        await pipe.execute()

    async def get(
        self, task_id: str, context: ServerCallContext
    ) -> Optional[Task]:
        """Retrieve a task for the resolved owner, or None if absent."""
        owner = self._owner_resolver(context)
        data = await self.redis.hgetall(self._task_key(owner, task_id))
        if not data:
            return None
        payload = data.get(b"task_payload") or data.get("task_payload")
        if payload is None:
            return None
        if isinstance(payload, bytes):
            payload = payload.decode()
        task_dict = json.loads(payload)
        task = Task()
        ParseDict(task_dict, task)
        return task

    async def delete(self, task_id: str, context: ServerCallContext) -> None:
        """Delete a task for the resolved owner; no-op when absent."""
        owner = self._owner_resolver(context)
        task_key = self._task_key(owner, task_id)
        index_key = self._index_key(owner)
        score_key = self._index_score_key(owner)

        prior_score_raw = await self.redis.hget(score_key, task_id)
        prior_score: Optional[int] = None
        if prior_score_raw is not None:
            if isinstance(prior_score_raw, bytes):
                prior_score_raw = prior_score_raw.decode()
            try:
                prior_score = int(prior_score_raw)
            except (TypeError, ValueError):
                prior_score = None

        pipe = self.redis.pipeline(transaction=True)
        if prior_score is not None:
            prior_micros = -prior_score
            prior_member = self._index_member(prior_micros, task_id)
            pipe.zrem(index_key, prior_member)
            pipe.hdel(score_key, task_id)
        pipe.delete(task_key)
        await pipe.execute()

    # ---------------- list() helpers ----------------

    @staticmethod
    def _encode_page_token(offset: int, owner: str) -> str:
        """Encode the cursor as base64(JSON({offset, owner}))."""
        payload = json.dumps({"offset": offset, "owner": owner}).encode("utf-8")
        return base64.b64encode(payload).decode("utf-8")

    @staticmethod
    def _decode_page_token(token: str, owner: str) -> int:
        """Decode the cursor, validating it matches the current owner.

        Raises InvalidParamsError on any malformed/owner-mismatched token.
        """
        encoded = token
        # Tolerate missing base64 padding the same way upstream does.
        missing_padding = len(encoded) % 4
        if missing_padding:
            encoded = encoded + ("=" * (4 - missing_padding))
        try:
            raw = base64.b64decode(encoded.encode("utf-8")).decode("utf-8")
            data = json.loads(raw)
        except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
            raise InvalidParamsError(
                f"Invalid page token: {token}"
            ) from exc
        if not isinstance(data, dict):
            raise InvalidParamsError(f"Invalid page token: {token}")
        offset = data.get("offset")
        token_owner = data.get("owner")
        if not isinstance(offset, int) or not isinstance(token_owner, str):
            raise InvalidParamsError(f"Invalid page token: {token}")
        if token_owner != owner:
            raise InvalidParamsError(f"Invalid page token: {token}")
        if offset < 0:
            raise InvalidParamsError(f"Invalid page token: {token}")
        return offset

    @staticmethod
    def _clamp_page_size(requested: int) -> int:
        """Resolve and clamp page_size per the upstream contract."""
        page_size = requested or DEFAULT_LIST_TASKS_PAGE_SIZE
        if page_size < 1:
            page_size = DEFAULT_LIST_TASKS_PAGE_SIZE
        if page_size > MAX_LIST_TASKS_PAGE_SIZE:
            page_size = MAX_LIST_TASKS_PAGE_SIZE
        return page_size

    def _decode_payload_to_task(self, raw: Any) -> Optional[Task]:
        """Decode a task_payload bytes/str blob back into a Task message."""
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            task_dict = json.loads(raw)
        except json.JSONDecodeError:
            return None
        task = Task()
        ParseDict(task_dict, task)
        return task

    @staticmethod
    def _passes_filters(task: Task, params: ListTasksRequest) -> bool:
        """Apply context_id / status / status_timestamp_after filters."""
        if params.context_id and task.context_id != params.context_id:
            return False
        if params.status:
            # Upstream compares the enum integer value directly for in-memory,
            # and the enum name string for DB. The integer comparison is exact
            # and sufficient for our Task message form.
            if not task.HasField("status"):
                return False
            if task.status.state != params.status:
                return False
        if params.HasField("status_timestamp_after"):
            if not (
                task.HasField("status") and task.status.HasField("timestamp")
            ):
                return False
            lhs = task.status.timestamp.ToJsonString()
            rhs = params.status_timestamp_after.ToJsonString()
            if lhs < rhs:
                return False
        return True

    async def list(
        self, params: ListTasksRequest, context: ServerCallContext
    ) -> ListTasksResponse:
        """List tasks for the resolved owner, applying filters + pagination.

        Pagination is offset-based against the per-owner secondary index. The
        emitted page token is opaque base64(JSON) and is validated against the
        owner extracted from the request context.
        """
        owner = self._owner_resolver(context)
        page_size = self._clamp_page_size(params.page_size)

        start_offset = 0
        if params.page_token:
            start_offset = self._decode_page_token(params.page_token, owner)

        index_key = self._index_key(owner)
        total_size = int(await self.redis.zcard(index_key))

        collected: List[Task] = []
        cursor = start_offset
        # next_cursor tracks the absolute index offset just past the last
        # member we consumed to produce ``collected``. This is what a
        # subsequent page_token must point to.
        next_cursor = start_offset
        window = page_size * self._OVER_FETCH
        page_full = False
        exhausted = False

        while len(collected) < page_size and cursor < total_size:
            stop = cursor + window - 1
            members = await self.redis.zrange(index_key, cursor, stop)
            if not members:
                exhausted = True
                break

            # Resolve task_ids and fetch their task_payload via a pipeline.
            task_ids: List[str] = []
            for member in members:
                if isinstance(member, bytes):
                    member = member.decode()
                # member format: "<micros>:<task_id>"
                _, _, tid = member.partition(":")
                task_ids.append(tid)

            pipe = self.redis.pipeline(transaction=False)
            for tid in task_ids:
                pipe.hget(self._task_key(owner, tid), "task_payload")
            payloads = await pipe.execute()

            for idx, (tid, payload) in enumerate(zip(task_ids, payloads)):
                # Whether we keep this row or skip it, the cursor advances
                # past it -- we've fully inspected this index position.
                next_cursor = cursor + idx + 1
                task = self._decode_payload_to_task(payload)
                if task is None:
                    continue
                if not self._passes_filters(task, params):
                    continue
                collected.append(task)
                if len(collected) >= page_size:
                    page_full = True
                    break

            consumed = len(members)
            cursor += consumed
            if page_full:
                break
            if consumed < window:
                # Exhausted the sorted set with this read.
                exhausted = True
                break

        # Trim and decide whether a next page exists.
        tasks_page = collected[:page_size]
        next_page_token: Optional[str] = None
        if (
            len(tasks_page) == page_size
            and not exhausted
            and next_cursor < total_size
        ):
            next_page_token = self._encode_page_token(next_cursor, owner)

        return ListTasksResponse(
            tasks=tasks_page,
            next_page_token=next_page_token or "",
            page_size=page_size,
            total_size=total_size,
        )


class RedisJSONTaskStore(TaskStore):
    """Redis JSON-backed TaskStore for native JSON operations.

    Requires Redis server with RedisJSON module. Provides better performance
    for complex nested data structures and JSONPath queries.
    """

    def __init__(self, redis_client: redis.Redis, prefix: str = "task:"):
        """Initialize the Redis JSON task store.

        Args:
            redis_client: Redis client instance with JSON support
            prefix: Key prefix for task storage
        """
        self.redis = redis_client
        self.prefix = prefix

    def _task_key(self, task_id: str) -> str:
        """Generate the Redis key for a task."""
        return f"{self.prefix}{task_id}"

    async def save(self, task: Task, context: ServerCallContext | None = None) -> None:
        """Save a task to Redis using JSON.

        Args:
            task: Task instance to save
            context: Optional server call context (unused, for interface compatibility)
        """
        task_data = task.model_dump() if hasattr(task, "model_dump") else task
        await self.redis.json().set(self._task_key(task.id), "$", task_data)  # type: ignore[misc]

    async def get(
        self, task_id: str, context: ServerCallContext | None = None
    ) -> Optional[Task]:
        """Retrieve a task from Redis using JSON.

        Args:
            task_id: Task identifier
            context: Optional server call context (unused, for interface compatibility)

        Returns:
            Task instance or None if not found
        """
        try:
            result = await self.redis.json().get(self._task_key(task_id))  # type: ignore[misc]
            if result:
                # RedisJSON get with JSONPath can return list or dict
                if isinstance(result, list) and result:
                    task_data = result[0]  # type: ignore[misc]
                elif isinstance(result, dict):
                    task_data = result  # type: ignore[assignment]
                else:
                    return None
                return Task(**task_data)  # type: ignore[misc]
            return None
        except (Exception,):  # type: ignore[misc]
            return None

    async def delete(
        self, task_id: str, context: ServerCallContext | None = None
    ) -> None:
        """Delete a task from Redis.

        Args:
            task_id: Task identifier
            context: Optional server call context (unused, for interface compatibility)
        """
        await self.redis.delete(self._task_key(task_id))  # type: ignore[misc]

    async def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis using JSON.

        Args:
            task_id: Task identifier
            updates: Dictionary of fields to update

        Returns:
            True if task was updated, False if task doesn't exist
        """
        try:
            task = await self.get(task_id)
            if task is None:
                return False

            task_data = task.model_dump() if hasattr(task, "model_dump") else task  # type: ignore[misc]
            task_data.update(updates)  # type: ignore[misc]
            updated_task = Task(**task_data)  # type: ignore[misc]
            await self.save(updated_task)
            return True
        except Exception:  # type: ignore[misc]
            return False

    async def list_task_ids(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern.

        Args:
            pattern: Pattern to match task IDs against

        Returns:
            List of task IDs
        """
        keys = await self.redis.keys(f"{self.prefix}{pattern}")  # type: ignore[misc]
        return [key.decode().replace(self.prefix, "") for key in keys]  # type: ignore[misc]

    async def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis.

        Args:
            task_id: Task identifier

        Returns:
            True if task exists, False otherwise
        """
        return bool(await self.redis.exists(self._task_key(task_id)))  # type: ignore[misc]
