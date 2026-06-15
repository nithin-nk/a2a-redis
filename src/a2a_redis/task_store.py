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


class _RedisTaskStoreBase(TaskStore):
    """Shared base for Redis-backed TaskStores (v1.1 contract).

    Provides owner-scoped keys, a per-owner secondary index, filtering, and
    pagination. Subclasses override only the payload storage hooks
    (``_write_payload`` / ``_read_payload`` / ``_delete_payload``) to choose
    between hash-encoded and RedisJSON-encoded persistence.
    """

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

    # ---------------- Key helpers ----------------

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

    # ---------------- Payload hooks (subclass override) ----------------

    def _write_payload(
        self,
        pipe: Any,
        task_key: str,
        task_dict: Dict[str, Any],
        owner: str,
        task: Task,
        last_updated: str,
    ) -> None:
        """Stage payload write on the given pipeline. Subclasses override."""
        raise NotImplementedError

    async def _read_payload(self, task_key: str) -> Optional[Dict[str, Any]]:
        """Return the stored task payload as a dict, or None if absent."""
        raise NotImplementedError

    def _stage_payload_delete(self, pipe: Any, task_key: str) -> None:
        """Stage payload deletion on the given pipeline."""
        raise NotImplementedError

    async def _fetch_payloads(
        self, owner: str, task_ids: List[str]
    ) -> List[Optional[Dict[str, Any]]]:
        """Fetch many task payloads as dicts. Subclasses may optimize."""
        results: List[Optional[Dict[str, Any]]] = []
        for tid in task_ids:
            results.append(await self._read_payload(self._task_key(owner, tid)))
        return results

    # ---------------- CRUD ----------------

    async def save(self, task: Task, context: ServerCallContext) -> None:
        """Save a task to Redis under the resolved owner scope."""
        owner = self._owner_resolver(context)
        task_dict = MessageToDict(task)
        micros = self._now_micros(task)
        last_updated = ""
        if task.status.HasField("timestamp"):
            last_updated = task.status.timestamp.ToDatetime().isoformat()

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
        self._write_payload(pipe, task_key, task_dict, owner, task, last_updated)
        await pipe.execute()

    async def get(self, task_id: str, context: ServerCallContext) -> Optional[Task]:
        """Retrieve a task for the resolved owner, or None if absent."""
        owner = self._owner_resolver(context)
        task_dict = await self._read_payload(self._task_key(owner, task_id))
        if not task_dict:
            return None
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
        self._stage_payload_delete(pipe, task_key)
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
            raise InvalidParamsError(f"Invalid page token: {token}") from exc
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

    @staticmethod
    def _dict_to_task(task_dict: Optional[Dict[str, Any]]) -> Optional[Task]:
        """Decode a task payload dict back into a Task message."""
        if not task_dict:
            return None
        try:
            task = Task()
            ParseDict(task_dict, task)
            return task
        except Exception:
            return None

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
            if not (task.HasField("status") and task.status.HasField("timestamp")):
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

            # Resolve task_ids and fetch their payloads via the subclass hook.
            task_ids: List[str] = []
            for member in members:
                if isinstance(member, bytes):
                    member = member.decode()
                # member format: "<micros>:<task_id>"
                _, _, tid = member.partition(":")
                task_ids.append(tid)

            payload_dicts = await self._fetch_payloads(owner, task_ids)

            for idx, (tid, task_dict) in enumerate(zip(task_ids, payload_dicts)):
                # Whether we keep this row or skip it, the cursor advances
                # past it -- we've fully inspected this index position.
                next_cursor = cursor + idx + 1
                task = self._dict_to_task(task_dict)
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
        if len(tasks_page) == page_size and not exhausted and next_cursor < total_size:
            next_page_token = self._encode_page_token(next_cursor, owner)

        return ListTasksResponse(
            tasks=tasks_page,
            next_page_token=next_page_token or "",
            page_size=page_size,
            total_size=total_size,
        )


class RedisTaskStore(_RedisTaskStoreBase):
    """Redis hash-backed TaskStore with owner-scoped keys (v1.1 contract)."""

    def _write_payload(
        self,
        pipe: Any,
        task_key: str,
        task_dict: Dict[str, Any],
        owner: str,
        task: Task,
        last_updated: str,
    ) -> None:
        """Stage HSET of the task hash, including metadata columns."""
        mapping: Dict[str, str] = {
            "task_payload": json.dumps(task_dict),
            "owner": owner,
            "context_id": task.context_id,
            "last_updated": last_updated,
            "protocol_version": "1.0",
        }
        pipe.hset(task_key, mapping=mapping)

    async def _read_payload(self, task_key: str) -> Optional[Dict[str, Any]]:
        """Read task_payload from the Redis hash and decode the JSON blob."""
        data = await self.redis.hgetall(task_key)
        if not data:
            return None
        payload = data.get(b"task_payload") or data.get("task_payload")
        if payload is None:
            return None
        if isinstance(payload, bytes):
            payload = payload.decode()
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return None

    def _stage_payload_delete(self, pipe: Any, task_key: str) -> None:
        """Stage DEL of the hash key."""
        pipe.delete(task_key)

    async def _fetch_payloads(
        self, owner: str, task_ids: List[str]
    ) -> List[Optional[Dict[str, Any]]]:
        """Pipeline HGET of each task_payload field, in order."""
        pipe = self.redis.pipeline(transaction=False)
        for tid in task_ids:
            pipe.hget(self._task_key(owner, tid), "task_payload")
        raw_values = await pipe.execute()

        results: List[Optional[Dict[str, Any]]] = []
        for raw in raw_values:
            if raw is None:
                results.append(None)
                continue
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                results.append(json.loads(raw))
            except json.JSONDecodeError:
                results.append(None)
        return results


class RedisJSONTaskStore(_RedisTaskStoreBase):
    """RedisJSON-backed TaskStore with owner-scoped keys (v1.1 contract).

    Stores the task payload as a native JSON document at the root path ``$``.
    Requires a Redis server with the RedisJSON module loaded (e.g.
    ``redis/redis-stack``). The per-owner secondary index and score hash are
    plain Redis structures, identical to :class:`RedisTaskStore`.
    """

    def _write_payload(
        self,
        pipe: Any,
        task_key: str,
        task_dict: Dict[str, Any],
        owner: str,
        task: Task,
        last_updated: str,
    ) -> None:
        """Stage a JSON.SET of the task document at root ``$``.

        Uses ``execute_command`` directly to avoid relying on the JSON helper
        wrapper from inside an async pipeline (the helper class is sync-only;
        the raw command works on any pipeline).
        """
        pipe.execute_command("JSON.SET", task_key, "$", json.dumps(task_dict))

    @staticmethod
    def _normalize_json_result(raw: Any) -> Optional[Dict[str, Any]]:
        """Coerce a JSON.GET response into a single task-dict (or None)."""
        if raw is None:
            return None
        # Async client returns bytes/str (no JSON helper decode); decode here.
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return None
        elif isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return None
        if isinstance(raw, list):
            if not raw:
                return None
            first = raw[0]
            return first if isinstance(first, dict) else None
        if isinstance(raw, dict):
            return raw
        return None

    async def _read_payload(self, task_key: str) -> Optional[Dict[str, Any]]:
        """Read the JSON document at the task key.

        Returns the decoded dict, or None if the key is absent or empty.
        """
        try:
            raw = await self.redis.execute_command("JSON.GET", task_key)
        except Exception:
            return None
        return self._normalize_json_result(raw)

    def _stage_payload_delete(self, pipe: Any, task_key: str) -> None:
        """Stage a JSON.DEL of the root document.

        Equivalent to ``DEL`` for a key whose only content is its JSON
        document, but uses the native JSON op for clarity.
        """
        pipe.execute_command("JSON.DEL", task_key)

    async def _fetch_payloads(
        self, owner: str, task_ids: List[str]
    ) -> List[Optional[Dict[str, Any]]]:
        """Pipeline JSON.GET of each task document, in order."""
        try:
            pipe = self.redis.pipeline(transaction=False)
            for tid in task_ids:
                pipe.execute_command("JSON.GET", self._task_key(owner, tid))
            raw_values = await pipe.execute()
        except Exception:
            # Fall back to sequential reads if pipelining JSON ops fails.
            return await super()._fetch_payloads(owner, task_ids)

        return [self._normalize_json_result(raw) for raw in raw_values]
