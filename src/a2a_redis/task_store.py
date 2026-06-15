"""Redis-backed task store implementations for the A2A Python SDK."""

import json
from typing import Any, Dict, List, Optional

import redis.asyncio as redis
from google.protobuf.json_format import MessageToDict, ParseDict

from a2a.server.context import ServerCallContext
from a2a.server.owner_resolver import OwnerResolver, resolve_user_scope
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import ListTasksRequest, ListTasksResponse, Task


class RedisTaskStore(TaskStore):
    """Redis hash-backed TaskStore with owner-scoped keys (v1.1 contract)."""

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

    async def save(self, task: Task, context: ServerCallContext) -> None:
        """Save a task to Redis under the resolved owner scope."""
        owner = self._owner_resolver(context)
        task_dict = MessageToDict(task)
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
        pipe = self.redis.pipeline()
        pipe.hset(self._task_key(owner, task.id), mapping=mapping)
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
        await self.redis.delete(self._task_key(owner, task_id))

    async def list(
        self, params: ListTasksRequest, context: ServerCallContext
    ) -> ListTasksResponse:
        """List tasks for the resolved owner (implemented in Slice 2)."""
        raise NotImplementedError("Slice 2")


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
