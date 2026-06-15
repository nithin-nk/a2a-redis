"""Redis Streams-backed ``QueueManager`` implementation.

Conforms to the v1.1 a2a SDK ``QueueManager`` interface: every method is
async and operates on ``EventQueueLegacy`` instances (here, our
``RedisStreamsEventQueue`` subclass).
"""

import asyncio
from typing import Dict, Optional

import redis.asyncio as redis
from a2a.server.events import EventQueueLegacy, QueueManager

from .streams_consumer_strategy import ConsumerGroupConfig
from .streams_queue import RedisStreamsEventQueue


class RedisStreamsQueueManager(QueueManager):
    """Redis Streams-backed ``QueueManager``.

    Provides guaranteed delivery with consumer groups, acknowledgments, and
    replay capability. See README.md for detailed use cases and trade-offs.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        prefix: str = "stream:",
        consumer_config: Optional[ConsumerGroupConfig] = None,
    ):
        self.redis = redis_client
        self.prefix = prefix
        self.consumer_config = consumer_config or ConsumerGroupConfig()
        self._queues: Dict[str, RedisStreamsEventQueue] = {}
        self._lock = asyncio.Lock()

    def _create_queue(self, task_id: str) -> RedisStreamsEventQueue:
        """Create a new ``RedisStreamsEventQueue`` for a task."""
        return RedisStreamsEventQueue(
            self.redis, task_id, self.prefix, self.consumer_config
        )

    async def add(self, task_id: str, queue: EventQueueLegacy) -> None:
        """Register a queue for a task ID.

        The ``queue`` argument is accepted for interface compatibility but
        the Redis-backed manager always constructs its own
        ``RedisStreamsEventQueue`` so that the underlying stream key/consumer
        group stays consistent.
        """
        del queue  # we own queue creation for Redis-backed task IDs
        async with self._lock:
            self._queues[task_id] = self._create_queue(task_id)

    async def get(self, task_id: str) -> Optional[EventQueueLegacy]:
        """Return the queue for ``task_id`` if one exists."""
        async with self._lock:
            return self._queues.get(task_id)

    async def tap(self, task_id: str) -> Optional[EventQueueLegacy]:
        """Return a child queue for ``task_id`` if a parent exists.

        Awaits the now-async ``RedisStreamsEventQueue.tap()``.
        """
        async with self._lock:
            queue = self._queues.get(task_id)
        if queue is None:
            return None
        return await queue.tap()

    async def close(self, task_id: str) -> None:
        """Close and remove the queue for ``task_id``."""
        async with self._lock:
            queue = self._queues.pop(task_id, None)
        if queue is not None:
            await queue.close()

    async def create_or_tap(self, task_id: str) -> EventQueueLegacy:
        """Create the queue if absent, otherwise return the existing one.

        Mirrors the InMemoryQueueManager behavior (returns the same parent
        instance on repeated calls for a single task id).
        """
        async with self._lock:
            if task_id not in self._queues:
                self._queues[task_id] = self._create_queue(task_id)
            return self._queues[task_id]
