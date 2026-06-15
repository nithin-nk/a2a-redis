"""Redis Pub/Sub-backed ``QueueManager`` implementation.

Conforms to the v1.1 a2a SDK ``QueueManager`` interface: all methods are
async and operate on ``EventQueueLegacy`` instances (here, our
``RedisPubSubEventQueue`` subclass).
"""

import asyncio
from typing import Dict, Optional

import redis.asyncio as redis
from a2a.server.events import EventQueueLegacy, QueueManager

from .pubsub_queue import RedisPubSubEventQueue


class RedisPubSubQueueManager(QueueManager):
    """Redis Pub/Sub-backed ``QueueManager`` for real-time fan-out."""

    def __init__(self, redis_client: redis.Redis, prefix: str = "pubsub:"):
        self.redis = redis_client
        self.prefix = prefix
        self._queues: Dict[str, RedisPubSubEventQueue] = {}
        self._lock = asyncio.Lock()

    def _create_queue(self, task_id: str) -> RedisPubSubEventQueue:
        """Create a new ``RedisPubSubEventQueue`` for a task."""
        return RedisPubSubEventQueue(self.redis, task_id, self.prefix)

    async def add(self, task_id: str, queue: EventQueueLegacy) -> None:
        """Register a queue for ``task_id``.

        The ``queue`` argument is accepted for interface compatibility but
        we always create our own ``RedisPubSubEventQueue`` so the channel
        configuration stays consistent.
        """
        del queue  # we own queue creation for Redis-backed task IDs
        async with self._lock:
            self._queues[task_id] = self._create_queue(task_id)

    async def get(self, task_id: str) -> Optional[EventQueueLegacy]:
        """Return the queue for ``task_id`` if one exists."""
        async with self._lock:
            return self._queues.get(task_id)

    async def tap(self, task_id: str) -> Optional[EventQueueLegacy]:
        """Return a sibling subscriber for ``task_id`` if a parent exists."""
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
        """Create the queue if absent, otherwise return the existing one."""
        async with self._lock:
            if task_id not in self._queues:
                self._queues[task_id] = self._create_queue(task_id)
            return self._queues[task_id]
