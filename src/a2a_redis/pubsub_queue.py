"""Redis Pub/Sub-backed ``EventQueueLegacy`` implementation.

Conforms to the v1.1 a2a SDK ``EventQueueLegacy`` interface (async ``tap``,
``close(immediate=...)``). Optimized for real-time fan-out with no
persistence or delivery guarantees; see README.md for trade-offs vs.
``RedisStreamsEventQueue``.
"""

import asyncio
from typing import Any, Dict, Optional

import redis.asyncio as redis
from redis.asyncio.client import PubSub

from a2a.server.events import EventQueueLegacy
from a2a.server.events.event_queue import DEFAULT_MAX_QUEUE_SIZE, Event

from .model_utils import (
    deserialize_event,
    deserialize_from_json,
    serialize_event,
    serialize_to_json,
)


class RedisPubSubEventQueue(EventQueueLegacy):
    """Redis Pub/Sub-backed ``EventQueueLegacy``.

    Subclasses ``EventQueueLegacy`` so it can stand in wherever the SDK
    expects an ``EventQueueLegacy``. The parent ``__init__`` is bypassed
    because we do not use an in-process ``asyncio.Queue``.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        task_id: str,
        prefix: str = "pubsub:",
    ):
        # Do NOT call super().__init__(); Redis owns storage.
        self.redis = redis_client
        self.task_id = task_id
        self.prefix = prefix
        self._channel = f"{prefix}{task_id}"

        # Mirror parent attributes so inherited helpers / isinstance work.
        self._is_closed = False
        self._children: list[EventQueueLegacy] = []
        self._lock = asyncio.Lock()

        # Backwards-compat alias used by existing tests.
        self._closed = False

        self._pubsub: Optional[PubSub] = None
        self._setup_complete = False

    async def _ensure_setup(self) -> None:
        """Lazily set up the pub/sub subscription."""
        if self._setup_complete or self._closed or self._is_closed:
            return

        self._pubsub = self.redis.pubsub()  # type: ignore[misc]
        await self._pubsub.subscribe(self._channel)  # type: ignore[misc]
        self._setup_complete = True

    async def enqueue_event(self, event: Event) -> None:
        """Publish an event to the pub/sub channel.

        Args:
            event: A v1.1 SDK event (``Message`` / ``Task`` /
                ``TaskStatusUpdateEvent`` / ``TaskArtifactUpdateEvent``).
        """
        if self._closed or self._is_closed:
            raise RuntimeError("Cannot enqueue to closed queue")

        await self._ensure_setup()

        event_structure = serialize_event(event)
        message = serialize_to_json(event_structure)
        await self.redis.publish(self._channel, message)  # type: ignore[misc]

    async def dequeue_event(self, no_wait: bool = False) -> Event:
        """Wait for the next published event on this subscriber."""
        if self._closed or self._is_closed:
            raise RuntimeError("Cannot dequeue from closed queue")

        await self._ensure_setup()

        if not self._pubsub:
            raise RuntimeError("Pub/sub not initialized")

        timeout = 0.1 if no_wait else 1.0

        try:
            message: Optional[Dict[str, Any]] = await asyncio.wait_for(  # type: ignore[assignment]
                self._pubsub.get_message(ignore_subscribe_messages=True),  # type: ignore[misc]
                timeout=timeout,
            )

            if message is None:
                raise RuntimeError("No events available")

            event_structure = deserialize_from_json(message["data"])
            return deserialize_event(event_structure)

        except asyncio.TimeoutError:
            raise RuntimeError("No events available")

    async def tap(
        self, max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE
    ) -> "RedisPubSubEventQueue":
        """Create another subscriber to the same channel.

        Pub/Sub broadcasts naturally to every subscriber, so each tap is a
        peer rather than a downstream child. The ``max_queue_size`` argument
        exists for signature compatibility with ``EventQueueLegacy.tap``.
        """
        del max_queue_size  # signature compatibility only
        child = RedisPubSubEventQueue(self.redis, self.task_id, self.prefix)
        self._children.append(child)
        return child

    async def close(self, immediate: bool = False) -> None:
        """Close the queue and unsubscribe from the channel."""
        async with self._lock:
            if (self._is_closed or self._closed) and not immediate:
                return
            self._is_closed = True
            self._closed = True

        if self._pubsub:
            try:
                await self._pubsub.unsubscribe(self._channel)  # type: ignore[misc]
                await self._pubsub.close()  # type: ignore[misc]
            except Exception:
                pass
            finally:
                self._pubsub = None
                self._setup_complete = False

        await asyncio.gather(
            *(child.close(immediate) for child in self._children),
            return_exceptions=True,
        )

    def is_closed(self) -> bool:
        """Check if the queue is closed."""
        return self._is_closed or self._closed

    def task_done(self) -> None:
        """No-op for pub/sub (no explicit completion signal)."""
        return None
