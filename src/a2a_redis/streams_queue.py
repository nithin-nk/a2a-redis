"""Redis Streams-backed EventQueueLegacy implementation.

Conforms to the v1.1 a2a SDK ``EventQueueLegacy`` interface (async ``tap``,
``close(immediate=...)``). Provides persistent, reliable delivery backed by
Redis Streams with consumer groups, acknowledgments, and replay.
"""

import asyncio
from typing import Optional

import redis.asyncio as redis
from a2a.server.events import EventQueueLegacy
from a2a.server.events.event_queue import DEFAULT_MAX_QUEUE_SIZE, Event

from .streams_consumer_strategy import ConsumerGroupConfig
from .model_utils import (
    deserialize_event,
    deserialize_from_json,
    serialize_event,
    serialize_to_json,
)


class RedisStreamsEventQueue(EventQueueLegacy):
    """Redis Streams-backed ``EventQueueLegacy``.

    Subclasses ``EventQueueLegacy`` so it can be returned anywhere the SDK
    expects an ``EventQueueLegacy`` instance, while delegating actual storage
    and delivery to Redis Streams. The parent ``EventQueueLegacy.__init__``
    is intentionally bypassed because we do not use an in-process
    ``asyncio.Queue``.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        task_id: str,
        prefix: str = "stream:",
        consumer_config: Optional[ConsumerGroupConfig] = None,
    ):
        """Initialize the Redis Streams event queue.

        We deliberately skip ``EventQueueLegacy.__init__`` (which sets up an
        ``asyncio.Queue``) because storage lives in Redis. We still set the
        attributes the parent class exposes (``_is_closed``, ``_children``,
        ``_lock``) so any inherited helpers behave sensibly.
        """
        # Do NOT call super().__init__(); we are replacing its storage entirely.
        self.redis = redis_client
        self.task_id = task_id
        self.prefix = prefix
        self._stream_key = f"{prefix}{task_id}"

        # Mirror the parent attributes so isinstance/inherited helpers work.
        self._is_closed = False
        self._children: list[EventQueueLegacy] = []
        self._lock = asyncio.Lock()

        # Backwards-compat alias used by existing tests.
        self._closed = False

        # Consumer group configuration
        self.consumer_config = consumer_config or ConsumerGroupConfig()
        self.consumer_group = self.consumer_config.get_consumer_group_name(task_id)
        self.consumer_id = self.consumer_config.get_consumer_id()

        # Consumer group is created lazily on first use.
        self._consumer_group_ensured = False

    async def _ensure_consumer_group(self) -> None:
        """Create the consumer group if it does not already exist."""
        try:
            await self.redis.xgroup_create(
                self._stream_key, self.consumer_group, id="0", mkstream=True
            )  # type: ignore[misc]
        except Exception as e:  # type: ignore[misc]
            if "BUSYGROUP" not in str(e):
                raise

    async def enqueue_event(self, event: Event) -> None:
        """Append an event to the Redis stream.

        Args:
            event: A v1.1 SDK event (``Message`` / ``Task`` /
                ``TaskStatusUpdateEvent`` / ``TaskArtifactUpdateEvent``).
        """
        if self._closed or self._is_closed:
            raise RuntimeError("Cannot enqueue to closed queue")

        if not self._consumer_group_ensured:
            await self._ensure_consumer_group()
            self._consumer_group_ensured = True

        event_structure = serialize_event(event)

        fields = {
            "event_type": event_structure["event_type"],
            "event_data": serialize_to_json(event_structure["event_data"]),
        }
        await self.redis.xadd(self._stream_key, fields)  # type: ignore[misc]

    async def dequeue_event(self, no_wait: bool = False) -> Event:
        """Read the next event from the stream.

        Raises:
            asyncio.QueueEmpty: When the queue has been closed (matches the
                cancellation signal expected by ``EventConsumer``).
            RuntimeError: When no events are available within the timeout, or
                an underlying Redis error occurs.
        """
        if self._closed or self._is_closed:
            raise asyncio.QueueEmpty("Queue is closed")

        if not self._consumer_group_ensured:
            await self._ensure_consumer_group()
            self._consumer_group_ensured = True

        timeout = 0 if no_wait else 1000  # ms; 0 = non-blocking

        try:
            result = await self.redis.xreadgroup(
                self.consumer_group,
                self.consumer_id,
                {self._stream_key: ">"},
                count=1,
                block=timeout,
            )  # type: ignore[misc]

            if not result or not result[0][1]:
                raise RuntimeError("No events available")

            _, messages = result[0]
            message_id, fields = messages[0]

            event_structure = {
                "event_type": fields[b"event_type"].decode()
                if b"event_type" in fields
                else None,
                "event_data": deserialize_from_json(fields[b"event_data"]),
            }

            await self.redis.xack(
                self._stream_key, self.consumer_group, message_id
            )  # type: ignore[misc]

            return deserialize_event(event_structure)

        except Exception as e:  # type: ignore[misc]
            if "NOGROUP" in str(e):
                await self._ensure_consumer_group()
                raise RuntimeError("Consumer group recreated, try again")
            raise RuntimeError(f"Error reading from stream: {e}")

    async def tap(
        self, max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE
    ) -> "RedisStreamsEventQueue":
        """Create a child queue that consumes the same stream independently.

        Streams natively support multiple subscribers, so taps share the
        underlying ``self.redis`` / stream key / consumer group. The
        ``max_queue_size`` argument is accepted for signature compatibility
        with ``EventQueueLegacy.tap`` and is otherwise unused.
        """
        del max_queue_size  # signature compatibility only
        child = RedisStreamsEventQueue(
            self.redis, self.task_id, self.prefix, self.consumer_config
        )
        self._children.append(child)
        return child

    async def close(self, immediate: bool = False) -> None:
        """Close the queue and clean up pending messages.

        Args:
            immediate: Forwarded to children for parity with the v1.1
                ``EventQueueLegacy.close`` signature. Redis Streams have no
                in-memory pending queue to flush, so the flag is otherwise
                informational.
        """
        async with self._lock:
            if (self._is_closed or self._closed) and not immediate:
                return
            self._is_closed = True
            self._closed = True

        try:
            pending = await self.redis.xpending_range(  # type: ignore[misc]
                self._stream_key,
                self.consumer_group,
                min="-",
                max="+",
                count=100,
                consumername=self.consumer_id,
            )

            if pending:
                message_ids = [msg["message_id"] for msg in pending]
                await self.redis.xack(
                    self._stream_key, self.consumer_group, *message_ids
                )  # type: ignore[misc]

        except Exception:  # type: ignore[misc]
            # Consumer group may not exist yet; safe to ignore.
            pass

        await asyncio.gather(
            *(child.close(immediate) for child in self._children),
            return_exceptions=True,
        )

    def is_closed(self) -> bool:
        """Check if the queue is closed."""
        return self._is_closed or self._closed

    def task_done(self) -> None:
        """No-op for Redis Streams (acknowledgement happens in ``dequeue_event``)."""
        return None
