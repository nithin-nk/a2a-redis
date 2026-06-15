# a2a-redis

Redis integrations for the Agent-to-Agent (A2A) Python SDK.

This package provides Redis-backed implementations of core A2A components for
persistent task storage, reliable event queue management, and push notification
configuration.

## Features

- **RedisTaskStore & RedisJSONTaskStore**: Owner-scoped, paginated task storage
  using Redis hashes or RedisJSON, with a per-owner secondary index.
- **RedisStreamsQueueManager & RedisStreamsEventQueue**: Persistent, reliable
  event queues backed by Redis Streams with consumer groups.
- **RedisPubSubQueueManager & RedisPubSubEventQueue**: Real-time, low-latency
  event broadcasting via Redis Pub/Sub.
- **RedisPushNotificationConfigStore**: Multi-config-per-task push notification
  storage with optional Fernet encryption at rest and a cross-owner dispatch
  index.
- **Consumer Group Strategies for Streams**: Flexible load balancing and
  instance isolation patterns.
- **`a2a-redis-migrate` CLI**: One-shot migration from the v0.2 key layout to
  the v0.3 owner-scoped layout.

## Supported versions

| Dependency                    | Version                                          |
|-------------------------------|--------------------------------------------------|
| Python                        | `>=3.11`                                         |
| `a2a-sdk`                     | `>=1.1.0, <2`                                    |
| `redis`                       | `>=4.0.0`                                        |
| `cryptography` *(optional)*   | `>=42.0` via `pip install "a2a-redis[encryption]"` |

Install the optional encryption extra when you want Fernet-encrypted push
notification configs at rest:

```bash
pip install "a2a-redis[encryption]"
```

## Installation

```bash
pip install a2a-redis
```

## Quick Start

`RedisTaskStore` (and every other owner-aware store) takes a
`ServerCallContext` on each call so it can resolve the owner scope. Inside an
A2A request handler the SDK passes a real context for you; the snippet below
constructs one directly for illustration.

```python
import asyncio

from a2a.server.context import ServerCallContext
from a2a.auth.user import UnauthenticatedUser
from a2a.types import Task, TaskStatus, TaskState

from a2a_redis import RedisTaskStore
from a2a_redis.utils import create_redis_client


async def main() -> None:
    redis_client = create_redis_client(url="redis://localhost:6379/0")
    task_store = RedisTaskStore(redis_client, prefix="myapp:tasks:")

    # In a real handler the SDK builds this for you from the inbound request.
    context = ServerCallContext(user=UnauthenticatedUser())

    task = Task(
        id="task-001",
        context_id="ctx-001",
        status=TaskStatus(state=TaskState.submitted),
    )
    await task_store.save(task, context)

    fetched = await task_store.get("task-001", context)
    assert fetched is not None
    print(fetched.id, fetched.status.state)


asyncio.run(main())
```

For a runnable walkthrough that wires the stores into a full A2A application,
see `examples/basic_usage.py` and the end-to-end example under
[`examples/e2e/`](examples/e2e/README.md).

## Wiring into an A2A server

```python
from a2a_redis import (
    RedisTaskStore,
    RedisStreamsQueueManager,
    RedisPushNotificationConfigStore,
)
from a2a_redis.utils import create_redis_client
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.apps import A2AStarletteApplication

redis_client = create_redis_client(url="redis://localhost:6379/0", max_connections=50)

task_store = RedisTaskStore(redis_client, prefix="myapp:tasks:")
queue_manager = RedisStreamsQueueManager(redis_client, prefix="myapp:queues:")
push_config_store = RedisPushNotificationConfigStore(redis_client, prefix="myapp:push:")

request_handler = DefaultRequestHandler(
    agent_executor=YourAgentExecutor(),
    task_store=task_store,
    queue_manager=queue_manager,
    push_config_store=push_config_store,
)

server = A2AStarletteApplication(
    agent_card=your_agent_card,
    http_handler=request_handler,
)
```

## Queue Components

The package provides both high-level queue managers and direct queue implementations:

### Queue Managers
- `RedisStreamsQueueManager` — Manages Redis Streams-based queues.
- `RedisPubSubQueueManager` — Manages Redis Pub/Sub-based queues.
- Both implement the A2A SDK's `QueueManager` interface, including the v1.1
  async `tap()` contract.

### Event Queues
- `RedisStreamsEventQueue` — Direct Redis Streams queue implementation.
- `RedisPubSubEventQueue` — Direct Redis Pub/Sub queue implementation.
- Both conform to the v1.1 `EventQueue` split. The legacy interface is
  available as `a2a.server.events.EventQueueLegacy` and re-exported from this
  package as `EventQueueLegacy` for convenience.

## Queue Manager Types: Streams vs Pub/Sub

### RedisStreamsQueueManager

**Key Features:**
- **Persistent storage**: Events remain in streams until explicitly trimmed.
- **Guaranteed delivery**: Consumer groups with acknowledgments prevent loss.
- **Load balancing**: Multiple consumers can share work via consumer groups.
- **Failure recovery**: Unacknowledged messages can be reclaimed.
- **Event replay**: Historical events can be re-read from any point in time.
- **Ordering**: Maintains strict insertion order with unique message IDs.

**Use Cases:**
- Task event queues requiring reliability.
- Audit trails and event history.
- Work distribution systems.
- Systems requiring failure recovery.
- Multi-consumer load balancing.

**Trade-offs:**
- Higher memory usage (events persist).
- More complex setup (consumer groups).
- Slightly higher latency than pub/sub.

### RedisPubSubQueueManager

**Key Features:**
- **Real-time delivery**: Events delivered immediately to active subscribers.
- **No persistence**: Events not stored, only delivered to active consumers.
- **Fire-and-forget**: No acknowledgments or delivery guarantees.
- **Broadcasting**: All subscribers receive all events.
- **Low latency**: Minimal overhead for immediate delivery.

**Use Cases:**
- Live status updates and notifications.
- Real-time dashboard updates.
- System event broadcasting.
- Non-critical event distribution.

**Not suitable for:**
- Critical event processing requiring guarantees.
- Systems requiring event replay or audit trails.
- Work queues requiring load balancing.

## Components

### Task Storage

#### RedisTaskStore
Stores task data in Redis hashes with JSON-serialized fields. Works with any
Redis server.

```python
from a2a.types import ListTasksRequest
from a2a_redis import RedisTaskStore

task_store = RedisTaskStore(redis_client, prefix="mytasks:")

# A2A TaskStore interface — every call takes a ServerCallContext.
await task_store.save(task, context)
task = await task_store.get("task-123", context)
await task_store.delete("task-123", context)

# Paginated list with optional filters.
page = await task_store.list(
    ListTasksRequest(page_size=50, context_id="ctx-001"),
    context,
)
for t in page.tasks:
    print(t.id, t.status.state)

if page.next_page_token:
    next_page = await task_store.list(
        ListTasksRequest(page_size=50, page_token=page.next_page_token),
        context,
    )
```

`list()` supports cursor-based pagination via `page_token` plus filters for
`context_id`, `status`, and `status_timestamp_after`. The cursor encodes the
owner resolved from the request context and is rejected on owner mismatch.

#### RedisJSONTaskStore
Same contract as `RedisTaskStore` but uses the Redis JSON module for native
nested-document storage. Requires Redis 8 or a Redis server with the RedisJSON
module installed.

```python
from a2a_redis import RedisJSONTaskStore

json_task_store = RedisJSONTaskStore(redis_client, prefix="mytasks:")
await json_task_store.save(task, context)
```

### Key schemas

`RedisTaskStore` and `RedisJSONTaskStore` write the following keys:

| Key                              | Type        | Purpose                                                                |
|----------------------------------|-------------|------------------------------------------------------------------------|
| `{prefix}{owner}:{task_id}`      | hash / JSON | The serialized task payload.                                           |
| `{prefix}idx:{owner}`            | sorted set  | Per-owner secondary index over `task_id`, ordered by insertion score.  |
| `{prefix}idxscore:{owner}`       | hash        | Per-owner monotonic score counter, used to assign new index entries.   |

`RedisPushNotificationConfigStore` writes:

| Key                                          | Type   | Purpose                                                                                                       |
|----------------------------------------------|--------|---------------------------------------------------------------------------------------------------------------|
| `{prefix}{owner}:{task_id}:{config_id}`      | string | One serialized (optionally Fernet-encrypted) push config per `(owner, task_id, config_id)`.                   |
| `{prefix}taskconfigs:{owner}:{task_id}`      | set    | Set of `config_id` values for a given `(owner, task_id)`; backs `get_info` / `delete_info`.                   |
| `{prefix}dispatch:{task_id}`                 | set    | Cross-owner set of `"{owner}:{config_id}"` members; backs `get_info_for_dispatch` for the notifier worker.    |

### Queue Managers

Both queue managers implement the A2A `QueueManager` interface with full async
support, including async `tap()`:

```python
import asyncio
from a2a_redis import RedisStreamsQueueManager, RedisPubSubQueueManager
from a2a_redis.streams_consumer_strategy import (
    ConsumerGroupConfig, ConsumerGroupStrategy,
)

# For reliable, persistent processing
streams_manager = RedisStreamsQueueManager(redis_client, prefix="myapp:streams:")

# For real-time, low-latency broadcasting
pubsub_manager = RedisPubSubQueueManager(redis_client, prefix="myapp:pubsub:")

# With a custom consumer group configuration (streams only)
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING)
streams_manager = RedisStreamsQueueManager(redis_client, consumer_config=config)


async def main() -> None:
    queue = await streams_manager.create_or_tap("task-123")

    await queue.enqueue_event({"type": "progress", "message": "Task started"})
    await queue.enqueue_event({"type": "progress", "message": "50% complete"})

    try:
        event = await queue.dequeue_event(no_wait=True)
        print(f"Got event: {event}")
        await queue.task_done()  # acknowledge (streams only)
    except RuntimeError:
        print("No events available")

    await queue.close()


asyncio.run(main())
```

### Consumer Group Strategies

The Streams queue manager supports different consumer group strategies:

```python
from a2a_redis.streams_consumer_strategy import (
    ConsumerGroupStrategy, ConsumerGroupConfig,
)

# Multiple instances share work across a single consumer group.
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING)

# Each instance gets its own consumer group.
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.INSTANCE_ISOLATED)

# Custom consumer group name.
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.CUSTOM, group_name="my_group")

streams_manager = RedisStreamsQueueManager(redis_client, consumer_config=config)
```

### RedisPushNotificationConfigStore

Stores push notification configurations per task. Implements the A2A
`PushNotificationConfigStore` interface and adds a cross-owner dispatch index
for the notifier worker.

```python
from a2a_redis import RedisPushNotificationConfigStore
from a2a.types import PushNotificationConfig

config_store = RedisPushNotificationConfigStore(redis_client, prefix="myapp:push:")

# Multiple configs per task are supported.
await config_store.set_info(
    "task-123",
    PushNotificationConfig(id="webhook_1", url="https://hook.example.com", token="t1"),
    context,
)
await config_store.set_info(
    "task-123",
    PushNotificationConfig(id="webhook_2", url="https://hook2.example.com", token="t2"),
    context,
)

# All configs for this owner+task.
configs = await config_store.get_info("task-123", context)

# Delete a single config or all configs for the task.
await config_store.delete_info("task-123", "webhook_1", context)
await config_store.delete_info("task-123", context=context)

# Cross-owner dispatch lookup (used by the notifier worker, no context).
all_configs = await config_store.get_info_for_dispatch("task-123")
```

### Encryption at rest

`RedisPushNotificationConfigStore` can transparently Fernet-encrypt every
serialized push config:

```bash
pip install "a2a-redis[encryption]"
```

```python
from cryptography.fernet import Fernet
from a2a_redis import RedisPushNotificationConfigStore

key = Fernet.generate_key()  # store this in a secret manager
config_store = RedisPushNotificationConfigStore(
    redis_client,
    prefix="myapp:push:",
    encryption_key=key,
)
```

Design note: decryption failures are **loud by design**. If a config was
written under a different key, the store raises rather than silently dropping
or returning plaintext. Rotate by writing through a new store instance and
re-saving each config via `set_info` — there is no in-place re-encrypt helper.

### Migration from v0.2

The v0.3 key layout is owner-scoped and not wire-compatible with v0.2. A
one-shot migration CLI ships with the package:

```bash
a2a-redis-migrate --help

a2a-redis-migrate \
    --redis-url redis://localhost:6379/0 \
    --default-owner legacy \
    --task-prefix task: \
    --push-prefix push_config: \
    --targets task,task-json,push-config
```

The script is idempotent: re-running it finds zero candidates once the
migration completes. See the module docstring in `src/a2a_redis/migrate.py`
for the full detection rule, supported targets, and known limitations — most
notably that **encrypted push configs are not migrated**. If you need
encryption at rest, run the migration first, then start the v0.3 store with
`encryption_key` set and re-save through `set_info`.

## End-to-end example

A complete agent + queue + push-notification example, runnable against a local
Redis, lives at [`examples/e2e/README.md`](examples/e2e/README.md). See
`examples/basic_usage.py` for a smaller component-by-component walkthrough.

## Requirements

- Python 3.11+
- `a2a-sdk >= 1.1.0, < 2`
- `redis >= 4.0.0`
- `uvicorn >= 0.35.0`

## Optional Dependencies

- `cryptography >= 42.0` (via `pip install "a2a-redis[encryption]"`) for
  Fernet-encrypted push configs.
- RedisJSON module (or Redis 8 / Redis Stack) for `RedisJSONTaskStore`.

## Development

```bash
# Clone the repository
git clone https://github.com/a2aproject/a2a-redis.git
cd a2a-redis

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uv sync --dev

# Run tests with coverage
uv run pytest --cov=a2a_redis --cov-report=term-missing

# Run linting and formatting
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pyright src/

# Install pre-commit hooks
uv run pre-commit install

# Run examples
uv run python examples/basic_usage.py
uv run python examples/redis_travel_agent.py
```

## Testing

Tests use Redis database 15 for isolation and include both mock and real Redis integration tests:

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_streams_queue_manager.py -v

# Run with coverage
uv run pytest --cov=a2a_redis --cov-report=term-missing
```

## License

MIT License
