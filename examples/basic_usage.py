"""Minimal demonstration of the a2a-redis component surface (v1.1).

This script shows the smallest end-to-end exercise of the two persistent
stores -- ``RedisTaskStore`` and ``RedisPushNotificationConfigStore`` --
using the v1.1 SDK contract (owner-scoped, ServerCallContext-aware).

For a full server + client + push-notification webhook example see
``examples/e2e/``; that flow is also exercised by ``tests/test_e2e.py``.
"""

from __future__ import annotations

import asyncio

import redis.asyncio as redis_async

from a2a.auth.user import User
from a2a.server.context import ServerCallContext
from a2a.types.a2a_pb2 import (
    ListTasksRequest,
    Task,
    TaskPushNotificationConfig,
    TaskState,
    TaskStatus,
)

from a2a_redis import (
    RedisPushNotificationConfigStore,
    RedisTaskStore,
)


class _DemoUser(User):
    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def user_name(self) -> str:
        return self._name


async def main() -> None:
    redis_client = redis_async.from_url('redis://localhost:6379/0')
    ctx = ServerCallContext(user=_DemoUser('alice'))

    task_store = RedisTaskStore(redis_client, prefix='basic_usage:task:')
    push_store = RedisPushNotificationConfigStore(
        redis_client, prefix='basic_usage:push:'
    )

    # --- TaskStore round-trip --------------------------------------------
    task = Task(
        id='task-001',
        context_id='ctx-001',
        status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED),
    )
    await task_store.save(task, ctx)

    fetched = await task_store.get('task-001', ctx)
    assert fetched is not None and fetched.id == 'task-001'

    page = await task_store.list(ListTasksRequest(page_size=10), ctx)
    print(f'TaskStore: stored {page.total_size} task(s) for alice')

    await task_store.delete('task-001', ctx)

    # --- PushNotificationConfigStore round-trip --------------------------
    cfg = TaskPushNotificationConfig(
        id='cfg-1',
        task_id='task-002',
        url='https://webhook.example.com/notify',
        token='secret-token',
    )
    await push_store.set_info('task-002', cfg, ctx)

    configs = await push_store.get_info('task-002', ctx)
    assert len(configs) == 1
    print(
        f'PushStore: alice has {len(configs)} config(s) on task-002 '
        f'-> {configs[0].url}'
    )

    await push_store.delete_info('task-002', ctx)
    await redis_client.aclose()
    print('Done.')


if __name__ == '__main__':
    asyncio.run(main())
