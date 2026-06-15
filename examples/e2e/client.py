"""End-to-end client scenarios for the a2a-redis example.

This module is both:

* an importable library of ``scenario_*`` coroutines that the test in
  ``tests/test_e2e.py`` calls directly, and
* a small CLI so the same scenarios can be reproduced manually against a
  running server / webhook pair.

All scenarios talk to the server over HTTP+JSON (the REST transport). User
identity is carried in the ``x-a2a-user`` header per the
``_HeaderUserContextBuilder`` installed by ``server.py``; the builder maps
that header to a ``User.user_name`` which the default
``resolve_user_scope`` owner resolver uses to partition Redis keys.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

import httpx

from a2a.client.client import ClientConfig
from a2a.client.client_factory import ClientFactory
from a2a.types.a2a_pb2 import (
    GetTaskRequest,
    ListTasksRequest,
    Message,
    Part,
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    TaskPushNotificationConfig,
    TaskState,
)
from a2a.utils.constants import TransportProtocol


# ---------------------------------------------------------------------------
# Client construction
# ---------------------------------------------------------------------------


async def _build_client(base_url: str, user_name: str):
    """Build a per-scenario A2A client that injects ``x-a2a-user``.

    A fresh httpx.AsyncClient is created so each scenario / user pairing
    carries its own default headers. The ClientFactory resolves the
    AgentCard from ``<base_url>/.well-known/agent-card.json`` and uses the
    HTTP+JSON transport.
    """
    httpx_client = httpx.AsyncClient(headers={"x-a2a-user": user_name})
    factory = ClientFactory(
        config=ClientConfig(
            httpx_client=httpx_client,
            streaming=True,
            supported_protocol_bindings=[TransportProtocol.HTTP_JSON],
        )
    )
    client = await factory.create_from_url(base_url)
    return client, httpx_client


def _user_message(text: str, message_id: str) -> Message:
    return Message(
        role=Role.ROLE_USER,
        message_id=message_id,
        parts=[Part(text=text)],
    )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


async def scenario_send_and_get(base_url: str, user_name: str = "alice") -> str:
    """Send a single non-streaming message, poll until complete, return id.

    Asserts the resulting artifact text matches ``HELLO (processed)``.
    """
    client, httpx_client = await _build_client(base_url, user_name)
    try:
        client._config.streaming = False
        config = SendMessageConfiguration(return_immediately=True)
        message = _user_message("hello", "msg-e2e-send-and-get")
        request = SendMessageRequest(message=message, configuration=config)

        events: list[Any] = []
        async for ev in client.send_message(request=request):
            events.append(ev)
        if not events:
            raise RuntimeError("No response events from send_message")
        task_id = events[0].task.id

        # Poll until the task reaches a terminal state. The executor sleeps
        # only briefly so a handful of polls is more than enough.
        for _ in range(50):
            task = await client.get_task(request=GetTaskRequest(id=task_id))
            if task.status.state == TaskState.TASK_STATE_COMPLETED:
                break
            await asyncio.sleep(0.05)
        else:  # pragma: no cover - defensive
            raise RuntimeError(
                f"Task {task_id} did not complete in time (state={task.status.state})"
            )

        if not task.artifacts:
            raise AssertionError("Completed task is missing artifacts")
        artifact = task.artifacts[0]
        text = artifact.parts[0].text if artifact.parts else ""
        if text != "HELLO (processed)":
            raise AssertionError(
                f'Unexpected artifact text {text!r}; expected "HELLO (processed)"'
            )
        return task_id
    finally:
        await client.close()
        await httpx_client.aclose()


async def scenario_streaming(base_url: str, user_name: str = "alice") -> list[Any]:
    """Stream a message and return all events collected before the stream closes.

    The transport yields ``StreamResponse`` wrapper messages whose ``WhichOneof``
    payload selects between ``task``, ``msg``, ``status_update``, and
    ``artifact_update``. Tests assert on the underlying typed events, so we
    unwrap each wrapper into its concrete payload before returning.
    """
    client, httpx_client = await _build_client(base_url, user_name)
    try:
        client._config.streaming = True
        message = _user_message("stream me", "msg-e2e-streaming")
        request = SendMessageRequest(message=message)

        events: list[Any] = []
        async for ev in client.send_message(request=request):
            unwrapped = _unwrap_stream_event(ev)
            events.append(unwrapped)
        return events
    finally:
        await client.close()
        await httpx_client.aclose()


def _unwrap_stream_event(ev: Any) -> Any:
    """Return the concrete event payload from a transport stream wrapper.

    The HTTP+JSON / JSON-RPC transports both yield a oneof-bearing wrapper
    around the actual Task / Message / TaskStatusUpdateEvent /
    TaskArtifactUpdateEvent. If the object exposes ``WhichOneof('payload')``
    we route through that; otherwise we return ``ev`` unchanged so callers
    can also handle already-unwrapped events.
    """
    which = getattr(ev, "WhichOneof", None)
    if which is None:
        return ev
    try:
        field = which("payload")
    except Exception:
        return ev
    if not field:
        return ev
    return getattr(ev, field, ev)


async def scenario_list_with_filters(
    base_url: str, user_name: str = "alice", count: int = 12
) -> dict[str, list[tuple[int, str]]]:
    """Create ``count`` tasks across two context_ids and exercise list filters.

    Returns a dict mapping scenario name -> list of (page_index, page_token)
    tuples describing the pages collected. Tests inspect both the per-page
    counts and the token chain to verify pagination is functioning.
    """
    client, httpx_client = await _build_client(base_url, user_name)
    try:
        client._config.streaming = False
        config = SendMessageConfiguration(return_immediately=True)

        contexts = ["ctx-a", "ctx-b"]
        # Create ``count`` tasks, interleaving context_ids so the per-context
        # filter has at least 2 non-trivial pages.
        for i in range(count):
            ctx = contexts[i % 2]
            message = Message(
                role=Role.ROLE_USER,
                message_id=f"msg-e2e-list-{i}",
                context_id=ctx,
                parts=[Part(text=f"list {i}")],
            )
            # Drain the iterator so the server actually processes the task.
            async for _ in client.send_message(
                request=SendMessageRequest(message=message, configuration=config)
            ):
                pass

        async def _paginate(req: ListTasksRequest) -> list[tuple[int, str]]:
            pages: list[tuple[int, str]] = []
            token = ""
            page_index = 0
            while True:
                if token:
                    req.page_token = token
                resp = await client.list_tasks(request=req)
                pages.append((len(resp.tasks), token))
                token = resp.next_page_token
                page_index += 1
                if not token or page_index > 20:
                    break
            return pages

        results: dict[str, list[tuple[int, str]]] = {}
        results["by_context_a"] = await _paginate(
            ListTasksRequest(page_size=5, context_id="ctx-a")
        )
        results["by_context_b"] = await _paginate(
            ListTasksRequest(page_size=5, context_id="ctx-b")
        )
        results["by_status_completed"] = await _paginate(
            ListTasksRequest(page_size=5, status=TaskState.TASK_STATE_COMPLETED)
        )
        results["all_paginated"] = await _paginate(ListTasksRequest(page_size=5))
        return results
    finally:
        await client.close()
        await httpx_client.aclose()


async def scenario_push_multi_owner_dispatch(
    base_url: str,
    webhook_url: str,
    redis_url: str = "redis://localhost:6379/15",
    push_prefix: str = "e2e:push:",
    encryption_key: str | None = None,
    encrypted: bool = False,
) -> tuple[str, int]:
    """Exercise cross-owner push-notification dispatch via Redis.

    Walks through the following sequence:

    1. ``alice`` sends an echo message that creates a task. We use the echo
       (not "wait") path because BasePushNotificationSender only fires for
       events that pass through the active task pipeline; the completion
       event is what we want to fan out.
    2. ``alice`` registers a push-notification config on her own owner scope
       via the SDK's REST API. The server checks task ownership before
       writing the config, so this path naturally covers alice.
    3. ``bob`` is given a second push-notification config on the same task_id
       by writing directly to the ``RedisPushNotificationConfigStore`` (the
       SDK's request-handler enforces owner-scoped task lookup before
       allowing set_info, so we cannot reuse the REST path for a user who
       doesn't own the task). This is exactly the situation the dispatch SET
       is designed for: configs across owners fan out via task_id.

    Returns ``(task_id, num_configs)``. Tests can use ``task_id`` to inspect
    the dispatch SET in Redis and ``num_configs`` to verify the expected
    minimum webhook delivery count.

    The ``encrypted`` flag controls whether bob's direct-to-Redis write
    encrypts the payload; it must match the server's --encryption-key. When
    encrypted, ``encryption_key`` is required.
    """
    # Step 1: alice creates a task by sending an echo message.
    alice, alice_http = await _build_client(base_url, "alice")
    try:
        alice._config.streaming = False

        config = SendMessageConfiguration(return_immediately=True)
        first_event = await anext(
            alice.send_message(
                request=SendMessageRequest(
                    message=_user_message("slow:push-please", "msg-e2e-push-1"),
                    configuration=config,
                )
            )
        )
        task_id = first_event.task.id

        # Step 2: alice registers her push config for this task via the API.
        await alice.create_task_push_notification_config(
            request=TaskPushNotificationConfig(
                id="alice-cfg",
                task_id=task_id,
                url=webhook_url,
                token="alice-token",
            )
        )
    finally:
        await alice.close()
        await alice_http.aclose()

    # Step 3: bob's config is written directly to the store with bob as the
    # owner. We construct the RedisPushNotificationConfigStore the same way
    # server.py does so encryption / prefix line up bit-for-bit.
    import redis.asyncio as redis_async
    from a2a.server.context import ServerCallContext
    from a2a.auth.user import User
    from a2a_redis import RedisPushNotificationConfigStore

    class _NamedUser(User):
        def __init__(self, name: str) -> None:
            self._name = name

        @property
        def is_authenticated(self) -> bool:
            return True

        @property
        def user_name(self) -> str:
            return self._name

    redis_client = redis_async.from_url(redis_url)
    try:
        store = RedisPushNotificationConfigStore(
            redis_client,
            prefix=push_prefix,
            encryption_key=encryption_key if encrypted else None,
        )
        bob_ctx = ServerCallContext(user=_NamedUser("bob"))
        await store.set_info(
            task_id,
            TaskPushNotificationConfig(
                id="bob-cfg",
                task_id=task_id,
                url=webhook_url,
                token="bob-token",
            ),
            bob_ctx,
        )
    finally:
        await redis_client.aclose()

    # Two configs registered -> two webhook deliveries per dispatch event.
    return task_id, 2


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


_SCENARIOS = {
    "scenario_send_and_get": scenario_send_and_get,
    "scenario_streaming": scenario_streaming,
    "scenario_list_with_filters": scenario_list_with_filters,
    "scenario_push_multi_owner_dispatch": scenario_push_multi_owner_dispatch,
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="a2a-redis e2e client")
    parser.add_argument(
        "scenario",
        choices=sorted(_SCENARIOS.keys()),
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:18000",
        help="Server base URL.",
    )
    parser.add_argument(
        "--webhook-url",
        default="http://localhost:18001/webhook",
        help="Webhook URL (only used by the push scenario).",
    )
    parser.add_argument("--user", default="alice")
    parser.add_argument("--count", type=int, default=12)
    return parser.parse_args(argv)


async def _dispatch(args: argparse.Namespace) -> Any:
    fn = _SCENARIOS[args.scenario]
    if args.scenario == "scenario_send_and_get":
        return await fn(args.base_url, args.user)
    if args.scenario == "scenario_streaming":
        events = await fn(args.base_url, args.user)
        return [type(e).__name__ for e in events]
    if args.scenario == "scenario_list_with_filters":
        return await fn(args.base_url, args.user, args.count)
    if args.scenario == "scenario_push_multi_owner_dispatch":
        return await fn(args.base_url, args.webhook_url)
    raise ValueError(f"Unknown scenario: {args.scenario}")  # pragma: no cover


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    result = asyncio.run(_dispatch(args))
    json.dump(result, sys.stdout, default=str, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
