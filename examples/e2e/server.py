"""Runnable A2A server backed by all three a2a-redis components.

Designed for end-to-end exercising of the package: it is the same server the
``tests/test_e2e.py`` integration test spins up via subprocess, and is also
usable for a manual smoke test alongside ``client.py``.

Identity in this example is read from a request header (``x-a2a-user``) by a
custom ``ServerCallContextBuilder``. That matches the
``sdkPatterns.user_auth_pattern`` shape (``DefaultServerCallContextBuilder``
override) and lets a single server be exercised by multiple authenticated
users (alice / bob / ...). When the header is absent, the default
``UnauthenticatedUser()`` is used.

Run with:

    python -m examples.e2e.server --port 18000 --redis-url redis://localhost:6379/0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import httpx
import redis.asyncio as redis_async
import uvicorn

from a2a.auth.user import UnauthenticatedUser, User
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import (
    DefaultServerCallContextBuilder,
    create_agent_card_routes,
    create_jsonrpc_routes,
    create_rest_routes,
)
from a2a.server.tasks import BasePushNotificationSender
from a2a.types.a2a_pb2 import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentProvider,
    AgentSkill,
)
from starlette.applications import Starlette
from starlette.requests import Request

from a2a_redis import (
    RedisPushNotificationConfigStore,
    RedisStreamsQueueManager,
    RedisTaskStore,
)
from examples.e2e.scripted_executor import ScriptedAgentExecutor


logger = logging.getLogger(__name__)

USER_HEADER = "x-a2a-user"


class _HeaderUser(User):
    """Authenticated user identified by ``user_name`` (read from a header)."""

    def __init__(self, user_name: str) -> None:
        self._user_name = user_name

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def user_name(self) -> str:
        return self._user_name


class _HeaderUserContextBuilder(DefaultServerCallContextBuilder):
    """Builds a ServerCallContext whose user comes from ``x-a2a-user``.

    Mirrors the multi-tenant pattern used by the upstream a2a-python push
    notification integration test (`_HeaderUserContextBuilder`).
    """

    def build_user(self, request: Request) -> User:
        user_name = request.headers.get(USER_HEADER)
        if user_name:
            return _HeaderUser(user_name)
        # Fall back to the default behaviour (Starlette scope-based) and then
        # finally to an explicit fallback so unauthenticated callers still
        # land on a deterministic owner string ("").
        if "user" in request.scope:
            return super().build_user(request)
        # Env-overridable default user; useful for ad-hoc single-tenant runs.
        default = os.environ.get("A2A_E2E_OWNER", "").strip()
        if default:
            return _HeaderUser(default)
        return UnauthenticatedUser()


def _build_agent_card(port: int) -> AgentCard:
    base_url = f"http://localhost:{port}"
    return AgentCard(
        name="redis-e2e-agent",
        description="Redis-backed agent used by the a2a-redis e2e example.",
        provider=AgentProvider(organization="a2a-redis e2e", url="https://example.com"),
        version="0.1.0",
        capabilities=AgentCapabilities(
            streaming=True,
            push_notifications=True,
        ),
        default_input_modes=["text"],
        default_output_modes=["text", "task-status"],
        skills=[
            AgentSkill(
                id="echo",
                name="echo",
                description=(
                    'Uppercases the input and appends " (processed)". '
                    'Special commands: "fail", "wait".'
                ),
                tags=["echo", "e2e"],
                examples=["hello", "wait", "fail"],
                input_modes=["text"],
                output_modes=["text", "task-status"],
            )
        ],
        supported_interfaces=[
            AgentInterface(
                protocol_binding="HTTP+JSON",
                protocol_version="1.0",
                url=f"{base_url}/a2a/rest",
            ),
            AgentInterface(
                protocol_binding="JSONRPC",
                protocol_version="1.0",
                url=f"{base_url}/a2a/jsonrpc",
            ),
        ],
    )


def build_app(
    redis_url: str,
    port: int,
    encryption_key: str | None = None,
) -> Starlette:
    """Wire all components and return the runnable Starlette app."""
    redis_client = redis_async.from_url(redis_url)

    task_store = RedisTaskStore(redis_client, prefix="e2e:task:")
    # queue_manager is kept around purely as an example of constructing one;
    # DefaultRequestHandler (v1.1) accepts it only for backward compatibility
    # and will not use it for event delivery.
    queue_manager = RedisStreamsQueueManager(  # noqa: F841
        redis_client, prefix="e2e:queue:"
    )
    push_config_store = RedisPushNotificationConfigStore(
        redis_client,
        prefix="e2e:push:",
        encryption_key=encryption_key,
    )

    agent_card = _build_agent_card(port)

    notifications_client = httpx.AsyncClient()
    push_sender = BasePushNotificationSender(
        httpx_client=notifications_client,
        config_store=push_config_store,
    )

    request_handler = DefaultRequestHandler(
        agent_executor=ScriptedAgentExecutor(),
        task_store=task_store,
        agent_card=agent_card,
        push_config_store=push_config_store,
        push_sender=push_sender,
    )

    context_builder = _HeaderUserContextBuilder()

    rest_routes = create_rest_routes(
        request_handler=request_handler,
        path_prefix="/a2a/rest",
        context_builder=context_builder,
    )
    jsonrpc_routes = create_jsonrpc_routes(
        request_handler=request_handler,
        rpc_url="/a2a/jsonrpc",
        context_builder=context_builder,
    )
    agent_card_routes = create_agent_card_routes(agent_card=agent_card)

    return Starlette(routes=[*agent_card_routes, *jsonrpc_routes, *rest_routes])


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="a2a-redis e2e server")
    parser.add_argument("--port", type=int, default=18000)
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument(
        "--encryption-key",
        default=None,
        help="Optional URL-safe base64-encoded Fernet key for push configs.",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    app = build_app(
        redis_url=args.redis_url,
        port=args.port,
        encryption_key=args.encryption_key,
    )
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=args.port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    # Emit READY once on stderr so the test harness can sync without
    # screen-scraping uvicorn's own startup output.
    print(f"READY port={args.port}", file=sys.stderr, flush=True)
    await server.serve()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
