"""Tiny webhook receiver used by the e2e push-notification scenario.

The push-notification dispatch path (BasePushNotificationSender) POSTs each
event to whatever URL was registered for the task. This module spins up a
Starlette server that captures those deliveries to a JSONL file so the test
(and the human running the scenarios manually) can poll for results without
parsing uvicorn's logs.

Endpoints
---------
* POST /webhook       -> append the JSON body + headers to the log file
* GET  /deliveries    -> return the current log as a JSON array
* GET  /reset         -> truncate the log
* GET  /health        -> liveness check

Run with:

    python -m examples.e2e.webhook_receiver --port 18001
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route


logger = logging.getLogger(__name__)

DEFAULT_LOG_PATH = "/tmp/a2a-e2e-webhook.log"


def _log_path() -> str:
    return os.environ.get("A2A_E2E_WEBHOOK_LOG", DEFAULT_LOG_PATH)


def _read_deliveries() -> list[dict[str, Any]]:
    path = _log_path()
    if not os.path.exists(path):
        return []
    out: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                # Skip corrupt lines rather than failing the whole read --
                # JSONL is line-oriented so individual breakage is recoverable.
                continue
    return out


def build_app() -> Starlette:
    """Build the Starlette app. Single shared lock guards file writes."""
    write_lock = asyncio.Lock()

    async def webhook(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:
            raw = (await request.body()).decode("utf-8", errors="replace")
            body = {"_raw": raw}

        record = {
            "received_at": time.time(),
            "headers": dict(request.headers),
            "body": body,
        }
        path = _log_path()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        line = json.dumps(record, default=str)
        async with write_lock:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        print(f"[webhook] received delivery -> {path}", file=sys.stderr, flush=True)
        return JSONResponse({"status": "received"})

    async def deliveries(_request: Request) -> JSONResponse:
        return JSONResponse(_read_deliveries())

    async def reset(_request: Request) -> JSONResponse:
        path = _log_path()
        async with write_lock:
            if os.path.exists(path):
                os.remove(path)
        print(f"[webhook] reset log {path}", file=sys.stderr, flush=True)
        return JSONResponse({"status": "reset"})

    async def health(_request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    return Starlette(
        routes=[
            Route("/webhook", webhook, methods=["POST"]),
            Route("/deliveries", deliveries, methods=["GET"]),
            Route("/reset", reset, methods=["GET"]),
            Route("/health", health, methods=["GET"]),
        ]
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="a2a-redis e2e webhook")
    parser.add_argument("--port", type=int, default=18001)
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    app = build_app()
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=args.port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    print(
        f"READY webhook port={args.port} log={_log_path()}",
        file=sys.stderr,
        flush=True,
    )
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
