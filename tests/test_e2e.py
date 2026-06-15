"""End-to-end orchestration test for the v1.1 example surface.

This module spins up the two example subprocesses (server + webhook receiver)
that live under ``examples/e2e/`` and drives them via the scenario coroutines
in ``examples.e2e.client``. It then asserts on both the high-level returned
artifacts/events *and* the raw Redis state that the example components write,
so the test acts as a real smoke check of the whole stack:

* ``RedisTaskStore`` (owner-scoped hash + sorted-set index)
* ``RedisPushNotificationConfigStore`` (cross-owner dispatch SET)
* ``BasePushNotificationSender`` (HTTP delivery to the webhook)

All tests are guarded by the ``e2e`` marker so ``pytest -m "not e2e"`` skips
the heavy subprocess machinery for ordinary unit runs.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import redis
import redis.asyncio as redis_async

from a2a.types.a2a_pb2 import TaskState


pytestmark = pytest.mark.e2e


REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_PORT = 18000
WEBHOOK_PORT = 18001
BASE_URL = f"http://127.0.0.1:{SERVER_PORT}"
WEBHOOK_URL = f"http://127.0.0.1:{WEBHOOK_PORT}/webhook"

# The e2e server is launched with --redis-url pointing at db 15 so we share
# the same logical db that conftest's redis_client uses. The example key
# prefixes (`e2e:*`) avoid clashing with other tests in this db.
REDIS_DB = 15
REDIS_URL = f"redis://localhost:6379/{REDIS_DB}"

WEBHOOK_LOG_PATH = "/tmp/a2a-e2e-webhook-test.log"


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------


async def wait_for_url(url: str, timeout: float = 15.0) -> None:
    """Poll ``url`` until it returns 2xx, or raise on timeout."""
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    async with httpx.AsyncClient(timeout=2.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(url)
                if 200 <= resp.status_code < 300:
                    return
            except Exception as exc:  # pragma: no cover - retry
                last_exc = exc
            await asyncio.sleep(0.1)
    raise TimeoutError(
        f"URL {url} did not become ready within {timeout}s (last error: {last_exc!r})"
    )


async def wait_for_log(
    proc: subprocess.Popen, marker: str, timeout: float = 15.0
) -> None:
    """Block until ``marker`` appears on ``proc.stderr`` or timeout elapses."""
    deadline = time.monotonic() + timeout
    loop = asyncio.get_running_loop()
    stderr = proc.stderr
    if stderr is None:
        raise RuntimeError("Process has no captured stderr")

    while time.monotonic() < deadline:
        if proc.poll() is not None:
            remaining = stderr.read() or b""
            raise RuntimeError(
                f"Process exited (rc={proc.returncode}) before emitting "
                f"{marker!r}. tail: {remaining[-2000:]!r}"
            )
        line = await loop.run_in_executor(None, stderr.readline)
        if not line:
            await asyncio.sleep(0.05)
            continue
        sys.stderr.write(f"[child] {line.decode(errors='replace')}")
        if marker.encode() in line:
            return
    raise TimeoutError(
        f"Did not see marker {marker!r} on child stderr within {timeout}s"
    )


def _spawn(
    argv: list[str], extra_env: dict[str, str] | None = None
) -> subprocess.Popen:
    env = os.environ.copy()
    # Make `python -m examples.e2e.X` importable from the repo root.
    pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + pythonpath if pythonpath else "")
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        argv,
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=3.0)
            return
        except subprocess.TimeoutExpired:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
            return
        except subprocess.TimeoutExpired:
            pass
        proc.kill()
        proc.wait(timeout=3.0)
    except Exception:  # pragma: no cover - best effort
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _redis_available() -> bool:
    try:
        c = redis.Redis(host="localhost", port=6379, db=REDIS_DB)
        c.ping()
        c.close()
        return True
    except Exception:
        return False


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def e2e_redis():
    """Module-scoped async Redis client targeting the e2e test db.

    Flushes the db before yielding so each module run starts clean. Skips the
    entire e2e module if Redis is unavailable.
    """
    if not _redis_available():
        pytest.skip("Redis server not available on localhost:6379")

    client = redis_async.Redis(
        host="localhost", port=6379, db=REDIS_DB, decode_responses=False
    )
    await client.ping()
    await client.flushdb()
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def webhook_proc():
    """Spawn the webhook receiver subprocess for the duration of the module."""
    # Point the receiver at a dedicated log file we can reason about.
    proc = _spawn(
        [
            sys.executable,
            "-m",
            "examples.e2e.webhook_receiver",
            "--port",
            str(WEBHOOK_PORT),
        ],
        extra_env={"A2A_E2E_WEBHOOK_LOG": WEBHOOK_LOG_PATH},
    )
    try:
        await wait_for_url(f"http://127.0.0.1:{WEBHOOK_PORT}/health", timeout=15.0)
        yield proc
    finally:
        _terminate(proc)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def server_proc(webhook_proc, e2e_redis):
    """Spawn the example A2A server pointed at the same Redis db as the test."""
    proc = _spawn(
        [
            sys.executable,
            "-m",
            "examples.e2e.server",
            "--port",
            str(SERVER_PORT),
            "--redis-url",
            REDIS_URL,
        ],
    )
    try:
        await wait_for_url(f"{BASE_URL}/.well-known/agent-card.json", timeout=20.0)
        yield proc
    finally:
        _terminate(proc)


@pytest_asyncio.fixture(loop_scope="module")
async def fresh_state(e2e_redis):
    """Flushes the test db and the webhook log before each test."""
    await e2e_redis.flushdb()
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.get(f"http://127.0.0.1:{WEBHOOK_PORT}/reset")
    except Exception:
        # Webhook may not be running yet for non-push tests; ignore.
        pass
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """Full subprocess + Redis assertions across the example surface."""

    @pytest.mark.asyncio(loop_scope="module")
    async def test_send_message_round_trip(self, server_proc, fresh_state, e2e_redis):
        from examples.e2e.client import scenario_send_and_get

        task_id = await scenario_send_and_get(BASE_URL, user_name="alice")

        # ---- Raw-Redis assertions on the task hash ----
        task_key = f"e2e:task:alice:{task_id}"
        assert await e2e_redis.exists(task_key) == 1, (
            f"Expected task hash at {task_key} to exist"
        )

        raw = await e2e_redis.hgetall(task_key)
        decoded = {
            (k.decode() if isinstance(k, bytes) else k): (
                v.decode() if isinstance(v, bytes) else v
            )
            for k, v in raw.items()
        }
        expected_fields = {
            "task_payload",
            "owner",
            "context_id",
            "last_updated",
            "protocol_version",
        }
        missing = expected_fields - set(decoded.keys())
        assert not missing, (
            f"Task hash {task_key} missing fields {missing}; got {decoded.keys()}"
        )
        assert decoded["owner"] == "alice"
        assert decoded["protocol_version"] == "1.0"
        assert decoded["context_id"], "context_id should be a non-empty string"
        # task_payload is JSON and must reference the task id.
        assert task_id in decoded["task_payload"]
        assert "HELLO (processed)" in decoded["task_payload"], (
            "Stored task payload should contain the artifact text"
        )

        # ---- Owner-scoped sorted-set index ----
        index_key = "e2e:task:idx:alice"
        members = await e2e_redis.zrange(index_key, 0, -1)
        assert len(members) == 1, f"Expected exactly one index member, got {members}"
        member = members[0]
        if isinstance(member, bytes):
            member = member.decode()
        assert ":" in member
        micros_str, member_task_id = member.split(":", 1)
        assert member_task_id == task_id
        assert micros_str.isdigit() and len(micros_str) == 20, (
            f"micros prefix should be zero-padded 20 digits, got {micros_str!r}"
        )

        # ---- idxscore hash ----
        score_key = "e2e:task:idxscore:alice"
        score_raw = await e2e_redis.hget(score_key, task_id)
        assert score_raw is not None, (
            f"Expected score entry for {task_id} in {score_key}"
        )
        score = float(score_raw.decode() if isinstance(score_raw, bytes) else score_raw)
        assert score < 0, f"Index score should be negative, got {score}"

    @pytest.mark.asyncio(loop_scope="module")
    async def test_streaming_events(self, server_proc, fresh_state):
        from examples.e2e.client import scenario_streaming
        from a2a.types.a2a_pb2 import (
            Task,
            TaskArtifactUpdateEvent,
            TaskStatusUpdateEvent,
        )

        events = await scenario_streaming(BASE_URL, user_name="alice")
        assert events, "Streaming returned no events"

        # Walk the stream and verify the expected canonical order. Extra
        # events between checkpoints are allowed -- the executor is free to
        # interleave additional status messages.
        def _state_of(ev) -> int | None:
            if isinstance(ev, Task) and ev.HasField("status"):
                return ev.status.state
            if isinstance(ev, TaskStatusUpdateEvent) and ev.HasField("status"):
                return ev.status.state
            return None

        # 1. First, a Task carrying SUBMITTED.
        idx = 0
        while idx < len(events) and not (
            isinstance(events[idx], Task)
            and _state_of(events[idx]) == TaskState.TASK_STATE_SUBMITTED
        ):
            idx += 1
        assert idx < len(events), (
            f"No SUBMITTED Task event found; got types "
            f"{[type(e).__name__ for e in events]}"
        )

        # 2. Then a TaskStatusUpdateEvent for WORKING.
        idx += 1
        working_idx = None
        for j in range(idx, len(events)):
            if (
                isinstance(events[j], TaskStatusUpdateEvent)
                and _state_of(events[j]) == TaskState.TASK_STATE_WORKING
            ):
                working_idx = j
                break
        assert working_idx is not None, (
            f"No WORKING TaskStatusUpdateEvent found after SUBMITTED; "
            f"got {[type(e).__name__ for e in events]}"
        )

        # 3. Then a TaskArtifactUpdateEvent.
        artifact_idx = None
        for j in range(working_idx + 1, len(events)):
            if isinstance(events[j], TaskArtifactUpdateEvent):
                artifact_idx = j
                break
        assert artifact_idx is not None, (
            f"No TaskArtifactUpdateEvent found after WORKING; got "
            f"{[type(e).__name__ for e in events]}"
        )

        # 4. Finally a TaskStatusUpdateEvent for COMPLETED.
        completed_idx = None
        for j in range(artifact_idx + 1, len(events)):
            if (
                isinstance(events[j], TaskStatusUpdateEvent)
                and _state_of(events[j]) == TaskState.TASK_STATE_COMPLETED
            ):
                completed_idx = j
                break
        assert completed_idx is not None, (
            f"No COMPLETED TaskStatusUpdateEvent found after artifact; got "
            f"{[type(e).__name__ for e in events]}"
        )

    @pytest.mark.asyncio(loop_scope="module")
    async def test_list_with_filters_and_pagination(
        self, server_proc, fresh_state, e2e_redis
    ):
        from examples.e2e.client import scenario_list_with_filters

        results = await scenario_list_with_filters(
            BASE_URL, user_name="alice", count=12
        )

        def _flatten(pages):
            return sum(count for count, _ in pages)

        assert _flatten(results["all_paginated"]) == 12, (
            f"Expected 12 total tasks across pages, got {results['all_paginated']}"
        )
        assert _flatten(results["by_context_a"]) == 6, (
            f"Expected 6 ctx-a tasks, got {results['by_context_a']}"
        )
        assert _flatten(results["by_context_b"]) == 6, (
            f"Expected 6 ctx-b tasks, got {results['by_context_b']}"
        )

        # page_size=5 over 12 items -> pages of 5, 5, 2.
        all_pages = [count for count, _ in results["all_paginated"]]
        assert all_pages == [5, 5, 2], (
            f"Expected page counts [5, 5, 2], got {all_pages}"
        )

        # All 12 tasks should be reflected in the owner-scoped sorted-set.
        index_key = "e2e:task:idx:alice"
        assert await e2e_redis.zcard(index_key) == 12

    @pytest.mark.asyncio(loop_scope="module")
    async def test_push_notification_multi_owner_dispatch(
        self, server_proc, webhook_proc, fresh_state, e2e_redis
    ):
        from examples.e2e.client import scenario_push_multi_owner_dispatch

        # Reset webhook log explicitly (fresh_state already did, but be sure).
        async with httpx.AsyncClient(timeout=2.0) as http:
            r = await http.get(f"http://127.0.0.1:{WEBHOOK_PORT}/reset")
            assert r.status_code == 200

        (
            task_id_from_scenario,
            expected_configs,
        ) = await scenario_push_multi_owner_dispatch(
            BASE_URL, WEBHOOK_URL, redis_url=REDIS_URL
        )
        assert expected_configs == 2
        assert task_id_from_scenario

        # Find the dispatch SET. There should be exactly one (one task).
        dispatch_keys = []
        async for key in e2e_redis.scan_iter(match=b"e2e:push:dispatch:*"):
            dispatch_keys.append(key)
        assert len(dispatch_keys) == 1, (
            f"Expected exactly one dispatch set, got {dispatch_keys}"
        )

        dispatch_members_raw = await e2e_redis.smembers(dispatch_keys[0])
        dispatch_members = {
            (m.decode() if isinstance(m, bytes) else m) for m in dispatch_members_raw
        }
        assert len(dispatch_members) == 2, (
            f"Expected 2 members in dispatch set, got {dispatch_members}"
        )
        owners = {m.split(":", 1)[0] for m in dispatch_members}
        assert owners == {"alice", "bob"}, (
            f"Dispatch set should fan out to alice and bob, got {owners}"
        )

        # Recover the task_id from the dispatch key.
        dispatch_key_str = (
            dispatch_keys[0].decode()
            if isinstance(dispatch_keys[0], bytes)
            else dispatch_keys[0]
        )
        task_id = dispatch_key_str.removeprefix("e2e:push:dispatch:")
        assert task_id

        # Poll the webhook for deliveries. The scenario sends one echo
        # message that completes -> push sender fans the completion event
        # out to both registered configs. We need to wait long enough for the
        # COMPLETED status to fan out (the scripted executor sleeps between
        # status transitions so external observers can wire push configs).
        import json as _json

        deadline = time.monotonic() + 15.0
        deliveries: list[dict] = []
        async with httpx.AsyncClient(timeout=2.0) as http:
            while time.monotonic() < deadline:
                r = await http.get(f"http://127.0.0.1:{WEBHOOK_PORT}/deliveries")
                if r.status_code == 200:
                    deliveries = r.json()
                    blob_all = _json.dumps(deliveries, default=str)
                    if len(deliveries) >= 2 and (
                        "COMPLETED" in blob_all or "completed" in blob_all
                    ):
                        break
                await asyncio.sleep(0.2)

        assert len(deliveries) >= 2, (
            f"Expected >=2 webhook deliveries within 10s, got "
            f"{len(deliveries)}: {deliveries!r}"
        )

        # Validate each delivery references the task and looks like a
        # COMPLETED transition. Different SDK versions wrap the event
        # differently, so we serialize the body to JSON and grep.
        completed_seen = 0
        for d in deliveries:
            body = d.get("body", {})
            blob = _json.dumps(body, default=str)
            assert task_id in blob, (
                f"Delivery body should reference task_id {task_id}: {blob[:400]}"
            )
            if "COMPLETED" in blob or "completed" in blob:
                completed_seen += 1
        assert completed_seen >= 1, (
            "At least one delivery should reference a COMPLETED transition; "
            f"got {deliveries!r}"
        )

    @pytest.mark.asyncio(loop_scope="module")
    async def test_push_notification_encrypted(self, e2e_redis):
        """Encrypted push-config round trip with a Fernet key.

        Spins up a *separate* server process with --encryption-key set, and a
        fresh webhook process so the two test runs don't share log state.
        Skips when the optional 'cryptography' dependency is unavailable.
        """
        try:
            from cryptography.fernet import Fernet
        except ImportError:
            pytest.skip("cryptography not installed -- skipping encrypted run")

        # Use distinct ports so the encrypted run does not collide with the
        # module-scoped server/webhook fixtures.
        enc_server_port = SERVER_PORT + 10
        enc_webhook_port = WEBHOOK_PORT + 10
        enc_base = f"http://127.0.0.1:{enc_server_port}"
        enc_webhook_url = f"http://127.0.0.1:{enc_webhook_port}/webhook"
        enc_log = "/tmp/a2a-e2e-webhook-enc.log"

        await e2e_redis.flushdb()
        key = Fernet.generate_key().decode("ascii")

        webhook_proc = _spawn(
            [
                sys.executable,
                "-m",
                "examples.e2e.webhook_receiver",
                "--port",
                str(enc_webhook_port),
            ],
            extra_env={"A2A_E2E_WEBHOOK_LOG": enc_log},
        )
        server_proc = None
        try:
            await wait_for_url(
                f"http://127.0.0.1:{enc_webhook_port}/health", timeout=15.0
            )
            server_proc = _spawn(
                [
                    sys.executable,
                    "-m",
                    "examples.e2e.server",
                    "--port",
                    str(enc_server_port),
                    "--redis-url",
                    REDIS_URL,
                    "--encryption-key",
                    key,
                ],
            )
            await wait_for_url(f"{enc_base}/.well-known/agent-card.json", timeout=20.0)

            # Reset webhook log for a clean baseline.
            async with httpx.AsyncClient(timeout=2.0) as http:
                await http.get(f"http://127.0.0.1:{enc_webhook_port}/reset")

            from examples.e2e.client import scenario_push_multi_owner_dispatch

            await scenario_push_multi_owner_dispatch(
                enc_base,
                enc_webhook_url,
                redis_url=REDIS_URL,
                encryption_key=key,
                encrypted=True,
            )

            # ---- Assert ciphertext at rest ----
            config_keys: list[bytes] = []
            async for k in e2e_redis.scan_iter(match=b"e2e:push:*"):
                if b"dispatch:" in k or b"taskconfigs:" in k:
                    continue
                config_keys.append(k)
            assert config_keys, "Expected at least one push-config key"

            saw_ciphertext = False
            for k in config_keys:
                raw = await e2e_redis.get(k)
                if raw is None:
                    continue
                # Fernet tokens start with 'gAAAAA' (base64 of version+ts).
                if raw.startswith(b"gAAAAA"):
                    saw_ciphertext = True
                    # And the plaintext token strings should NOT be present.
                    assert b"alice-token" not in raw
                    assert b"bob-token" not in raw
            assert saw_ciphertext, (
                f"Expected Fernet ciphertext in stored configs, got "
                f"{[(k, (await e2e_redis.get(k))[:40]) for k in config_keys]}"
            )

            # ---- Assert webhook still receives the decrypted payload ----
            deadline = time.monotonic() + 10.0
            deliveries: list[dict] = []
            async with httpx.AsyncClient(timeout=2.0) as http:
                while time.monotonic() < deadline:
                    r = await http.get(
                        f"http://127.0.0.1:{enc_webhook_port}/deliveries"
                    )
                    if r.status_code == 200:
                        deliveries = r.json()
                        if len(deliveries) >= 2:
                            break
                    await asyncio.sleep(0.2)
            assert len(deliveries) >= 2, (
                f"Expected >=2 deliveries from encrypted run, got {len(deliveries)}"
            )
        finally:
            if server_proc is not None:
                _terminate(server_proc)
            _terminate(webhook_proc)
