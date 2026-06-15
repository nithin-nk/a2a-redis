"""Tests for the one-shot v0.2 -> v0.3 migration script."""

from __future__ import annotations

import json

import pytest

from a2a.server.context import ServerCallContext
from a2a_redis.migrate import MigrationReport, main, migrate
from a2a_redis.push_notification_config_store import RedisPushNotificationConfigStore
from a2a_redis.task_store import RedisJSONTaskStore, RedisTaskStore

from tests.conftest import SampleUser, TEST_CONTEXT


pytestmark = pytest.mark.asyncio


def _legacy_context() -> ServerCallContext:
    """Context whose resolved owner is 'legacy' (matches default_owner used)."""
    return ServerCallContext(user=SampleUser("legacy"))


async def _redis_json_available(redis_client) -> bool:
    try:
        modules = await redis_client.execute_command("MODULE", "LIST")
    except Exception:
        return False
    for entry in modules or []:
        if isinstance(entry, (list, tuple)):
            for i, item in enumerate(entry):
                if isinstance(item, bytes):
                    item = item.decode()
                if isinstance(item, str) and item.lower() == "name":
                    name = entry[i + 1] if i + 1 < len(entry) else None
                    if isinstance(name, bytes):
                        name = name.decode()
                    if name and "json" in name.lower():
                        return True
    return False


# v0.2 hash-store layout: each Task field is stored as its own hash field.
# Nested dicts/lists go through json.dumps; TaskStatus was wrapped in
# {"_type": "a2a.types.TaskStatus", "_data": {...}}.
def _seed_v02_task_hash(redis_client, prefix: str, task_id: str,
                       context_id: str = "ctx-1",
                       state: str = "submitted"):
    return redis_client.hset(
        f"{prefix}{task_id}",
        mapping={
            "id": task_id,
            "context_id": context_id,
            "status": json.dumps(
                {
                    "_type": "a2a.types.TaskStatus",
                    "_data": {"state": state},
                }
            ),
        },
    )


def _seed_v02_task_json(redis_client, prefix: str, task_id: str,
                       context_id: str = "ctx-j",
                       state: str = "working"):
    payload = {
        "id": task_id,
        "context_id": context_id,
        "status": {"state": state},
    }
    return redis_client.execute_command(
        "JSON.SET", f"{prefix}{task_id}", "$", json.dumps(payload)
    )


def _seed_v02_push_config_hash(redis_client, prefix: str, task_id: str,
                              configs: dict):
    """Each config_id -> JSON-encoded config dict (no "id" field — the field
    name IS the id)."""
    mapping = {cid: json.dumps(data) for cid, data in configs.items()}
    return redis_client.hset(f"{prefix}{task_id}", mapping=mapping)


class TestMigrate:
    """Migration script integration tests."""

    async def test_migrate_task_hash_format(self, redis_client):
        prefix = "mig_task_h:"
        await _seed_v02_task_hash(redis_client, prefix, "t1", "ctx-1", "working")

        report = await migrate(
            redis_client,
            default_owner="legacy",
            task_prefix=prefix,
            push_prefix="mig_push_h:",
            targets={"task"},
        )
        assert report.scanned == 1
        assert report.migrated == 1
        assert not report.errors

        # Old key gone, new owner-scoped key present.
        assert not await redis_client.exists(f"{prefix}t1")
        assert await redis_client.exists(f"{prefix}legacy:t1")

        # Round-trip through the v0.3 store.
        store = RedisTaskStore(redis_client, prefix=prefix)
        task = await store.get("t1", _legacy_context())
        assert task is not None
        assert task.id == "t1"
        assert task.context_id == "ctx-1"
        # Status state was decoded from the v0.2 enum string.
        from a2a.types.a2a_pb2 import TASK_STATE_WORKING
        assert task.status.state == TASK_STATE_WORKING

    async def test_migrate_dry_run_writes_nothing(self, redis_client):
        prefix = "mig_dry:"
        await _seed_v02_task_hash(redis_client, prefix, "td1")
        await _seed_v02_task_hash(redis_client, prefix, "td2")

        report = await migrate(
            redis_client,
            default_owner="legacy",
            task_prefix=prefix,
            push_prefix="unused:",
            targets={"task"},
            dry_run=True,
        )
        assert report.scanned == 2
        assert report.migrated == 2
        assert report.dry_run is True

        # Nothing was written. Old keys still there, no new owner-scoped keys.
        assert await redis_client.exists(f"{prefix}td1")
        assert await redis_client.exists(f"{prefix}td2")
        assert not await redis_client.exists(f"{prefix}legacy:td1")
        assert not await redis_client.exists(f"{prefix}legacy:td2")
        assert not await redis_client.exists(f"{prefix}idx:legacy")
        assert not await redis_client.exists(f"{prefix}idxscore:legacy")

    async def test_migrate_is_idempotent(self, redis_client):
        prefix = "mig_idem:"
        await _seed_v02_task_hash(redis_client, prefix, "ti1")

        first = await migrate(
            redis_client,
            default_owner="legacy",
            task_prefix=prefix,
            push_prefix="unused:",
            targets={"task"},
        )
        assert first.migrated == 1

        # Verify new key exists.
        assert await redis_client.exists(f"{prefix}legacy:ti1")

        # Second run finds zero old-format keys.
        second = await migrate(
            redis_client,
            default_owner="legacy",
            task_prefix=prefix,
            push_prefix="unused:",
            targets={"task"},
        )
        assert second.scanned == 0
        assert second.migrated == 0
        # New key still intact.
        assert await redis_client.exists(f"{prefix}legacy:ti1")

    async def test_migrate_skips_already_migrated_keys(self, redis_client):
        prefix = "mig_mix:"
        # Old-format key.
        await _seed_v02_task_hash(redis_client, prefix, "old1")
        # Pre-existing new-format owner-scoped key.
        await redis_client.hset(
            f"{prefix}legacy:newkey",
            mapping={
                "task_payload": json.dumps(
                    {"id": "newkey", "context_id": "ctx-new"}
                ),
                "owner": "legacy",
                "context_id": "ctx-new",
                "last_updated": "",
                "protocol_version": "1.0",
            },
        )
        # And a couple of v0.3 index keys that share the prefix.
        await redis_client.zadd(f"{prefix}idx:legacy", {"0:newkey": 0})
        await redis_client.hset(f"{prefix}idxscore:legacy", "newkey", "0")

        report = await migrate(
            redis_client,
            default_owner="legacy",
            task_prefix=prefix,
            push_prefix="unused:",
            targets={"task"},
        )
        # Only the one old key should have been processed.
        assert report.scanned == 1
        assert report.migrated == 1
        # The pre-existing new-format key is untouched.
        assert await redis_client.exists(f"{prefix}legacy:newkey")
        # The newly migrated key exists.
        assert await redis_client.exists(f"{prefix}legacy:old1")
        # Old key gone.
        assert not await redis_client.exists(f"{prefix}old1")

    async def test_migrate_push_config(self, redis_client):
        prefix = "mig_push:"
        task_id = "tp1"
        await _seed_v02_push_config_hash(
            redis_client,
            prefix,
            task_id,
            {
                "cfg-a": {"url": "https://hook.example/a", "token": "tok-a"},
            },
        )

        report = await migrate(
            redis_client,
            default_owner="legacy",
            task_prefix="unused:",
            push_prefix=prefix,
            targets={"push-config"},
        )
        assert report.scanned == 1
        assert report.migrated == 1
        assert not report.errors

        # Old hash gone.
        assert not await redis_client.exists(f"{prefix}{task_id}")

        # Round-trip via the v0.3 store under owner=legacy.
        store = RedisPushNotificationConfigStore(redis_client, prefix=prefix)
        configs = await store.get_info(task_id, _legacy_context())
        assert len(configs) == 1
        assert configs[0].id == "cfg-a"
        assert configs[0].url == "https://hook.example/a"
        assert configs[0].token == "tok-a"

    async def test_migrate_redis_json_task(self, redis_client):
        if not await _redis_json_available(redis_client):
            pytest.skip("RedisJSON module not loaded")

        prefix = "mig_task_j:"
        await _seed_v02_task_json(redis_client, prefix, "tj1", "ctx-j", "completed")

        report = await migrate(
            redis_client,
            default_owner="legacy",
            task_prefix=prefix,
            push_prefix="unused:",
            targets={"task-json"},
        )
        assert report.scanned == 1
        assert report.migrated == 1

        # Old JSON key gone, new owner-scoped JSON key present.
        assert not await redis_client.exists(f"{prefix}tj1")
        assert await redis_client.exists(f"{prefix}legacy:tj1")

        store = RedisJSONTaskStore(redis_client, prefix=prefix)
        task = await store.get("tj1", _legacy_context())
        assert task is not None
        assert task.id == "tj1"
        assert task.context_id == "ctx-j"
        from a2a.types.a2a_pb2 import TASK_STATE_COMPLETED
        assert task.status.state == TASK_STATE_COMPLETED

    async def test_migrate_cli_main(self, redis_client, monkeypatch):
        import asyncio

        prefix = "mig_cli:"
        await _seed_v02_task_hash(redis_client, prefix, "tcli1")

        # The redis_client fixture is wired to db=15 on localhost.
        argv = [
            "--redis-url", "redis://localhost:6379/15",
            "--default-owner", "legacy",
            "--task-prefix", prefix,
            "--push-prefix", "mig_cli_push:",
            "--targets", "task",
            "--batch-size", "10",
        ]
        # main() calls asyncio.run() internally; hop to a thread so we don't
        # collide with the pytest-asyncio event loop.
        rc = await asyncio.to_thread(main, argv)
        assert rc == 0

        # Verify the migration was actually applied.
        assert not await redis_client.exists(f"{prefix}tcli1")
        assert await redis_client.exists(f"{prefix}legacy:tcli1")

    async def test_migration_report_format(self):
        report = MigrationReport(
            scanned=5, migrated=3,
            skipped=["k1 (bad)"],
            errors=["e1"],
            dry_run=True,
        )
        text = report.format()
        assert "scanned : 5" in text
        assert "migrated: 3" in text
        assert "skipped : 1" in text
        assert "errors  : 1" in text
        assert "dry_run : True" in text
        assert "k1 (bad)" in text
        assert "e1" in text
