"""Tests for RedisPushNotificationConfigStore (v1.1 contract)."""

from __future__ import annotations

import pytest

from a2a.types.a2a_pb2 import TaskPushNotificationConfig

from a2a_redis.push_notification_config_store import RedisPushNotificationConfigStore

from tests.conftest import TEST_CONTEXT, TEST_CONTEXT_OTHER


pytestmark = pytest.mark.asyncio


def make_push_config(
    config_id: str, url: str = "https://example.com/hook", token: str = "tok"
) -> TaskPushNotificationConfig:
    """Build a TaskPushNotificationConfig proto for tests."""
    return TaskPushNotificationConfig(id=config_id, url=url, token=token)


class TestRedisPushNotificationConfigStore:
    """Integration tests for RedisPushNotificationConfigStore."""

    async def test_set_then_get_round_trip(self, redis_client):
        store = RedisPushNotificationConfigStore(redis_client, prefix="rt:")
        task_id = "task-rt"
        cfg = make_push_config("cfg-1", "https://hook.example/a")

        await store.set_info(task_id, cfg, TEST_CONTEXT)
        configs = await store.get_info(task_id, TEST_CONTEXT)

        assert len(configs) == 1
        assert configs[0].id == "cfg-1"
        assert configs[0].url == "https://hook.example/a"
        assert configs[0].token == "tok"

    async def test_multi_config_per_task(self, redis_client):
        store = RedisPushNotificationConfigStore(redis_client, prefix="multi:")
        task_id = "task-multi"

        await store.set_info(task_id, make_push_config("c1", "https://h1"), TEST_CONTEXT)
        await store.set_info(task_id, make_push_config("c2", "https://h2"), TEST_CONTEXT)

        configs = await store.get_info(task_id, TEST_CONTEXT)
        ids = sorted(c.id for c in configs)
        urls = sorted(c.url for c in configs)
        assert ids == ["c1", "c2"]
        assert urls == ["https://h1", "https://h2"]

    async def test_owner_isolation_on_get_info(self, redis_client):
        store = RedisPushNotificationConfigStore(redis_client, prefix="iso:")
        task_id = "task-iso"

        await store.set_info(task_id, make_push_config("c1"), TEST_CONTEXT)

        assert await store.get_info(task_id, TEST_CONTEXT_OTHER) == []
        # Owner that did the write still sees it.
        assert len(await store.get_info(task_id, TEST_CONTEXT)) == 1

    async def test_delete_specific_config(self, redis_client):
        store = RedisPushNotificationConfigStore(redis_client, prefix="delone:")
        task_id = "task-delone"

        await store.set_info(task_id, make_push_config("c1", "https://h1"), TEST_CONTEXT)
        await store.set_info(task_id, make_push_config("c2", "https://h2"), TEST_CONTEXT)

        await store.delete_info(task_id, TEST_CONTEXT, config_id="c1")

        configs = await store.get_info(task_id, TEST_CONTEXT)
        assert len(configs) == 1
        assert configs[0].id == "c2"

    async def test_delete_all_configs_for_task(self, redis_client):
        store = RedisPushNotificationConfigStore(redis_client, prefix="delall:")
        task_id = "task-delall"

        await store.set_info(task_id, make_push_config("c1"), TEST_CONTEXT)
        await store.set_info(task_id, make_push_config("c2"), TEST_CONTEXT)

        await store.delete_info(task_id, TEST_CONTEXT, config_id=None)

        assert await store.get_info(task_id, TEST_CONTEXT) == []

    async def test_delete_under_wrong_owner_is_noop(self, redis_client):
        store = RedisPushNotificationConfigStore(redis_client, prefix="wrongown:")
        task_id = "task-wrongown"

        await store.set_info(task_id, make_push_config("c1"), TEST_CONTEXT)

        # Wrong owner attempts both targeted and bulk deletes — both are no-ops.
        await store.delete_info(task_id, TEST_CONTEXT_OTHER, config_id="c1")
        await store.delete_info(task_id, TEST_CONTEXT_OTHER, config_id=None)

        configs = await store.get_info(task_id, TEST_CONTEXT)
        assert len(configs) == 1
        assert configs[0].id == "c1"

    async def test_get_info_for_dispatch_returns_configs_across_owners(
        self, redis_client
    ):
        store = RedisPushNotificationConfigStore(redis_client, prefix="disp:")
        task_id = "task-disp"

        await store.set_info(
            task_id, make_push_config("c1", "https://h1"), TEST_CONTEXT
        )
        await store.set_info(
            task_id, make_push_config("c2", "https://h2"), TEST_CONTEXT_OTHER
        )

        configs = await store.get_info_for_dispatch(task_id)

        ids = sorted(c.id for c in configs)
        urls = sorted(c.url for c in configs)
        assert ids == ["c1", "c2"]
        assert urls == ["https://h1", "https://h2"]

        # Sanity: per-owner get_info still partitions correctly.
        own_a = await store.get_info(task_id, TEST_CONTEXT)
        own_b = await store.get_info(task_id, TEST_CONTEXT_OTHER)
        assert len(own_a) == 1 and own_a[0].id == "c1"
        assert len(own_b) == 1 and own_b[0].id == "c2"

    async def test_encryption_round_trip(self, redis_client):
        Fernet = pytest.importorskip("cryptography.fernet").Fernet
        key = Fernet.generate_key()

        store = RedisPushNotificationConfigStore(
            redis_client, prefix="enc:", encryption_key=key
        )
        task_id = "task-enc"
        cfg = make_push_config("c1", "https://hook.example/secret", token="s3cret")

        await store.set_info(task_id, cfg, TEST_CONTEXT)

        # Raw bytes in Redis must NOT contain the plaintext payload.
        raw_key = store._config_key(
            store.owner_resolver(TEST_CONTEXT), task_id, "c1"
        )
        raw = await redis_client.get(raw_key)
        assert raw is not None
        assert b"https://hook.example/secret" not in raw
        assert b"s3cret" not in raw

        # Round-trip via get_info still returns the original config.
        configs = await store.get_info(task_id, TEST_CONTEXT)
        assert len(configs) == 1
        assert configs[0].url == "https://hook.example/secret"
        assert configs[0].token == "s3cret"

    async def test_tampered_ciphertext_raises(self, redis_client):
        fernet_mod = pytest.importorskip("cryptography.fernet")
        Fernet = fernet_mod.Fernet
        InvalidToken = fernet_mod.InvalidToken

        key = Fernet.generate_key()
        store = RedisPushNotificationConfigStore(
            redis_client, prefix="tamp:", encryption_key=key
        )
        task_id = "task-tamp"
        await store.set_info(
            task_id, make_push_config("c1", "https://h"), TEST_CONTEXT
        )

        raw_key = store._config_key(
            store.owner_resolver(TEST_CONTEXT), task_id, "c1"
        )
        # Overwrite with garbage so Fernet decrypt fails.
        await redis_client.set(raw_key, b"not-a-valid-fernet-token")

        with pytest.raises(InvalidToken):
            await store.get_info(task_id, TEST_CONTEXT)

    async def test_encryption_required_when_key_present_but_data_unencrypted(
        self, redis_client
    ):
        fernet_mod = pytest.importorskip("cryptography.fernet")
        Fernet = fernet_mod.Fernet
        InvalidToken = fernet_mod.InvalidToken

        # First, write plaintext via an unencrypted store.
        plain_store = RedisPushNotificationConfigStore(
            redis_client, prefix="mix:"
        )
        task_id = "task-mix"
        await plain_store.set_info(
            task_id, make_push_config("c1", "https://h"), TEST_CONTEXT
        )

        # Now read with an encrypted store using the SAME prefix —
        # the existing payload is unencrypted JSON, so Fernet.decrypt rejects it.
        key = Fernet.generate_key()
        enc_store = RedisPushNotificationConfigStore(
            redis_client, prefix="mix:", encryption_key=key
        )
        with pytest.raises(InvalidToken):
            await enc_store.get_info(task_id, TEST_CONTEXT)
