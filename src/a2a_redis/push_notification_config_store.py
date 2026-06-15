"""Redis-backed push notification config store implementation for the A2A Python SDK."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from google.protobuf.json_format import MessageToJson, Parse

from a2a.server.context import ServerCallContext
from a2a.server.owner_resolver import OwnerResolver, resolve_user_scope
from a2a.server.tasks.push_notification_config_store import (
    PushNotificationConfigStore,
)
from a2a.types.a2a_pb2 import TaskPushNotificationConfig


if TYPE_CHECKING:
    from cryptography.fernet import Fernet


logger = logging.getLogger(__name__)


class RedisPushNotificationConfigStore(PushNotificationConfigStore):
    """Redis-backed implementation of the A2A PushNotificationConfigStore interface.

    Key layout:
      - {prefix}{owner}:{task_id}:{config_id}
            Stores the (possibly Fernet-encrypted) serialized
            TaskPushNotificationConfig bytes.
      - {prefix}taskconfigs:{owner}:{task_id}
            A Redis SET of config_ids for the (owner, task_id) pair. Used to
            enumerate configs in get_info without SCAN.
      - {prefix}dispatch:{task_id}
            A Redis SET of "{owner}:{config_id}" members. Used by
            get_info_for_dispatch to fan out across owners without
            consulting context.
    """

    _fernet: "Fernet | None"

    def __init__(
        self,
        redis_client,
        prefix: str = "push_config:",
        encryption_key: str | bytes | None = None,
        owner_resolver: OwnerResolver = resolve_user_scope,
    ) -> None:
        """Initialize the Redis push notification config store.

        Args:
            redis_client: Async Redis client instance.
            prefix: Key prefix for all keys owned by this store.
            encryption_key: Optional URL-safe base64-encoded 32-byte Fernet key.
                If provided, the cryptography library must be available;
                serialized configs are encrypted at rest.
            owner_resolver: Function resolving the owner string from a
                ServerCallContext. Defaults to resolve_user_scope.
        """
        self.redis = redis_client
        self.prefix = prefix
        self.owner_resolver = owner_resolver
        self._fernet = None

        if encryption_key:
            try:
                from cryptography.fernet import Fernet  # noqa: PLC0415
            except ImportError as e:
                raise ImportError(
                    "RedisPushNotificationConfigStore with encryption requires the "
                    "'cryptography' library. Install with: "
                    "'pip install a2a-redis[encryption]'"
                ) from e

            if isinstance(encryption_key, str):
                encryption_key = encryption_key.encode("utf-8")
            self._fernet = Fernet(encryption_key)
            logger.debug("Encryption enabled for Redis push notification config store.")

    # ---- key helpers ----

    def _config_key(self, owner: str, task_id: str, config_id: str) -> str:
        return f"{self.prefix}{owner}:{task_id}:{config_id}"

    def _taskconfigs_key(self, owner: str, task_id: str) -> str:
        return f"{self.prefix}taskconfigs:{owner}:{task_id}"

    def _dispatch_key(self, task_id: str) -> str:
        return f"{self.prefix}dispatch:{task_id}"

    # ---- serialization ----

    def _serialize(self, config: TaskPushNotificationConfig) -> bytes:
        json_payload = MessageToJson(config).encode("utf-8")
        if self._fernet is not None:
            return self._fernet.encrypt(json_payload)
        return json_payload

    def _deserialize(self, payload: bytes) -> TaskPushNotificationConfig:
        if self._fernet is not None:
            # InvalidToken (or anything else) propagates to the caller —
            # tampered/wrong-key ciphertext should be a hard error, not a
            # silent skip.
            decrypted = self._fernet.decrypt(payload)
            return Parse(decrypted.decode("utf-8"), TaskPushNotificationConfig())

        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        return Parse(payload_str, TaskPushNotificationConfig())

    # ---- ABC implementation ----

    async def set_info(
        self,
        task_id: str,
        notification_config: TaskPushNotificationConfig,
        context: ServerCallContext,
    ) -> None:
        """Sets or updates the push notification configuration for a task."""
        owner = self.owner_resolver(context)

        config_to_save = TaskPushNotificationConfig()
        config_to_save.CopyFrom(notification_config)
        if not config_to_save.id:
            config_to_save.id = task_id

        config_id = config_to_save.id
        data = self._serialize(config_to_save)

        pipe = self.redis.pipeline()
        pipe.set(self._config_key(owner, task_id, config_id), data)
        pipe.sadd(self._taskconfigs_key(owner, task_id), config_id)
        pipe.sadd(self._dispatch_key(task_id), f"{owner}:{config_id}")
        await pipe.execute()

        logger.debug(
            "Push notification config for task %s with config id %s for owner %s saved/updated.",
            task_id,
            config_id,
            owner,
        )

    async def get_info(
        self,
        task_id: str,
        context: ServerCallContext,
    ) -> list[TaskPushNotificationConfig]:
        """Retrieves all push notification configurations for a task, for the given owner."""
        owner = self.owner_resolver(context)
        members = await self.redis.smembers(self._taskconfigs_key(owner, task_id))
        if not members:
            return []

        config_ids = sorted(
            m.decode("utf-8") if isinstance(m, bytes) else m for m in members
        )
        keys = [self._config_key(owner, task_id, cid) for cid in config_ids]
        values = await self.redis.mget(keys)

        configs: list[TaskPushNotificationConfig] = []
        for cid, value in zip(config_ids, values):
            if value is None:
                # Stale set entry — the value key was deleted independently.
                continue
            try:
                configs.append(self._deserialize(value))
            except Exception:
                logger.exception(
                    "Could not deserialize push notification config for task %s, config %s, owner %s",
                    task_id,
                    cid,
                    owner,
                )
                raise
        return configs

    async def get_info_for_dispatch(
        self,
        task_id: str,
    ) -> list[TaskPushNotificationConfig]:
        """Retrieves all push notification configurations for a task, across all owners.

        Used by the push-notification dispatch path. No ServerCallContext, no
        owner filter.
        """
        members = await self.redis.smembers(self._dispatch_key(task_id))
        if not members:
            return []

        decoded: list[tuple[str, str]] = []
        for m in members:
            s = m.decode("utf-8") if isinstance(m, bytes) else m
            owner, _, config_id = s.partition(":")
            if not config_id:
                # Malformed entry, skip.
                continue
            decoded.append((owner, config_id))
        decoded.sort()

        keys = [self._config_key(owner, task_id, cid) for owner, cid in decoded]
        values = await self.redis.mget(keys)

        configs: list[TaskPushNotificationConfig] = []
        for (owner, cid), value in zip(decoded, values):
            if value is None:
                continue
            try:
                configs.append(self._deserialize(value))
            except Exception:
                logger.exception(
                    "Could not deserialize push notification config for task %s, config %s, owner %s",
                    task_id,
                    cid,
                    owner,
                )
                raise
        return configs

    async def delete_info(
        self,
        task_id: str,
        context: ServerCallContext,
        config_id: str | None = None,
    ) -> None:
        """Deletes push notification configurations for a task.

        If config_id is provided, only that specific configuration is deleted.
        If config_id is None, all configurations for the task for the owner are deleted.
        """
        owner = self.owner_resolver(context)
        taskconfigs_key = self._taskconfigs_key(owner, task_id)
        dispatch_key = self._dispatch_key(task_id)

        if config_id is not None:
            is_member = await self.redis.sismember(taskconfigs_key, config_id)
            if not is_member:
                logger.warning(
                    "Attempted to delete push notification config for task %s, owner %s with config_id: %s that does not exist.",
                    task_id,
                    owner,
                    config_id,
                )
                return

            pipe = self.redis.pipeline()
            pipe.delete(self._config_key(owner, task_id, config_id))
            pipe.srem(taskconfigs_key, config_id)
            pipe.srem(dispatch_key, f"{owner}:{config_id}")
            await pipe.execute()
            logger.info(
                "Deleted push notification config %s for task %s, owner %s.",
                config_id,
                task_id,
                owner,
            )
            return

        members = await self.redis.smembers(taskconfigs_key)
        if not members:
            logger.warning(
                "Attempted to delete push notification config for task %s, owner %s that does not exist.",
                task_id,
                owner,
            )
            return

        config_ids = [m.decode("utf-8") if isinstance(m, bytes) else m for m in members]
        pipe = self.redis.pipeline()
        for cid in config_ids:
            pipe.delete(self._config_key(owner, task_id, cid))
            pipe.srem(dispatch_key, f"{owner}:{cid}")
        pipe.delete(taskconfigs_key)
        await pipe.execute()
        logger.info(
            "Deleted all push notification configs for task %s, owner %s.",
            task_id,
            owner,
        )
