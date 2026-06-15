"""One-shot migration from a2a-redis v0.2 to v0.3 owner-scoped key layout.

This module is a CLI tool, not part of the runtime API. It walks the keys
written by the v0.2 ``RedisTaskStore``, ``RedisJSONTaskStore``, and
``RedisPushNotificationConfigStore`` and rewrites them into the v0.3
owner-scoped layout:

    Old (v0.2)                              -> New (v0.3)
    {prefix}{task_id}             (hash)    -> {prefix}{owner}:{task_id}
                                               + {prefix}idx:{owner}
                                               + {prefix}idxscore:{owner}
    {prefix}{task_id}             (JSON)    -> {prefix}{owner}:{task_id}
                                               + {prefix}idx:{owner}
                                               + {prefix}idxscore:{owner}
    {push_prefix}{task_id}        (hash)    -> {push_prefix}{owner}:{task_id}:{config_id}
                                               + {push_prefix}taskconfigs:{owner}:{task_id}
                                               + {push_prefix}dispatch:{task_id}

Detection rule (and natural idempotency guard): an "old" key is one whose
suffix after ``prefix`` contains no ``:``. Every v0.3 key written by this
migration (or by a running v0.3 store) has at least one ``:`` after the
prefix, so re-running the migration finds zero candidates.

Limitations:
    * Encrypted push configs are NOT supported. v0.2 stored plaintext JSON
      and the v0.3 store will re-serialize as plaintext. If you need
      encryption at rest, run the v0.3 store with ``encryption_key`` set
      AFTER migration completes and re-save through the public API.

Usage::

    a2a-redis-migrate \\
        --redis-url redis://localhost:6379/0 \\
        --default-owner legacy \\
        --task-prefix task: \\
        --push-prefix push_config: \\
        --targets task,task-json,push-config
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import redis.asyncio as redis_async
from google.protobuf.json_format import MessageToDict, ParseDict

from a2a.types.a2a_pb2 import Task


logger = logging.getLogger("a2a_redis.migrate")


# v0.2 used pydantic enum value strings; v0.3 uses protobuf enum names.
_PYDANTIC_TO_PROTO_STATE = {
    "unspecified": "TASK_STATE_UNSPECIFIED",
    "submitted": "TASK_STATE_SUBMITTED",
    "working": "TASK_STATE_WORKING",
    "completed": "TASK_STATE_COMPLETED",
    "failed": "TASK_STATE_FAILED",
    "canceled": "TASK_STATE_CANCELED",
    "cancelled": "TASK_STATE_CANCELED",
    "input-required": "TASK_STATE_INPUT_REQUIRED",
    "input_required": "TASK_STATE_INPUT_REQUIRED",
    "rejected": "TASK_STATE_REJECTED",
    "auth-required": "TASK_STATE_AUTH_REQUIRED",
    "auth_required": "TASK_STATE_AUTH_REQUIRED",
}


VALID_TARGETS = {"task", "task-json", "push-config"}


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class MigrationReport:
    """Summary of one migration run."""

    scanned: int = 0
    migrated: int = 0
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    dry_run: bool = False

    def format(self) -> str:
        """Return a human-readable summary string."""
        lines = [
            "a2a-redis migration report",
            f"  dry_run : {self.dry_run}",
            f"  scanned : {self.scanned}",
            f"  migrated: {self.migrated}",
            f"  skipped : {len(self.skipped)}",
            f"  errors  : {len(self.errors)}",
        ]
        if self.skipped:
            lines.append("  -- skipped --")
            for s in self.skipped[:20]:
                lines.append(f"    {s}")
            if len(self.skipped) > 20:
                lines.append(f"    ... and {len(self.skipped) - 20} more")
        if self.errors:
            lines.append("  -- errors --")
            for e in self.errors[:20]:
                lines.append(f"    {e}")
            if len(self.errors) > 20:
                lines.append(f"    ... and {len(self.errors) - 20} more")
        return "\n".join(lines)

    def __str__(self) -> str:  # pragma: no cover - thin wrapper
        return self.format()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decode(value: Any) -> Any:
    """Best-effort UTF-8 decode of bytes; pass anything else through."""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value
    return value


def _suffix_after_prefix(key: str, prefix: str) -> str:
    if key.startswith(prefix):
        return key[len(prefix):]
    return key


def _is_old_format(key: str, prefix: str) -> bool:
    """An old v0.2 key has no ``:`` after the prefix."""
    suffix = _suffix_after_prefix(key, prefix)
    return ":" not in suffix


def _normalize_v02_task_dict(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a v0.2 pydantic-shape task dict to a v0.3-Parse-friendly dict.

    Handles:
      * ``_type``/``_data`` wrapper produced by the v0.2 hash store.
      * Pydantic enum state strings (``"submitted"`` etc.).
      * snake_case / camelCase keys (ParseDict accepts both).
      * Strips unknown / non-protobuf fields when possible.
    """
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        # Unwrap v0.2 pydantic-marker payload.
        if isinstance(v, dict) and "_type" in v and "_data" in v:
            v = v["_data"]
        if isinstance(v, dict):
            v = _normalize_v02_task_dict(v)
        elif isinstance(v, list):
            v = [
                _normalize_v02_task_dict(item) if isinstance(item, dict) else item
                for item in v
            ]
        if k in ("state",) and isinstance(v, str):
            mapped = _PYDANTIC_TO_PROTO_STATE.get(v.lower())
            if mapped is not None:
                v = mapped
        out[k] = v
    return out


def _v02_hash_to_task_dict(raw_hash: Dict[Any, Any]) -> Dict[str, Any]:
    """Decode the v0.2 hash field/value pairs into a single task dict.

    Each value may be a JSON-encoded string (for nested dict/list) or a raw
    scalar. The v0.2 ``_serialize_data`` path produced ``"null"`` for None
    fields; we drop those.
    """
    decoded: Dict[str, Any] = {}
    for raw_k, raw_v in raw_hash.items():
        k = _decode(raw_k)
        v = _decode(raw_v)
        if not isinstance(k, str):
            continue
        if isinstance(v, str):
            if v == "null":
                continue
            try:
                v = json.loads(v)
            except (json.JSONDecodeError, ValueError):
                # Leave scalar strings alone.
                pass
        decoded[k] = v
    return _normalize_v02_task_dict(decoded)


def _v02_json_to_task_dict(raw: Any) -> Optional[Dict[str, Any]]:
    """Coerce a JSON.GET response for a v0.2 RedisJSON task into a dict."""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
    elif isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if isinstance(raw, list):
        if not raw:
            return None
        first = raw[0]
        if not isinstance(first, dict):
            return None
        raw = first
    if not isinstance(raw, dict):
        return None
    return _normalize_v02_task_dict(raw)


def _build_task(task_dict: Dict[str, Any]) -> Task:
    """Parse a normalized task dict into a v0.3 protobuf Task message.

    Unknown fields are ignored so v0.2-only fields (``kind``, ``metadata``
    wrapped weirdly, etc.) don't kill the migration.
    """
    task = Task()
    ParseDict(task_dict, task, ignore_unknown_fields=True)
    return task


def _task_micros_and_iso(task: Task) -> Tuple[int, str]:
    """Return (micros_since_epoch, ISO string) for the task's status ts."""
    if task.HasField("status") and task.status.HasField("timestamp"):
        dt = task.status.timestamp.ToDatetime(tzinfo=timezone.utc)
        micros = int(dt.timestamp() * 1_000_000)
        return micros, task.status.timestamp.ToDatetime().isoformat()
    return 0, ""


def _index_member(micros: int, task_id: str) -> str:
    return f"{micros:020d}:{task_id}"


async def _module_has_json(client: "redis_async.Redis") -> bool:
    """Return True iff the connected Redis has the RedisJSON module loaded."""
    try:
        modules = await client.execute_command("MODULE", "LIST")
    except Exception:
        return False
    for entry in modules or []:
        if isinstance(entry, (list, tuple)):
            for i, item in enumerate(entry):
                item = _decode(item)
                if isinstance(item, str) and item.lower() == "name":
                    name = entry[i + 1] if i + 1 < len(entry) else None
                    name = _decode(name)
                    if isinstance(name, str) and "json" in name.lower():
                        return True
        elif isinstance(entry, dict):
            name = entry.get(b"name") or entry.get("name")
            name = _decode(name)
            if isinstance(name, str) and "json" in name.lower():
                return True
    return False


async def _key_is_json_type(client: "redis_async.Redis", key: str) -> bool:
    """Return True if TYPE reports ``ReJSON-RL`` for the given key."""
    try:
        kind = await client.execute_command("TYPE", key)
    except Exception:
        return False
    kind = _decode(kind)
    if isinstance(kind, str):
        return kind.lower() in ("rejson-rl", "rejson")
    return False


async def _key_is_hash_type(client: "redis_async.Redis", key: str) -> bool:
    try:
        kind = await client.execute_command("TYPE", key)
    except Exception:
        return False
    kind = _decode(kind)
    return isinstance(kind, str) and kind.lower() == "hash"


async def _scan_old_keys(
    client: "redis_async.Redis", prefix: str, batch_size: int
) -> List[str]:
    """SCAN ``{prefix}*`` and return only old-format candidate keys."""
    candidates: List[str] = []
    pattern = f"{prefix}*"
    async for raw_key in client.scan_iter(match=pattern, count=batch_size):
        key = _decode(raw_key)
        if not isinstance(key, str):
            continue
        if _is_old_format(key, prefix):
            candidates.append(key)
    return candidates


# ---------------------------------------------------------------------------
# Task migration (hash backend)
# ---------------------------------------------------------------------------


async def _migrate_task_hash(
    client: "redis_async.Redis",
    *,
    default_owner: str,
    prefix: str,
    dry_run: bool,
    batch_size: int,
    report: MigrationReport,
) -> None:
    keys = await _scan_old_keys(client, prefix, batch_size)
    # Only HASH-typed keys belong to the v0.2 RedisTaskStore.
    candidates: List[str] = []
    for key in keys:
        if await _key_is_hash_type(client, key):
            candidates.append(key)

    logger.info("task(hash): %d candidate key(s)", len(candidates))
    for i in range(0, len(candidates), batch_size):
        batch = candidates[i: i + batch_size]
        await _process_task_hash_batch(
            client,
            batch,
            default_owner=default_owner,
            prefix=prefix,
            dry_run=dry_run,
            report=report,
        )


async def _process_task_hash_batch(
    client: "redis_async.Redis",
    batch: List[str],
    *,
    default_owner: str,
    prefix: str,
    dry_run: bool,
    report: MigrationReport,
) -> None:
    # Fetch all old hashes for the batch.
    fetch_pipe = client.pipeline(transaction=False)
    for key in batch:
        fetch_pipe.hgetall(key)
    raw_hashes = await fetch_pipe.execute()

    write_pipe = client.pipeline(transaction=False)
    pending_writes = 0

    for old_key, raw_hash in zip(batch, raw_hashes):
        report.scanned += 1
        suffix = _suffix_after_prefix(old_key, prefix)
        task_id = suffix
        if not raw_hash:
            report.skipped.append(f"{old_key} (empty hash)")
            continue
        try:
            task_dict = _v02_hash_to_task_dict(raw_hash)
            task = _build_task(task_dict)
        except Exception as exc:
            logger.warning("task(hash) %s: cannot decode v0.2 hash: %s", old_key, exc)
            report.skipped.append(f"{old_key} (decode failure: {exc})")
            continue

        if not task.id:
            task.id = task_id

        new_task_dict = MessageToDict(task)
        micros, last_updated = _task_micros_and_iso(task)
        new_key = f"{prefix}{default_owner}:{task.id}"
        index_key = f"{prefix}idx:{default_owner}"
        score_key = f"{prefix}idxscore:{default_owner}"

        mapping: Dict[str, str] = {
            "task_payload": json.dumps(new_task_dict),
            "owner": default_owner,
            "context_id": task.context_id,
            "last_updated": last_updated,
            "protocol_version": "1.0",
        }

        if dry_run:
            logger.info("DRY task(hash) %s -> %s", old_key, new_key)
            report.migrated += 1
            continue

        new_score = -micros
        new_member = _index_member(micros, task.id)
        write_pipe.hset(new_key, mapping=mapping)
        write_pipe.zadd(index_key, {new_member: new_score})
        write_pipe.hset(score_key, task.id, str(new_score))
        write_pipe.delete(old_key)
        pending_writes += 1
        report.migrated += 1
        logger.info("task(hash) %s -> %s", old_key, new_key)

    if pending_writes:
        try:
            await write_pipe.execute()
        except Exception as exc:
            # transaction=False means partial application is possible; capture
            # but don't abort the migration as a whole.
            logger.exception("task(hash) batch write failed: %s", exc)
            report.errors.append(f"task(hash) batch: {exc}")


# ---------------------------------------------------------------------------
# Task migration (JSON backend)
# ---------------------------------------------------------------------------


async def _migrate_task_json(
    client: "redis_async.Redis",
    *,
    default_owner: str,
    prefix: str,
    dry_run: bool,
    batch_size: int,
    report: MigrationReport,
) -> None:
    if not await _module_has_json(client):
        logger.info("task-json: RedisJSON module not loaded; skipping")
        return

    keys = await _scan_old_keys(client, prefix, batch_size)
    candidates: List[str] = []
    for key in keys:
        if await _key_is_json_type(client, key):
            candidates.append(key)

    logger.info("task-json: %d candidate key(s)", len(candidates))
    for i in range(0, len(candidates), batch_size):
        batch = candidates[i: i + batch_size]
        await _process_task_json_batch(
            client,
            batch,
            default_owner=default_owner,
            prefix=prefix,
            dry_run=dry_run,
            report=report,
        )


async def _process_task_json_batch(
    client: "redis_async.Redis",
    batch: List[str],
    *,
    default_owner: str,
    prefix: str,
    dry_run: bool,
    report: MigrationReport,
) -> None:
    # JSON.GET via execute_command on a pipeline works for both sync/async.
    fetch_pipe = client.pipeline(transaction=False)
    for key in batch:
        fetch_pipe.execute_command("JSON.GET", key)
    try:
        raw_docs = await fetch_pipe.execute()
    except Exception as exc:
        logger.exception("task-json fetch batch failed: %s", exc)
        report.errors.append(f"task-json fetch batch: {exc}")
        return

    write_pipe = client.pipeline(transaction=False)
    pending_writes = 0

    for old_key, raw in zip(batch, raw_docs):
        report.scanned += 1
        suffix = _suffix_after_prefix(old_key, prefix)
        task_id = suffix
        task_dict = _v02_json_to_task_dict(raw)
        if task_dict is None:
            report.skipped.append(f"{old_key} (empty/invalid JSON)")
            continue
        try:
            task = _build_task(task_dict)
        except Exception as exc:
            logger.warning("task-json %s: cannot decode v0.2 JSON: %s", old_key, exc)
            report.skipped.append(f"{old_key} (decode failure: {exc})")
            continue

        if not task.id:
            task.id = task_id

        new_task_dict = MessageToDict(task)
        micros, _ = _task_micros_and_iso(task)
        new_key = f"{prefix}{default_owner}:{task.id}"
        index_key = f"{prefix}idx:{default_owner}"
        score_key = f"{prefix}idxscore:{default_owner}"

        if dry_run:
            logger.info("DRY task-json %s -> %s", old_key, new_key)
            report.migrated += 1
            continue

        new_score = -micros
        new_member = _index_member(micros, task.id)
        write_pipe.execute_command(
            "JSON.SET", new_key, "$", json.dumps(new_task_dict)
        )
        write_pipe.zadd(index_key, {new_member: new_score})
        write_pipe.hset(score_key, task.id, str(new_score))
        write_pipe.execute_command("JSON.DEL", old_key)
        pending_writes += 1
        report.migrated += 1
        logger.info("task-json %s -> %s", old_key, new_key)

    if pending_writes:
        try:
            await write_pipe.execute()
        except Exception as exc:
            logger.exception("task-json batch write failed: %s", exc)
            report.errors.append(f"task-json batch: {exc}")


# ---------------------------------------------------------------------------
# Push config migration
# ---------------------------------------------------------------------------


async def _migrate_push_config(
    client: "redis_async.Redis",
    *,
    default_owner: str,
    prefix: str,
    dry_run: bool,
    batch_size: int,
    report: MigrationReport,
) -> None:
    keys = await _scan_old_keys(client, prefix, batch_size)
    candidates: List[str] = []
    for key in keys:
        # Old layout was a hash; new layout's per-config key is a string and
        # also has ':' after the prefix (excluded by _is_old_format).
        if await _key_is_hash_type(client, key):
            candidates.append(key)

    logger.info("push-config: %d candidate key(s)", len(candidates))
    for i in range(0, len(candidates), batch_size):
        batch = candidates[i: i + batch_size]
        await _process_push_config_batch(
            client,
            batch,
            default_owner=default_owner,
            prefix=prefix,
            dry_run=dry_run,
            report=report,
        )


async def _process_push_config_batch(
    client: "redis_async.Redis",
    batch: List[str],
    *,
    default_owner: str,
    prefix: str,
    dry_run: bool,
    report: MigrationReport,
) -> None:
    fetch_pipe = client.pipeline(transaction=False)
    for key in batch:
        fetch_pipe.hgetall(key)
    raw_hashes = await fetch_pipe.execute()

    write_pipe = client.pipeline(transaction=False)
    pending_writes = 0

    for old_key, raw_hash in zip(batch, raw_hashes):
        report.scanned += 1
        suffix = _suffix_after_prefix(old_key, prefix)
        task_id = suffix
        if not raw_hash:
            report.skipped.append(f"{old_key} (empty hash)")
            continue

        any_written = False
        for raw_cid, raw_value in raw_hash.items():
            cid = _decode(raw_cid)
            value = _decode(raw_value)
            if not isinstance(cid, str) or not isinstance(value, str):
                report.skipped.append(f"{old_key}#{cid!r} (non-string field)")
                continue
            try:
                config_data = json.loads(value)
            except json.JSONDecodeError as exc:
                report.skipped.append(f"{old_key}#{cid} (bad JSON: {exc})")
                continue
            if not isinstance(config_data, dict):
                report.skipped.append(f"{old_key}#{cid} (not an object)")
                continue

            # The v0.3 layout stores a serialized TaskPushNotificationConfig
            # (protobuf JSON). The v0.2 hash stored the bare
            # PushNotificationConfig dict without an ``id`` (the hash field
            # name *was* the id), and may have included extra pydantic-only
            # keys we don't know how to map. Inject the id, drop unknown
            # fields, and attach task_id so dispatch lookups still resolve.
            wrapper: Dict[str, Any] = {"id": cid, "task_id": task_id}
            for key in ("url", "token", "authentication", "tenant"):
                if key in config_data:
                    wrapper[key] = config_data[key]
            payload = json.dumps(wrapper).encode("utf-8")

            new_config_key = f"{prefix}{default_owner}:{task_id}:{cid}"
            taskconfigs_key = f"{prefix}taskconfigs:{default_owner}:{task_id}"
            dispatch_key = f"{prefix}dispatch:{task_id}"

            if dry_run:
                logger.info(
                    "DRY push-config %s#%s -> %s", old_key, cid, new_config_key
                )
                any_written = True
                continue

            write_pipe.set(new_config_key, payload)
            write_pipe.sadd(taskconfigs_key, cid)
            write_pipe.sadd(dispatch_key, f"{default_owner}:{cid}")
            pending_writes += 1
            any_written = True
            logger.info("push-config %s#%s -> %s", old_key, cid, new_config_key)

        if any_written:
            report.migrated += 1
            if not dry_run:
                write_pipe.delete(old_key)
                pending_writes += 1

    if pending_writes:
        try:
            await write_pipe.execute()
        except Exception as exc:
            logger.exception("push-config batch write failed: %s", exc)
            report.errors.append(f"push-config batch: {exc}")


# ---------------------------------------------------------------------------
# Public migrate() entrypoint
# ---------------------------------------------------------------------------


async def migrate(
    redis_client: "redis_async.Redis",
    *,
    default_owner: str,
    task_prefix: str = "task:",
    push_prefix: str = "push_config:",
    targets: Iterable[str] = frozenset({"task", "task-json", "push-config"}),
    dry_run: bool = False,
    batch_size: int = 500,
) -> MigrationReport:
    """Migrate v0.2 keys into the v0.3 owner-scoped layout.

    Args:
        redis_client: A connected ``redis.asyncio.Redis`` client.
        default_owner: Owner string written into the new keys for every
            v0.2 record (v0.2 had no owner concept).
        task_prefix: Prefix used by the v0.2 task store. Defaults to ``task:``.
        push_prefix: Prefix used by the v0.2 push config store. Defaults
            to ``push_config:``.
        targets: Subset of ``{"task", "task-json", "push-config"}``.
        dry_run: If True, log what would be written but make no changes.
        batch_size: Max records per Redis pipeline / SCAN batch.

    Returns:
        A :class:`MigrationReport` summarising the run.
    """
    selected: Set[str] = set(targets)
    invalid = selected - VALID_TARGETS
    if invalid:
        raise ValueError(
            f"Unknown migration target(s): {sorted(invalid)}. "
            f"Valid targets: {sorted(VALID_TARGETS)}"
        )

    report = MigrationReport(dry_run=dry_run)

    if "task" in selected:
        await _migrate_task_hash(
            redis_client,
            default_owner=default_owner,
            prefix=task_prefix,
            dry_run=dry_run,
            batch_size=batch_size,
            report=report,
        )
    if "task-json" in selected:
        await _migrate_task_json(
            redis_client,
            default_owner=default_owner,
            prefix=task_prefix,
            dry_run=dry_run,
            batch_size=batch_size,
            report=report,
        )
    if "push-config" in selected:
        await _migrate_push_config(
            redis_client,
            default_owner=default_owner,
            prefix=push_prefix,
            dry_run=dry_run,
            batch_size=batch_size,
            report=report,
        )

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="a2a-redis-migrate",
        description=(
            "One-shot migrator from a2a-redis v0.2 keys to the v0.3 "
            "owner-scoped layout."
        ),
    )
    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6379/0",
        help="Redis connection URL (default: %(default)s).",
    )
    parser.add_argument(
        "--default-owner",
        required=True,
        help="Owner string to write into the new keys for v0.2 records.",
    )
    parser.add_argument(
        "--task-prefix",
        default="task:",
        help="Prefix used by the v0.2 task store (default: %(default)s).",
    )
    parser.add_argument(
        "--push-prefix",
        default="push_config:",
        help="Prefix used by the v0.2 push config store (default: %(default)s).",
    )
    parser.add_argument(
        "--targets",
        default="task,task-json,push-config",
        help=(
            "Comma-separated subset of {task, task-json, push-config} "
            "(default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen, write nothing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Max records per Redis pipeline batch (default: %(default)s).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable INFO logging on the migrator.",
    )
    return parser


def _parse_targets(raw: str) -> Set[str]:
    parts = {p.strip() for p in raw.split(",") if p.strip()}
    invalid = parts - VALID_TARGETS
    if invalid:
        raise argparse.ArgumentTypeError(
            f"Unknown target(s): {sorted(invalid)}. "
            f"Valid: {sorted(VALID_TARGETS)}"
        )
    return parts


async def _run_async(args: argparse.Namespace) -> MigrationReport:
    targets = _parse_targets(args.targets)
    client = redis_async.Redis.from_url(args.redis_url, decode_responses=False)
    try:
        await client.ping()
        return await migrate(
            client,
            default_owner=args.default_owner,
            task_prefix=args.task_prefix,
            push_prefix=args.push_prefix,
            targets=targets,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )
    finally:
        try:
            await client.aclose()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.setLevel(logging.INFO if args.verbose else logging.WARNING)

    try:
        report = asyncio.run(_run_async(args))
    except Exception as exc:
        logger.exception("Migration failed: %s", exc)
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1

    print(report.format())
    return 0 if not report.errors else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
