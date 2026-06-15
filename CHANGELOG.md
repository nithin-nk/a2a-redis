# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
uses [Conventional Commits](https://www.conventionalcommits.org/) categories.

## 0.3.0a1 — UNRELEASED

This is the first alpha for the 1.x-aligned `a2a-redis` line. It aligns the
package with `a2a-sdk >= 1.1.0`, introduces owner-scoped storage, splits the
event-queue stack along the v1.1 `EventQueue` boundary, adds optional
at-rest encryption for push configs, and ships a one-shot migration CLI for
the v0.2 key layout.

### Breaking Changes

- **chore(deps)!**: Pin `a2a-sdk` to `>=1.1.0, <2` (previously `>=0.2.16`).
  Applications must upgrade the SDK in lockstep; the v0.2 and v1.x ABIs are
  not interchangeable.
- **feat(task-store)!**: `RedisTaskStore.save`, `get`, `delete`, and `list`
  now require a `ServerCallContext`. Keys are owner-scoped
  (`{prefix}{owner}:{task_id}`) and indexed via a per-owner sorted set
  (`{prefix}idx:{owner}`) plus a score counter (`{prefix}idxscore:{owner}`).
  The v0.2-era helpers `update_task`, `list_task_ids`, and `task_exists` are
  removed; use `save`, `list`, and `get` against the new contract instead.
- **feat(push-config)!**: `RedisPushNotificationConfigStore` now supports
  multiple configs per task, is owner-scoped, and gains a cross-owner
  dispatch set (`{prefix}dispatch:{task_id}`) for the notifier worker.
  `set_info`, `get_info`, and `delete_info` take a `ServerCallContext`;
  `get_info_for_dispatch(task_id)` is the contextless dispatch lookup.
- **feat(events)!**: Queue stack conforms to the v1.1 `EventQueue` split.
  `tap()` on both `RedisStreamsQueueManager` and `RedisPubSubQueueManager`
  is now async. The in-tree `EventQueueProtocol` is removed; consumers that
  still need the pre-1.1 surface should import
  `a2a.server.events.EventQueueLegacy` (also re-exported from `a2a_redis`).

### Features

- **feat(task-store)**: `RedisTaskStore.list()` supports cursor-based
  pagination and filtering by `context_id`, `status`, and
  `status_timestamp_after`. The cursor encodes the resolved owner and is
  rejected on mismatch.
- **feat(task-store)**: `RedisJSONTaskStore` reaches full parity with
  `RedisTaskStore` via a shared `_RedisTaskStoreBase` implementation —
  including owner-scoped keys, `list()`, and the new filters — while
  preserving native nested-document storage via the RedisJSON module.
- **feat(push-config)**: Optional Fernet encryption at rest, opt-in via the
  new `[encryption]` extra (`pip install "a2a-redis[encryption]"`) and an
  `encryption_key` constructor argument. Decryption errors fail loudly
  rather than returning plaintext.
- **feat(migrate)**: New `a2a-redis-migrate` CLI for a one-shot v0.2 → v0.3
  data move, covering hash-based `RedisTaskStore`, `RedisJSONTaskStore`,
  and the push-config store. The script is idempotent. Encrypted push
  configs are out of scope — re-save through the v0.3 API after migration
  to encrypt at rest.

### Documentation

- **docs**: README refreshed for the v0.3 contract — supported-version
  table, owner-aware quick start, key-schema reference for task and
  push-config stores, encryption opt-in, and a migration walkthrough.
- **docs**: End-to-end example added under `examples/e2e/`, wiring the
  Redis stores into a working A2A agent.

### Migration

If you are upgrading from a `0.2.x` release:

1. Stop writers against the old key layout.
2. Upgrade your application to `a2a-sdk >= 1.1.0` and `a2a-redis 0.3.x`.
3. Run `a2a-redis-migrate` against your Redis instance with the prefixes
   your deployment used. See `a2a-redis-migrate --help` and the
   `src/a2a_redis/migrate.py` module docstring for flags and limitations.
4. If you want push configs encrypted at rest, start the v0.3 store with
   `encryption_key` set and re-save each config via `set_info` — the
   migration tool intentionally does not handle encryption.
