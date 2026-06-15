# End-to-end example: a2a-redis

This directory contains a runnable, end-to-end exercise of the three
`a2a-redis` components against the A2A v1.1 SDK:

* `RedisTaskStore` (task persistence + per-owner secondary index)
* `RedisPushNotificationConfigStore` (per-task, per-owner push configs +
  cross-owner dispatch SET)
* `RedisStreamsQueueManager` (constructed for completeness; v1.1's
  `DefaultRequestHandler` no longer routes events through it)

The same flow is exercised end-to-end by `tests/test_e2e.py`. Use that for
CI. Use the instructions below to drive each scenario by hand.

## Prerequisites

You need four terminals (or a tmux session).

### Terminal 1 — Redis

```bash
docker run --rm -p 6379:6379 redis/redis-stack:latest
```

`redis-stack` ships RedisJSON, which is required for the `RedisJSONTaskStore`
parity scenarios. The example server uses the plain `RedisTaskStore`, so a
stock `redis:7` image works too.

### Terminal 2 — Webhook receiver

```bash
python -m examples.e2e.webhook_receiver --port 18001
```

This dumps every received push delivery to `/tmp/a2a-e2e-webhook.log` and
exposes a few helpers:

* `GET  http://localhost:18001/deliveries` — array of received deliveries
* `GET  http://localhost:18001/reset`      — truncate the log
* `GET  http://localhost:18001/health`     — liveness

Override the log path with `A2A_E2E_WEBHOOK_LOG=/tmp/foo.log`.

### Terminal 3 — A2A server

```bash
python -m examples.e2e.server --port 18000 --redis-url redis://localhost:6379/0
```

Add `--encryption-key <fernet-key>` to encrypt push configs at rest. Generate
a key with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.

The server prints `READY port=18000` on stderr once it is bound.

### Terminal 4 — Client scenarios

```bash
# Simple echo round-trip
python -m examples.e2e.client scenario_send_and_get

# Streaming
python -m examples.e2e.client scenario_streaming

# List with filters + pagination (creates 12 tasks)
python -m examples.e2e.client scenario_list_with_filters --count 12

# Push fan-out across two owners (alice + bob)
python -m examples.e2e.client scenario_push_multi_owner_dispatch \
    --webhook-url http://localhost:18001/webhook

# Inspect the deliveries
curl -s http://localhost:18001/deliveries | jq .
```

## What each scenario demonstrates

| Scenario                              | a2a-redis surface exercised                          |
| ------------------------------------- | ---------------------------------------------------- |
| `scenario_send_and_get`               | `RedisTaskStore.save` / `RedisTaskStore.get`         |
| `scenario_streaming`                  | event delivery via the active task pipeline          |
| `scenario_list_with_filters`          | `RedisTaskStore.list` + filters + pagination cursor  |
| `scenario_push_multi_owner_dispatch`  | `RedisPushNotificationConfigStore` set/get + cross-owner `get_info_for_dispatch` -> `BasePushNotificationSender` |

## Identity / multi-tenancy

`server.py` installs a `_HeaderUserContextBuilder` that reads the
`x-a2a-user` header off each request and turns it into a `User` whose
`user_name` is the value. The default `resolve_user_scope` owner resolver
then partitions Redis keys per user. Clients in `client.py` pass that header
via the underlying `httpx.AsyncClient`'s default headers.
