# a2a-redis examples

* [`basic_usage.py`](basic_usage.py) — minimal `RedisTaskStore` +
  `RedisPushNotificationConfigStore` round-trip against a local Redis. Start
  here.
* [`e2e/`](e2e/) — full end-to-end example: scripted `AgentExecutor`,
  Starlette server, push-notification webhook receiver, and a client with
  one function per scenario. The same flow is exercised by
  `tests/test_e2e.py`.

The old `redis_travel_agent.py` example was removed during the v1.1
migration — it relied on `A2AStarletteApplication` and the pre-v1.1
`DefaultRequestHandler` signature, both of which no longer exist. See
`e2e/server.py` for the replacement (route-based Starlette assembly).
