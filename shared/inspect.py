"""Backend inspection helper for FM run_pipeline() drivers."""

from __future__ import annotations

import json
from typing import cast

import redis


def print_all_task_results(redis_client: redis.Redis) -> None:
    """Scan the Redis backend for every `celery-task-meta-*` key and
    print task_id, state, task name, and result/error. Useful after a
    run to confirm every header + body landed in SUCCESS.
    """
    states: dict[str, int] = {}
    for key in redis_client.scan_iter(match="celery-task-meta-*"):
        raw = cast(bytes | None, redis_client.get(key))
        if not raw:
            continue
        meta = json.loads(raw)
        task_id = meta.get("task_id") or key.decode().split("celery-task-meta-")[-1]
        state = meta.get("status", "UNKNOWN")
        name = meta.get("name") or "?"
        result = meta.get("result")
        states[state] = states.get(state, 0) + 1
        print(f"  [{state:<8}] {task_id}  task={name}  result={result!r}")

    summary = ", ".join(f"{s}={n}" for s, n in sorted(states.items()))
    print(f"backend totals: {summary or '(no task results found)'}")
