"""send_email stub and its supporting counters, shared by fm4..fm6.

The FM files that demonstrate the idempotency lock (FM-4, FM-5, FM-6)
all share an identical send_email() mock and the Redis counters that
let run_pipeline() assert "exactly one send happened". Each FM supplies
its own redis_client and key prefix so the counters don't bleed across
independent runs.
"""

from __future__ import annotations

import time
from typing import cast

import redis


# Real email APIs take 1–3s on a healthy day. We model that so the
# idempotency lock is genuinely held when the duplicate notify arrives.
SEND_EMAIL_DURATION_SECONDS = 3


def send_email(
    message: str,
    redis_client: redis.Redis,
    send_count_key: str,
) -> None:
    print(f"  send_email: {message} (taking {SEND_EMAIL_DURATION_SECONDS}s...)")
    time.sleep(SEND_EMAIL_DURATION_SECONDS)
    redis_client.incr(send_count_key)


def reset_send_count(redis_client: redis.Redis, send_count_key: str) -> None:
    redis_client.delete(send_count_key)


def read_send_count(redis_client: redis.Redis, send_count_key: str) -> int:
    raw = cast(bytes | None, redis_client.get(send_count_key))
    return int(raw) if raw else 0


def reset_lock_contention_count(
    redis_client: redis.Redis, lock_contention_key: str
) -> None:
    redis_client.delete(lock_contention_key)


def read_lock_contention_count(
    redis_client: redis.Redis, lock_contention_key: str
) -> int:
    raw = cast(bytes | None, redis_client.get(lock_contention_key))
    return int(raw) if raw else 0
