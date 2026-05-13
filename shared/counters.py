"""Redis attempt counters shared by fm2..fm6.

Replaces the inline _attempts_key / _reset / _read_attempts trio that was
duplicated in every FM file. Each FM supplies its own ATTEMPTS_KEY_PREFIX
constant (e.g. "fm3:crash_attempts") so runs don't bleed into each other.
"""

from __future__ import annotations

from typing import cast

import redis


def incr_attempts(
    redis_client: redis.Redis,
    doc_id: str,
    key_prefix: str,
) -> int:
    """Increment and return the attempt counter for doc_id."""
    return int(cast(int, redis_client.incr(f"{key_prefix}:{doc_id}")))


def read_attempts(
    redis_client: redis.Redis,
    doc_id: str,
    key_prefix: str,
) -> int:
    """Return current attempt count for doc_id (0 if not set)."""
    raw = cast(bytes | None, redis_client.get(f"{key_prefix}:{doc_id}"))
    return int(raw) if raw else 0


def reset_attempts(
    redis_client: redis.Redis,
    doc_ids: list[str],
    key_prefix: str,
) -> None:
    """Delete attempt counters for all doc_ids (used before each test run)."""
    keys = [f"{key_prefix}:{d}" for d in doc_ids]
    if keys:
        redis_client.delete(*keys)
