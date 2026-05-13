"""Redis attempt counters shared by fm2..fm6."""

from __future__ import annotations

from typing import cast

from shared.redis import client

ATTEMPTS_KEY_PREFIX = "attempts"


def incr_attempts(doc_id: str) -> int:
    """Increment and return the attempt counter for doc_id."""
    return int(cast(int, client.incr(f"{ATTEMPTS_KEY_PREFIX}:{doc_id}")))


def read_attempts(doc_id: str) -> int:
    """Return current attempt count for doc_id (0 if not set)."""
    raw = cast(bytes | None, client.get(f"{ATTEMPTS_KEY_PREFIX}:{doc_id}"))
    return int(raw) if raw else 0


def reset_attempts(doc_ids: list[str]) -> None:
    """Delete attempt counters for all doc_ids (used before each test run)."""
    keys = [f"{ATTEMPTS_KEY_PREFIX}:{d}" for d in doc_ids]
    if keys:
        client.delete(*keys)
