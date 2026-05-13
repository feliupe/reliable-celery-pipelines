"""send_email stub and its supporting counters, shared by fm4..fm6."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import cast

from shared.redis import client

# Real email APIs take 1–3s on a healthy day. We model that so the
# idempotency lock is genuinely held when the duplicate notify arrives.
SEND_EMAIL_DURATION_SECONDS = 2

NOTIFY_LOCK_TTL_SECONDS = 600
NOTIFY_STATE_NOT_SENT = "not_sent"
NOTIFY_STATE_SENT = "sent"

SEND_COUNT_KEY = "send_email:count"
LOCK_CONTENTION_KEY = "notify:lock_contention_count"


class ClaimResult(Enum):
    CLAIMED = "claimed"
    ALREADY_SENT = "already_sent"
    CONTENDED = "contended"


@dataclass
class NotifyCoordinator:
    pipeline_id: str
    namespace: str = "notify"

    @property
    def _state_key(self) -> str:
        return f"{self.namespace}:state:{self.pipeline_id}"

    def try_claim(self) -> ClaimResult:
        claimed = client.set(
            self._state_key,
            NOTIFY_STATE_NOT_SENT,
            nx=True,
            ex=NOTIFY_LOCK_TTL_SECONDS,
        )
        if claimed:
            return ClaimResult.CLAIMED

        state = client.get(self._state_key)
        if state in (NOTIFY_STATE_SENT, NOTIFY_STATE_SENT.encode()):
            return ClaimResult.ALREADY_SENT

        incr_lock_contention_count()
        return ClaimResult.CONTENDED

    def mark_sent(self) -> None:
        # No TTL — sent is terminal.
        client.set(self._state_key, NOTIFY_STATE_SENT)

    def is_claimed(self) -> bool:
        return bool(client.exists(self._state_key))


def send_email(message: str) -> None:
    print(f"  send_email: {message} (taking {SEND_EMAIL_DURATION_SECONDS}s...)")
    time.sleep(SEND_EMAIL_DURATION_SECONDS)
    client.incr(SEND_COUNT_KEY)


def reset_send_count() -> None:
    client.delete(SEND_COUNT_KEY)


def read_send_count() -> int:
    raw = cast(bytes | None, client.get(SEND_COUNT_KEY))
    return int(raw) if raw else 0


def reset_lock_contention_count() -> None:
    client.delete(LOCK_CONTENTION_KEY)


def read_lock_contention_count() -> int:
    raw = cast(bytes | None, client.get(LOCK_CONTENTION_KEY))
    return int(raw) if raw else 0


def incr_lock_contention_count() -> None:
    client.incr(LOCK_CONTENTION_KEY)
