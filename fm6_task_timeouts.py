"""Fix for FM-6: a task that hangs (without crashing) no longer consumes a
worker slot indefinitely.

Delta from fm5_transient_failures.py
--------------------------------------
- soft_time_limit + time_limit (backstop) added to every task.
- @hard_timeout decorator defined inline: raises HardTimeoutExceeded via
  SIGALRM before Celery's time_limit fires, keeping the exception inside
  @enveloped's catch path (no FAILURE chord-part write; chord advances).
- SOFT_HANG + HARD_HANG_MANUAL sentinels and their parse_document branches.
- FLAKE_SCHEDULE adds doc7 (soft-honoring hang) and doc8 (manual-hard hang).

The gap FM-6 closes
--------------------
FM-2 recovers from hard crashes (SIGKILL). FM-3 caps poison redeliveries.
FM-5 retries transient errors. None handle a task that simply hangs —
the worker stays alive, the broker sees no redelivery, x-delivery-count
never increments. The chord stalls forever.

Why NOT Celery's own time_limit for chord members
--------------------------------------------------
Celery's on_timeout(soft=False) calls mark_as_failure UNCONDITIONALLY
before any reject/redelivery decision (celery/worker/request.py:521-538).
That writes a FAILURE-state chord-part result, which triggers ChordError
in the coordinator — the body never fires.

FM-3's SIGKILL avoids this because on_worker_lost skips mark_as_failure
when requeuing (`if not requeue` at celery/worker/request.py:622).
on_timeout has no equivalent guard.

Manual hard timeout via @hard_timeout
--------------------------------------
For chord members we enforce the hard deadline from inside the task body
via signal.setitimer(ITIMER_REAL) → SIGALRM → HardTimeoutExceeded.
@enveloped catches HardTimeoutExceeded and returns a SUCCESS-state
Result(status="FAILURE") envelope. No Celery on_timeout, no FAILURE
chord-part write, no chord poisoning.

SIGALRM is safe: Celery uses SIGUSR1 for soft timeout and parent-side
SIGKILL for hard timeout; SIGALRM is untouched in the child. Linux/Unix only.

Celery's own time_limit stays as a last-resort backstop (set far above any
legitimate task runtime) for tasks that don't yield to Python signals at all.

Why timeout exceptions are NOT in @transient_retryable's exceptions
--------------------------------------------------------------------
A hang isn't a transient blip — retrying the same stuck call will hang
again. Make the failure visible (envelope), let the operator decide.

Run
---
  docker-compose up -d
  celery -A fm6_task_timeouts worker --loglevel=info --concurrency=2 --beat
  python fm6_task_timeouts.py
"""

from __future__ import annotations

import functools
import os
import signal
import time
import uuid
from collections.abc import Callable
from typing import Any

from celery import Celery, chord
from celery.exceptions import SoftTimeLimitExceeded
from celery.schedules import schedule
from shared.counters import incr_attempts, read_attempts, reset_attempts
from shared.decorators import (
    enveloped,
    transient_retryable,
)  # FM-5: bounded retries on transient errors
from shared.dlq import declare_dlq, drain_dlq_messages
from shared.flake import (
    CRASH_ONCE,
    FAIL,
    FLAKE_FOREVER,
    HARD_HANG_MANUAL,
    POISON,
    SOFT_HANG,
    FlakeEntry,
)
from shared.fm_asserts import (
    assert_fm1_chord_body_fired,
    assert_fm2_redelivery_happened,
    assert_fm3_poison_bounded_at_dlq,
    assert_fm4_notify_idempotent,
    assert_fm5_doc_attempts,
    assert_fm5_retryable_result,
    assert_fm6_hang_envelopes,
)
from shared.idempotency import (
    ClaimResult,
    NotifyCoordinator,
    read_lock_contention_count,
    read_send_count,
    send_email,
)
from shared.result import FetchPayload, NotifyPayload, ParsePayload, Result
from shared.wait import wait_until

from shared.redis import REDIS_URL, client as redis_client

app = Celery(
    "fm6_task_timeouts",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# FM-3: broker topology
# ---------------------------------------------------------------------------

dead_letter_queue, DELIVERY_LIMIT = declare_dlq(app, "fm6")

DRAIN_INTERVAL_SECONDS = 5
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# FM-6: timeout constants
# ---------------------------------------------------------------------------

# Demo values — production: align soft to p99 + slack; manual hard to ~2-3x soft.
SOFT_TIMEOUT_SECONDS = 2  # FM-6: Celery SIGUSR1 → SoftTimeLimitExceeded → @enveloped
MANUAL_HARD_TIMEOUT_SECONDS = (
    5  # FM-6: @hard_timeout SIGALRM → HardTimeoutExceeded → @enveloped
)
BACKSTOP_HARD_TIMEOUT_SECONDS = (
    300  # FM-6: Celery time_limit — last resort, never exercised in demo
)

# notify needs wider limits: 3s send_email sleep + busy-retry evaluation.
NOTIFY_SOFT_TIMEOUT_SECONDS = 60
NOTIFY_HARD_TIMEOUT_SECONDS = 300

# Must exceed MANUAL_HARD so the alarm fires before the sleep returns.
HANG_DURATION_SECONDS = 30


# ---------------------------------------------------------------------------
# FM-4: idempotency machinery
# ---------------------------------------------------------------------------

NOTIFY_RETRY_DELAY_SECONDS = 10

# ---------------------------------------------------------------------------
# FM-5: transient error type
# ---------------------------------------------------------------------------


class TransientServiceError(Exception):
    """Stand-in for 503 / connection-reset / read-timeout from an external service."""


# ---------------------------------------------------------------------------
# FM-6: @hard_timeout — kept inline because this IS the lesson
# ---------------------------------------------------------------------------


class HardTimeoutExceeded(Exception):
    """Raised by @hard_timeout when the per-task budget is exceeded."""


def hard_timeout(seconds: float) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Manual per-task hard timeout via signal.setitimer + SIGALRM.

    Bypasses Celery's time_limit because on_timeout(soft=False) calls
    mark_as_failure unconditionally (celery/worker/request.py:521-538),
    writing a FAILURE-state chord-part result and poisoning the chord.
    By raising HardTimeoutExceeded from inside the task body we stay
    inside @enveloped's catch path: SUCCESS-state FAILURE envelope, chord
    coordinator advances normally.

    Innermost decorator — order: @app.task → @enveloped → @transient_retryable
    → @hard_timeout → body. The alarm covers only the body, not decorators.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            def _on_alarm(signum: int, _frame: Any) -> None:
                raise HardTimeoutExceeded(
                    f"{self.name} exceeded {seconds}s hard timeout"
                )

            prev_handler = signal.signal(signal.SIGALRM, _on_alarm)
            signal.setitimer(signal.ITIMER_REAL, seconds)
            try:
                return func(self, *args, **kwargs)
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, prev_handler)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Per-doc behavior schedule
# ---------------------------------------------------------------------------

FLAKE_SCHEDULE: dict[str, FlakeEntry] = {
    "doc1": FAIL,  # FM-0: raises RuntimeError → FAILURE envelope (FM-1 proven)
    "doc3": CRASH_ONCE,  # FM-2: SIGKILL on attempt 1; broker redelivers; succeeds attempt 2
    "doc4": POISON,  # FM-3: permanent SIGKILL → x-delivery-limit → DLQ → drain_dlq finalizes
    "doc5": 2,  # FM-5: TransientServiceError 2x, then success on attempt 3
    "doc6": FLAKE_FOREVER,  # FM-5: TransientServiceError always → retries exhausted → FAILURE envelope
    "doc7": SOFT_HANG,  # FM-6: hangs → SoftTimeLimitExceeded → @enveloped → FAILURE envelope
    "doc8": HARD_HANG_MANUAL,  # FM-6: hangs, ignores soft, SIGALRM fires → @enveloped → FAILURE envelope
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(
    name="fetch_document",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=SOFT_TIMEOUT_SECONDS,
    time_limit=BACKSTOP_HARD_TIMEOUT_SECONDS,
)
@enveloped
@transient_retryable(exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
@hard_timeout(MANUAL_HARD_TIMEOUT_SECONDS)
def fetch_document(self, doc_id: str) -> FetchPayload:
    return FetchPayload(doc_id=doc_id, bytes=len(doc_id) * 100)


@app.task(
    name="parse_document",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=SOFT_TIMEOUT_SECONDS,  # FM-6: SIGUSR1 → SoftTimeLimitExceeded
    time_limit=BACKSTOP_HARD_TIMEOUT_SECONDS,  # FM-6: last-resort backstop
)
@enveloped
@transient_retryable(
    exceptions=(TransientServiceError,), max_retries=MAX_RETRIES
)  # FM-5: bounded retries
@hard_timeout(
    MANUAL_HARD_TIMEOUT_SECONDS
)  # FM-6: SIGALRM hard cap → HardTimeoutExceeded → @enveloped
def parse_document(self, fetched: dict) -> ParsePayload:
    fetch_result = Result.from_dict(fetched, FetchPayload)
    doc_id = fetch_result.payload.doc_id if fetch_result.payload else "unknown"
    attempts = incr_attempts(doc_id)
    flake = FLAKE_SCHEDULE.get(doc_id)

    # FM-0+: @enveloped catches this RuntimeError → FAILURE envelope
    if flake is FAIL:
        raise RuntimeError(f"parser crashed on {doc_id}")

    # FM-2+: SIGKILL on attempt 1; redelivered and succeeds on attempt 2
    if flake is CRASH_ONCE and attempts == 1:
        print(f"  worker pid={os.getpid()}: crashing on {doc_id} (attempt 1)")
        os.kill(os.getpid(), signal.SIGKILL)

    # FM-3+: permanent SIGKILL; x-delivery-limit caps the loop; drain_dlq finalizes
    if flake is POISON:
        print(
            f"  worker pid={os.getpid()}: poison crash on {doc_id} "
            f"(attempt {attempts}/{DELIVERY_LIMIT})"
        )
        os.kill(os.getpid(), signal.SIGKILL)

    # FM-5+: raise TransientServiceError N times then succeed (int N), or always (FLAKE_FOREVER).
    if flake is FLAKE_FOREVER or (isinstance(flake, int) and attempts <= flake):
        raise TransientServiceError(f"503 from parser-svc on {doc_id}")

    # FM-6+: hang past soft limit. No try/except: SoftTimeLimitExceeded propagates
    # through @transient_retryable (not in exceptions=) and is caught by @enveloped.
    if flake is SOFT_HANG:
        print(f"  worker pid={os.getpid()}: hanging on {doc_id} (soft-honoring)")
        time.sleep(HANG_DURATION_SECONDS)

    # FM-6+: hang, catch + ignore the soft signal, let @hard_timeout fire next.
    # HardTimeoutExceeded passes through @transient_retryable and is caught by @enveloped.
    if flake is HARD_HANG_MANUAL:
        try:
            print(
                f"  worker pid={os.getpid()}: hanging on {doc_id} "
                f"(ignoring soft, manual hard at {MANUAL_HARD_TIMEOUT_SECONDS}s)"
            )
            time.sleep(HANG_DURATION_SECONDS)
        except SoftTimeLimitExceeded:
            print(
                f"  worker pid={os.getpid()}: caught soft on {doc_id}, "
                f"ignoring; waiting for manual hard"
            )
            time.sleep(HANG_DURATION_SECONDS)

    print(f"  worker pid={os.getpid()}: parsed {doc_id} (attempt {attempts})")
    return ParsePayload(doc_id=doc_id, parsed=True, attempts=attempts)


@app.task(
    name="notify",
    bind=True,
    max_retries=5,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=NOTIFY_SOFT_TIMEOUT_SECONDS,  # FM-6: wider limit for send_email + retry eval
    time_limit=NOTIFY_HARD_TIMEOUT_SECONDS,
)
@enveloped
def notify(self, results: list[dict], pipeline_id: str) -> NotifyPayload:
    """FM-4: idempotency lock."""
    coordinator = NotifyCoordinator(pipeline_id)
    # Results carry five envelope shapes: normal completion, retryable-exhausted,
    # soft-timeout, manual-hard-timeout, and DLQ-finalized poison. All carry
    # status="SUCCESS"|"FAILURE".
    typed: list[Result[ParsePayload]] = [
        Result.from_dict(r, ParsePayload) for r in results
    ]
    ok = [r for r in typed if r.status == "SUCCESS"]
    failed = [r for r in typed if r.status == "FAILURE"]

    match coordinator.try_claim():
        case ClaimResult.ALREADY_SENT:
            print(f"  notify({pipeline_id}): already sent — skipping")
            return NotifyPayload(
                final=True,
                pipeline_id=pipeline_id,
                sent=False,
                ok=len(ok),
                failed=len(failed),
            )
        case ClaimResult.CONTENDED:
            print(
                f"  notify({pipeline_id}): lock held by another worker; retrying in {NOTIFY_RETRY_DELAY_SECONDS}s"
            )
            raise self.retry(countdown=NOTIFY_RETRY_DELAY_SECONDS)
        case ClaimResult.CLAIMED:
            pass

    send_email(
        f"Your pipeline documents are ready. "
        f"Id: {pipeline_id}. "
        f"Processed: {len(ok)}. "
        f"Failed: {len(failed)}.",
    )
    coordinator.mark_sent()

    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in ok:
        doc_id = r.payload.doc_id if r.payload else "?"
        print(f"  ok:     {doc_id}")
    for r in failed:
        doc_id = r.payload.doc_id if r.payload else "?"
        print(f"  failed: {doc_id}: {r.error}")
    return NotifyPayload(
        final=True,
        pipeline_id=pipeline_id,
        sent=True,
        ok=len(ok),
        failed=len(failed),
    )


@app.task(name="drain_dlq")
def drain_dlq() -> None:
    """FM-3: beat task — reads DLQ, writes SUCCESS-state envelopes, advances chord."""

    drain_dlq_messages(app, dead_letter_queue)


app.conf.beat_schedule = {
    "drain-dlq": {
        "task": "drain_dlq",
        "schedule": schedule(DRAIN_INTERVAL_SECONDS),
    },
}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _expected_attempts(doc_id: str) -> int:
    """Predicted parse_document entry count per doc.

    FAIL              → 1  (raises immediately; RuntimeError not in transient_retryable)
    CRASH_ONCE        → 2  (crash + broker redelivery)
    POISON            → DELIVERY_LIMIT  (broker cap)
    FLAKE_FOREVER     → 1 + MAX_RETRIES
    N (int ≥ 0)       → N + 1
    SOFT_HANG         → 1  (timeout exception not retried)
    HARD_HANG_MANUAL  → 1  (timeout exception not retried)
    """
    flake = FLAKE_SCHEDULE.get(doc_id)
    if flake is POISON:
        return DELIVERY_LIMIT
    if flake is FLAKE_FOREVER:
        return MAX_RETRIES + 1
    if flake is CRASH_ONCE:
        return 2
    if flake is FAIL:
        return 1
    if flake is SOFT_HANG or flake is HARD_HANG_MANUAL:
        return 1
    if isinstance(flake, int):
        return flake + 1
    return 1  # happy path (no entry)


def run_pipeline() -> None:
    docs = list(FLAKE_SCHEDULE.keys()) + ["doc2"]
    pipeline_id = str(uuid.uuid4())
    coordinator = NotifyCoordinator(pipeline_id)

    redis_client.flushall()

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    chord_result = pipeline.apply_async()
    print(f"chord submitted: id={chord_result.id} pipeline_id={pipeline_id}")

    # Slowest path: POISON at ~SOFT_TIMEOUT * DELIVERY_LIMIT ≈ 10-15s (redelivery loop).
    # HARD_HANG_MANUAL bounded by MANUAL_HARD_TIMEOUT_SECONDS (single attempt).
    # Total ~15-25s for 8 docs at --concurrency=2. 120s comfortable.
    print("waiting for chord notify to claim the lock...")
    wait_until(
        lambda: coordinator.is_claimed(),
        timeout=120,
        message="chord notify never claimed the lock within 120s",
    )

    print("--- triggering concurrent duplicate notify ---")
    duplicate_result = notify.delay([], pipeline_id=pipeline_id)

    print("waiting for both notifies to complete...")
    wait_until(
        lambda: chord_result.ready() and duplicate_result.ready(),
        timeout=30,
        message="tasks did not finish within 30s",
    )

    first = Result.from_dict(chord_result.get(timeout=1), NotifyPayload)
    second = Result.from_dict(duplicate_result.get(timeout=1), NotifyPayload)
    print(f"chord notify result:     {first}")
    print(f"duplicate notify result: {second}")

    sends = read_send_count()
    contention = read_lock_contention_count()
    print(f"send_email invocations:    {sends}")
    print(f"lock contention retries:   {contention}")

    print("parse_document entries (from Redis):")
    for d in docs:
        actual = read_attempts(d)
        expected = _expected_attempts(d)
        print(
            f"  {d}: {actual} (expected {expected}, schedule={FLAKE_SCHEDULE.get(d, 'happy')})"
        )

    doc3_attempts = read_attempts("doc3")
    doc4_attempts = read_attempts("doc4")
    doc2_attempts = read_attempts("doc2")

    assert assert_fm1_chord_body_fired(first)
    assert_fm2_redelivery_happened(doc3_attempts, doc2_attempts)
    assert_fm3_poison_bounded_at_dlq(doc4_attempts, delivery_limit=DELIVERY_LIMIT)
    assert_fm4_notify_idempotent(first, second, pipeline_id, sends, contention)
    # ok: doc2 (happy) + doc3 (CRASH_ONCE recovers) + doc5 (flake 2x recovers) = 3
    # failed: doc1 (FAIL) + doc4 (POISON→DLQ) + doc6 (exhausted) + doc7 + doc8 (hangs) = 5
    assert_fm5_retryable_result(first, expected_ok=3, expected_failed=5)
    for d in docs:
        assert_fm5_doc_attempts(
            d,
            read_attempts(d),
            _expected_attempts(d),
            is_poison=(FLAKE_SCHEDULE.get(d) is POISON),
        )
    assert_fm6_hang_envelopes(first, expected_ok=3, expected_failed=5)
    print(
        f"FM-6 fixed: doc7 hang → soft timeout → envelope; "
        f"doc8 hang → manual hard timeout → envelope. "
        f"Tasks no longer pin workers indefinitely."
    )


if __name__ == "__main__":
    run_pipeline()
