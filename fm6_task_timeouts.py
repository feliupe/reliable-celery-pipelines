"""Fix for FM-6: a task that hangs (without crashing) no longer
consumes a worker slot indefinitely.

Layered on fm5_transient_failures.py; the DLQ, idempotent notify,
acks_late survivability, and retry decorators are inherited
verbatim. Read those files first.

The gap FM-6 closes
-------------------
FM-2's acks_late + reject_on_worker_lost recovers a crashed
worker. FM-3 caps poison crashes via x-delivery-limit. FM-5
catches transient blips via @transient_retryable. But none of those handle
a task that simply hangs — stuck in a downstream socket read,
waiting on a deadlocked lock, looping over an unbounded result
set. The worker stays alive, the broker never sees a redelivery,
x-delivery-count never increments. The chord stalls forever.

Technique: per-task timeouts (soft + hard)
------------------------------------------
Celery gives every task two timeout knobs:

  soft_time_limit  — at expiration, SIGUSR1 fires inside the
                     worker. Python's signal handler raises
                     celery.exceptions.SoftTimeLimitExceeded in
                     the main thread, interrupting whatever the
                     task is doing (including time.sleep, socket
                     reads — anything that yields to the signal
                     handler). The task can catch and clean up.

  time_limit       — hard ceiling. The parent worker process
                     SIGKILLs the child running the task. No
                     Python-level catch possible.

Both are set on every task. The soft limit is the friendly
deadline; the hard limit is the safety net for tasks that don't
honor the soft signal. In this demo Celery's time_limit is set
very high (BACKSTOP_HARD_TIMEOUT_SECONDS) and is not exercised
— see the next section for why, and the section after for what
we use instead.

Hard time_limit doesn't cleanly compose with chord-member retries
-----------------------------------------------------------------
Hard time_limit fires Celery's `on_timeout(soft=False)` handler,
which calls `mark_as_failure` UNCONDITIONALLY before any reject /
redelivery decision (celery/worker/request.py:521-538). That
triggers `on_chord_part_return(state=FAILURE, ...)` and writes a
FAILURE-state entry into the chord's result set
(celery/backends/base.py:161-172). When the chord later joins, it
sees the FAILURE and raises ChordError — the body never fires.

FM-3's poison SIGKILL avoids this because the `on_worker_lost`
path skips `mark_as_failure` when the message is being requeued
(`if not requeue` guard at celery/worker/request.py:622).
`on_timeout` has no equivalent guard.

Manual hard timeout via @hard_timeout decorator
-----------------------------------------------
For chord members we enforce a per-task hard timeout from inside
the task body via signal.setitimer(ITIMER_REAL) → SIGALRM. The
decorator raises HardTimeoutExceeded, which @enveloped
catches and converts to a SUCCESS-state Result(status="FAILURE")
envelope. No Celery on_timeout, no FAILURE chord-part write, no
chord poisoning.

SIGALRM is safe to use: Celery's worker uses SIGUSR1 for soft
timeout and parent-side SIGKILL for hard timeout; SIGALRM is
otherwise untouched in the worker child. Linux-only — on Windows
fall back to a threading.Timer-based variant.

Celery's own time_limit stays configured as a last-resort backstop
(BACKSTOP_HARD_TIMEOUT_SECONDS, set far above any legitimate task
runtime) for tasks that don't yield to Python signals at all
(some C extensions) — the chord will fail via ChordError in that
case, but at least the worker isn't pinned forever.

Why not reconcile FAILURE → SUCCESS later
-----------------------------------------
The chord's Redis zset stores entries by encoded (state + result),
so a later mark_as_done for a task with an existing FAILURE entry
adds a SECOND entry rather than overwriting. The body fires only
on exact readycount == chord_size (celery/backends/redis.py:496-500),
so once readycount has exceeded chord_size due to the extra entry,
the body never fires. A reconciler would need direct zrem of the
FAILURE entry plus careful counter management. Fragile; bypasses
Celery's API. The decorator approach avoids the FAILURE write
entirely.

How this composes with prior FMs
--------------------------------
SoftTimeLimitExceeded propagates up through @transient_retryable
(not in exceptions= — see below) and is caught by @enveloped,
returning a Result(status="FAILURE") envelope.

HardTimeoutExceeded (from @hard_timeout) takes the same envelope
path as SoftTimeLimitExceeded. doc5 below demonstrates a task
that ignores soft timeout — manual hard fires next, envelope
returned.

Why timeout exceptions are NOT in transient_retryable's exceptions
------------------------------------------------------------------
A hang isn't a transient blip. If a downstream service is wedged,
retrying the same call without backoff or remediation will hang
again. Make the failure visible (envelope), let the operator
decide. Same reasoning applies to both SoftTimeLimitExceeded and
HardTimeoutExceeded.

Per-doc scenario
----------------
  doc1  poison → SIGKILL every time             → DLQ (FM-3)
  doc2  transient flake 2x, succeeds attempt 3  → retryable recovery
  doc3  transient flake forever                 → retryable envelope
  doc4  hang, soft timeout fires                → envelope (FM-6)
  doc5  hang, ignores soft, manual hard fires   → envelope (FM-6)

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

import redis
from celery import Celery, chord
from celery.exceptions import SoftTimeLimitExceeded
from celery.schedules import schedule
from kombu import Exchange, Queue

from shared.counters import incr_attempts, read_attempts, reset_attempts
from shared.decorators import enveloped, transient_retryable
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
    read_lock_contention_count,
    read_send_count,
    reset_lock_contention_count,
    reset_send_count,
    send_email,
)
from shared.result import FetchPayload, NotifyPayload, ParsePayload, Result
from shared.wait import wait_until

REDIS_URL = "redis://localhost:6379/0"
ATTEMPTS_KEY_PREFIX = "fm6:attempts"

app = Celery(
    "fm6_task_timeouts",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# Broker topology — see fm3_dlq_reconciliation.py. Renamed fm5.* → fm6.*
# so this file's queues coexist with prior FMs.
# ---------------------------------------------------------------------------

DLX_NAME = "fm6.dlx"
DLQ_NAME = "fm6.dead_letters"
PIPELINE_QUEUE = "fm6.pipeline"
DELIVERY_LIMIT = 3

dlx_exchange = Exchange(DLX_NAME, type="direct", durable=True)
dead_letter_queue = Queue(
    DLQ_NAME,
    exchange=dlx_exchange,
    routing_key="dead",
    durable=True,
    queue_arguments={"x-queue-type": "quorum"},
)

pipeline_exchange = Exchange("fm6.pipeline", type="direct", durable=True)
pipeline_queue = Queue(
    PIPELINE_QUEUE,
    exchange=pipeline_exchange,
    routing_key="pipeline",
    durable=True,
    queue_arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": DLX_NAME,
        "x-dead-letter-routing-key": "dead",
        "x-delivery-limit": DELIVERY_LIMIT,
    },
)

app.conf.task_queues = (pipeline_queue,)
app.conf.task_default_queue = PIPELINE_QUEUE
app.conf.update(task_default_exchange="fm6.pipeline", task_default_routing_key="pipeline")


def _declare_dlq_topology() -> None:
    with app.connection_for_write() as conn:
        with conn.channel() as ch:
            dlx_exchange.declare(channel=ch)
            dead_letter_queue.declare(channel=ch)


_declare_dlq_topology()

app.conf.worker_detect_quorum_queues = True
app.conf.broker_connection_retry_on_startup = True
app.conf.worker_cancel_long_running_tasks_on_connection_loss = True


redis_client = redis.Redis.from_url(REDIS_URL)
DRAIN_INTERVAL_SECONDS = 5
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------

# Demo values. Production: align soft to your p99 + slack; manual
# hard to ~2-3x soft (real ceiling); backstop far above both.
SOFT_TIMEOUT_SECONDS = 2  # Celery soft → envelope via @enveloped
MANUAL_HARD_TIMEOUT_SECONDS = 5  # @hard_timeout → envelope via @enveloped
BACKSTOP_HARD_TIMEOUT_SECONDS = 300  # Celery time_limit, never exercised in demo

# notify includes a deliberate 3s send_email sleep + the busy-retry
# countdown evaluation; its limits are independently sized.
NOTIFY_SOFT_TIMEOUT_SECONDS = 60
NOTIFY_HARD_TIMEOUT_SECONDS = 300

# Hang duration. Must exceed MANUAL_HARD so manual hard fires.
HANG_DURATION_SECONDS = 30


# ---------------------------------------------------------------------------
# FM-6 lesson: @hard_timeout (stays inline — this IS the lesson)
# ---------------------------------------------------------------------------


class HardTimeoutExceeded(Exception):
    """Raised by @hard_timeout when the per-task budget is exceeded."""


def hard_timeout(
    seconds: float,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Manual per-task hard timeout via signal.setitimer + SIGALRM.

    Bypasses Celery's time_limit because that path writes a
    FAILURE-state chord-part result via mark_as_failure before
    redelivery (celery/worker/request.py:521-538), poisoning the
    chord. By raising HardTimeoutExceeded from inside the task
    body we keep the exception inside @enveloped's catch path:
    it converts to a Result(status="FAILURE") envelope, the chord
    coordinator advances normally.

    SIGALRM is safe to use here: Celery uses SIGUSR1 for soft
    timeout and parent-side SIGKILL for hard timeout; SIGALRM is
    untouched in the worker child. Linux/Unix only.

    Innermost decorator — order: @app.task → @enveloped
    → @transient_retryable → @hard_timeout → body. The alarm covers
    only the body, not decorator overhead.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> Any:
            def _on_alarm(signum, frame) -> None:
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


class _Sentinel:
    """Identity-based marker for FLAKE_SCHEDULE entries."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"<{self.name}>"


POISON = _Sentinel("POISON")  # SIGKILL the worker (FM-3 path)
FLAKE_FOREVER = _Sentinel("FLAKE_FOREVER")  # always raise TransientServiceError (FM-5 envelope path)
SOFT_HANG = _Sentinel("SOFT_HANG")  # hang past soft limit, envelope via @enveloped
HARD_HANG_MANUAL = _Sentinel("HARD_HANG_MANUAL")  # hang, ignore soft, manual @hard_timeout fires → envelope


FLAKE_SCHEDULE: dict[str, _Sentinel | int] = {
    "doc1": POISON,
    "doc2": 2,
    "doc3": FLAKE_FOREVER,
    "doc4": SOFT_HANG,
    "doc5": HARD_HANG_MANUAL,
}


# ---------------------------------------------------------------------------
# Transient error type
# ---------------------------------------------------------------------------


class TransientServiceError(Exception):
    """Stand-in for 503 / connection-reset / read-timeout from an external
    service."""


# ---------------------------------------------------------------------------
# Idempotency machinery (FM-4)
# ---------------------------------------------------------------------------

NOTIFY_STATE_NOT_SENT = b"0"
NOTIFY_STATE_SENT = b"1"
NOTIFY_LOCK_TTL_SECONDS = 600
NOTIFY_RETRY_DELAY_SECONDS = 10


def _notify_state_key(pipeline_id: str) -> str:
    return f"fm6:notify:state:{pipeline_id}"


SEND_COUNT_KEY = "fm6:send_email:count"
LOCK_CONTENTION_KEY = "fm6:notify:lock_contention_count"


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
    soft_time_limit=SOFT_TIMEOUT_SECONDS,
    time_limit=BACKSTOP_HARD_TIMEOUT_SECONDS,
)
@enveloped
@transient_retryable(exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
@hard_timeout(MANUAL_HARD_TIMEOUT_SECONDS)
def parse_document(self, fetched: dict) -> ParsePayload:
    fetch_result = Result.from_dict(fetched, FetchPayload)
    doc_id = fetch_result.payload.doc_id if fetch_result.payload else "unknown"
    attempts = incr_attempts(redis_client, doc_id, ATTEMPTS_KEY_PREFIX)
    flake = FLAKE_SCHEDULE.get(doc_id, 0)

    if flake is POISON:
        print(
            f"  worker pid={os.getpid()}: poison crash on {doc_id} "
            f"(attempt {attempts}/{DELIVERY_LIMIT})"
        )
        os.kill(os.getpid(), signal.SIGKILL)

    if flake is SOFT_HANG:
        # No try/except: SoftTimeLimitExceeded propagates up,
        # @transient_retryable passes it through (not in exceptions=),
        # @enveloped converts to Result(status="FAILURE").
        print(f"  worker pid={os.getpid()}: hanging on {doc_id} (soft-honoring)")
        time.sleep(HANG_DURATION_SECONDS)

    if flake is HARD_HANG_MANUAL:
        # Hang, catch + ignore the soft signal, let the manual
        # @hard_timeout fire next. HardTimeoutExceeded propagates up
        # through @transient_retryable (not retriable) and is caught by
        # @enveloped → Result(status="FAILURE") envelope. No
        # FAILURE chord-part write, no chord poisoning.
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

    if flake is FLAKE_FOREVER or (isinstance(flake, int) and attempts <= flake):
        raise TransientServiceError(f"503 from parser-svc on {doc_id}")

    print(f"  worker pid={os.getpid()}: parsed {doc_id} (attempt {attempts})")
    return ParsePayload(doc_id=doc_id, parsed=True, attempts=attempts)


@app.task(
    name="notify",
    bind=True,
    max_retries=5,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=NOTIFY_SOFT_TIMEOUT_SECONDS,
    time_limit=NOTIFY_HARD_TIMEOUT_SECONDS,
)
@enveloped
def notify(self, results: list[dict], pipeline_id: str) -> NotifyPayload:
    """See fm4_duplicated_runs.py for the idempotency contract.
    @enveloped sits outside; self.retry() raises Retry which @enveloped
    passes through to Celery's framework."""
    state_key = _notify_state_key(pipeline_id)

    claimed = redis_client.set(
        state_key,
        NOTIFY_STATE_NOT_SENT,
        nx=True,
        ex=NOTIFY_LOCK_TTL_SECONDS,
    )

    if not claimed:
        state = redis_client.get(state_key)
        if state == NOTIFY_STATE_SENT:
            print(f"  notify({pipeline_id}): already sent — skipping")
            typed: list[Result[ParsePayload]] = [
                Result.from_dict(r, ParsePayload) for r in results
            ]
            ok = [r for r in typed if r.status == "SUCCESS"]
            failed = [r for r in typed if r.status == "FAILURE"]
            return NotifyPayload(
                final=True, pipeline_id=pipeline_id,
                sent=False, ok=len(ok), failed=len(failed),
            )
        redis_client.incr(LOCK_CONTENTION_KEY)
        print(
            f"  notify({pipeline_id}): lock held by another worker; "
            f"retrying in {NOTIFY_RETRY_DELAY_SECONDS}s"
        )
        raise self.retry(countdown=NOTIFY_RETRY_DELAY_SECONDS)

    # Cast header results to typed objects at the boundary.
    # Results carry four envelope shapes: normal completion,
    # retryable-exhausted, soft-timeout, manual-hard-timeout, and
    # DLQ-finalized poison. All carry status="SUCCESS"|"FAILURE".
    typed = [Result.from_dict(r, ParsePayload) for r in results]
    ok = [r for r in typed if r.status == "SUCCESS"]
    failed = [r for r in typed if r.status == "FAILURE"]

    send_email(
        f"Your pipeline documents are ready. "
        f"Id: {pipeline_id}. "
        f"Processed: {len(ok)}. "
        f"Failed: {len(failed)}.",
        redis_client,
        SEND_COUNT_KEY,
    )
    redis_client.set(state_key, NOTIFY_STATE_SENT)

    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in ok:
        doc_id = r.payload.doc_id if r.payload else "?"
        print(f"  ok:     {doc_id}")
    for r in failed:
        doc_id = r.payload.doc_id if r.payload else "?"
        print(f"  failed: {doc_id}: {r.error}")
    return NotifyPayload(
        final=True, pipeline_id=pipeline_id,
        sent=True, ok=len(ok), failed=len(failed),
    )


@app.task(name="drain_dlq")
def drain_dlq() -> None:
    from shared.dlq import drain_dlq_messages
    drain_dlq_messages(app, dead_letter_queue)


app.conf.beat_schedule = {
    "drain-dlq": {
        "task": "drain_dlq",
        "schedule": schedule(DRAIN_INTERVAL_SECONDS),
    },
}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _expected_attempts(doc_id: str) -> int:
    """parse_document entries per doc.

    POISON                       → DELIVERY_LIMIT (broker cap)
    SOFT_HANG / HARD_HANG_MANUAL → 1 (no retry — timeout exceptions
                                     not in transient_retryable's exceptions=)
    FLAKE_FOREVER                → 1 + MAX_RETRIES
    N (int ≥ 0)                  → N + 1
    """
    flake = FLAKE_SCHEDULE.get(doc_id, 0)
    if flake is POISON:
        return DELIVERY_LIMIT
    if flake is SOFT_HANG or flake is HARD_HANG_MANUAL:
        return 1
    if flake is FLAKE_FOREVER:
        return MAX_RETRIES + 1
    assert isinstance(flake, int)
    return flake + 1


def run_pipeline() -> None:
    docs = list(FLAKE_SCHEDULE.keys())
    pipeline_id = str(uuid.uuid4())
    state_key = _notify_state_key(pipeline_id)

    reset_attempts(redis_client, docs, ATTEMPTS_KEY_PREFIX)
    reset_send_count(redis_client, SEND_COUNT_KEY)
    reset_lock_contention_count(redis_client, LOCK_CONTENTION_KEY)
    redis_client.delete(state_key)

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    chord_result = pipeline.apply_async()
    print(f"chord submitted: id={chord_result.id} pipeline_id={pipeline_id}")

    # Lock-claim budget: with 5 docs at --concurrency=2, header work
    # serializes in pairs. Slowest path is POISON at
    # ~SOFT_TIMEOUT * DELIVERY_LIMIT ≈ 10-15s (redelivery loop).
    # HARD_HANG_MANUAL is bounded by MANUAL_HARD_TIMEOUT_SECONDS
    # (single attempt). Total header time ~15-25s. 120s is comfortable.
    print("waiting for chord notify to claim the lock...")
    wait_until(
        lambda: bool(redis_client.exists(state_key)),
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

    sends = read_send_count(redis_client, SEND_COUNT_KEY)
    contention = read_lock_contention_count(redis_client, LOCK_CONTENTION_KEY)
    print(f"send_email invocations:    {sends}")
    print(f"lock contention retries:   {contention}")

    doc1_attempts = read_attempts(redis_client, "doc1", ATTEMPTS_KEY_PREFIX)
    doc2_attempts = read_attempts(redis_client, "doc2", ATTEMPTS_KEY_PREFIX)
    print("parse_document entries (from Redis):")
    for d in docs:
        actual = read_attempts(redis_client, d, ATTEMPTS_KEY_PREFIX)
        expected = _expected_attempts(d)
        print(f"  {d}: {actual} (expected {expected}, schedule={FLAKE_SCHEDULE[d]})")

    assert assert_fm1_chord_body_fired(first)
    assert_fm2_redelivery_happened(doc1_attempts, doc2_attempts)
    assert_fm3_poison_bounded_at_dlq(doc1_attempts, delivery_limit=DELIVERY_LIMIT)
    assert_fm4_notify_idempotent(first, second, pipeline_id, sends, contention)
    assert_fm5_retryable_result(first, expected_ok=1, expected_failed=4)
    for d in docs:
        assert_fm5_doc_attempts(
            d,
            read_attempts(redis_client, d, ATTEMPTS_KEY_PREFIX),
            _expected_attempts(d),
            is_poison=(FLAKE_SCHEDULE[d] is POISON),
        )
    assert_fm6_hang_envelopes(first, expected_ok=1, expected_failed=4)
    print(
        f"FM-6 fixed: doc4 hang → soft timeout → envelope; "
        f"doc5 hang → manual hard timeout → envelope. "
        f"Tasks no longer pin workers indefinitely."
    )


if __name__ == "__main__":
    run_pipeline()
