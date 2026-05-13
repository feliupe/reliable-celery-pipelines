"""Fix for FM-5: intermittent failures from external services no
longer kill the doc.

Layered on fm4_duplicated_runs.py (FM-4); the DLQ reconciliation,
idempotent notify, and acks_late survivability are inherited
verbatim. Read those files first.

Technique: bounded retries with exponential backoff + jitter,
implemented as two shared decorators from shared/decorators.py:

    @app.task(name=..., bind=True, acks_late=True, ...)
    @enveloped                # converts escapes to Result(status="FAILURE")
    @transient_retryable(...) # catches transients, schedules retry
    def task(self, ...):
        ...

Order matters. Swap envelope and retryable and you'll either eat
Celery's Retry signal (no retries) or break FM-1 again (chord dies
on terminal failure). bind=True is mandatory — both decorators read
self.request / call self.retry.

How this composes with FM-3 and FM-4
------------------------------------
Three distinct failure-handling paths now coexist, keyed by how
the body exits:

  - SIGKILL (no Python exception)     → no ack → broker redelivery →
    x-delivery-count increments → DLQ at limit → drain_dlq finalizes
    a Result(status="FAILURE") chord-member envelope (FM-3 path).

  - raise TransientServiceError       → @transient_retryable catches →
    self.retry ACKs the original and schedules a new delivery → fresh
    x-delivery-count. Transient retries DO NOT accumulate toward DLQ.

  - raise TransientServiceError (exhausted) → @transient_retryable
    re-raises → @enveloped returns Result(status="FAILURE") → chord
    member completes SUCCESS Celery state with a typed FAILURE payload
    (same shape drain_dlq writes).

All three converge into notify, which aggregates by result.status.

Per-doc scenario (deterministic for reproducible asserts)
---------------------------------------------------------
  doc1  poison → SIGKILL every time → DLQ path           (FM-3)
  doc2  transient flake 2x, succeeds on attempt 3        (FM-5 recovery)
  doc3  transient flake forever, retries exhaust         (FM-5 envelope)

Run
---
  docker-compose up -d
  celery -A fm5_transient_failures worker --loglevel=info --concurrency=2 --beat
  python fm5_transient_failures.py
"""

from __future__ import annotations

import os
import signal
import uuid

import redis
from celery import Celery, chord
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
ATTEMPTS_KEY_PREFIX = "fm5:attempts"

app = Celery(
    "fm5_transient_failures",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# Broker topology — see fm3_dlq_reconciliation.py. Renamed fm4.* → fm5.*
# so this file's queues coexist with prior FMs without redeclare
# collisions.
# ---------------------------------------------------------------------------

DLX_NAME = "fm5.dlx"
DLQ_NAME = "fm5.dead_letters"
PIPELINE_QUEUE = "fm5.pipeline"
DELIVERY_LIMIT = 3

dlx_exchange = Exchange(DLX_NAME, type="direct", durable=True)
dead_letter_queue = Queue(
    DLQ_NAME,
    exchange=dlx_exchange,
    routing_key="dead",
    durable=True,
    queue_arguments={"x-queue-type": "quorum"},
)

pipeline_exchange = Exchange("fm5.pipeline", type="direct", durable=True)
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
app.conf.task_default_exchange = "fm5.pipeline"
app.conf.task_default_routing_key = "pipeline"


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
# Transient error type
# ---------------------------------------------------------------------------


class TransientServiceError(Exception):
    """Stand-in for 503 / connection-reset / read-timeout from an external
    service. In real code these are mapped from the HTTP client."""


# ---------------------------------------------------------------------------
# Per-doc behavior schedule (demo-only)
# ---------------------------------------------------------------------------


class _Sentinel:
    """Identity-based marker for FLAKE_SCHEDULE entries. Using a small
    class (rather than mixing strings and -1) lets every schedule value
    be either a sentinel or an int count, with no ambiguous overlap."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"<{self.name}>"


POISON = _Sentinel("POISON")  # SIGKILL the worker every call → DLQ path
FLAKE_FOREVER = _Sentinel("FLAKE_FOREVER")  # always raise TransientServiceError → exhaust retries


# doc_id → either POISON, FLAKE_FOREVER, or an integer N meaning
# "raise transient N times then succeed".
FLAKE_SCHEDULE: dict[str, _Sentinel | int] = {
    "doc1": POISON,
    "doc2": 2,
    "doc3": FLAKE_FOREVER,
}


# ---------------------------------------------------------------------------
# Idempotency machinery (FM-4)
# ---------------------------------------------------------------------------

NOTIFY_STATE_NOT_SENT = b"0"
NOTIFY_STATE_SENT = b"1"
NOTIFY_LOCK_TTL_SECONDS = 600
NOTIFY_RETRY_DELAY_SECONDS = 10


def _notify_state_key(pipeline_id: str) -> str:
    return f"fm5:notify:state:{pipeline_id}"


SEND_COUNT_KEY = "fm5:send_email:count"
LOCK_CONTENTION_KEY = "fm5:notify:lock_contention_count"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="fetch_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@enveloped
@transient_retryable(exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
def fetch_document(self, doc_id: str) -> FetchPayload:
    return FetchPayload(doc_id=doc_id, bytes=len(doc_id) * 100)


@app.task(name="parse_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@enveloped
@transient_retryable(exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
def parse_document(self, fetched: dict) -> ParsePayload:
    fetch_result = Result.from_dict(fetched, FetchPayload)
    doc_id = fetch_result.payload.doc_id if fetch_result.payload else "unknown"
    # The counter increments on EVERY entry, regardless of outcome:
    # for doc1 it counts SIGKILL attempts (DLQ assertion); for doc2/3
    # it counts retryable attempts (recovery/exhaustion assertions).
    attempts = incr_attempts(redis_client, doc_id, ATTEMPTS_KEY_PREFIX)
    flake = FLAKE_SCHEDULE.get(doc_id, 0)

    if flake is POISON:
        print(
            f"  worker pid={os.getpid()}: poison crash on {doc_id} "
            f"(attempt {attempts}/{DELIVERY_LIMIT})"
        )
        os.kill(os.getpid(), signal.SIGKILL)

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
)
@enveloped
def notify(self, results: list[dict], pipeline_id: str) -> NotifyPayload:
    """Send the completion email at most once per pipeline_id.

    Uses FM-4's busy-retry pattern. @enveloped sits outside and only
    fires on final success or unhandled exception; self.retry() raises
    Retry which @enveloped passes through to Celery's framework.
    """
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
    # Results carry a mix of envelope shapes from three paths:
    # normal completion, retryable-exhaustion, DLQ-finalized.
    # All carry status="SUCCESS"|"FAILURE".
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
    """How many parse_document entries we expect for a given doc.

      POISON         → DELIVERY_LIMIT crashes (broker cap)
      FLAKE_FOREVER  → 1 initial + MAX_RETRIES retries
      N (int ≥ 0)    → N flakes then success = N + 1 calls
    """
    flake = FLAKE_SCHEDULE.get(doc_id, 0)
    if flake is POISON:
        return DELIVERY_LIMIT
    if flake is FLAKE_FOREVER:
        return MAX_RETRIES + 1
    assert isinstance(flake, int)
    return flake + 1


def run_pipeline() -> None:
    docs = ["doc1", "doc2", "doc3"]
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

    # Lock-claim budget: max of (poison DLQ path ≈ 15-20s, retryable
    # exhaustion ≈ 1+2+4 backoff = ~7-10s, retryable recovery ≈ 3-5s).
    # All in parallel for headers, so ≈ 20s. 90s is comfortable.
    print("waiting for chord notify to claim the lock...")
    wait_until(
        lambda: bool(redis_client.exists(state_key)),
        timeout=90,
        message="chord notify never claimed the lock within 90s",
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

    assert_fm1_chord_body_fired(first)
    assert_fm2_redelivery_happened(doc1_attempts, doc2_attempts)
    assert_fm3_poison_bounded_at_dlq(doc1_attempts, delivery_limit=DELIVERY_LIMIT)
    assert_fm4_notify_idempotent(first, second, pipeline_id, sends, contention)
    assert_fm5_retryable_result(first, expected_ok=1, expected_failed=2)
    for d in docs:
        assert_fm5_doc_attempts(
            d,
            read_attempts(redis_client, d, ATTEMPTS_KEY_PREFIX),
            _expected_attempts(d),
            is_poison=(FLAKE_SCHEDULE[d] is POISON),
        )
    print(
        f"FM-5 fixed: doc2 recovered via retryable; "
        f"doc3 exhausted retries → envelope; doc1 → DLQ; "
        f"send_email idempotent (1 send across 2 notifies)."
    )


if __name__ == "__main__":
    run_pipeline()
