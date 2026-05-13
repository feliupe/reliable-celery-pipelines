"""Fix for FM-5: intermittent failures from external services no longer kill the doc.

Delta from fm4_duplicated_runs.py
-----------------------------------
- @transient_retryable stacked between @enveloped and the body on
  fetch_document and parse_document: catches TransientServiceError and
  schedules an exponential-backoff retry; on exhaustion re-raises so
  @enveloped converts it to a FAILURE envelope.
- TransientServiceError class defined (stand-in for 503/connection-reset).
- FLAKE_FOREVER sentinel + handling added to parse_document body.
- FLAKE_SCHEDULE adds doc5 (flake 2x → succeeds) and doc6 (flake forever).


Order matters: @transient_retryable re-raises on exhaustion; @enveloped
(outer) catches that and returns a FAILURE envelope. Swapping them would
either eat the Retry signal (no retries) or break FM-1 (chord dies on
terminal failure).

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

from celery import Celery, chord
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
    POISON,
    FlakeEntry,
)  # FM-5: FLAKE_FOREVER
from shared.fm_asserts import (
    assert_fm1_chord_body_fired,
    assert_fm2_redelivery_happened,
    assert_fm3_poison_bounded_at_dlq,
    assert_fm4_notify_idempotent,
    assert_fm5_doc_attempts,
    assert_fm5_retryable_result,
)
from shared.idempotency import (
    ClaimResult,
    NotifyCoordinator,
    read_lock_contention_count,
    read_send_count,
    reset_lock_contention_count,
    reset_send_count,
    send_email,
)
from shared.result import FetchPayload, NotifyPayload, ParsePayload, Result
from shared.wait import wait_until

from shared.redis import REDIS_URL, client as redis_client

app = Celery(
    "fm5_transient_failures",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# FM-3: broker topology (renamed fm4.* → fm5.*)
# ---------------------------------------------------------------------------

dead_letter_queue, DELIVERY_LIMIT = declare_dlq(app, "fm5")

DRAIN_INTERVAL_SECONDS = 5  # FM-3: DLQ drain cadence
MAX_RETRIES = 3  # FM-5: @transient_retryable retry budget


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
# Per-doc behavior schedule
# ---------------------------------------------------------------------------

FLAKE_SCHEDULE: dict[str, FlakeEntry] = {
    "doc1": FAIL,  # FM-0: raises RuntimeError → FAILURE envelope (FM-1 proven)
    "doc3": CRASH_ONCE,  # FM-2: SIGKILL on attempt 1; broker redelivers; succeeds attempt 2
    "doc4": POISON,  # FM-3: permanent SIGKILL → x-delivery-limit → DLQ → drain_dlq finalizes
    "doc5": 2,  # FM-5: TransientServiceError 2x, then success on attempt 3
    "doc6": FLAKE_FOREVER,  # FM-5: TransientServiceError always → retries exhausted → FAILURE envelope
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="fetch_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@enveloped
@transient_retryable(
    exceptions=(TransientServiceError,), max_retries=MAX_RETRIES
)  # FM-5: bounded retries
def fetch_document(self, doc_id: str) -> FetchPayload:
    return FetchPayload(doc_id=doc_id, bytes=len(doc_id) * 100)


@app.task(name="parse_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@enveloped
@transient_retryable(
    exceptions=(TransientServiceError,), max_retries=MAX_RETRIES
)  # FM-5: bounded retries
def parse_document(self, fetched: dict) -> ParsePayload:
    fetch_result = Result.from_dict(fetched, FetchPayload)
    doc_id = fetch_result.payload.doc_id if fetch_result.payload else "unknown"
    # Counter increments on EVERY entry: for CRASH_ONCE/POISON it counts broker
    # redeliveries; for FLAKE/int it counts self.retry() calls.
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
    # @transient_retryable catches this, schedules a self.retry() with backoff.
    # On exhaustion it re-raises → @enveloped converts to FAILURE envelope.
    if flake is FLAKE_FOREVER or (isinstance(flake, int) and attempts <= flake):
        raise TransientServiceError(f"503 from parser-svc on {doc_id}")

    print(f"  worker pid={os.getpid()}: parsed {doc_id} (attempt {attempts})")
    return ParsePayload(doc_id=doc_id, parsed=True, attempts=attempts)


@app.task(
    name="notify",
    bind=True,
    max_retries=5,  # FM-4: busy-retry budget for the lock-held branch
    acks_late=True,
    reject_on_worker_lost=True,
)
@enveloped
def notify(self, results: list[dict], pipeline_id: str) -> NotifyPayload:
    """FM-4: idempotency lock. @enveloped passes through self.retry() Retry signal."""
    coordinator = NotifyCoordinator(pipeline_id)
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

    FAIL         → 1  (raises immediately; no retry for RuntimeError)
    CRASH_ONCE   → 2  (crash + redelivery)
    POISON       → DELIVERY_LIMIT  (broker cap)
    FLAKE_FOREVER → 1 + MAX_RETRIES
    N (int ≥ 0)  → N + 1  (N flakes then success)
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
    if isinstance(flake, int):
        return flake + 1
    return 1  # happy path (no entry)


def run_pipeline() -> None:
    docs = ["doc1", "doc2", "doc3", "doc4", "doc5", "doc6"]
    pipeline_id = str(uuid.uuid4())
    coordinator = NotifyCoordinator(pipeline_id)

    reset_attempts(docs)
    reset_send_count()
    reset_lock_contention_count()
    redis_client.flushall()

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    chord_result = pipeline.apply_async()
    print(f"chord submitted: id={chord_result.id} pipeline_id={pipeline_id}")

    # Budget: max of (poison DLQ path ≈15-20s, retryable exhaustion ≈7-10s).
    # All headers run in parallel. 90s comfortable.
    print("waiting for chord notify to claim the lock...")
    wait_until(
        lambda: coordinator.is_claimed(),
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
    # failed: doc1 (FAIL) + doc4 (POISON→DLQ) + doc6 (FLAKE_FOREVER exhausted) = 3
    assert_fm5_retryable_result(first, expected_ok=3, expected_failed=3)
    for d in docs:
        assert_fm5_doc_attempts(
            d,
            read_attempts(d),
            _expected_attempts(d),
            is_poison=(FLAKE_SCHEDULE.get(d) is POISON),
        )
    print(
        f"FM-5 fixed: doc5 recovered via retryable; "
        f"doc6 exhausted retries → envelope; doc4 → DLQ; "
        f"send_email idempotent (1 send across 2 notifies)."
    )


if __name__ == "__main__":
    run_pipeline()
