"""Fix for FM-4: notify is idempotent — duplicate executions don't double-send.

Delta from fm3_dlq_reconciliation.py
--------------------------------------
- notify body: optimistic Redis lock keyed by pipeline_id prevents
  send_email from firing more than once, even when notify runs concurrently.
- notify adds max_retries=5 for the busy-retry path.
- Runner fires a second notify.delay() to exercise the idempotency branch.

No new doc is added — FM-4's failure mode is exercised by the runner
firing a duplicate notify.delay() rather than per-doc behavior. The
FLAKE_SCHEDULE and parse_document body are identical to FM-3.

Why duplicates are a real risk
-------------------------------
  - FM-2 (acks_late): a worker that crashes between "did the work" and
    "acked the message" sees notify redelivered.
  - drain_dlq + manual RabbitMQ UI requeue can trigger a second
    on_chord_part_return for the same task_id, firing the body twice.
  - Explicit re-fire from an operator script.

Run
---
  docker-compose up -d
  celery -A fm4_duplicated_runs worker --loglevel=info --concurrency=2 --beat
  python fm4_duplicated_runs.py

--concurrency=2 required: the duplicate notify must land while the chord's
notify holds the lock (state=0) to exercise the busy-retry branch.
"""

from __future__ import annotations

import os
import signal
import uuid

from celery import Celery, chord
from celery.schedules import schedule
from shared.counters import incr_attempts, read_attempts, reset_attempts
from shared.decorators import enveloped
from shared.dlq import declare_dlq, drain_dlq_messages
from shared.redis import REDIS_URL, client as redis_client
from shared.flake import (
    CRASH_ONCE,
    FAIL,
    POISON,
    FlakeEntry,
)
from shared.fm_asserts import (
    assert_fm1_chord_body_fired,
    assert_fm2_redelivery_happened,
    assert_fm3_poison_bounded_at_dlq,
    assert_fm4_notify_idempotent,
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

app = Celery(
    "fm4_duplicated_runs",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


dead_letter_queue, DELIVERY_LIMIT = declare_dlq(app, "fm4")

DRAIN_INTERVAL_SECONDS = 2


# ---------------------------------------------------------------------------
# FM-4: idempotency machinery
# ---------------------------------------------------------------------------

NOTIFY_RETRY_DELAY_SECONDS = 2

# ---------------------------------------------------------------------------
# Per-doc behavior schedule
# ---------------------------------------------------------------------------

FLAKE_SCHEDULE: dict[str, FlakeEntry] = {
    "doc1": FAIL,  # FM-0: raises RuntimeError → FAILURE envelope (FM-1 proven)
    "doc3": CRASH_ONCE,  # FM-2: SIGKILL on attempt 1; broker redelivers; succeeds attempt 2
    "doc4": POISON,  # FM-3: permanent SIGKILL → x-delivery-limit → DLQ → drain_dlq finalizes
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="fetch_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@enveloped
def fetch_document(self, doc_id: str) -> FetchPayload:
    return FetchPayload(doc_id=doc_id, bytes=len(doc_id) * 100)


@app.task(name="parse_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@enveloped
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
    """Send the completion email at most once per pipeline_id.

    FM-4 idempotency contract:
      SETNX wins (state=NOT_SENT) → send → flip to SENT
      state=NOT_SENT (lock held)  → busy-retry in 10s
      state=SENT                  → fast skip, return sent=False
    """
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

    # Not fully transactional: a crash between send_email() and mark_sent()
    # leaves state=NOT_SENT, allowing a redelivery to resend. End-to-end
    # exactly-once requires the email API to honour an idempotency key.
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


def run_pipeline() -> None:
    docs = ["doc1", "doc2", "doc3", "doc4"]
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

    # Wait for notify to claim the lock (state=NOT_SENT). The lock isn't
    # claimed until ALL header members complete. Budget 60s.
    print("waiting for chord notify to claim the lock...")
    wait_until(
        lambda: coordinator.is_claimed(),
        timeout=60,
        message="chord notify never claimed the lock within 60s",
    )

    # Duplicate fire: chord's notify is mid-send (sleeping in send_email).
    # The duplicate should see state=NOT_SENT → lock-contention retry →
    # state=SENT → skip.
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

    doc3_attempts = read_attempts("doc3")
    doc4_attempts = read_attempts("doc4")
    doc2_attempts = read_attempts("doc2")
    print("parse_document entries (from Redis):")
    print(f"  doc1: {read_attempts('doc1')} (expected 1)")
    print(f"  doc2: {doc2_attempts} (expected 1)")
    print(f"  doc3: {doc3_attempts} (expected 2 — crash + redelivery)")
    print(f"  doc4: {doc4_attempts} (expected ~{DELIVERY_LIMIT})")

    assert assert_fm1_chord_body_fired(first)
    assert_fm2_redelivery_happened(doc3_attempts, doc2_attempts)
    assert_fm3_poison_bounded_at_dlq(doc4_attempts, delivery_limit=DELIVERY_LIMIT)
    assert_fm4_notify_idempotent(first, second, pipeline_id, sends, contention)
    print(
        f"FM-4 fixed: send_email idempotent (1 send across 2 notifies); "
        f"busy-retry exercised ({contention}x)."
    )


if __name__ == "__main__":
    run_pipeline()
