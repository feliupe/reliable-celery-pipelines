"""Fix for FM-3: poison messages no longer loop forever; chords no longer
stall when a header is dead-lettered.

Delta from fm2_worker_crash.py
-------------------------------
- Quorum pipeline queue with x-dead-letter-exchange + x-delivery-limit:
  after DELIVERY_LIMIT redeliveries the broker dead-letters the message
  instead of requeuing. The crash loop is bounded at the broker level.
- DLX exchange + DLQ queue declared at startup.
- drain_dlq beat task: reads dead-lettered messages, writes a
  SUCCESS-state Result(status="FAILURE") envelope via mark_as_done,
  advancing Celery's native on_chord_part_return so the body fires.
- POISON sentinel + its parse_document branch for doc4: crashes every
  attempt until the broker DLQs it.

FLAKE_SCHEDULE grows by one entry ("doc4": POISON). docs grows by one
element ("doc4").

Why mark_as_done (SUCCESS) not mark_as_failure
-----------------------------------------------
A FAILURE-state chord member causes ChordError in the coordinator, sending
the body to link_error instead of firing it. We write SUCCESS with a
Result(status="FAILURE") payload — the same envelope shape @enveloped
produces — so the coordinator advances normally. See shared/dlq.py.

Hard dependencies on other FMs
-------------------------------
FM-2 (acks_late + reject_on_worker_lost): without it the broker acks the
message at receipt and redelivery never happens — no x-delivery-count
increment, no DLQ landing.

Run
---
  docker-compose up -d
  celery -A fm3_dlq_reconciliation worker --loglevel=info --concurrency=2 --beat
  python fm3_dlq_reconciliation.py

If a previous run created the pipeline queue with different x-args, reset
with `docker-compose down -v` to avoid PRECONDITION_FAILED on redeclare.
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

from shared.redis import REDIS_URL, client
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
)
from shared.result import FetchPayload, NotifyPayload, ParsePayload, Result
from shared.wait import wait_until

app = Celery(
    "fm3_dlq_reconciliation",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# FM-3: broker topology — quorum queue + dead-letter exchange + DLQ
# ---------------------------------------------------------------------------

dead_letter_queue, DELIVERY_LIMIT = declare_dlq(app, "fm3")


# ---------------------------------------------------------------------------
# FM-3: DLQ drain cadence
# ---------------------------------------------------------------------------

DRAIN_INTERVAL_SECONDS = 5


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

    # FM-3+: permanent SIGKILL; x-delivery-limit caps the loop; drain_dlq
    # finalizes the chord member by writing a SUCCESS-state FAILURE envelope.
    if flake is POISON:
        print(
            f"  worker pid={os.getpid()}: poison crash on {doc_id} "
            f"(attempt {attempts}/{DELIVERY_LIMIT})"
        )
        os.kill(os.getpid(), signal.SIGKILL)

    print(f"  worker pid={os.getpid()}: parsed {doc_id} (attempt {attempts})")
    return ParsePayload(doc_id=doc_id, parsed=True, attempts=attempts)


@app.task(name="notify", bind=True, acks_late=True, reject_on_worker_lost=True)
@enveloped
def notify(self, results: list[dict], pipeline_id: str) -> NotifyPayload:
    typed: list[Result[ParsePayload]] = [
        Result.from_dict(r, ParsePayload) for r in results
    ]
    ok = [r for r in typed if r.status == "SUCCESS"]
    failed = [r for r in typed if r.status == "FAILURE"]
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
        ok=len(ok),
        failed=len(failed),
    )


@app.task(name="drain_dlq")
def drain_dlq() -> None:
    """FM-3: beat task — see shared/dlq.py and module docstring."""

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
    client.flushall()

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    chord_result = pipeline.apply_async()
    print(f"chord submitted: id={chord_result.id} pipeline_id={pipeline_id}")

    # Budget: DELIVERY_LIMIT crashes (~5-15s) + drain interval (≤5s) + chord body. 30s comfortable.
    print("waiting for chord body...")
    wait_until(
        chord_result.ready,
        timeout=30,
        interval=1,
        message="chord body did not fire within 30s",
    )

    raw = chord_result.get(timeout=1)
    print(f"pipeline result: {raw}")
    notify_result = Result.from_dict(raw, NotifyPayload)

    doc2_attempts = read_attempts("doc2")
    doc3_attempts = read_attempts("doc3")
    doc4_attempts = read_attempts("doc4")
    print("parse_document entries (from Redis):")
    print(f"  doc1: {read_attempts('doc1')} (expected 1)")
    print(f"  doc2: {doc2_attempts} (expected 1)")
    print(f"  doc3: {doc3_attempts} (expected 2 — crash + redelivery)")
    print(
        f"  doc4: {doc4_attempts} (expected ~{DELIVERY_LIMIT}, bounded by x-delivery-limit)"
    )

    assert assert_fm1_chord_body_fired(notify_result)
    assert_fm2_redelivery_happened(doc3_attempts, doc2_attempts)
    assert_fm3_poison_bounded_at_dlq(doc4_attempts, delivery_limit=DELIVERY_LIMIT)
    print(
        f"FM-3 fixed: poison capped at {doc4_attempts} crashes (DLQ); "
        f"drain_dlq finalized chord-member; body fired via native coordinator."
    )


if __name__ == "__main__":
    run_pipeline()
