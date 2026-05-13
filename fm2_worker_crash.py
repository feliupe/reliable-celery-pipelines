"""Fix for FM-2: a worker crash mid-task no longer loses the message.

Delta from fm1_mid_pipeline_error.py
-------------------------------------
- acks_late=True + reject_on_worker_lost=True on every task: the broker
  holds the message until the task acks successfully; a SIGKILL'd child
  causes the master to NACK/reject back to the queue for redelivery.
- Redis attempts counter (shared.counters): tracks parse_document entries
  across process boundaries.
- CRASH_ONCE sentinel + its parse_document branch: doc3 SIGKILLs the
  worker on its first attempt, succeeds on the redelivered second attempt.

FLAKE_SCHEDULE grows by one entry ("doc3": CRASH_ONCE). docs grows by
one element ("doc3").

Caveats (addressed in later FMs)
---------------------------------
Redelivery means the task body executes more than once — non-idempotent
side effects fire twice (→ FM-4).
Redelivery is unbounded for a message that always crashes (→ FM-3).

Run
---
  docker-compose up -d
  celery -A fm2_worker_crash worker --loglevel=info --concurrency=2
  python fm2_worker_crash.py

--concurrency=2 required: a sibling worker absorbs the redelivered
message immediately rather than waiting for the master to respawn
the killed child.
"""

from __future__ import annotations

import os
import signal
import uuid

from celery import Celery, chord

from shared import redis
from shared.counters import incr_attempts, read_attempts, reset_attempts
from shared.redis import REDIS_URL
from shared.decorators import enveloped
from shared.flake import CRASH_ONCE, FAIL, FlakeEntry
from shared.fm_asserts import (
    assert_fm1_chord_body_fired,
    assert_fm2_redelivery_happened,
)
from shared.result import FetchPayload, NotifyPayload, ParsePayload, Result
from shared.wait import wait_until

app = Celery(
    "fm2_worker_crash",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)

# ---------------------------------------------------------------------------
# Per-doc behavior schedule
# ---------------------------------------------------------------------------

FLAKE_SCHEDULE: dict[str, FlakeEntry] = {
    "doc1": FAIL,  # FM-0: raises RuntimeError → FAILURE envelope (FM-1 proven)
    "doc3": CRASH_ONCE,  # FM-2: SIGKILL on attempt 1; broker redelivers; succeeds attempt 2
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


# FM-2: acks_late defers the broker ack until the task returns successfully.
# reject_on_worker_lost tells the master to NACK (not ack) when a child dies
# mid-task, so the broker requeues instead of discarding the message.
# Applied uniformly: in production you don't want any step to silently lose
# a crashed message.
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

    # FM-2+: SIGKILL on attempt 1. SIGKILL is uncatchable — bypasses Python
    # cleanup and Celery hooks. acks_late + reject_on_worker_lost requeue
    # the message; the sibling worker picks it up and succeeds on attempt 2.
    if flake is CRASH_ONCE and attempts == 1:
        print(f"  worker pid={os.getpid()}: crashing on {doc_id} (attempt 1)")
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
        attempts = r.payload.attempts if r.payload else "?"
        print(f"  ok:     {doc_id} (attempts={attempts})")
    for r in failed:
        doc_id = r.payload.doc_id if r.payload else "?"
        print(f"  failed: {doc_id}: {r.error}")
    return NotifyPayload(
        final=True,
        pipeline_id=pipeline_id,
        ok=len(ok),
        failed=len(failed),
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_pipeline() -> None:
    docs = ["doc1", "doc2", "doc3"]
    pipeline_id = str(uuid.uuid4())
    redis.client.flushall()

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    result = pipeline.apply_async()

    # Worst case: crash detection + sibling pickup + parse. 30s is comfortable.
    wait_until(
        result.ready,
        timeout=30,
        message=(
            "chord body did not fire within 30s — FM-2 not fixed. "
            "Without acks_late + reject_on_worker_lost the SIGKILL'd "
            "message is lost and the chord stalls forever."
        ),
    )

    raw = result.get(timeout=1)
    print(f"pipeline result: {raw}")
    notify_result = Result.from_dict(raw, NotifyPayload)

    doc3_attempts = read_attempts("doc3")
    doc2_attempts = read_attempts("doc2")
    print("parse_document entries (from Redis):")
    print(f"  doc1: {read_attempts('doc1')} (expected 1, raises immediately)")
    print(f"  doc2: {doc2_attempts} (expected 1)")
    print(f"  doc3: {doc3_attempts} (expected 2 — crash + redelivery)")

    assert assert_fm1_chord_body_fired(notify_result)
    assert_fm2_redelivery_happened(doc3_attempts, doc2_attempts)
    print("FM-2 fixed: SIGKILL'd parse_document was redelivered and completed.")


if __name__ == "__main__":
    run_pipeline()
