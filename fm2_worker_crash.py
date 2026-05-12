"""Fix for FM-2: a worker crash mid-task no longer loses the message.

Failure mode
------------
A worker is processing a message when the process dies hard — OOM
kill, SIGKILL, kernel panic, container eviction. Default Celery
behavior:

  acks_late=False (the default)
      The broker is acked the moment the worker pulls the message,
      BEFORE the task body runs. If the worker dies mid-task the
      broker has already discarded the message; nothing redelivers.
      The task silently never completes, and any chord/group waiting
      on it stalls forever.

Fix
---
Two task-level settings, both required:

  acks_late=True
      Defer the ack until the task returns successfully. If the
      worker dies mid-task the broker still holds the message.

  reject_on_worker_lost=True
      When Celery's master detects a child died mid-task it would by
      default ack the message anyway (and record a WorkerLostError
      result). With this flag the master REJECTS the message back to
      the queue so a respawned or sibling worker can retry it.

Both together: child dies → broker requeues → respawned/sibling worker
picks it up → task body runs again → chord completes.

Demo: parse_document SIGKILLs its own child on doc1's first attempt.
SIGKILL is uncatchable — bypasses Python finally blocks and Celery
shutdown hooks; same signature as OOM-killer / container eviction. A
counter in Redis tracks attempts across the crash; the run asserts
doc1 ran twice (crash + redelivery) and the chord still completed.

Caveats (companion fixes)
-------------------------
Redelivery means the task body executes more than once. Non-idempotent
side effects (email, charge, external write) fire twice. → FM-4.
Redelivery is unbounded; a poison message that always crashes the
worker loops forever. → FM-3.

Run
---
  docker-compose up -d
  celery -A fm2_worker_crash worker --loglevel=info --concurrency=2
  python fm2_worker_crash.py

Concurrency must be >= 2: a sibling worker absorbs the redelivered
message immediately rather than waiting for the master to respawn the
killed child. Both work; >= 2 is faster and timing-stable.
"""

from __future__ import annotations

import os
import signal

import redis
from celery import Celery, chord

from shared.wait import wait_until

REDIS_URL = "redis://localhost:6379/0"

app = Celery(
    "fm2_worker_crash",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)

# Cross-process attempt counter. self.request.retries doesn't help —
# that tracks self.retry() calls, not broker redeliveries from a
# crashed worker. We need state that survives a SIGKILL'd child.
redis_client = redis.Redis.from_url(REDIS_URL)


def _attempts_key(doc_id: str) -> str:
    return f"crash_attempts:{doc_id}"


# Pipeline-wide settings. acks_late + reject_on_worker_lost is applied
# uniformly to every task in the chain — in production you don't want
# any step to silently swallow a crashed message. The demo only
# exercises the flags on parse_document (it's the one that crashes),
# but the uniform application matches realistic config.
@app.task(name="fetch_document", acks_late=True, reject_on_worker_lost=True)
def fetch_document(doc_id: str) -> dict:
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


@app.task(name="parse_document", acks_late=True, reject_on_worker_lost=True)
def parse_document(fetched: dict) -> dict:
    doc_id = fetched["doc_id"]
    attempts = redis_client.incr(_attempts_key(doc_id))

    # Crash injection: doc1's first execution dies hard mid-task.
    # SIGKILL is uncatchable — bypasses Python cleanup and Celery
    # shutdown. From the master's POV the child just disappeared.
    if doc_id == "doc1" and attempts == 1:
        print(f"  worker pid={os.getpid()}: crashing on {doc_id} (attempt 1)")
        os.kill(os.getpid(), signal.SIGKILL)

    print(f"  worker pid={os.getpid()}: parsed {doc_id} (attempt {attempts})")
    return {"doc_id": doc_id, "ok": True, "parsed": True, "attempts": attempts}


@app.task(name="notify", acks_late=True, reject_on_worker_lost=True)
def notify(results: list[dict]) -> dict:
    ok = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in ok:
        print(f"  ok:     {r['doc_id']} (attempts={r.get('attempts')})")
    for r in failed:
        print(f"  failed: {r['doc_id']}: {r.get('error')}")
    return {"final": True, "ok": len(ok), "failed": len(failed), "results": results}


def _reset(doc_ids: list[str]) -> None:
    keys = [_attempts_key(d) for d in doc_ids]
    if keys:
        redis_client.delete(*keys)


def _read_attempts(doc_id: str) -> int:
    raw = redis_client.get(_attempts_key(doc_id))
    return int(raw) if raw else 0


def run_pipeline() -> None:
    docs = ["doc1", "doc2"]
    _reset(docs)

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s())
    result = pipeline.apply_async()

    # Worst case: crash detection (~few s) + respawn or sibling
    # pickup + parse. 30s is comfortable.
    wait_until(
        result.ready,
        timeout=30,
        message=(
            "chord body did not fire within 30s — FM-2 not fixed. "
            "Without acks_late + reject_on_worker_lost the SIGKILL'd "
            "message is lost and the chord stalls forever."
        ),
    )

    value = result.get(timeout=1)
    print(f"pipeline result: {value}")
    assert "final" in value, "Notify task did not run."

    # Mechanical proof the redelivery actually happened: parse ran
    # twice for doc1 (crash + redelivery) and once for doc2.
    doc1_attempts = _read_attempts("doc1")
    doc2_attempts = _read_attempts("doc2")
    print("parse attempts (from Redis):")
    print(f"  doc1: {doc1_attempts} (expected 2 — crash + redelivery)")
    print(f"  doc2: {doc2_attempts} (expected 1)")
    assert doc1_attempts == 2, (
        f"doc1 should have run twice (crash + redelivery); got {doc1_attempts}"
    )
    assert doc2_attempts == 1, f"doc2 should have run once; got {doc2_attempts}"

    print("FM-2 fixed: SIGKILL'd parse_document was redelivered and completed.")


if __name__ == "__main__":
    run_pipeline()
