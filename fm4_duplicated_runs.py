"""Fix for FM-4: notify is idempotent — duplicate executions don't
double-send.

Layered on top of fm3_dlq_reconciliation.py (FM-3); the broker
topology, drain_dlq beat task, and acks_late survivability are
inherited verbatim. Read that file first.

Why duplicates are a real risk
------------------------------
Three independent paths can fire the chord body twice for the same
pipeline_id:

  - FM-2 (acks_late + reject_on_worker_lost): a worker that crashes
    between "did the work" and "acked the message" sees the message
    redelivered. For header tasks that's the point; for notify it's
    a second email.
  - drain_dlq writes a chord-member result via mark_as_done, which
    advances on_chord_part_return. A manual requeue of a DLQ'd
    message from the RabbitMQ UI can trigger a second
    on_chord_part_return for the same task_id → body fires again.
  - Any explicit re-fire (operator script, retry from outside the
    chord).

Technique: optimistic lock keyed by pipeline_id in Redis
--------------------------------------------------------
The lock value itself encodes the send state:

  SET key 0 NX EX <lock_ttl>          → claim acquired (we will send)
  SETNX failed → GET key:
        b"0"  another worker mid-send → self.retry(countdown=10s)
        b"1"  already sent            → no-op success envelope
  After send_email() →
        SET key 1                     → durable sent marker, no TTL

The SENT marker has no TTL: once we've sent, that fact is permanent.
The NOT_SENT lock has a TTL so a crashed claimant doesn't wedge the
pipeline.

Chord-body signature: .s() not .si()
------------------------------------
fm3_dlq_reconciliation.py uses notify.si(pipeline_id) to ignore the
header results list. Here we switch to notify.s(pipeline_id=...) so
header results flow in as `results` — letting notify aggregate
ok/failed across both doc2's normal envelope and doc1's
DLQ-finalized envelope (both shapes carry an `ok` field).

Run
---
  docker-compose up -d
  celery -A fm4_duplicated_runs worker --loglevel=info --concurrency=2 --beat
  python fm4_duplicated_runs.py

--concurrency=2 is required for FM-4: with --concurrency=1 the
duplicate notify is queued behind the chord's notify and never lands
while the lock is held, so the busy-retry branch never fires.
"""

from __future__ import annotations

import os
import signal
import uuid

import redis
from celery import Celery, chord
from celery.schedules import schedule
from kombu import Exchange, Queue

from shared.fm_asserts import (
    assert_fm1_chord_body_fired,
    assert_fm3_poison_bounded_at_dlq,
    assert_fm4_notify_idempotent,
)
from shared.idempotency import (
    read_lock_contention_count,
    read_send_count,
    reset_lock_contention_count,
    reset_send_count,
    send_email,
)
from shared.wait import wait_until

REDIS_URL = "redis://localhost:6379/0"

app = Celery(
    "fm4_duplicated_runs",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# Broker topology — see fm3_dlq_reconciliation.py for the rationale.
# Renamed fm3.* → fm4.* so this file's queues coexist with FM-3's
# without colliding on declare (RabbitMQ rejects redeclare with
# different x-args).
# ---------------------------------------------------------------------------

DLX_NAME = "fm4.dlx"
DLQ_NAME = "fm4.dead_letters"
PIPELINE_QUEUE = "fm4.pipeline"
DELIVERY_LIMIT = 3

dlx_exchange = Exchange(DLX_NAME, type="direct", durable=True)
dead_letter_queue = Queue(
    DLQ_NAME,
    exchange=dlx_exchange,
    routing_key="dead",
    durable=True,
    queue_arguments={"x-queue-type": "quorum"},
)

pipeline_exchange = Exchange("fm4.pipeline", type="direct", durable=True)
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
app.conf.task_default_exchange = "fm4.pipeline"
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


def _attempts_key(doc_id: str) -> str:
    return f"fm4:crash_attempts:{doc_id}"


# ---------------------------------------------------------------------------
# Idempotency machinery
# ---------------------------------------------------------------------------

NOTIFY_STATE_NOT_SENT = b"0"
NOTIFY_STATE_SENT = b"1"
# Lock TTL bounds a crashed claimant. SENT keys (value=1) accumulate
# forever — production code pairs this with a sweeper job.
NOTIFY_LOCK_TTL_SECONDS = 600
NOTIFY_RETRY_DELAY_SECONDS = 10


def _notify_state_key(pipeline_id: str) -> str:
    return f"fm4:notify:state:{pipeline_id}"


SEND_COUNT_KEY = "fm4:send_email:count"
LOCK_CONTENTION_KEY = "fm4:notify:lock_contention_count"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="fetch_document", acks_late=True, reject_on_worker_lost=True)
def fetch_document(doc_id: str) -> dict:
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


@app.task(name="parse_document", acks_late=True, reject_on_worker_lost=True)
def parse_document(fetched: dict) -> dict:
    doc_id = fetched["doc_id"]
    attempts = redis_client.incr(_attempts_key(doc_id))

    if doc_id == "doc1":
        print(
            f"  worker pid={os.getpid()}: poison crash on {doc_id} "
            f"(attempt {attempts}/{DELIVERY_LIMIT})"
        )
        os.kill(os.getpid(), signal.SIGKILL)

    print(f"  worker pid={os.getpid()}: parsed {doc_id} (attempt {attempts})")
    return {"doc_id": doc_id, "ok": True, "parsed": True, "attempts": attempts}


@app.task(
    name="notify",
    bind=True,
    max_retries=5,
    acks_late=True,
    reject_on_worker_lost=True,
)
def notify(self, results: list[dict], pipeline_id: str) -> dict:
    """Send the completion email at most once per pipeline_id.

    Three branches, keyed off the lock state:
      - SETNX wins → state=NOT_SENT, we send, then flip to SENT
      - state=NOT_SENT (lock held) → busy-retry in 10s
      - state=SENT → fast skip, return summary with sent=False
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
            return _summary(results, pipeline_id, sent=False)
        # state == NOT_SENT: another worker is mid-send. By the time
        # we retry it'll be SENT (skip) or the TTL will have expired
        # (we claim).
        redis_client.incr(LOCK_CONTENTION_KEY)
        print(
            f"  notify({pipeline_id}): lock held by another worker; "
            f"retrying in {NOTIFY_RETRY_DELAY_SECONDS}s"
        )
        raise self.retry(countdown=NOTIFY_RETRY_DELAY_SECONDS)

    # results carries a mix of envelope shapes: normal completions
    # from parse_document and DLQ-finalized envelopes from drain_dlq.
    # Both have an `ok` field, so this aggregation is uniform.
    ok = [r for r in results if isinstance(r, dict) and r.get("ok")]
    failed = [r for r in results if isinstance(r, dict) and not r.get("ok")]

    # Not fully transactional: a worker crash between send_email()
    # and the SET below leaves state=0, so a redelivery will resend.
    # End-to-end exactly-once requires the email API to honor an
    # idempotency key.
    send_email(
        f"Your pipeline documents are ready. "
        f"Id: {pipeline_id}. "
        f"Processed: {len(ok)}. "
        f"Failed: {len(failed)}.",
        redis_client,
        SEND_COUNT_KEY,
    )
    # SET without `ex` clears the TTL — the sent fact is permanent.
    # INCR would inherit the lock's TTL and the marker could age out
    # before a late redelivery, allowing a resend.
    redis_client.set(state_key, NOTIFY_STATE_SENT)

    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in ok:
        print(f"  ok:     {r.get('doc_id')}")
    for r in failed:
        print(f"  failed: {r.get('doc_id')}: {r.get('error')}")
    return _summary(results, pipeline_id, sent=True)


def _summary(results: list[dict], pipeline_id: str, sent: bool) -> dict:
    ok = [r for r in results if isinstance(r, dict) and r.get("ok")]
    failed = [r for r in results if isinstance(r, dict) and not r.get("ok")]
    return {
        "final": True,
        "pipeline_id": pipeline_id,
        "sent": sent,
        "ok": len(ok),
        "failed": len(failed),
    }


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


def _reset(doc_ids: list[str]) -> None:
    keys = [_attempts_key(d) for d in doc_ids]
    if keys:
        redis_client.delete(*keys)


def _read_attempts(doc_id: str) -> int:
    raw = redis_client.get(_attempts_key(doc_id))
    return int(raw) if raw else 0


def run_pipeline() -> None:
    docs = ["doc1", "doc2"]
    pipeline_id = str(uuid.uuid4())
    state_key = _notify_state_key(pipeline_id)

    _reset(docs)
    reset_send_count(redis_client, SEND_COUNT_KEY)
    reset_lock_contention_count(redis_client, LOCK_CONTENTION_KEY)
    redis_client.delete(state_key)

    # .s() instead of .si() so header results flow into notify.
    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    chord_result = pipeline.apply_async()
    print(f"chord submitted: id={chord_result.id} pipeline_id={pipeline_id}")

    # Polling for the lock key is more reliable than a fixed sleep:
    # too early and the duplicate wins SETNX; too late and the
    # chord's notify has already flipped to SENT, so the duplicate
    # fast-paths to skip without exercising the busy-retry branch.
    #
    # The lock isn't claimed until ALL header members complete:
    # doc2 finishes fast, doc1 needs DELIVERY_LIMIT crashes + a
    # drain_dlq tick to be finalized. Budget 60s.
    print("waiting for chord notify to claim the lock...")
    wait_until(
        lambda: bool(redis_client.exists(state_key)),
        timeout=60,
        message="chord notify never claimed the lock within 60s",
    )

    # Duplicate fire with the same pipeline_id. The chord's notify
    # is mid-send (sleeping in send_email); this duplicate should
    # see state=NOT_SENT, increment LOCK_CONTENTION_KEY, retry in
    # 10s, then find state=SENT and skip.
    print("--- triggering concurrent duplicate notify ---")
    duplicate_result = notify.delay([], pipeline_id=pipeline_id)

    # Worst case: SEND_EMAIL_DURATION + NOTIFY_RETRY_DELAY + slack.
    print("waiting for both notifies to complete...")
    wait_until(
        lambda: chord_result.ready() and duplicate_result.ready(),
        timeout=30,
        message="tasks did not finish within 30s",
    )

    first = chord_result.get(timeout=1)
    second = duplicate_result.get(timeout=1)
    print(f"chord notify result:     {first}")
    print(f"duplicate notify result: {second}")

    sends = read_send_count(redis_client, SEND_COUNT_KEY)
    contention = read_lock_contention_count(redis_client, LOCK_CONTENTION_KEY)
    print(f"send_email invocations:    {sends}")
    print(f"lock contention retries:   {contention}")

    doc1_attempts = _read_attempts("doc1")
    doc2_attempts = _read_attempts("doc2")
    print("attempts (from Redis):")
    print(f"  doc1: {doc1_attempts} (expected ~{DELIVERY_LIMIT}, bounded by x-delivery-limit)")
    print(f"  doc2: {doc2_attempts} (expected 1)")

    assert_fm1_chord_body_fired(first)
    assert_fm3_poison_bounded_at_dlq(doc1_attempts, delivery_limit=DELIVERY_LIMIT)
    assert doc2_attempts == 1, f"doc2 should have run once; got {doc2_attempts}"
    assert_fm4_notify_idempotent(first, second, pipeline_id, sends, contention)
    print(
        f"FM-4 fixed: send_email idempotent (1 send across 2 notifies); "
        f"busy-retry exercised ({contention}x)."
    )


if __name__ == "__main__":
    run_pipeline()
