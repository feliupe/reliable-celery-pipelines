"""Fix for FM-3: poison messages no longer loop forever; chords no
longer stall when a header is dead-lettered.

Failure mode
------------
A poison message — one whose handler reliably crashes the worker —
combined with FM-2's redelivery (acks_late + reject_on_worker_lost)
loops forever. Every crash requeues; the next worker picks it up and
crashes again. The chord never completes; workers are consumed.

Fix (two parts, both required)
------------------------------
1. Broker-level cap: quorum queue + x-delivery-limit
   ------------------------------------------------
   The pipeline queue is declared as a quorum queue with
   x-delivery-limit set. After that many redeliveries the broker
   dead-letters the message instead of requeuing. The crash loop is
   bounded at the BROKER, not the worker — important, because a
   worker-side counter would have to live somewhere durable across
   the very crash it's trying to bound.

   Quorum queues are required: classic queues don't track
   x-delivery-count and have no equivalent ceiling.

2. DLQ-driven chord finalization (Celery beat)
   ------------------------------------------
   Once the poison message is dead-lettered, the chord's coordinator
   is still waiting for that header's result. A periodic beat task
   `drain_dlq` reads each message in fm3.dead_letters, extracts the
   chord context from its AMQP headers, and writes a SUCCESS result
   with a failure envelope through app.backend.mark_as_done(...).

   That triggers Celery's native on_chord_part_return: the chord's
   group counter advances, and when it equals chord_size the body
   is dispatched the same way it would be for any header completion.
   No wall-clock threshold, no chord registry, no body bypass —
   we use Celery's own coordinator instead of fighting it.

Why mark_as_done (SUCCESS), not mark_as_failure
-----------------------------------------------
A chord member written with state=FAILURE causes
_unpack_chord_result (celery/backends/redis.py) to raise ChordError
when collecting the final results. That sends the failure to the
body's link_error rather than firing the body. The chord effectively
never completes from the body's perspective.

The fix mirrors what fm1_mid_pipeline_error.py does inside the task
body: never write FAILURE; always write SUCCESS with an envelope
payload `{ok: False, error: ...}` that ENCODES the failure. The
chord coordinator sees clean SUCCESS states across all members and
dispatches the body normally. The body inspects each envelope's
`ok` flag if it cares; in this file's case, notify.si(pipeline_id)
ignores header results entirely (immutable signature), so the
envelope shape doesn't matter — only its state being SUCCESS does.

Hard dependencies on other fixes
--------------------------------
FM-2 (acks_late + reject_on_worker_lost): without it the broker
acks the message at receipt and there's no redelivery to count
toward x-delivery-limit. No DLQ landing, no finalization.

FM-4 (idempotency on notify): drain_dlq's mark_as_done is
idempotent (overwrites the result key), but a manual requeue of a
DLQ'd message from the management UI can produce a second
on_chord_part_return for the same task_id. Redis backend cleans up
chord state after dispatch so the second call no-ops, but FM-4
remains the canonical defense against event-level double-fire.

FM-6 (time_limit): tasks that hang without crashing don't redeliver
and therefore never reach the DLQ. The hard time_limit is what
converts a hang into a worker death → redelivery → DLQ → drain.
Without FM-6, this file cannot recover from hangs.

Run
---
  docker-compose up -d
  celery -A fm3_dlq_reconciliation worker --loglevel=info --concurrency=2 --beat
  python fm3_dlq_reconciliation.py

`--beat` runs the scheduler in-worker, fine for the demo. In
production beat is a separate process so the worker fleet can scale
independently of schedule dispatch.

If a previous run created the pipeline queue with different x-args
(e.g. classic queue, no DLX), RabbitMQ refuses to redeclare with the
new args. Reset with `docker-compose down -v` if startup fails on
PRECONDITION_FAILED.
"""

from __future__ import annotations

import os
import signal
import uuid

import redis
from celery import Celery, chord
from celery.schedules import schedule
from kombu import Exchange, Queue

from shared.fm_asserts import assert_fm1_chord_body_fired, assert_fm3_poison_bounded_at_dlq
from shared.inspect import print_all_task_results
from shared.wait import wait_until

REDIS_URL = "redis://localhost:6379/0"

app = Celery(
    "fm3_dlq_reconciliation",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# Broker topology: quorum pipeline queue + dead-letter exchange/queue
# ---------------------------------------------------------------------------

DLX_NAME = "fm3.dlx"
DLQ_NAME = "fm3.dead_letters"
PIPELINE_QUEUE = "fm3.pipeline"

# Dead-letter target. Quorum so the DLQ itself survives broker
# restarts — losing dead-lettered messages defeats the point.
dlx_exchange = Exchange(DLX_NAME, type="direct", durable=True)
dead_letter_queue = Queue(
    DLQ_NAME,
    exchange=dlx_exchange,
    routing_key="dead",
    durable=True,
    queue_arguments={"x-queue-type": "quorum"},
)

# x-delivery-limit is a quorum-queue-only feature; classic queues
# don't track delivery count and can't bound infinite redelivery at
# the broker level. After the limit is hit, the broker dead-letters
# instead of requeuing, breaking the FM-2-induced crash loop.
DELIVERY_LIMIT = 3
pipeline_exchange = Exchange("fm3.pipeline", type="direct", durable=True)
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

# Only the pipeline queue is a task queue (= the worker consumes from
# it). The DLQ is declared on the broker (below) so the DLX has
# somewhere to route dead-lettered messages, but Celery does NOT
# consume from it — DLQ contents are pulled by drain_dlq via
# basic.get, not by Celery's task consumer. If the DLQ were a task
# queue, the worker would pick up the poison message from the DLQ
# and re-enter the same crash loop x-delivery-limit was meant to
# break (the DLQ has no x-delivery-limit of its own).
app.conf.task_queues = (pipeline_queue,)
app.conf.task_default_queue = PIPELINE_QUEUE
app.conf.task_default_exchange = "fm3.pipeline"
app.conf.task_default_routing_key = "pipeline"


def _declare_dlq_topology() -> None:
    """Declare the DLX exchange and DLQ on the broker so the
    pipeline queue's dead-lettering has a target. Idempotent — safe
    to call from both worker and driver processes at import time."""
    with app.connection_for_write() as conn:
        with conn.channel() as ch:
            dlx_exchange.declare(channel=ch)
            dead_letter_queue.declare(channel=ch)


_declare_dlq_topology()

# Quorum queues don't support global (per-connection) QoS; only
# per-channel. With this flag, Celery inspects task_queues, finds the
# quorum queue, and sends basic.qos with apply_global=False so the
# broker doesn't reject basic.consume with "NOT_IMPLEMENTED - queue
# '...' does not support global qos". Defaults to True in Celery 5.5+
# (added in 5.5; absent in 5.4 entirely — requires the upgrade).
app.conf.worker_detect_quorum_queues = True

# Future-default flips in Celery 6.0; setting them explicitly silences
# the pending-deprecation warnings and locks behavior.
app.conf.broker_connection_retry_on_startup = True
app.conf.worker_cancel_long_running_tasks_on_connection_loss = True


# ---------------------------------------------------------------------------
# DLQ drain cadence
# ---------------------------------------------------------------------------

redis_client = redis.Redis.from_url(REDIS_URL)

# Demo value. Production: ~30s. Lower = faster recovery, higher
# broker load. The drain itself is cheap (basic.get returns None
# when empty), so a tight cadence is fine.
DRAIN_INTERVAL_SECONDS = 5


def _attempts_key(doc_id: str) -> str:
    return f"fm3:crash_attempts:{doc_id}"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="fetch_document", acks_late=True, reject_on_worker_lost=True)
def fetch_document(doc_id: str) -> dict:
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


@app.task(name="parse_document", acks_late=True, reject_on_worker_lost=True)
def parse_document(fetched: dict) -> dict:
    """doc1 simulates a poison message: every execution SIGKILLs the
    worker mid-task. With FM-2's flags each crash requeues; with
    x-delivery-limit, redelivery is capped and the broker
    dead-letters the message. drain_dlq then finalizes the chord."""
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


@app.task(name="notify", acks_late=True, reject_on_worker_lost=True)
def notify(pipeline_id: str) -> dict:
    """The chord body. Takes a pipeline identifier, not the header
    results — in a real system it queries a database for per-doc
    state of this run and dispatches downstream actions.

    Decoupling notify from chord-piped args means the chord can fire
    it identically regardless of how each header reached its
    terminal state: a normal worker completion, a worker-side
    failure converted to envelope (FM-1), or a DLQ-finalization
    envelope (this file). All three paths write a SUCCESS-state
    result via the result backend; the chord's on_chord_part_return
    advances the same way."""
    print(f"notify: finalizing pipeline {pipeline_id}")
    return {"final": True, "pipeline_id": pipeline_id}


@app.task(name="drain_dlq")
def drain_dlq() -> None:
    """Beat task — see shared/dlq.py and module docstring for the protocol."""
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
    _reset(docs)

    # pipeline_id is the domain identifier — what notify uses to
    # query its own state. Distinct from chord_id (a Celery internal
    # task_id), though for the demo either would work as the lookup
    # key.
    pipeline_id = str(uuid.uuid4())

    # .si() — immutable signature. Without it, the chord would prepend
    # the header results list to notify's args. With .si(), notify is
    # invoked as notify(pipeline_id) regardless of whether each
    # header succeeded naturally or was finalized via DLQ drain.
    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.si(pipeline_id))
    chord_result = pipeline.apply_async()
    print(f"chord submitted: id={chord_result.id} pipeline_id={pipeline_id}")

    # Wait for the chord body. Celery's coordinator dispatches it as
    # soon as all members reach a SUCCESS-state result — doc2 from
    # the worker, doc1 from drain_dlq's envelope write.
    # Budget: DELIVERY_LIMIT crashes (~5-15s) + drain interval (≤5s)
    # + body run (~1s) << 90s.
    print("waiting for chord body...")
    wait_until(
        chord_result.ready,
        timeout=90,
        interval=1,
        message="chord body did not fire within 90s",
    )

    value = chord_result.get(timeout=1)
    print(f"pipeline result: {value}")

    doc1_attempts = _read_attempts("doc1")
    doc2_attempts = _read_attempts("doc2")
    print("attempts (from Redis):")
    print(f"  doc1: {doc1_attempts} (expected ~{DELIVERY_LIMIT}, bounded by x-delivery-limit)")
    print(f"  doc2: {doc2_attempts} (expected 1)")

    assert_fm1_chord_body_fired(value)
    assert value["pipeline_id"] == pipeline_id, (
        f"notify ran with wrong pipeline_id: {value['pipeline_id']!r}"
    )
    assert_fm3_poison_bounded_at_dlq(doc1_attempts, delivery_limit=DELIVERY_LIMIT)
    assert doc2_attempts == 1, f"doc2 should have run once; got {doc2_attempts}"
    print(
        f"FM-3 fixed: poison capped at {doc1_attempts} crashes (DLQ); "
        f"drain_dlq finalized chord-member; body fired via native coordinator."
    )


if __name__ == "__main__":
    run_pipeline()
    print_all_task_results(redis_client)
