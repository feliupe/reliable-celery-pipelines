"""Fix for FM-5: intermittent failures from external services no
longer kill the doc.

Layered on 4_duplicated_runs.py (FM-4); the DLQ reconciliation,
idempotent notify, and acks_late survivability are inherited
verbatim. Read those files first.

Technique: bounded retries with exponential backoff + jitter,
implemented as two reusable decorators on the task body. Stacked
on top of @app.task in this order (top-down):

    @app.task(name=..., bind=True, acks_late=True, ...)
    @always_returns_envelope    # converts escapes to {ok: False, ...}
    @retryable(...)             # catches transients, schedules retry
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
    a chord-member envelope (FM-3 path).

  - raise TransientServiceError       → @retryable catches → self.retry
    ACKs the original and schedules a new delivery → fresh
    x-delivery-count. Transient retries DO NOT accumulate toward DLQ.

  - raise TransientServiceError (exhausted) → @retryable re-raises →
    @always_returns_envelope returns {ok: False, error: ...} → chord
    member completes SUCCESS-state with an envelope payload (same
    shape drain_dlq writes).

All three converge into notify, which aggregates by the `ok` flag.
notify itself is NOT decorated — its FM-4 idempotency machinery is
its own retry mechanism (busy-retry → self.retry on lock contention)
and the chord body must return the {sent: True/False} contract, not
an envelope.

Per-doc scenario (deterministic for reproducible asserts)
---------------------------------------------------------
  doc1  poison → SIGKILL every time → DLQ path           (FM-3)
  doc2  transient flake 2x, succeeds on attempt 3        (FM-5 recovery)
  doc3  transient flake forever, retries exhaust         (FM-5 envelope)

Run
---
  docker-compose up -d
  celery -A 5_transient_failures worker --loglevel=info --concurrency=2 --beat
  python 5_transient_failures.py
"""

import functools
import os
import random
import signal
import time
import uuid

import redis
from celery import Celery, chord
from celery.app.task import Context
from celery.exceptions import MaxRetriesExceededError, Retry
from celery.schedules import schedule
from kombu import Exchange, Queue

REDIS_URL = "redis://localhost:6379/0"

app = Celery(
    "5_transient_failures",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# Broker topology — see 3_dlq_reconciliation.py. Renamed fm4.* → fm5.*
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


def _declare_dlq_topology():
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


def _attempts_key(doc_id: str) -> str:
    return f"fm5:attempts:{doc_id}"


# ---------------------------------------------------------------------------
# Retry decorators
# ---------------------------------------------------------------------------


class TransientServiceError(Exception):
    """Stand-in for 503 / connection-reset / read-timeout from an external
    service. In real code these are mapped from the HTTP client."""


def always_returns_envelope(func):
    """Convert any escaping exception into a `{ok: False, error: ...}`
    payload so the chord aggregator sees a uniform list of outcomes.

    Does NOT catch celery.exceptions.Retry — that's the signal
    self.retry() raises to schedule a retry, and Celery's framework
    needs to see it. Swallowing it would silently disable retries.

    Must wrap the task body OUTSIDE @retryable: retryable re-raises
    the original on exhaustion, and this decorator is what turns
    that into the envelope.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Retry:
            raise
        except Exception as exc:
            base = args[0] if args and isinstance(args[0], dict) else {}
            return {
                **base,
                "ok": False,
                "error": str(exc),
                "attempts": self.request.retries + 1,
            }

    return wrapper


def retryable(retriable_exceptions=(), max_retries=3, backoff_base=2, backoff_cap=10):
    """Catch the named exceptions and retry with exponential backoff +
    jitter, up to max_retries. After exhaustion, re-raise the original
    exception so @always_returns_envelope can turn it into a payload.

    Jitter matters under load: without it, a fleet of workers
    retrying the same downstream service synchronizes and re-DDoSes
    it the moment it recovers.

    Innermost decorator — directly above the task body. Anything
    raised that isn't in retriable_exceptions passes through to
    @always_returns_envelope.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except retriable_exceptions as exc:
                try:
                    countdown = min(
                        backoff_base**self.request.retries, backoff_cap
                    ) + random.uniform(0, 1)
                    print(
                        f"  retry {self.name} (attempt "
                        f"{self.request.retries + 1}): {exc}; "
                        f"backoff {countdown:.2f}s"
                    )
                    raise self.retry(
                        exc=exc, countdown=countdown, max_retries=max_retries
                    )
                except MaxRetriesExceededError:
                    print(f"  {self.name} retries exhausted: {exc}")
                    raise exc

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Per-doc behavior schedule (demo-only)
# ---------------------------------------------------------------------------

POISON = "poison"  # SIGKILL the worker every call → DLQ path
FLAKE_FOREVER = -1  # always raise TransientServiceError → exhaust retries


# doc_id → either POISON, or an integer N meaning "raise transient N
# times then succeed" (N == FLAKE_FOREVER means never succeed).
FLAKE_SCHEDULE = {
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
SEND_EMAIL_DURATION_SECONDS = 3


def send_email(message: str) -> None:
    print(f"  send_email: {message} (taking {SEND_EMAIL_DURATION_SECONDS}s...)")
    time.sleep(SEND_EMAIL_DURATION_SECONDS)
    redis_client.incr(SEND_COUNT_KEY)


def _reset_send_count() -> None:
    redis_client.delete(SEND_COUNT_KEY)


def _read_send_count() -> int:
    raw = redis_client.get(SEND_COUNT_KEY)
    return int(raw) if raw else 0


def _reset_lock_contention_count() -> None:
    redis_client.delete(LOCK_CONTENTION_KEY)


def _read_lock_contention_count() -> int:
    raw = redis_client.get(LOCK_CONTENTION_KEY)
    return int(raw) if raw else 0


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="fetch_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@always_returns_envelope
@retryable(retriable_exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
def fetch_document(self, doc_id):
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


@app.task(name="parse_document", bind=True, acks_late=True, reject_on_worker_lost=True)
@always_returns_envelope
@retryable(retriable_exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
def parse_document(self, fetched):
    doc_id = fetched["doc_id"]
    # The counter increments on EVERY entry, regardless of outcome:
    # for doc1 it counts SIGKILL attempts (DLQ assertion); for doc2/3
    # it counts retryable attempts (recovery/exhaustion assertions).
    attempts = redis_client.incr(_attempts_key(doc_id))
    schedule = FLAKE_SCHEDULE.get(doc_id, 0)

    if schedule == POISON:
        print(
            f"  worker pid={os.getpid()}: poison crash on {doc_id} "
            f"(attempt {attempts}/{DELIVERY_LIMIT})"
        )
        os.kill(os.getpid(), signal.SIGKILL)

    if schedule == FLAKE_FOREVER or (
        isinstance(schedule, int) and attempts <= schedule
    ):
        raise TransientServiceError(f"503 from parser-svc on {doc_id}")

    print(f"  worker pid={os.getpid()}: parsed {doc_id} (attempt {attempts})")
    return {"doc_id": doc_id, "ok": True, "parsed": True, "attempts": attempts}


@app.task(
    name="notify",
    bind=True,
    max_retries=5,
    acks_late=True,
    reject_on_worker_lost=True,
)
def notify(self, results, pipeline_id):
    """Send the completion email at most once per pipeline_id.

    Not decorated with @retryable / @always_returns_envelope:
      - It has its own busy-retry mechanism (self.retry on lock
        contention) — layering @retryable would conflate the two.
      - The chord-body contract here is {sent: True/False, ...};
        @always_returns_envelope would return {ok: False, ...} on
        any failure, breaking the duplicate-detection assertions.
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
        redis_client.incr(LOCK_CONTENTION_KEY)
        print(
            f"  notify({pipeline_id}): lock held by another worker; "
            f"retrying in {NOTIFY_RETRY_DELAY_SECONDS}s"
        )
        raise self.retry(countdown=NOTIFY_RETRY_DELAY_SECONDS)

    # results carries a mix of envelope shapes from three paths:
    # normal completion, retryable-exhaustion envelope, DLQ-finalized
    # envelope. All three have an `ok` field.
    ok = [r for r in results if isinstance(r, dict) and r.get("ok")]
    failed = [r for r in results if isinstance(r, dict) and not r.get("ok")]

    send_email(
        f"Your pipeline documents are ready. "
        f"Id: {pipeline_id}. "
        f"Processed: {len(ok)}. "
        f"Failed: {len(failed)}."
    )
    redis_client.set(state_key, NOTIFY_STATE_SENT)

    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in ok:
        print(f"  ok:     {r.get('doc_id')}")
    for r in failed:
        print(f"  failed: {r.get('doc_id')}: {r.get('error')}")
    return _summary(results, pipeline_id, sent=True)


def _summary(results, pipeline_id, sent):
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
def drain_dlq():
    with app.connection_for_write() as conn:
        with conn.channel() as ch:
            bound_dlq = dead_letter_queue(ch)
            while True:
                msg = bound_dlq.get(no_ack=False)
                if msg is None:
                    return
                _finalize_dlq_message(msg)


def _finalize_dlq_message(msg):
    headers = msg.headers or {}
    task_id = headers.get("id")
    group_id = headers.get("group")
    group_index = headers.get("group_index")
    task_name = headers.get("task")

    try:
        args, _, embed = msg.payload
    except (ValueError, TypeError):
        print(f"drain_dlq: skipping non-v2 DLQ message (task_id={task_id!r})")
        msg.ack()
        return
    chord_sig = (embed or {}).get("chord")

    if not task_id or not chord_sig:
        print(f"drain_dlq: skipping non-chord DLQ message (task_id={task_id!r})")
        msg.ack()
        return

    context = Context()
    context.id = task_id
    context.group = group_id
    context.group_index = group_index
    context.chord = app.signature(chord_sig)
    context.task = task_name

    envelope = {
        "doc_id": _infer_doc_id_from_args(args),
        "ok": False,
        "error": "DLQ'd: x-delivery-limit exceeded",
        "task_id": task_id,
    }
    print(
        f"drain_dlq: finalizing chord-member {task_id} "
        f"(group={group_id}, task={task_name}) with envelope"
    )
    app.backend.mark_as_done(task_id, envelope, request=context)
    msg.ack()


def _infer_doc_id_from_args(args):
    try:
        first = args[0]
        if isinstance(first, dict):
            return first.get("doc_id")
    except (IndexError, TypeError):
        pass
    return None


app.conf.beat_schedule = {
    "drain-dlq": {
        "task": "drain_dlq",
        "schedule": schedule(DRAIN_INTERVAL_SECONDS),
    },
}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _reset(doc_ids):
    keys = [_attempts_key(d) for d in doc_ids]
    if keys:
        redis_client.delete(*keys)


def _read_attempts(doc_id: str) -> int:
    raw = redis_client.get(_attempts_key(doc_id))
    return int(raw) if raw else 0


def _expected_attempts(doc_id: str) -> int:
    """How many parse_document entries we expect for a given doc.

      POISON         → DELIVERY_LIMIT crashes (broker cap)
      FLAKE_FOREVER  → 1 initial + MAX_RETRIES retries
      N (int ≥ 0)    → N flakes then success = N + 1 calls
    """
    schedule = FLAKE_SCHEDULE.get(doc_id, 0)
    if schedule == POISON:
        return DELIVERY_LIMIT
    if schedule == FLAKE_FOREVER:
        return MAX_RETRIES + 1
    return schedule + 1


def print_all_task_results():
    """Scan the Redis backend for every `celery-task-meta-*` key and
    print task_id, state, task name, and result/error."""
    import json

    states = {}
    for key in redis_client.scan_iter(match="celery-task-meta-*"):
        raw = redis_client.get(key)
        if not raw:
            continue
        meta = json.loads(raw)
        task_id = meta.get("task_id") or key.decode().split("celery-task-meta-")[-1]
        state = meta.get("status", "UNKNOWN")
        name = meta.get("name") or "?"
        result = meta.get("result")
        states[state] = states.get(state, 0) + 1
        print(f"  [{state:<8}] {task_id}  task={name}  result={result!r}")

    summary = ", ".join(f"{s}={n}" for s, n in sorted(states.items()))
    print(f"backend totals: {summary or '(no task results found)'}")


def run_pipeline():
    docs = ["doc1", "doc2", "doc3"]
    pipeline_id = str(uuid.uuid4())
    state_key = _notify_state_key(pipeline_id)

    _reset(docs)
    _reset_send_count()
    _reset_lock_contention_count()
    redis_client.delete(state_key)

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    chord_result = pipeline.apply_async()
    print(f"chord submitted: id={chord_result.id} pipeline_id={pipeline_id}")

    # Lock-claim budget: max of (poison DLQ path ≈ 15-20s, retryable
    # exhaustion ≈ 1+2+4 backoff = ~7-10s, retryable recovery ≈ 3-5s).
    # All in parallel for headers, so ≈ 20s. 90s is comfortable.
    print("waiting for chord notify to claim the lock...")
    deadline = time.time() + 90
    while time.time() < deadline and not redis_client.exists(state_key):
        time.sleep(0.5)
    assert redis_client.exists(state_key), (
        "chord notify never claimed the lock within 90s"
    )

    print("--- triggering concurrent duplicate notify ---")
    duplicate_result = notify.delay([], pipeline_id=pipeline_id)

    print("waiting for both notifies to complete...")
    deadline = time.time() + 30
    while time.time() < deadline:
        if chord_result.ready() and duplicate_result.ready():
            break
        time.sleep(0.5)
    assert chord_result.ready() and duplicate_result.ready(), (
        "tasks did not finish within 30s"
    )

    first = chord_result.get(timeout=1)
    second = duplicate_result.get(timeout=1)
    print(f"chord notify result:     {first}")
    print(f"duplicate notify result: {second}")

    # FM-4: idempotency on notify.
    assert first["sent"] is True, "chord notify should have sent the email"
    assert second["sent"] is False, "duplicate should have skipped send_email"
    assert first["pipeline_id"] == pipeline_id
    sends = _read_send_count()
    contention = _read_lock_contention_count()
    print(f"send_email invocations:    {sends}")
    print(f"lock contention retries:   {contention}")
    assert sends == 1, f"send_email should run exactly once; got {sends}"
    assert contention >= 1, f"expected ≥1 lock-contention retry; got {contention}"

    # FM-5: the chord aggregated 1 success (doc2 recovered) and 2
    # failures (doc1 via DLQ, doc3 via retryable exhaustion).
    assert first["ok"] == 1, f"expected 1 ok (doc2 recovered); got {first['ok']}"
    assert first["failed"] == 2, (
        f"expected 2 failed (doc1 DLQ + doc3 exhausted); got {first['failed']}"
    )

    # Mechanical: parse_document was entered exactly the predicted
    # number of times per doc. The `attempts` field on each envelope
    # is task-reported and could lie; this counter is independent
    # state only the call site can increment.
    print("parse_document entries (from Redis):")
    for d in docs:
        actual = _read_attempts(d)
        expected = _expected_attempts(d)
        print(f"  {d}: {actual} (expected {expected}, schedule={FLAKE_SCHEDULE[d]})")
        # ±1 tolerance on the poison count — exact x-delivery-count
        # inclusive/exclusive semantics vary slightly between RabbitMQ
        # versions.
        if FLAKE_SCHEDULE[d] == POISON:
            assert expected <= actual <= expected + 1, (
                f"{d}: expected ~{expected} crashes; got {actual}"
            )
        else:
            assert actual == expected, (
                f"{d}: expected {expected} calls; got {actual}"
            )

    print(
        f"FM-5 fixed: doc2 recovered via retryable; "
        f"doc3 exhausted retries → envelope; doc1 → DLQ; "
        f"send_email idempotent (1 send across 2 notifies)."
    )


if __name__ == "__main__":
    run_pipeline()
