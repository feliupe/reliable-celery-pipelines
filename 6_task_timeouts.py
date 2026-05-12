"""Fix for FM-6: a task that hangs (without crashing) no longer
consumes a worker slot indefinitely.

Layered on 5_transient_failures.py; the DLQ, idempotent notify,
acks_late survivability, and retry decorators are inherited
verbatim. Read those files first.

The gap FM-6 closes
-------------------
FM-2's acks_late + reject_on_worker_lost recovers a crashed
worker. FM-3 caps poison crashes via x-delivery-limit. FM-5
catches transient blips via @retryable. But none of those handle
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
decorator raises HardTimeoutExceeded, which @always_returns_envelope
catches and converts to a SUCCESS-state envelope. No Celery
on_timeout, no FAILURE chord-part write, no chord poisoning.

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
SoftTimeLimitExceeded propagates up through @retryable (not in
retriable_exceptions — see below) and is caught by
@always_returns_envelope, returning the same {ok: False, ...}
shape as a retry-exhausted task.

HardTimeoutExceeded (from @hard_timeout) takes the same envelope
path as SoftTimeLimitExceeded. doc5 below demonstrates a task
that ignores soft timeout — manual hard fires next, envelope
returned.

Why timeout exceptions are NOT in retriable_exceptions
------------------------------------------------------
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
  celery -A 6_task_timeouts worker --loglevel=info --concurrency=2 --beat
  python 6_task_timeouts.py
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
from celery.exceptions import MaxRetriesExceededError, Retry, SoftTimeLimitExceeded
from celery.schedules import schedule
from kombu import Exchange, Queue

REDIS_URL = "redis://localhost:6379/0"

app = Celery(
    "6_task_timeouts",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)


# ---------------------------------------------------------------------------
# Broker topology — see 3_dlq_reconciliation.py. Renamed fm5.* → fm6.*
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
app.conf.task_default_exchange = "fm6.pipeline"
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


# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------

# Demo values. Production: align soft to your p99 + slack; manual
# hard to ~2-3x soft (real ceiling); backstop far above both.
SOFT_TIMEOUT_SECONDS = 2  # Celery soft → envelope via @always_returns_envelope
MANUAL_HARD_TIMEOUT_SECONDS = 5  # @hard_timeout → envelope via @always_returns_envelope
BACKSTOP_HARD_TIMEOUT_SECONDS = 300  # Celery time_limit, never exercised in demo

# notify includes a deliberate 3s send_email sleep + the busy-retry
# countdown evaluation; its limits are independently sized.
NOTIFY_SOFT_TIMEOUT_SECONDS = 60
NOTIFY_HARD_TIMEOUT_SECONDS = 300

# Hang duration. Must exceed MANUAL_HARD so manual hard fires.
HANG_DURATION_SECONDS = 30


def _attempts_key(doc_id: str) -> str:
    return f"fm6:attempts:{doc_id}"


# ---------------------------------------------------------------------------
# Retry decorators (FM-5)
# ---------------------------------------------------------------------------


class TransientServiceError(Exception):
    """Stand-in for 503 / connection-reset / read-timeout from an external
    service."""


def always_returns_envelope(func):
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
                "error": f"{type(exc).__name__}: {exc}",
                "attempts": self.request.retries + 1,
            }

    return wrapper


def retryable(retriable_exceptions=(), max_retries=3, backoff_base=2, backoff_cap=10):
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


class HardTimeoutExceeded(Exception):
    """Raised by @hard_timeout when the per-task budget is exceeded."""


def hard_timeout(seconds):
    """Manual per-task hard timeout via signal.setitimer + SIGALRM.

    Bypasses Celery's time_limit because that path writes a
    FAILURE-state chord-part result via mark_as_failure before
    redelivery (celery/worker/request.py:521-538), poisoning the
    chord. By raising HardTimeoutExceeded from inside the task
    body we keep the exception inside the existing envelope path:
    @always_returns_envelope converts it to a SUCCESS-state
    envelope, the chord coordinator advances normally.

    SIGALRM is safe to use here: Celery uses SIGUSR1 for soft
    timeout and parent-side SIGKILL for hard timeout; SIGALRM is
    untouched in the worker child. Linux/Unix only.

    Innermost decorator — order: @app.task → @always_returns_envelope
    → @retryable → @hard_timeout → body. The alarm covers only the
    body, not decorator overhead.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            def _on_alarm(signum, frame):
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

POISON = "poison"  # SIGKILL the worker (FM-3 path)
FLAKE_FOREVER = -1  # always raise TransientServiceError (FM-5 envelope path)
SOFT_HANG = "soft_hang"  # hang past soft limit, envelope via @always_returns_envelope
HARD_HANG_MANUAL = (
    "hard_hang_manual"  # hang, ignore soft, manual @hard_timeout fires → envelope
)


FLAKE_SCHEDULE = {
    "doc1": POISON,
    "doc2": 2,
    "doc3": FLAKE_FOREVER,
    "doc4": SOFT_HANG,
    "doc5": HARD_HANG_MANUAL,
}


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


@app.task(
    name="fetch_document",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=SOFT_TIMEOUT_SECONDS,
    time_limit=BACKSTOP_HARD_TIMEOUT_SECONDS,
)
@always_returns_envelope
@retryable(retriable_exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
@hard_timeout(MANUAL_HARD_TIMEOUT_SECONDS)
def fetch_document(self, doc_id):
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


@app.task(
    name="parse_document",
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=SOFT_TIMEOUT_SECONDS,
    time_limit=BACKSTOP_HARD_TIMEOUT_SECONDS,
)
@always_returns_envelope
@retryable(retriable_exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
@hard_timeout(MANUAL_HARD_TIMEOUT_SECONDS)
def parse_document(self, fetched):
    doc_id = fetched["doc_id"]
    attempts = redis_client.incr(_attempts_key(doc_id))
    schedule = FLAKE_SCHEDULE.get(doc_id, 0)

    if schedule == POISON:
        print(
            f"  worker pid={os.getpid()}: poison crash on {doc_id} "
            f"(attempt {attempts}/{DELIVERY_LIMIT})"
        )
        os.kill(os.getpid(), signal.SIGKILL)

    if schedule == SOFT_HANG:
        # No try/except: SoftTimeLimitExceeded propagates up,
        # @retryable passes it through (not in retriable_exceptions),
        # @always_returns_envelope converts to {ok: False, ...}.
        print(f"  worker pid={os.getpid()}: hanging on {doc_id} (soft-honoring)")
        time.sleep(HANG_DURATION_SECONDS)

    if schedule == HARD_HANG_MANUAL:
        # Hang, catch + ignore the soft signal, let the manual
        # @hard_timeout fire next. HardTimeoutExceeded propagates up
        # through @retryable (not retriable) and is caught by
        # @always_returns_envelope → SUCCESS-state envelope. No
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
    soft_time_limit=NOTIFY_SOFT_TIMEOUT_SECONDS,
    time_limit=NOTIFY_HARD_TIMEOUT_SECONDS,
)
def notify(self, results, pipeline_id):
    """See 4_duplicated_runs.py for the idempotency contract. Not
    decorated — the chord-body return shape is {sent: True/False}
    and @always_returns_envelope would clobber that on any failure."""
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

    # results carries four envelope shapes: normal completion,
    # retryable-exhausted, soft-timeout, manual-hard-timeout, and
    # DLQ-finalized poison. All carry an `ok` field.
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
    """parse_document entries per doc.

    POISON                       → DELIVERY_LIMIT (broker cap)
    SOFT_HANG / HARD_HANG_MANUAL → 1 (no retry — timeout exceptions
                                     not in retriable_exceptions)
    FLAKE_FOREVER                → 1 + MAX_RETRIES
    N (int ≥ 0)                  → N + 1
    """
    schedule = FLAKE_SCHEDULE.get(doc_id, 0)
    if schedule == POISON:
        return DELIVERY_LIMIT
    if schedule in (SOFT_HANG, HARD_HANG_MANUAL):
        return 1
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
    docs = list(FLAKE_SCHEDULE.keys())
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

    # Lock-claim budget: with 5 docs at --concurrency=2, header work
    # serializes in pairs. Slowest path is POISON at
    # ~SOFT_TIMEOUT * DELIVERY_LIMIT ≈ 10-15s (redelivery loop).
    # HARD_HANG_MANUAL is bounded by MANUAL_HARD_TIMEOUT_SECONDS
    # (single attempt). Total header time ~15-25s. 120s is comfortable.
    print("waiting for chord notify to claim the lock...")
    deadline = time.time() + 120
    while time.time() < deadline and not redis_client.exists(state_key):
        time.sleep(0.5)
    assert redis_client.exists(
        state_key
    ), "chord notify never claimed the lock within 120s"

    print("--- triggering concurrent duplicate notify ---")
    duplicate_result = notify.delay([], pipeline_id=pipeline_id)

    print("waiting for both notifies to complete...")
    deadline = time.time() + 30
    while time.time() < deadline:
        if chord_result.ready() and duplicate_result.ready():
            break
        time.sleep(0.5)
    assert (
        chord_result.ready() and duplicate_result.ready()
    ), "tasks did not finish within 30s"

    first = chord_result.get(timeout=1)
    second = duplicate_result.get(timeout=1)
    print(f"chord notify result:     {first}")
    print(f"duplicate notify result: {second}")

    # FM-4 preserved: idempotency on notify.
    assert first["sent"] is True
    assert second["sent"] is False
    assert first["pipeline_id"] == pipeline_id
    sends = _read_send_count()
    contention = _read_lock_contention_count()
    print(f"send_email invocations:    {sends}")
    print(f"lock contention retries:   {contention}")
    assert sends == 1, f"send_email should run exactly once; got {sends}"
    assert contention >= 1, f"expected ≥1 lock-contention retry; got {contention}"

    # FM-6: 1 success (doc2), 4 envelopes (doc1 DLQ, doc3 retry-exhaust,
    # doc4 soft-timeout envelope, doc5 manual-hard-timeout envelope).
    assert first["ok"] == 1, f"expected 1 ok (doc2); got {first['ok']}"
    assert first["failed"] == 4, (
        f"expected 4 failed (doc1 DLQ, doc3 retry-exhaust, "
        f"doc4 soft-timeout, doc5 manual-hard-timeout); got {first['failed']}"
    )

    print("parse_document entries (from Redis):")
    for d in docs:
        actual = _read_attempts(d)
        expected = _expected_attempts(d)
        print(f"  {d}: {actual} (expected {expected}, schedule={FLAKE_SCHEDULE[d]})")
        if FLAKE_SCHEDULE[d] == POISON:
            # ±1 tolerance — exact x-delivery-count inclusive/exclusive
            # semantics vary slightly between RabbitMQ versions.
            assert (
                expected <= actual <= expected + 1
            ), f"{d}: expected ~{expected} attempts; got {actual}"
        else:
            assert (
                actual == expected
            ), f"{d}: expected {expected} attempts; got {actual}"

    print(
        f"FM-6 fixed: doc4 hang → soft timeout → envelope; "
        f"doc5 hang → manual hard timeout → envelope. "
        f"Tasks no longer pin workers indefinitely."
    )


if __name__ == "__main__":
    run_pipeline()
