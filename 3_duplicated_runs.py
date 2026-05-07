"""Fix for FM-3: notify is idempotent — duplicate executions don't double-send.

Why duplicates happen at all: with acks_late=True (the FM-5 fix) and
retries (the FM-2 fix), a worker that crashes between "did the work" and
"acked the message" will see the message redelivered. From Celery's
perspective the second execution is indistinguishable from the first;
your task body has to enforce idempotency itself.

Technique: optimistic lock keyed by pipeline_id in Redis. The lock value
itself encodes the send state.

  SET key 0 NX EX <lock_ttl>           → claim acquired (we will send)
  SETNX failed → GET key:
        0  another worker mid-send  → self.retry(countdown=10s)
        1  already sent            → no-op success envelope
  After send_email() →
        SET key 1                    → durable sent marker, no TTL

pipeline_id is passed explicitly to notify (cascaded as a chord-body
kwarg). It could equally come from self.request.group inside the body —
that's the chord's group_id — but explicit propagation makes the
contract obvious from the caller.

Run
---
  docker-compose up -d
  celery -A 3_duplicated_runs worker --loglevel=info --concurrency=2
  python 3_duplicated_runs.py

Concurrency must be ≥ 2 so a second notify can land while the first is
still mid-send (the demo asserts the busy-retry branch actually fires).
With --concurrency=1 the second notify is queued behind the first and
only ever sees state=SENT, never NOT_SENT.
"""

import functools
import random
import time
import uuid

import redis
from celery import Celery, chord
from celery.exceptions import MaxRetriesExceededError, Retry

REDIS_URL = "redis://localhost:6379/0"

app = Celery(
    "3_duplicated_runs",
    broker="amqp://guest:guest@localhost:5672//",
    backend=REDIS_URL,
)

# Direct Redis client for our own state (lock + counters). Going through
# Celery's backend abstraction (`app.backend.client`) works but tightly
# couples our code to "the result backend happens to be Redis"; using
# redis-py directly is honest about the dependency.
redis_client = redis.Redis.from_url(REDIS_URL)


# ---------------------------------------------------------------------------
# Decorators (carried over from FM-2)
# ---------------------------------------------------------------------------


def always_returns_envelope(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Retry:
            raise  # framework signal — let Celery schedule the retry
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
                    raise self.retry(
                        exc=exc, countdown=countdown, max_retries=max_retries
                    )
                except MaxRetriesExceededError:
                    raise exc

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Idempotency machinery
# ---------------------------------------------------------------------------

NOTIFY_STATE_NOT_SENT = b"0"
NOTIFY_STATE_SENT = b"1"
# Lock TTL bounds a crashed worker's claim — short, so another worker
# can take over after a SIGKILL between SETNX and the state-flip.
# The SENT marker has no TTL: once we've sent, that fact is permanent.
# Trade-off: SENT keys (value=1) accumulate forever — production code
# would pair this with a separate sweeper job. Crash-claim keys (value=0)
# self-clean via the lock TTL.
NOTIFY_LOCK_TTL_SECONDS = 600
NOTIFY_RETRY_DELAY_SECONDS = 10  # spec'd by the task description


def _notify_state_key(pipeline_id: str) -> str:
    return f"notify:state:{pipeline_id}"


# ---------------------------------------------------------------------------
# Side-effect mock + assertion harness
# ---------------------------------------------------------------------------

SEND_COUNT_KEY = "send_email:count"
LOCK_CONTENTION_KEY = "notify:lock_contention_count"

# Real email APIs take 1–3s on a healthy day, longer when degraded. We
# model that here so the lock is genuinely held while a concurrent
# invocation arrives — exercising the busy-retry branch.
SEND_EMAIL_DURATION_SECONDS = 3


def send_email(message: str) -> None:
    """Stand-in for an email API call (Mailgun / SES / SendGrid). Bumps a
    Redis counter so the demo can assert exactly-once delivery across the
    duplicate notify execution. Sleeps to model API latency."""
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


@app.task(name="fetch_document", bind=True)
@always_returns_envelope
@retryable()
def fetch_document(self, doc_id):
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


@app.task(name="parse_document", bind=True)
@always_returns_envelope
@retryable()
def parse_document(self, fetched):
    # FM-3 demo doesn't exercise parse-side failures; kept as a no-op
    # success so the chord body has realistic input.
    return {"doc_id": fetched["doc_id"], "ok": True, "parsed": True}


@app.task(name="notify", bind=True, max_retries=5)
@always_returns_envelope
@retryable()
def notify(self, results, pipeline_id):
    """Aggregate the chord results and send a single completion email,
    even if this task body is executed more than once for the same
    pipeline_id (e.g. broker redelivery after a worker crash).
    """
    state_key = _notify_state_key(pipeline_id)

    # SETNX-style atomic claim. nx=True means "only set if key absent".
    # redis-py returns True if we won the race, None if some other worker
    # already claimed (or sent). `not claimed` works for both falsy cases.
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
        # state == NOTIFY_STATE_NOT_SENT: another worker is mid-send.
        # Back off and try again — by then it'll either be SENT (skip) or
        # the TTL will have expired (we get to claim).
        redis_client.incr(LOCK_CONTENTION_KEY)
        print(
            f"  notify({pipeline_id}): lock held by another worker; "
            f"retrying in {NOTIFY_RETRY_DELAY_SECONDS}s"
        )
        raise self.retry(countdown=NOTIFY_RETRY_DELAY_SECONDS)

    ok = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]

    # NOTE: this guard isn't fully transactional. The email side effect
    # and the state-flip below are separate operations — a worker crash
    # between send_email() and SET leaves state=0, so a redelivery will
    # resend. End-to-end exactly-once requires the email API itself to
    # honor an idempotency key.
    send_email(
        f"Your pipeline documents are ready. "
        f"Id: {pipeline_id}. "
        f"Processed: {len(ok)}. "
        f"Failed: {len(failed)}."
    )
    # SET (not INCR) and no TTL: once we've sent, that fact is permanent.
    # INCR would keep the original lock TTL, so the marker would age out
    # and a late redelivery could resend. SET without `ex` clears the TTL.
    redis_client.set(state_key, NOTIFY_STATE_SENT)

    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in ok:
        print(f"  ok:     {r['doc_id']}")
    for r in failed:
        print(f"  failed: {r['doc_id']}: {r['error']}")
    return _summary(results, pipeline_id, sent=True)


def _summary(results, pipeline_id, sent):
    ok = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    return {
        "final": True,
        "pipeline_id": pipeline_id,
        "sent": sent,
        "ok": len(ok),
        "failed": len(failed),
        "results": results,
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def run_pipeline():
    docs = ["doc1", "doc2"]
    pipeline_id = str(uuid.uuid4())
    state_key = _notify_state_key(pipeline_id)

    # Reset cross-run state (Redis persists across script invocations).
    _reset_send_count()
    _reset_lock_contention_count()
    redis_client.delete(state_key)

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    chord_result = pipeline.apply_async()

    # Wait for the chord's notify to claim the lock. Polling Redis is
    # more reliable than a fixed sleep:
    #   - Too early (duplicate fires before the chord's notify): the
    #     duplicate wins SETNX, sends the email itself, and the chord's
    #     notify ends up on the busy-retry → skip path. The assertion
    #     `first["sent"] is True` then fails.
    #   - Too late (first has already flipped to SENT): the duplicate
    #     hits the fast skip path, never the busy-retry branch, and the
    #     contention assertion fails.
    print("waiting for chord notify to claim the lock...")
    deadline = time.time() + 15
    while time.time() < deadline and not redis_client.exists(state_key):
        time.sleep(0.1)
    assert redis_client.exists(state_key), "chord notify never claimed the lock"

    # Fire a duplicate notify with the same pipeline_id. With the chord's
    # notify mid-send (sleeping inside send_email), this duplicate should
    # see state=NOT_SENT, increment lock_contention_count, retry in 10s.
    # By the time the retry runs, state=SENT and it skips.
    print("--- triggering concurrent duplicate notify ---")
    duplicate_result = notify.delay([], pipeline_id=pipeline_id)

    # Worst-case timeline: SEND_EMAIL_DURATION + NOTIFY_RETRY_DELAY + slack.
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

    assert first["sent"] is True, "chord notify should have sent the email"
    assert second["sent"] is False, "duplicate should have skipped send_email"

    sends = _read_send_count()
    contention = _read_lock_contention_count()
    print(f"send_email invocations:    {sends}")
    print(f"lock contention retries:   {contention}")
    assert (
        sends == 1
    ), f"send_email should have run exactly once across both notifies, got {sends}"
    # Proves the busy-retry branch actually fired. Without this, the demo
    # would still pass even if the duplicate fast-pathed straight to SENT.
    assert contention >= 1, (
        f"expected ≥1 lock-contention retry (the duplicate should have hit "
        f"the busy branch while the first was sleeping), got {contention}"
    )

    print("FM-3 fixed: send_email idempotent + busy-retry path exercised.")


if __name__ == "__main__":
    run_pipeline()
