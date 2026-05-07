"""Fix for FM-2: a single transient blip from an external service no longer
kills the doc.

Technique: bounded retries with exponential backoff + jitter at the task
level. This is a *safety net* layered on top of the inner HTTP client's
own retries (real impl would use tenacity / urllib3 Retry — not modeled
here, just signified). The inner client absorbs short blips; this catches
the cases that outlast its budget (e.g. a 30s service degradation, a
broker reconnect that drops a single call).

Two reusable decorators carry the behavior:

  @retryable(retriable_exceptions=(...))   — innermost: catches the named
      transients, re-raises via self.retry with backoff+jitter, and after
      max_retries propagates the original exception.

  @always_returns_envelope                 — outermost (above @retryable):
      converts any escaping exception into a uniform `{ok: False, ...}`
      payload so the chord aggregator (FM-1 fix) always fires.

Order matters: @app.task → @always_returns_envelope → @retryable → body.
Swap envelope and retryable and you'll either eat Celery's Retry signal
(no retries happen) or break FM-1 again (chord dies on terminal failure).

Per-doc flake schedule (deterministic, so the demo is reproducible):
  doc1 flakes 2 times then succeeds        — retry recovers it
  doc2 flakes every time                   — retries exhaust, envelope returned

Run
---
  docker-compose up -d
  celery -A 2_transient_failure worker --loglevel=info --concurrency=1
  python 2_transient_failure.py
"""

import functools
import random
import time

import redis
from celery import Celery, chord
from celery.exceptions import MaxRetriesExceededError, Retry

REDIS_URL = "redis://localhost:6379/0"

app = Celery(
    "2_transient_failure",
    broker="amqp://guest:guest@localhost:5672//",
    # Redis backend (not SQLite) so we have a place to share the attempt
    # counter between worker and client. SQLite's result backend doesn't
    # expose a generic atomic-counter API.
    backend=REDIS_URL,
)

# Direct Redis client for fixture state. Could go through `redis_client`,
# but that couples our code to "the result backend happens to be Redis";
# redis-py directly is honest about the dependency.
redis_client = redis.Redis.from_url(REDIS_URL)

MAX_RETRIES = 3


class TransientServiceError(Exception):
    """Stand-in for 503 / connection-reset / read-timeout from the external
    parser service. In real code these are mapped from the HTTP client."""


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


def always_returns_envelope(func):
    """Convert any escaping exception into a `{ok: False, error: ...}`
    payload so chord aggregators see a uniform list of outcomes.

    Critically does NOT catch celery.exceptions.Retry — that's the signal
    self.retry() raises to schedule a retry, and Celery's framework needs
    to see it. Swallowing it would silently disable retries.

    Placement: must wrap the task body *outside* @retryable. retryable
    re-raises the original transient on exhaustion, and this decorator
    is what turns that into the envelope.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Retry:
            raise  # framework signal — let Celery schedule the retry
        except Exception as exc:
            # If the first arg is a dict (the typical chained-task shape),
            # carry its identifying fields through so downstream tasks can
            # still associate the failure with its input.
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

    Jitter matters under load: without it, a fleet of workers retrying the
    same downstream service synchronizes and re-DDoSes it the moment it
    recovers.

    Placement: innermost — directly above the task body. Anything raised
    by the body that isn't in retriable_exceptions passes straight through
    to @always_returns_envelope.
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
                        f"  retry {self.name} (attempt {self.request.retries + 1}): "
                        f"{exc}; backoff {countdown:.2f}s"
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
# Fault-injection harness (demo-only; not part of the fix)
# ---------------------------------------------------------------------------

FLAKE_SCHEDULE = {
    "doc1": 2,  # flakes twice, succeeds on attempt 3
    "doc2": -1,  # always flakes
}


def _count_key(doc_id: str) -> str:
    return f"calls:{doc_id}"


def _incr_calls(doc_id: str) -> int:
    """Atomic per-doc call counter living in the Celery result backend
    (Redis). Worker writes; client reads. Replaces an in-process dict that
    only worked under --concurrency=1 with an arbiter that also survives
    worker restarts and concurrent execution."""
    return redis_client.incr(_count_key(doc_id))


def _read_calls(doc_id: str) -> int:
    raw = redis_client.get(_count_key(doc_id))
    return int(raw) if raw else 0


def _reset_calls(doc_ids):
    keys = [_count_key(d) for d in doc_ids]
    if keys:
        redis_client.delete(*keys)


def _expected_calls(doc_id: str) -> int:
    """Derive how many times the parser service should have been called
    for a given doc, from FLAKE_SCHEDULE and MAX_RETRIES.

      flakes == -1   → exhausts retries: 1 initial + MAX_RETRIES retries
      flakes == N≥0  → flakes N times then succeeds: N + 1 total calls
    """
    flakes = FLAKE_SCHEDULE.get(doc_id, 0)
    return MAX_RETRIES + 1 if flakes == -1 else flakes + 1


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


# bind=True is required for any task wrapped by @always_returns_envelope or
# @retryable — both decorators read self.request / call self.retry.
@app.task(name="fetch_document", bind=True)
@always_returns_envelope
@retryable()
def fetch_document(self, doc_id):
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


# Decorator stack, top-down:
#   @app.task               — registers the fully-wrapped function as a Celery task
#   @always_returns_envelope— last line of defense; converts escaped exceptions
#   @retryable              — catches transients and reschedules with backoff
@app.task(name="parse_document", bind=True)
@always_returns_envelope
@retryable(retriable_exceptions=(TransientServiceError,), max_retries=MAX_RETRIES)
def parse_document(self, fetched):
    doc_id = fetched["doc_id"]
    flakes = FLAKE_SCHEDULE.get(doc_id, 0)
    calls = _incr_calls(doc_id)
    if flakes == -1 or calls <= flakes:
        raise TransientServiceError(f"503 from parser-svc on {doc_id}")
    return {
        "doc_id": doc_id,
        "ok": True,
        "parsed": True,
        "attempts": self.request.retries + 1,
    }


@app.task(name="notify", bind=True)
@always_returns_envelope
@retryable()
def notify(self, results):
    ok = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in ok:
        print(f"  ok:     {r['doc_id']} (after {r.get('attempts')} attempts)")
    for r in failed:
        print(f"  failed: {r['doc_id']}: {r['error']}")
    return {"final": True, "ok": len(ok), "failed": len(failed), "results": results}


def run_pipeline():
    docs = ["doc1", "doc2"]

    # Counters live in Redis and persist across runs; reset before each
    # invocation so assertions reflect this run's calls only.
    _reset_calls(docs)

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s())
    result = pipeline.apply_async()

    # Worst case: 3 retries * (up to 10s backoff + 1s jitter) ~= 33s per doc,
    # but retries run in parallel across docs, so 60s is comfortable.
    deadline = time.time() + 60
    while time.time() < deadline:
        if result.ready():
            break
        time.sleep(0.5)

    assert result.ready(), "chord body did not fire within 60s — FM-2 not fixed"
    value = result.get(timeout=1)
    print(f"pipeline result: {value}")
    assert "final" in value, "Notify task did not run."

    by_doc = {r["doc_id"]: r for r in value["results"]}
    assert by_doc["doc1"]["ok"], "doc1 should have recovered via retry"
    assert not by_doc["doc2"]["ok"], "doc2 should have exhausted retries gracefully"

    # Mechanical check: the parser service was called exactly the number
    # of times FLAKE_SCHEDULE + MAX_RETRIES predicts. The `attempts` field
    # in the envelope is task-reported and could lie; this counter is
    # independent state in Redis that only the call site can increment.
    print("call counts (from Redis):")
    for d in docs:
        actual = _read_calls(d)
        expected = _expected_calls(d)
        print(f"  {d}: {actual} (expected {expected})")
        assert actual == expected, (
            f"{d}: expected {expected} calls "
            f"(FLAKE_SCHEDULE={FLAKE_SCHEDULE[d]}, MAX_RETRIES={MAX_RETRIES}), "
            f"got {actual}"
        )

    print(
        "FM-2 fixed: transient retry recovered doc1; "
        "doc2 exhausted retries without breaking the chord."
    )


if __name__ == "__main__":
    run_pipeline()
