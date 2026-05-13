"""Fix for FM-1: chord callback fires even if header tasks fail.

Technique: use @enveloped on every header task. The decorator catches any
escaping exception and returns a typed Result[T](status="FAILURE") envelope
instead of raising — so the chord coordinator always sees a SUCCESS Celery
state, regardless of whether the task succeeded or failed at the domain level.

The chord body (notify) receives a uniform list of Result[ParsePayload] dicts
and filters by result.status to separate successes from failures.

This replaces the previous manual try/except approach with a decorator-based
pattern that is consistent across all FMs and composes cleanly with the retry
machinery added in FM-5.

Run
---
  docker-compose up -d
  celery -A fm1_mid_pipeline_error worker --loglevel=info
  python fm1_mid_pipeline_error.py                  # FAIL_PARSE=1 (default): notify runs, reports failures
  FAIL_PARSE=0 python fm1_mid_pipeline_error.py     # happy path
"""

from __future__ import annotations

import os
import uuid

from celery import Celery, chord

from shared.decorators import enveloped
from shared.fm_asserts import assert_fm1_chord_body_fired
from shared.result import FetchPayload, NotifyPayload, ParsePayload, Result
from shared.wait import wait_until

app = Celery(
    "fm1_mid_pipeline_error",
    broker="amqp://guest:guest@localhost:5672//",
    backend="redis://localhost:6379/0",
)


@app.task(name="fetch_document", bind=True)
@enveloped
def fetch_document(self, doc_id: str) -> FetchPayload:
    return FetchPayload(doc_id=doc_id, bytes=len(doc_id) * 100)


@app.task(name="parse_document", bind=True)
@enveloped
def parse_document(self, fetched: dict) -> ParsePayload:
    """Intentionally crashes when FAIL_PARSE=1 (the default).

    @enveloped catches the RuntimeError and returns a FAILURE envelope —
    the chord body fires regardless, proving FM-1 is fixed.
    """
    fetch_result = Result.from_dict(fetched, FetchPayload)
    doc_id = fetch_result.payload.doc_id if fetch_result.payload else "unknown"
    if os.environ.get("FAIL_PARSE", "1") == "1":
        raise RuntimeError(f"parser crashed on {doc_id}")
    return ParsePayload(doc_id=doc_id, parsed=True)


@app.task(name="notify", bind=True)
@enveloped
def notify(self, results: list[dict], pipeline_id: str) -> NotifyPayload:
    """Cast header results to typed objects at the boundary."""
    typed: list[Result[ParsePayload]] = [
        Result.from_dict(r, ParsePayload) for r in results
    ]
    ok = [r for r in typed if r.status == "SUCCESS"]
    failed = [r for r in typed if r.status == "FAILURE"]
    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in failed:
        doc_id = r.payload.doc_id if r.payload else "?"
        print(f"  failure: {doc_id}: {r.error}")
    return NotifyPayload(
        final=True,
        pipeline_id=pipeline_id,
        ok=len(ok),
        failed=len(failed),
    )


def run_pipeline() -> None:
    docs = ["doc1", "doc2"]
    pipeline_id = str(uuid.uuid4())

    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s(pipeline_id=pipeline_id))
    result = pipeline.apply_async()

    wait_until(
        result.ready,
        timeout=10,
        message="chord body did not fire within 10s — FM-1 not fixed",
    )
    raw = result.get(timeout=1)
    print(f"pipeline result: {raw}")

    notify_result = Result.from_dict(raw, NotifyPayload)

    assert assert_fm1_chord_body_fired(notify_result)
    print("FM-1 fixed: notify ran despite header task failures.")


if __name__ == "__main__":
    run_pipeline()
