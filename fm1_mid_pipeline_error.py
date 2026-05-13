"""Fix for FM-1: chord callback fires even if header tasks fail.

Delta from fm0_naive.py
-----------------------
- bind=True + @enveloped on every task: escaping exceptions become
  Result(status="FAILURE") envelopes — the chord coordinator sees
  Celery state SUCCESS on all members and dispatches notify.


Run
---
  docker-compose up -d
  celery -A fm1_mid_pipeline_error worker --loglevel=info
  python fm1_mid_pipeline_error.py
"""

from __future__ import annotations

import uuid

from celery import Celery, chord

from shared.decorators import (
    enveloped,
)
from shared.flake import (
    FAIL,
    FlakeEntry,
)
from shared.fm_asserts import assert_fm1_chord_body_fired
from shared.result import FetchPayload, NotifyPayload, ParsePayload, Result
from shared.wait import wait_until

app = Celery(
    "fm1_mid_pipeline_error",
    broker="amqp://guest:guest@localhost:5672//",
    backend="redis://localhost:6379/0",
)

# ---------------------------------------------------------------------------
# Per-doc behavior schedule
# ---------------------------------------------------------------------------

FLAKE_SCHEDULE: dict[str, FlakeEntry] = {
    "doc1": FAIL,  # FM-0: raises RuntimeError — @enveloped converts it to a FAILURE envelope
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(name="fetch_document", bind=True)
@enveloped
def fetch_document(self, doc_id: str) -> FetchPayload:
    return FetchPayload(doc_id=doc_id, bytes=len(doc_id) * 100)


@app.task(name="parse_document", bind=True)
@enveloped
def parse_document(self, fetched: dict) -> ParsePayload:
    fetch_result = Result.from_dict(fetched, FetchPayload)
    doc_id = fetch_result.payload.doc_id if fetch_result.payload else "unknown"
    flake = FLAKE_SCHEDULE.get(doc_id)

    # FM-0+: @enveloped catches this RuntimeError and returns a FAILURE envelope
    # instead of letting it escape — the chord body fires regardless.
    if flake is FAIL:
        raise RuntimeError(f"parser crashed on {doc_id}")

    return ParsePayload(doc_id=doc_id, parsed=True)


@app.task(name="notify", bind=True)
@enveloped
def notify(self, results: list[dict], pipeline_id: str) -> NotifyPayload:
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


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


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
