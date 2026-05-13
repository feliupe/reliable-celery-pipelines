"""Naive baseline — demonstrates FM-1: the chord callback never fires on
partial header failure.

No fixes applied here. parse_document raises unconditionally for doc1
(FLAKE_SCHEDULE["doc1"] = FAIL). Celery propagates the exception as a
FAILURE-state chord member; the chord coordinator never dispatches the
body; the pipeline stalls forever.

The FLAKE_SCHEDULE dispatch shape is introduced here so every later FM
can copy-paste this file and merely add new sentinels and new behavior
branches — no structural rewrites needed.

Run
---
  docker-compose up -d
  celery -A fm0_naive worker --loglevel=info
  python fm0_naive.py     # doc1 fails, chord stalls — FM-1 on display
"""

from __future__ import annotations

import time

from celery import Celery, chord
from celery.exceptions import ChordError

from shared.flake import (
    FAIL,
    FlakeEntry,
)  # FM-0: deterministic RuntimeError to demonstrate FM-1

app = Celery(
    "fm0_naive",
    broker="amqp://guest:guest@localhost:5672//",
    backend="redis://localhost:6379/0",
)

# ---------------------------------------------------------------------------
# Per-doc behavior schedule
# ---------------------------------------------------------------------------

# Each entry maps doc_id → FlakeEntry (a sentinel or int). parse_document
# reads this table and dispatches accordingly. Later FMs add new entries and
# new branches; existing entries never change.
FLAKE_SCHEDULE: dict[str, FlakeEntry] = {
    "doc1": FAIL,  # FM-0: raises RuntimeError — the bug FM-1 fixes
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


# Explicit task names so the client (running as __main__) and the worker
# (running with `-A fm0_naive`) agree on the task registry keys.
@app.task(name="fetch_document")
def fetch_document(doc_id: str) -> dict:
    return {"doc_id": doc_id, "bytes": len(doc_id) * 100}


@app.task(name="parse_document")
def parse_document(fetched: dict) -> dict:
    doc_id = fetched["doc_id"]
    flake = FLAKE_SCHEDULE.get(doc_id)

    # FM-0+: doc1 always raises — no @enveloped to catch it, so Celery
    # records a FAILURE-state result and the chord coordinator stalls.
    if flake is FAIL:
        raise RuntimeError(f"parser crashed on {doc_id}")

    return {"doc_id": doc_id, "parsed": True}


@app.task(name="notify")
def notify(results: list[dict]) -> dict:
    print(f"notify aggregated: {results}")
    return {"final": True, "results": results}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_pipeline() -> None:
    docs = ["doc1", "doc2"]
    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    print(
        "submitting chord: "
        "header=[fetch|parse(doc1), fetch|parse(doc2)], "
        "callback=notify"
    )
    pipeline = chord(header, body=notify.s())
    result = pipeline.apply_async()

    deadline = time.time() + 10
    while time.time() < deadline:
        if result.ready():
            break
        print("Not ready.")
        time.sleep(0.5)

    if not result.ready():
        print(
            "FM-1: callback did not fire. Pipeline is dead. "
            "No aggregation, no final state, no visibility."
        )
        return

    value = result.get(timeout=1, propagate=False)
    print(f"pipeline result: {value}")

    # Without @enveloped the exception surfaces as ChordError. This assert
    # documents the bug: the line below fails, proving FM-1 is un-fixed here.
    assert not isinstance(value, ChordError), (
        "ChordError reached the runner — notify was not called. "
        "This is the FM-1 failure mode; fm1_mid_pipeline_error.py fixes it."
    )


if __name__ == "__main__":
    run_pipeline()
