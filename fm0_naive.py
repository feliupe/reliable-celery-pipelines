"""Naive baseline demonstrating FM-1: pipeline dies mid-way on partial failure.


Run instructions
----------------
  # 1. start the broker
  docker-compose up -d

  # 2. start a worker (in one terminal)
  celery -A fm0_naive worker --loglevel=info

  # 3. run the script (in another terminal)
  python fm0_naive.py                 # failure case: FM-1 is demonstrated
  FAIL_PARSE=0 python fm0_naive.py    # happy path: proves the wiring works

"""

from __future__ import annotations

import os
import time

from celery import Celery, chord
from celery.exceptions import ChordError

app = Celery(
    "fm0_naive",
    broker="amqp://guest:guest@localhost:5672//",
    backend="redis://localhost:6379/0",
)


# Explicit task names so the client (running as __main__) and the worker
# (running with `-A fm0_naive`) agree on the task registry keys. Without
# `name=`, Celery auto-derives task names from the app's main module —
# which can be `__main__` for the client vs the module name for the
# worker. Explicit names sidestep the mismatch entirely.
@app.task(name="fetch_document")
def fetch_document(doc_id: str) -> dict:
    return {"doc_id": doc_id, "bytes": len(doc_id) * 100}


# parse_document raises unconditionally when FAIL_PARSE=1 (the default).
# Deterministic failure — not random — so the demo is reproducible. The
# raised exception propagates: no try/except, no retry, no errback. That
# is the whole point of FM-1.
@app.task(name="parse_document")
def parse_document(fetched: dict) -> dict:
    doc_id = fetched["doc_id"]

    raise RuntimeError(f"parser crashed on {doc_id}")
    return {"doc_id": doc_id, "parsed": True}


@app.task(name="notify")
def notify(results: list[dict]) -> dict:
    print(f"notify aggregated: {results}")
    return {"final": True, "results": results}


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
        print("Not ready.")
        if result.ready():
            print("Ready.")
            break
        time.sleep(0.5)

    if not result.ready():
        print(
            "FM-1: callback did not fire. pipeline is dead. "
            "no aggregation, no final state, no visibility."
        )
        return

    value = result.get(timeout=1, propagate=False)
    print(f"pipeline result: {value}")

    assert not isinstance(value, ChordError), "Something failed: 'notify' not called."


if __name__ == "__main__":
    run_pipeline()
