"""Fix for FM-1: chord callback fires even if header tasks fail.

Technique: never raise from a header task. Catch the exception, return a
failure record, and let the aggregator see a uniform list of outcomes —
each entry is either a success dict or `{"ok": False, "error": ...}`.
Raising is reserved for retryable transients (FM-2), not business
failures.

Run
---
  docker-compose up -d
  celery -A fm1_mid_pipeline_error worker --loglevel=info
  python fm1_mid_pipeline_error.py                  # FAIL_PARSE=1 (default): notify runs, reports failures
  FAIL_PARSE=0 python fm1_mid_pipeline_error.py     # happy path
"""

import os
import time

from celery import Celery, chord

app = Celery(
    "fm1_mid_pipeline_error",
    broker="amqp://guest:guest@localhost:5672//",
    backend="redis://localhost:6379/0",
)


@app.task(name="fetch_document")
def fetch_document(doc_id):
    return {"doc_id": doc_id, "ok": True, "bytes": len(doc_id) * 100}


@app.task(name="parse_document")
def parse_document(fetched):
    doc_id = fetched["doc_id"]
    try:
        if os.environ.get("FAIL_PARSE", "1") == "1":
            raise RuntimeError(f"parser crashed on {doc_id}")
        return {"doc_id": doc_id, "ok": True, "parsed": True}
    except Exception as e:
        return {"doc_id": doc_id, "ok": False, "error": str(e)}


@app.task(name="notify")
def notify(results):
    ok = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    print(f"notify: {len(ok)} ok, {len(failed)} failed")
    for r in failed:
        print(f"  failure: {r['doc_id']}: {r['error']}")
    return {"final": True, "ok": len(ok), "failed": len(failed), "results": results}


def run_pipeline():
    docs = ["doc1", "doc2"]
    header = [fetch_document.s(d) | parse_document.s() for d in docs]
    pipeline = chord(header, body=notify.s())
    result = pipeline.apply_async()

    deadline = time.time() + 10
    while time.time() < deadline:
        if result.ready():
            break
        time.sleep(0.5)

    assert result.ready(), "chord body did not fire within 10s — FM-1 not fixed"
    value = result.get(timeout=1)
    print(f"pipeline result: {value}")
    assert "final" in value, "Notify task did not run."
    print(f"Notify task run with 2 errors as expected.")


if __name__ == "__main__":
    run_pipeline()
