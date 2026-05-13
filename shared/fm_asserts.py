"""Shared FM assertion helpers.

Each function proves one failure-mode guarantee. Named so the call site
reads like a test: assert_fm3_poison_bounded_at_dlq(attempts, ...).

Each run_pipeline() ends with a cumulative block — FM-6's driver calls
assert_fm1 through assert_fm6 — making it obvious that every prior fix
still holds.

Assertions that take Result objects accept typed Result[NotifyPayload]
instances reconstructed from Celery's JSON result via Result.from_dict().
Integer/counter arguments are still plain ints (read from Redis before
calling).
"""

from __future__ import annotations

from typing import Any

from shared.result import NotifyPayload, Result


# ---------------------------------------------------------------------------
# FM-1
# ---------------------------------------------------------------------------

def assert_fm1_chord_body_fired(result: Result[NotifyPayload]) -> None:
    """FM-1: chord body fired and notify ran despite header task failures."""
    assert result.status == "SUCCESS", (
        f"FM-1 regression: chord body never fired — status={result.status!r}, "
        f"error={result.error!r}"
    )
    assert result.payload is not None, "FM-1 regression: notify payload is None"
    assert result.payload.final is True, "FM-1 regression: notify payload.final is not True"


# ---------------------------------------------------------------------------
# FM-2
# ---------------------------------------------------------------------------

def assert_fm2_redelivery_happened(
    doc1_attempts: int,
    doc2_attempts: int,
    *,
    min_doc1: int = 2,
) -> None:
    """FM-2: acks_late + reject_on_worker_lost caused at least one redelivery.

    min_doc1=2 holds for fm2 (crash + redelivery = exactly 2) and also for
    fm3+ where doc1 crashes DELIVERY_LIMIT times before the DLQ catches it.
    """
    assert doc1_attempts >= min_doc1, (
        f"FM-2 regression: doc1 should have been redelivered at least once "
        f"(expected >= {min_doc1}); got {doc1_attempts}"
    )
    assert doc2_attempts == 1, (
        f"FM-2 regression: doc2 should have run once; got {doc2_attempts}"
    )


# ---------------------------------------------------------------------------
# FM-3
# ---------------------------------------------------------------------------

def assert_fm3_poison_bounded_at_dlq(
    doc1_attempts: int,
    *,
    delivery_limit: int,
) -> None:
    """FM-3: poison message crash loop was capped by x-delivery-limit.

    ±1 tolerance: x-delivery-count inclusive/exclusive semantics vary
    slightly between RabbitMQ versions.
    """
    assert delivery_limit <= doc1_attempts <= delivery_limit + 1, (
        f"FM-3 regression: doc1 should have crashed ~{delivery_limit}x "
        f"before DLQ; got {doc1_attempts}"
    )


# ---------------------------------------------------------------------------
# FM-4
# ---------------------------------------------------------------------------

def assert_fm4_notify_idempotent(
    first: Result[NotifyPayload],
    second: Result[NotifyPayload],
    pipeline_id: str,
    sends: int,
    contention: int,
) -> None:
    """FM-4: exactly one send_email across duplicate notify fires; busy-retry exercised."""
    assert first.status == "SUCCESS", (
        f"FM-4 regression: chord notify result status={first.status!r}"
    )
    assert first.payload is not None, "FM-4 regression: chord notify payload is None"
    assert first.payload.sent is True, (
        "FM-4 regression: chord notify should have sent the email"
    )
    assert second.payload is not None, "FM-4 regression: duplicate notify payload is None"
    assert second.payload.sent is False, (
        "FM-4 regression: duplicate notify should have skipped send_email"
    )
    assert first.payload.pipeline_id == pipeline_id, (
        f"FM-4 regression: wrong pipeline_id: {first.payload.pipeline_id!r}"
    )
    assert sends == 1, (
        f"FM-4 regression: send_email should run exactly once; got {sends}"
    )
    assert contention >= 1, (
        f"FM-4 regression: expected ≥1 lock-contention retry; got {contention}"
    )


# ---------------------------------------------------------------------------
# FM-5
# ---------------------------------------------------------------------------

def assert_fm5_retryable_result(
    result: Result[NotifyPayload],
    *,
    expected_ok: int,
    expected_failed: int,
) -> None:
    """FM-5: chord aggregated the expected ok/failed header-result counts."""
    assert result.payload is not None, "FM-5 regression: notify payload is None"
    assert result.payload.ok == expected_ok, (
        f"FM-5 regression: expected {expected_ok} ok; got {result.payload.ok}"
    )
    assert result.payload.failed == expected_failed, (
        f"FM-5 regression: expected {expected_failed} failed; got {result.payload.failed}"
    )


def assert_fm5_doc_attempts(
    doc_id: str,
    actual: int,
    expected: int,
    *,
    is_poison: bool = False,
) -> None:
    """FM-5: parse_document was entered the predicted number of times per doc."""
    if is_poison:
        assert expected <= actual <= expected + 1, (
            f"FM-5 regression: {doc_id} expected ~{expected} crashes; got {actual}"
        )
    else:
        assert actual == expected, (
            f"FM-5 regression: {doc_id} expected {expected} calls; got {actual}"
        )


# ---------------------------------------------------------------------------
# FM-6
# ---------------------------------------------------------------------------

def assert_fm6_hang_envelopes(
    result: Result[NotifyPayload],
    *,
    expected_ok: int,
    expected_failed: int,
) -> None:
    """FM-6: hanging docs produced FAILURE envelopes — chord didn't stall."""
    assert result.payload is not None, "FM-6 regression: notify payload is None"
    assert result.payload.ok == expected_ok, (
        f"FM-6 regression: expected {expected_ok} ok; got {result.payload.ok}"
    )
    assert result.payload.failed == expected_failed, (
        f"FM-6 regression: expected {expected_failed} failed "
        f"(hanging docs → timeout envelopes); got {result.payload.failed}"
    )
