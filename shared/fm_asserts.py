"""Shared FM assertion helpers.

Each function proves one failure-mode guarantee. Named so the call
site reads like a test: assert_fm3_poison_bounded_at_dlq(attempts, ...).

Each run_pipeline() ends with a cumulative block — FM-5's driver calls
assert_fm1, assert_fm3, assert_fm4, and assert_fm5 — making it obvious
that every prior fix still holds.

All functions accept plain values (ints, dicts) rather than redis clients
so they stay pure and easy to test in isolation.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# FM-1
# ---------------------------------------------------------------------------

def assert_fm1_chord_body_fired(value: dict) -> None:
    """FM-1: chord body fired and notify ran despite header task failures."""
    assert "final" in value, f"FM-1 regression: chord body never fired — {value!r}"


# ---------------------------------------------------------------------------
# FM-2
# ---------------------------------------------------------------------------

def assert_fm2_redelivery_happened(
    doc1_attempts: int,
    doc2_attempts: int,
    *,
    expected_doc1: int = 2,
) -> None:
    """FM-2: SIGKILL'd task was redelivered and re-executed exactly once more."""
    assert doc1_attempts == expected_doc1, (
        f"FM-2 regression: doc1 should have run {expected_doc1}x "
        f"(crash + redelivery); got {doc1_attempts}"
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

    ±1 tolerance because x-delivery-count inclusive/exclusive semantics
    vary slightly between RabbitMQ versions.
    """
    assert delivery_limit <= doc1_attempts <= delivery_limit + 1, (
        f"FM-3 regression: doc1 should have crashed ~{delivery_limit}x "
        f"before DLQ; got {doc1_attempts}"
    )


# ---------------------------------------------------------------------------
# FM-4
# ---------------------------------------------------------------------------

def assert_fm4_notify_idempotent(
    first: dict,
    second: dict,
    pipeline_id: str,
    sends: int,
    contention: int,
) -> None:
    """FM-4: exactly one send_email across duplicate notify fires; busy-retry exercised."""
    assert first["sent"] is True, (
        "FM-4 regression: chord notify should have sent the email"
    )
    assert second["sent"] is False, (
        "FM-4 regression: duplicate notify should have skipped send_email"
    )
    assert first["pipeline_id"] == pipeline_id, (
        f"FM-4 regression: wrong pipeline_id in result: {first['pipeline_id']!r}"
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
    result: dict,
    *,
    expected_ok: int,
    expected_failed: int,
) -> None:
    """FM-5: chord aggregated the expected ok/failed envelope counts."""
    assert result["ok"] == expected_ok, (
        f"FM-5 regression: expected {expected_ok} ok; got {result['ok']}"
    )
    assert result["failed"] == expected_failed, (
        f"FM-5 regression: expected {expected_failed} failed; got {result['failed']}"
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
    result: dict,
    *,
    expected_ok: int,
    expected_failed: int,
) -> None:
    """FM-6: hanging docs produced envelopes — no chord stall, no chord poisoning."""
    assert result["ok"] == expected_ok, (
        f"FM-6 regression: expected {expected_ok} ok (recovered docs); "
        f"got {result['ok']}"
    )
    assert result["failed"] == expected_failed, (
        f"FM-6 regression: expected {expected_failed} failed "
        f"(hanging docs → timeout envelopes); got {result['failed']}"
    )
