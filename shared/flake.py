"""Per-doc failure-mode sentinels shared by all FM files.

Each sentinel names a behavior injected into parse_document for a specific
doc_id. The FLAKE_SCHEDULE dict in each FM maps doc_id → FlakeEntry.

Sentinel introduction timeline:
  FAIL             FM-0  deterministic RuntimeError → proves FM-1 is needed
  CRASH_ONCE       FM-2  SIGKILL on attempt 1 only → proves FM-2 (redelivery)
  POISON           FM-3  SIGKILL always → proves FM-3 (DLQ cap + drain)
  FLAKE_FOREVER    FM-5  TransientServiceError always → proves FM-5 (exhaustion)
  SOFT_HANG        FM-6  sleep past soft limit → proves FM-6 (soft timeout)
  HARD_HANG_MANUAL FM-6  sleep, ignore soft, manual SIGALRM fires → FM-6 (hard timeout)

int N in FLAKE_SCHEDULE means "raise TransientServiceError N times then succeed"
(FM-5).
"""

from __future__ import annotations


class _Sentinel:
    """Identity-based marker. Using a class (not a string) makes every
    FLAKE_SCHEDULE value either a sentinel or an int, with no ambiguous overlap."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"<{self.name}>"


FAIL = _Sentinel("FAIL")
CRASH_ONCE = _Sentinel("CRASH_ONCE")
POISON = _Sentinel("POISON")
FLAKE_FOREVER = _Sentinel("FLAKE_FOREVER")
SOFT_HANG = _Sentinel("SOFT_HANG")
HARD_HANG_MANUAL = _Sentinel("HARD_HANG_MANUAL")

# Type alias for FLAKE_SCHEDULE values.
FlakeEntry = _Sentinel | int
