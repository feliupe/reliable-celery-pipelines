"""Poll-until-ready helper for FM run_pipeline() runners."""

from __future__ import annotations

import time
from collections.abc import Callable


def wait_until(
    predicate: Callable[[], bool],
    timeout: float,
    interval: float = 0.5,
    *,
    message: str,
) -> None:
    """Poll `predicate` until it returns truthy or `timeout` seconds elapse.

    Raises AssertionError(message) on timeout — used in run_pipeline()
    runners to fail loudly when the chord body, lock claim, or duplicate
    notify doesn't land in time.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)
