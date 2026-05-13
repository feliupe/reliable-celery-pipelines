"""Task decorators shared by all FM files.

Two decorators — one per concern:

  @enveloped
      Wraps a task body's return value in Result[T].to_celery_dict().
      Catches any escaping exception (except celery.exceptions.Retry)
      and returns a FAILURE envelope instead of raising.
      Requires bind=True on the Celery task (needs self.request.retries).

  @transient_retryable(exceptions=..., max_retries=..., ...)
      Inner decorator (closer to body). Retries the named exception
      classes with exponential backoff + jitter. On exhaustion, re-raises
      the original exception so @enveloped (outer) can convert it to a
      FAILURE envelope.

Stacking for fm5..fm6:
    @app.task(bind=True, ...)
    @enveloped
    @transient_retryable(exceptions=(TransientServiceError,), max_retries=3)
    @hard_timeout(seconds)         # fm6 only, stays inline
    def body(self, ...): ...

Stacking for fm1..fm4 (no retry):
    @app.task(bind=True, ...)
    @enveloped
    def body(self, ...): ...

Why @enveloped must be outer:
    @transient_retryable re-raises on exhaustion; @enveloped catches that
    re-raise and returns a FAILURE envelope. Swap the order and you would
    either eat the Retry signal (silently disabling retries) or let
    exceptions escape the chord member entirely (breaking FM-1).
"""

from __future__ import annotations

import functools
import random
from collections.abc import Callable
from typing import Any

from celery.exceptions import MaxRetriesExceededError, Retry

from shared.result import FetchPayload, Result

# ---------------------------------------------------------------------------
# @enveloped
# ---------------------------------------------------------------------------


def enveloped(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a task body's return value in a standardised Result[T] envelope.

    Success path → Result.success(payload, attempts).to_celery_dict()
    Failure path → Result.failure(str(exc), attempts, context).to_celery_dict()

    Retry signal is NEVER caught — it must propagate to Celery's framework.
    """

    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        attempts = self.request.retries + 1
        try:
            payload = func(self, *args, **kwargs)
            return Result.success(payload, attempts=attempts).to_celery_dict()
        except Retry:
            raise
        except Exception as exc:
            return Result.failure(
                str(exc), attempts=attempts, payload={}
            ).to_celery_dict()

    return wrapper


# ---------------------------------------------------------------------------
# @transient_retryable
# ---------------------------------------------------------------------------


def transient_retryable(
    exceptions: tuple[type[BaseException], ...] = (),
    max_retries: int = 3,
    backoff_base: int = 2,
    backoff_cap: int = 10,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Retry the named exceptions with exponential backoff + jitter.

    On exhaustion, re-raises the original exception so @enveloped (outer)
    can convert it to a FAILURE envelope. Non-listed exceptions pass through.

    Jitter prevents a fleet of workers from synchronizing retries and
    hammering a recovering downstream service.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return func(self, *args, **kwargs)
            except exceptions as exc:
                try:
                    countdown = min(
                        backoff_base**self.request.retries, backoff_cap
                    ) + random.uniform(0, 1)
                    print(
                        f"  retry {self.name} (attempt "
                        f"{self.request.retries + 1}): {exc}; "
                        f"backoff {countdown:.2f}s"
                    )
                    raise self.retry(
                        exc=exc, countdown=countdown, max_retries=max_retries
                    )
                except MaxRetriesExceededError:
                    print(f"  {self.name} retries exhausted: {exc}")
                    raise exc

        return wrapper

    return decorator
