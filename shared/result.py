"""Typed result envelope for all pipeline tasks.

Every task decorated with @enveloped returns a serialized Result[T] dict.
At the boundary (notify body, run_pipeline driver), reconstruct typed objects
with Result.from_dict(raw, PayloadType).

Celery uses JSON serialization by default — dataclasses are not directly
JSON-serializable. The decorator calls result.to_celery_dict() which returns
a plain dict. Type hints on notify signatures and run_pipeline locals are
for static analysis; runtime values are always dicts until reconstructed.
"""

import dataclasses
from dataclasses import dataclass, asdict
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

T = TypeVar("T")
_U = TypeVar("_U")  # free TypeVar for from_dict — T is bound to the class's Generic[T]


@dataclass
class Result(Generic[T]):
    """Standard task result envelope.

    status:   "SUCCESS" or "FAILURE" — never a boolean
    payload:  domain dataclass on SUCCESS; partial context (e.g. FetchPayload
              with doc_id) on FAILURE if available, otherwise None
    error:    None on SUCCESS; exception message string on FAILURE
    attempts: number of task executions including Celery retries;
              None when unknown (e.g. after a SIGKILL / broker redelivery)
    """

    status: Literal["SUCCESS", "FAILURE"]
    payload: T | None
    error: str | None
    attempts: int | None

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def success(cls, payload: T, *, attempts: int) -> "Result[T]":
        return cls(status="SUCCESS", payload=payload, error=None, attempts=attempts)

    @classmethod
    def failure(
        cls,
        error: str,
        *,
        attempts: int | None,
        context: T | None = None,
    ) -> "Result[T]":
        return cls(status="FAILURE", payload=context, error=error, attempts=attempts)

    # ------------------------------------------------------------------
    # Celery serialization
    # ------------------------------------------------------------------

    def to_celery_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict for Celery's JSON result backend."""
        return {
            "status": self.status,
            "payload": (
                asdict(self.payload)  # type: ignore[arg-type]
                if self.payload is not None and dataclasses.is_dataclass(self.payload)
                else self.payload
            ),
            "error": self.error,
            "attempts": self.attempts,
        }

    @staticmethod
    def from_dict(d: dict[str, Any], payload_cls: type[_U]) -> "Result[_U]":
        """Reconstruct a typed Result from a Celery result dict.

        Usage in notify / run_pipeline:
            typed = Result.from_dict(raw_dict, ParsePayload)
        """
        raw_payload = d.get("payload")
        if raw_payload is not None and isinstance(raw_payload, dict):
            try:
                payload: _U | None = payload_cls(**raw_payload)
            except (TypeError, KeyError):
                payload = None
        else:
            payload = None
        raw_status = d["status"]
        if raw_status not in ("SUCCESS", "FAILURE"):
            raise ValueError(f"unexpected status: {raw_status!r}")
        status: Literal["SUCCESS", "FAILURE"] = raw_status
        return Result(
            status=status,
            payload=payload,
            error=d.get("error"),
            attempts=d.get("attempts"),
        )


if TYPE_CHECKING:
    @dataclass
    class SuccessResult(Result[T]):  # type: ignore[misc]
        """Result[T] narrowed to SUCCESS: payload is guaranteed non-None."""
        payload: T  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Domain payload dataclasses
# ---------------------------------------------------------------------------


@dataclass
class FetchPayload:
    """Return type of fetch_document task body."""

    doc_id: str
    bytes: int


@dataclass
class ParsePayload:
    """Return type of parse_document task body."""

    doc_id: str
    parsed: bool = False
    attempts: int = 1


@dataclass
class NotifyPayload:
    """Return type of notify task body — shared by ALL FMs.

    Fields:
      final:       always True; signals the chord body ran
      pipeline_id: unique run identifier, generated in run_pipeline()
      ok:          count of SUCCESS header results
      failed:      count of FAILURE header results
      sent:        whether send_email was called; True by default for
                   fm1..fm3 which have no duplicate-detection logic
    """

    final: bool
    pipeline_id: str
    ok: int
    failed: int
    sent: bool = True
