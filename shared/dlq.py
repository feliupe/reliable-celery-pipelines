"""DLQ drain helpers shared by fm3..fm6.

Each FM file keeps its own @app.task(name="drain_dlq") shim that calls
drain_dlq_messages(app, dead_letter_queue) — the app instance and queue
object are per-file, so they can't be centralised here.

Why mark_as_done (SUCCESS) rather than mark_as_failure
-------------------------------------------------------
A chord member written with state=FAILURE causes _unpack_chord_result
(celery/backends/redis.py) to raise ChordError when collecting results,
sending the failure to the body's link_error rather than firing the body.
Writing SUCCESS with a Result(status="FAILURE") envelope lets the chord
coordinator advance normally and delivers the failure detail to notify,
which inspects result.status rather than the Celery task state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shared.result import FetchPayload, Result

if TYPE_CHECKING:
    from celery import Celery
    from kombu import Queue


def drain_dlq_messages(app: Celery, dead_letter_queue: Queue) -> None:
    """Pull every message currently in the DLQ and write a SUCCESS-state
    envelope to the result backend for each, advancing the chord coordinator.
    """
    with app.connection_for_write() as conn:
        with conn.channel() as ch:
            bound_dlq = dead_letter_queue(ch)
            while True:
                msg = bound_dlq.get(no_ack=False)
                if msg is None:
                    return
                _finalize_dlq_message(app, msg)


def _finalize_dlq_message(app: Celery, msg: Any) -> None:
    """Extract chord context from a DLQ'd task message and write a
    SUCCESS-state failure envelope so the chord coordinator advances.

    Celery protocol v2 splits task metadata across two places:
      - AMQP headers carry id, task, group, group_index, etc.
      - The message body is a tuple (args, kwargs, embed), where
        embed holds {callbacks, errbacks, chain, chord}.
    RabbitMQ's DLX preserves both verbatim when dead-lettering.
    """
    from celery.app.task import Context

    headers = msg.headers or {}
    task_id = headers.get("id")
    group_id = headers.get("group")
    group_index = headers.get("group_index")
    task_name = headers.get("task")

    try:
        args, _, embed = msg.payload
    except (ValueError, TypeError):
        print(f"drain_dlq: skipping non-v2 DLQ message (task_id={task_id!r})")
        msg.ack()
        return
    chord_sig = (embed or {}).get("chord")

    if not task_id or not chord_sig:
        print(f"drain_dlq: skipping non-chord DLQ message (task_id={task_id!r})")
        msg.ack()
        return

    context = Context()
    context.id = task_id
    context.group = group_id
    context.group_index = group_index
    context.chord = app.signature(chord_sig)
    context.task = task_name

    doc_id = _infer_doc_id_from_args(args)
    context = FetchPayload(doc_id=doc_id, bytes=0) if doc_id else None
    envelope = Result.failure(
        "DLQ'd: x-delivery-limit exceeded",
        # attempts is None — the broker counted redeliveries, not Python retries
        attempts=None,
        context=context,
    ).to_celery_dict()
    # Embed task_id in the payload dict for downstream observability
    if envelope["payload"] is None:
        envelope["payload"] = {}
    envelope["payload"]["task_id"] = task_id
    print(
        f"drain_dlq: finalizing chord-member {task_id} "
        f"(group={group_id}, task={task_name}) with envelope"
    )
    app.backend.mark_as_done(task_id, envelope, request=context)
    msg.ack()


def _infer_doc_id_from_args(args: Any) -> str | None:
    """Best-effort: extract doc_id from the task's positional args.

    After the envelope migration, parse_document receives a serialized
    Result[FetchPayload] dict as its first arg, so args[0] looks like
    {"status": "SUCCESS", "payload": {"doc_id": ..., "bytes": ...}, ...}.
    """
    try:
        first = args[0]
        if not isinstance(first, dict):
            return None
        if "status" in first and "payload" in first:
            payload = first.get("payload") or {}
            return payload.get("doc_id")
        return first.get("doc_id")
    except (IndexError, TypeError):
        pass
    return None
