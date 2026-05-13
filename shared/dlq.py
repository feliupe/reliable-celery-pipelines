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

from kombu import Exchange, Queue

from shared.result import FetchPayload, Result

if TYPE_CHECKING:
    from celery import Celery
    from kombu import Message


def declare_dlq(app: "Celery", namespace: str, delivery_limit: int = 3) -> tuple[Queue, int]:
    """Build DLX/DLQ/pipeline topology, configure app, and declare on the broker.

    Returns (dead_letter_queue, delivery_limit) for use in drain_dlq and run_pipeline.
    """
    dlx_exchange = Exchange(f"{namespace}.dlx", type="direct", durable=True)
    dlq = Queue(
        f"{namespace}.dead_letters",
        exchange=dlx_exchange,
        routing_key="dead",
        durable=True,
        queue_arguments={"x-queue-type": "quorum"},
    )
    pipeline_exchange = Exchange(f"{namespace}.pipeline", type="direct", durable=True)
    pipeline_queue = Queue(
        f"{namespace}.pipeline",
        exchange=pipeline_exchange,
        routing_key="pipeline",
        durable=True,
        queue_arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": f"{namespace}.dlx",
            "x-dead-letter-routing-key": "dead",
            "x-delivery-limit": delivery_limit,
        },
    )

    # The DLQ is NOT a task queue — if it were, the worker would consume the
    # poison message from the DLQ and re-enter the crash loop.
    app.conf.task_queues = (pipeline_queue,)
    app.conf.task_default_queue = f"{namespace}.pipeline"
    app.conf.update(
        task_default_exchange=f"{namespace}.pipeline",
        task_default_routing_key="pipeline",
    )
    # Quorum queues don't support global (per-connection) QoS.
    # worker_detect_quorum_queues sends basic.qos with apply_global=False.
    app.conf.worker_detect_quorum_queues = True
    app.conf.broker_connection_retry_on_startup = True
    app.conf.worker_cancel_long_running_tasks_on_connection_loss = True

    with app.connection_for_write() as conn:
        with conn.channel() as ch:
            dlx_exchange.declare(channel=ch)
            dlq.declare(channel=ch)

    return dlq, delivery_limit


def drain_dlq_messages(app: "Celery", dead_letter_queue: Queue) -> None:
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


def _finalize_dlq_message(app: Celery, msg: Message) -> None:
    """Extract chord context from a DLQ'd task message and write a
    SUCCESS-state failure envelope so the chord coordinator advances.

    Celery protocol v2 splits task metadata across two places:
      - AMQP headers carry id, task, group, group_index, etc.
      - The message body is a tuple (args, kwargs, embed), where
        embed holds {callbacks, errbacks, chain, chord}.
    RabbitMQ's DLX preserves both verbatim when dead-lettering.
    Ref: https://docs.celeryq.dev/en/main/internals/protocol.html#task-messages
    """
    from celery.app.task import Context

    headers = msg.headers or {}
    task_id = headers.get("id")
    group_id = headers.get("group")
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
    context.chord = app.signature(chord_sig)  # type: ignore[assignment]
    context.task = task_name  # type: ignore[attr-defined]

    envelope = Result.failure(
        "DLQ'd: x-delivery-limit exceeded",
        # attempts is None — the broker counted redeliveries, not Python retries
        attempts=None,
        payload={},
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
