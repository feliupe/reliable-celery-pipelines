"""Run every FM scenario end-to-end and assert each one passes.

For each FM module:
  1. Reset state (FLUSHDB on Redis; delete this FM's queues/exchanges on RabbitMQ).
  2. Start the matching celery worker subprocess with the flags the FM needs
     (--concurrency=2 / --beat where required).
  3. Wait until the worker logs `celery@... ready.`.
  4. Run the runner (`python fmX_*.py`) as a subprocess; success = exit 0.
  5. Tear the worker down.

Docker compose is brought up at the start if rabbitmq/redis aren't already
listening. The compose stack is NOT torn down on exit by default — pass
`--down` to stop containers when finished.

fm0_naive is the broken baseline and is skipped by default; pass `--include-fm0`
to run it (it returns 0 on the documented stall, so it "passes" trivially).

Usage
-----
  python run_all_fms.py                 # fm1..fm6
  python run_all_fms.py --include-fm0   # fm0..fm6
  python run_all_fms.py --only fm3 fm6  # subset
  python run_all_fms.py --down          # also `docker compose down` at the end
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import redis

REPO_DIR = Path(__file__).resolve().parent
REDIS_URL = "redis://localhost:6379/0"
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_CONTAINER = "reliable-celery-rabbitmq"


@dataclass(frozen=True)
class FMSpec:
    name: str  # celery -A target / module name
    needs_beat: bool
    concurrency: int
    # Broker resources this FM owns. We delete them between runs so a stale
    # x-delivery-count or leftover message can't leak across scenarios.
    queues: tuple[str, ...]
    exchanges: tuple[str, ...]
    runner_timeout_seconds: int


FM_SPECS: list[FMSpec] = [
    FMSpec(
        "fm0_naive",
        needs_beat=False,
        concurrency=1,
        queues=(),
        exchanges=(),
        runner_timeout_seconds=30,
    ),
    FMSpec(
        "fm1_mid_pipeline_error",
        needs_beat=False,
        concurrency=1,
        queues=(),
        exchanges=(),
        runner_timeout_seconds=30,
    ),
    FMSpec(
        "fm2_worker_crash",
        needs_beat=False,
        concurrency=2,
        queues=(),
        exchanges=(),
        runner_timeout_seconds=30,
    ),
    FMSpec(
        "fm3_dlq_reconciliation",
        needs_beat=True,
        concurrency=2,
        queues=("fm3.pipeline", "fm3.dead_letters"),
        exchanges=("fm3.pipeline", "fm3.dlx"),
        runner_timeout_seconds=60,
    ),
    FMSpec(
        "fm4_duplicated_runs",
        needs_beat=True,
        concurrency=2,
        queues=("fm4.pipeline", "fm4.dead_letters"),
        exchanges=("fm4.pipeline", "fm4.dlx"),
        runner_timeout_seconds=60,
    ),
    FMSpec(
        "fm5_transient_failures",
        needs_beat=True,
        concurrency=2,
        queues=("fm5.pipeline", "fm5.dead_letters"),
        exchanges=("fm5.pipeline", "fm5.dlx"),
        runner_timeout_seconds=90,
    ),
    FMSpec(
        "fm6_task_timeouts",
        needs_beat=True,
        concurrency=2,
        queues=("fm6.pipeline", "fm6.dead_letters"),
        exchanges=("fm6.pipeline", "fm6.dlx"),
        runner_timeout_seconds=90,
    ),
]


# ---------------------------------------------------------------------------
# Docker / service readiness
# ---------------------------------------------------------------------------


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_services_up() -> None:
    redis_up = _port_open("localhost", 6379)
    rabbit_up = _port_open(RABBITMQ_HOST, RABBITMQ_PORT)
    if redis_up and rabbit_up:
        print("[setup] redis + rabbitmq already reachable")
    else:
        print("[setup] starting docker compose stack...")
        subprocess.run(
            ["docker", "compose", "up", "-d"],
            cwd=REPO_DIR,
            check=True,
        )

    deadline = time.time() + 180
    while time.time() < deadline:
        if _port_open("localhost", 6379) and _port_open(RABBITMQ_HOST, RABBITMQ_PORT):
            break
        time.sleep(1)
    else:
        raise RuntimeError("redis/rabbitmq did not become reachable within 180s")

    # RabbitMQ accepts TCP before it's ready to declare queues; poll rabbitmqctl.
    # CI runners on cold image pulls regularly need >60s here.
    deadline = time.time() + 180
    while time.time() < deadline:
        rc = subprocess.run(
            ["docker", "exec", RABBITMQ_CONTAINER, "rabbitmqctl", "await_startup"],
            capture_output=True,
        ).returncode
        if rc == 0:
            print("[setup] rabbitmq ready")
            return
        time.sleep(1)
    raise RuntimeError("rabbitmq did not finish startup within 180s")


# ---------------------------------------------------------------------------
# State reset
# ---------------------------------------------------------------------------


def reset_state(spec: FMSpec) -> None:
    redis.Redis.from_url(REDIS_URL).flushdb()
    # Always purge the default celery queue — fm0/fm1/fm2 ride it and a
    # leftover unacked message from a previous run could leak across FMs.
    subprocess.run(
        [
            "docker",
            "exec",
            RABBITMQ_CONTAINER,
            "rabbitmqadmin",
            "purge",
            "queue",
            "name=celery",
        ],
        capture_output=True,
    )
    for q in spec.queues:
        subprocess.run(
            [
                "docker",
                "exec",
                RABBITMQ_CONTAINER,
                "rabbitmqadmin",
                "delete",
                "queue",
                f"name={q}",
            ],
            capture_output=True,
        )
    for ex in spec.exchanges:
        subprocess.run(
            [
                "docker",
                "exec",
                RABBITMQ_CONTAINER,
                "rabbitmqadmin",
                "delete",
                "exchange",
                f"name={ex}",
            ],
            capture_output=True,
        )


# ---------------------------------------------------------------------------
# Worker lifecycle
# ---------------------------------------------------------------------------


def start_worker(spec: FMSpec, log_path: Path) -> subprocess.Popen[bytes]:
    cmd = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        spec.name,
        "worker",
        "--loglevel=info",
        f"--concurrency={spec.concurrency}",
    ]
    if spec.needs_beat:
        cmd.append("--beat")
    log = log_path.open("wb")
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    return subprocess.Popen(
        cmd,
        cwd=REPO_DIR,
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )


def wait_for_worker_ready(log_path: Path, timeout: float = 45) -> None:
    deadline = time.time() + timeout
    needle = b"ready."
    while time.time() < deadline:
        if log_path.exists():
            data = log_path.read_bytes()
            if needle in data:
                return
        time.sleep(0.3)
    raise RuntimeError(f"worker never logged 'ready.' (see {log_path})")


def stop_worker(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), 15)  # SIGTERM whole group
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), 9)  # SIGKILL fallback
        proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Runner run
# ---------------------------------------------------------------------------


def run_runner(spec: FMSpec, log_path: Path) -> int:
    with log_path.open("wb") as log:
        proc = subprocess.run(
            [sys.executable, f"{spec.name}.py"],
            cwd=REPO_DIR,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=spec.runner_timeout_seconds,
        )
    return proc.returncode


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@dataclass
class FMResult:
    name: str
    ok: bool
    detail: str


def run_one(spec: FMSpec, logs_dir: Path) -> FMResult:
    worker_log = logs_dir / f"{spec.name}.worker.log"
    runner_log = logs_dir / f"{spec.name}.runner.log"
    worker_log.unlink(missing_ok=True)
    runner_log.unlink(missing_ok=True)

    print(f"\n=== {spec.name} ===")
    reset_state(spec)

    worker = start_worker(spec, worker_log)
    try:
        wait_for_worker_ready(worker_log)
        print(
            f"[{spec.name}] worker ready; running runner "
            f"(timeout {spec.runner_timeout_seconds}s)"
        )
        try:
            rc = run_runner(spec, runner_log)
        except subprocess.TimeoutExpired:
            return FMResult(
                spec.name, False, f"runner timed out (>{spec.runner_timeout_seconds}s)"
            )
        if rc == 0:
            print(f"[{spec.name}] PASS")
            return FMResult(spec.name, True, "exit 0")
        return FMResult(spec.name, False, f"runner exit {rc}; see {runner_log}")
    except Exception as exc:
        return FMResult(spec.name, False, f"{type(exc).__name__}: {exc}")
    finally:
        stop_worker(worker)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--include-fm0",
        action="store_true",
        help="Include fm0_naive (broken baseline; exits 0 on the documented stall)",
    )
    ap.add_argument(
        "--only",
        nargs="+",
        metavar="NAME",
        help="Run only the given FM module names (e.g. fm3_dlq_reconciliation)",
    )
    ap.add_argument(
        "--down", action="store_true", help="`docker compose down` at the end"
    )
    args = ap.parse_args()

    if args.only:
        specs = [s for s in FM_SPECS if s.name in set(args.only)]
    else:
        specs = [s for s in FM_SPECS if args.include_fm0 or s.name != "fm0_naive"]
    if not specs:
        print("no FMs selected", file=sys.stderr)
        return 2

    logs_dir = REPO_DIR / ".fm_run_logs"
    logs_dir.mkdir(exist_ok=True)
    print(f"logs → {logs_dir}")

    ensure_services_up()

    results: list[FMResult] = []
    for spec in specs:
        results.append(run_one(spec, logs_dir))

    if args.down:
        print("\n[teardown] docker compose down")
        subprocess.run(["docker", "compose", "down"], cwd=REPO_DIR, check=False)

    print("\n=== summary ===")
    width = max(len(r.name) for r in results)
    for r in results:
        mark = "PASS" if r.ok else "FAIL"
        print(f"  {r.name.ljust(width)}  {mark}  {r.detail}")

    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
