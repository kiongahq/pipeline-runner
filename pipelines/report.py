"""Step reporting from inside executing pipelines.

Each flow reports its own step transitions to the control plane
(``POST /api/v1/pipelines/runs/{id}/steps``), which recomputes run status and
progress deterministically. Reporting never fails a run, but it is reliable:

- Every report carries an ``Idempotency-Key`` (uuid5 of run, step, attempt and
  status), so a retried request names the same logical report, and an
  ``X-Kionga-Report-Seq`` header that increases strictly per run.
- Transitions of one step are sent one at a time in the order they happened, so
  a late ``running`` can never overtake that step's ``succeeded``.
- 5xx, 429 and transport errors are retried with exponential backoff and jitter
  up to a deadline; other 4xx responses are not retried.
- Terminal reports (succeeded, failed, skipped) are written to an on-disk outbox
  before sending and removed once the gateway answers, so a report lost to a
  gateway outage or a runner restart is replayed at startup and every minute.

The idempotency key and sequence travel as headers because the gateway rejects
unknown JSON fields in the report body.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx

from .redact import Redactor, runner_redactor

TERMINAL_STATUSES = frozenset({"succeeded", "failed", "skipped"})
REPORT_NAMESPACE = uuid.UUID("6f6b8a52-58f5-4b55-9d0e-0c6a3f1e2b7d")
OUTBOX_VARIABLE = "KIONGA_PIPELINE_REPORT_OUTBOX"
DEADLINE_VARIABLE = "KIONGA_PIPELINE_REPORT_DEADLINE_SECONDS"
DEFAULT_TERMINAL_DEADLINE_SECONDS = 120.0
MAX_TERMINAL_DEADLINE_SECONDS = 3600.0
PROGRESS_DEADLINE_SECONDS = 15.0
BACKOFF_BASE_SECONDS = 0.5
BACKOFF_CAP_SECONDS = 15.0
REQUEST_TIMEOUT_SECONDS = 5.0
OUTBOX_REPLAY_INTERVAL_SECONDS = 60.0

_client_lock = threading.Lock()
_shared_client: httpx.Client | None = None


def _client() -> httpx.Client:
    """One connection-reusing client for every report from this process."""
    global _shared_client
    with _client_lock:
        if _shared_client is None:
            _shared_client = httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS)
        return _shared_client


class _Sequence:
    """Strictly increasing per-run sequence numbers.

    Seeded from the wall clock in microseconds, so numbers also keep increasing
    across a runner restart; the per-run counter guarantees strictness when two
    threads report within the same microsecond.
    """

    def __init__(self, clock: Callable[[], int] = time.time_ns, keep: int = 4096):
        self._clock, self._keep = clock, keep
        self._last: OrderedDict[str, int] = OrderedDict()
        self._lock = threading.Lock()

    def next(self, run_id: str) -> int:
        with self._lock:
            value = max(self._last.get(run_id, 0) + 1, self._clock() // 1000)
            self._last[run_id] = value
            self._last.move_to_end(run_id)
            while len(self._last) > self._keep:
                self._last.popitem(last=False)
            return value


_sequence = _Sequence()
# Striped locks serialise the transitions of one step without a lock per step.
_step_locks = tuple(threading.Lock() for _ in range(64))


def _step_lock(run_id: str, step: str) -> threading.Lock:
    return _step_locks[hash((run_id, step)) % len(_step_locks)]


def report_key(run_id: str, step: str, attempt: Any, status: str) -> str:
    """Idempotency key of one logical step transition."""
    return str(uuid.uuid5(REPORT_NAMESPACE, "\x1f".join([run_id, step, str(attempt or 0), status])))


def _terminal_deadline() -> float:
    try:
        configured = float(os.environ.get(DEADLINE_VARIABLE, DEFAULT_TERMINAL_DEADLINE_SECONDS))
    except ValueError:
        configured = DEFAULT_TERMINAL_DEADLINE_SECONDS
    return min(max(configured, 1.0), MAX_TERMINAL_DEADLINE_SECONDS)


def _outbox_dir() -> Path | None:
    """Outbox directory; KIONGA_PIPELINE_REPORT_OUTBOX="" disables it."""
    configured = os.environ.get(OUTBOX_VARIABLE)
    if configured is None:
        return Path.home() / ".kionga" / "report-outbox"
    return Path(configured) if configured.strip() else None


def _outbox_write(report: dict[str, Any]) -> Path | None:
    directory = _outbox_dir()
    if directory is None:
        return None
    try:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = directory / f"{report['seq']:020d}-{report['idempotency_key']}.json"
        temporary = path.with_suffix(".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            json.dump(report, handle)
        os.replace(temporary, path)
        return path
    except OSError as error:
        print(f"step report outbox unavailable ({directory}): {error}")
        return None


def _headers(report: dict[str, Any]) -> dict[str, str]:
    headers = {
        "X-Kionga-Actor": "pipeline-engine",
        "Idempotency-Key": report["idempotency_key"],
        "X-Kionga-Report-Seq": str(report["seq"]),
    }
    # Read at send time, so a replayed report uses the current credential and
    # the outbox never stores one.
    if token := os.environ.get("KIONGA_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _retry_after(response: httpx.Response) -> float | None:
    try:
        return max(float(response.headers.get("Retry-After", "")), 0.0)
    except ValueError:
        return None


def _deliver(gateway: str, report: dict[str, Any], deadline: float) -> str:
    """Send one report. Returns "delivered", "rejected" (a non-retryable
    answer) or "pending" (the deadline passed while the gateway was failing)."""
    sleep, clock = time.sleep, time.monotonic
    url = f"{gateway.rstrip('/')}/api/v1/pipelines/runs/{report['run_id']}/steps"
    label = f"{report['payload']['step']} -> {report['payload']['status']}"
    end = clock() + deadline
    failures = 0
    while True:
        wait: float | None = None
        try:
            response = _client().post(url, json=report["payload"], headers=_headers(report))
        except httpx.TransportError as error:
            reason = str(error) or type(error).__name__
        except httpx.HTTPError as error:
            print(f"step report failed ({label}): {error}")
            return "rejected"
        else:
            if response.status_code < 400:
                return "delivered"
            if response.status_code != 429 and response.status_code < 500:
                print(f"step report rejected ({label}): HTTP {response.status_code} {response.text[:200]}")
                return "rejected"
            reason, wait = f"HTTP {response.status_code}", _retry_after(response)
        failures += 1
        backoff = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (failures - 1)))
        # Full jitter spreads retries from concurrent nodes; not security relevant.
        delay = wait if wait is not None else random.uniform(0, backoff)  # nosec B311
        remaining = end - clock()
        if delay > remaining:
            print(f"step report pending after {failures} attempts ({label}): {reason}")
            return "pending"
        sleep(delay)


def report_step(run_id: str, step: str, status: str, message: str = "", **facts) -> None:
    """Report a step transition. Optional facts: attempt, exit_code,
    workload_kind, workload_id, image_digest, at (ISO-8601 UTC)."""
    gateway = os.environ.get("KIONGA_URL")
    if not gateway or not run_id:
        return
    payload = {"step": step, "status": status, "message": runner_redactor().redact(message)}
    payload.update({key: value for key, value in facts.items() if value is not None})
    terminal = status in TERMINAL_STATUSES
    with _step_lock(run_id, step):
        report = {
            "run_id": run_id,
            "payload": payload,
            "idempotency_key": report_key(run_id, step, facts.get("attempt"), status),
            "seq": _sequence.next(run_id),
        }
        path = _outbox_write(report) if terminal else None
        outcome = _deliver(gateway, report, _terminal_deadline() if terminal else PROGRESS_DEADLINE_SECONDS)
        if path is not None and outcome != "pending":
            path.unlink(missing_ok=True)


def replay_outbox(deadline: float | None = None) -> dict[str, int]:
    """Resend terminal reports a previous runner process could not deliver,
    oldest first. Stops at the first report that is still pending, so the
    gateway receives them in their original order."""
    counts = {"delivered": 0, "rejected": 0, "pending": 0}
    directory, gateway = _outbox_dir(), os.environ.get("KIONGA_URL")
    if directory is None or not gateway or not directory.is_dir():
        return counts
    for path in sorted(directory.glob("*.json")):
        try:
            report = json.loads(path.read_text())
            _ = (report["payload"]["step"], report["run_id"], int(report["seq"]), report["idempotency_key"])
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(f"discarding unreadable step report {path.name}: {error}")
            path.unlink(missing_ok=True)
            continue
        outcome = _deliver(gateway, report, _terminal_deadline() if deadline is None else deadline)
        counts[outcome] += 1
        if outcome == "pending":
            break
        path.unlink(missing_ok=True)
    return counts


def replay_outbox_forever(stop: threading.Event, interval: float = OUTBOX_REPLAY_INTERVAL_SECONDS) -> None:
    """Replay at startup and then periodically, so a run whose final report
    outlasted its retry deadline finishes once the gateway is back, without
    waiting for a runner restart. A report still being sent may go out twice with
    the same Idempotency-Key; applying the same transition again is harmless."""
    while True:
        try:
            counts = replay_outbox()
            if counts["delivered"] or counts["rejected"]:
                print(f"replayed undelivered step reports: {counts}")
        except Exception as error:  # reporting never takes the runner down
            print(f"step report replay failed: {error}")
        if stop.wait(interval):
            return


@contextmanager
def reported_step(run_id: str, step: str):
    """running -> succeeded/failed bracket around one pipeline step."""
    report_step(run_id, step, "running")
    try:
        yield
    except Exception as error:
        report_step(run_id, step, "failed", str(error))
        raise
    report_step(run_id, step, "succeeded")


class LogShipper:
    """Batches node output to the control plane's log store.

    Lines are sent at most every `interval` seconds or `batch` lines, from a
    background thread, so a slow gateway never blocks the container. Like
    step reports, shipping is best-effort: failures are printed and dropped
    rather than failing the run.
    """

    def __init__(self, run_id: str, node: str, attempt: int, workload_id: str = "",
                 batch: int = 200, interval: float = 1.0, post=None, redactor: Redactor | None = None):
        self.run_id, self.node, self.attempt, self.workload_id = run_id, node, attempt, workload_id
        self.redactor = redactor if redactor is not None else runner_redactor()
        self.batch, self.interval = batch, interval
        self._post = post or _post_logs
        self._pending: list[dict] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def add(self, message: str, stream: str = "", partial: bool = False) -> None:
        """Queue one line. `partial` marks a line cut at a length cap, so a
        trailing fragment of a secret is masked as well."""
        message = self.redactor.redact(message, partial_end=partial)
        entry = {"ts": datetime.now(timezone.utc).isoformat(), "node": self.node, "attempt": self.attempt,
                 "workload_kind": "docker-container", "workload_id": self.workload_id, "message": message}
        if stream:
            entry["stream"] = stream
        with self._lock:
            self._pending.append(entry)
            full = len(self._pending) >= self.batch
        if full:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            entries, self._pending = self._pending, []
        for start in range(0, len(entries), 500):
            self._post(self.run_id, entries[start:start + 500])

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            self.flush()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self.flush()


def _post_logs(run_id: str, entries: list[dict]) -> None:
    gateway = os.environ.get("KIONGA_URL")
    if not gateway or not run_id or not entries:
        return
    headers = {"X-Kionga-Actor": "pipeline-engine"}
    if token := os.environ.get("KIONGA_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    try:
        httpx.post(f"{gateway.rstrip('/')}/api/v1/pipelines/runs/{run_id}/logs", json={"entries": entries},
                   headers=headers, timeout=10).raise_for_status()
    except httpx.HTTPError as error:
        print(f"log shipping failed ({len(entries)} lines): {error}")
