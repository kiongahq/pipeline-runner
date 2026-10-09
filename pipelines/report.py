"""Step reporting from inside executing pipelines.

Each flow reports its own step transitions to the control plane
(``POST /api/v1/pipelines/runs/{id}/steps``), which recomputes run status and
progress deterministically. Reporting is best-effort: a gateway outage never
fails a training run, but failures are printed so they are visible in the
flow logs.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

import httpx


def report_step(run_id: str, step: str, status: str, message: str = "", **facts) -> None:
    """Report a step transition. Optional facts: attempt, exit_code,
    workload_kind, workload_id, image_digest, at (ISO-8601 UTC)."""
    gateway = os.environ.get("MLAIOPS_URL")
    if not gateway or not run_id:
        return
    headers = {"X-MLAIOps-Actor": "pipeline-engine"}
    if token := os.environ.get("MLAIOPS_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    payload = {"step": step, "status": status, "message": message}
    payload.update({key: value for key, value in facts.items() if value is not None})
    try:
        httpx.post(
            f"{gateway.rstrip('/')}/api/v1/pipelines/runs/{run_id}/steps",
            json=payload,
            headers=headers,
            timeout=5,
        ).raise_for_status()
    except httpx.HTTPError as error:
        print(f"step report failed ({step} -> {status}): {error}")


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
                 batch: int = 200, interval: float = 1.0, post=None):
        self.run_id, self.node, self.attempt, self.workload_id = run_id, node, attempt, workload_id
        self.batch, self.interval = batch, interval
        self._post = post or _post_logs
        self._pending: list[dict] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def add(self, message: str, stream: str = "") -> None:
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
    gateway = os.environ.get("MLAIOPS_URL")
    if not gateway or not run_id or not entries:
        return
    headers = {"X-MLAIOps-Actor": "pipeline-engine"}
    if token := os.environ.get("MLAIOPS_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    try:
        httpx.post(f"{gateway.rstrip('/')}/api/v1/pipelines/runs/{run_id}/logs", json={"entries": entries},
                   headers=headers, timeout=10).raise_for_status()
    except httpx.HTTPError as error:
        print(f"log shipping failed ({len(entries)} lines): {error}")
