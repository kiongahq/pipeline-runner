"""Step reporting from inside executing pipelines.

Each flow reports its own step transitions to the control plane
(``POST /api/v1/pipelines/runs/{id}/steps``), which recomputes run status and
progress deterministically. Reporting is best-effort: a gateway outage never
fails a training run, but failures are printed so they are visible in the
flow logs.
"""

from __future__ import annotations

import os
from contextlib import contextmanager

import httpx


def report_step(run_id: str, step: str, status: str, message: str = "") -> None:
    gateway = os.environ.get("MLAIOPS_URL")
    if not gateway or not run_id:
        return
    headers = {"X-MLAIOps-Actor": "pipeline-engine"}
    if token := os.environ.get("MLAIOPS_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    try:
        httpx.post(
            f"{gateway.rstrip('/')}/api/v1/pipelines/runs/{run_id}/steps",
            json={"step": step, "status": status, "message": message},
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
