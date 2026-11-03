"""Serve pipeline deployments to the Prefect server.

``python -m pipelines.serve`` registers every platform flow as a deployment
named ``mlaiops`` (the name the gateway resolves) and executes scheduled runs
in-process. This is the Compose pipeline-runner service.
"""

from __future__ import annotations

import threading

from prefect import serve

from .definition import pipeline_definition
from .report import replay_outbox
from .training import training_pipeline


def _replay_reports() -> None:
    counts = replay_outbox()
    if any(counts.values()):
        print(f"replayed undelivered step reports: {counts}")


def main() -> None:
    # Terminal step reports a previous process could not deliver; in the
    # background so a gateway outage never delays serving new runs.
    threading.Thread(target=_replay_reports, name="report-outbox-replay", daemon=True).start()
    serve(
        training_pipeline.to_deployment(name="mlaiops"),
        pipeline_definition.to_deployment(name="mlaiops"),
    )


if __name__ == "__main__":
    main()
