"""Serve pipeline deployments to the Prefect server.

``python -m pipelines.serve`` registers every platform flow as a deployment
named ``kionga`` (the name the gateway resolves) and executes scheduled runs
in-process. This is the Compose pipeline-runner service.
"""

from __future__ import annotations

import threading

from prefect import serve

from .definition import pipeline_definition
from .report import replay_outbox_forever
from .training import training_pipeline


def main() -> None:
    # Terminal step reports not yet delivered; in the background so a gateway
    # outage never delays serving new runs.
    threading.Thread(target=replay_outbox_forever, args=(threading.Event(),),
                     name="report-outbox-replay", daemon=True).start()
    serve(
        training_pipeline.to_deployment(name="kionga"),
        pipeline_definition.to_deployment(name="kionga"),
    )


if __name__ == "__main__":
    main()
