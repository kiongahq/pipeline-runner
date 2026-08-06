"""Serve pipeline deployments to the Prefect server.

``python -m pipelines.serve`` registers every platform flow as a deployment
named ``mlaiops`` (the name the gateway resolves) and executes scheduled runs
in-process. This is the Compose pipeline-runner service.
"""

from __future__ import annotations

from prefect import serve

from .definition import pipeline_definition
from .training import training_pipeline


def main() -> None:
    serve(
        training_pipeline.to_deployment(name="mlaiops"),
        pipeline_definition.to_deployment(name="mlaiops"),
    )


if __name__ == "__main__":
    main()
