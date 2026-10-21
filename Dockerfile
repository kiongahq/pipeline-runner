# Pipeline runner image: serves platform flows as Prefect deployments and
# executes them:
#   docker build -t ghcr.io/kiongahq/pipeline-runner .
#
# Python (3.11) and ML library versions are pinned to the model server image
# (mlflow/Dockerfile in kiongahq/deploy: mlflow 3.16.1), so models
# trained here load there. Unpinned versions are how training-serving skew
# happens. requirements.txt is the single list for the image and for tests.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipelines /app/pipelines

ENV PYTHONPATH=/app
ENV MLAIOPS_PIPELINE_LOG_LIMIT_BYTES=65536
ENV MLAIOPS_PIPELINE_JOB_TIMEOUT_SECONDS=3600
ENV MLAIOPS_PIPELINE_PIDS_LIMIT=256

ENTRYPOINT ["python", "-m", "pipelines.serve"]
