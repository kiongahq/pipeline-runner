# Pipeline runner image: serves platform flows as Prefect deployments and
# executes them. Build from the repository root:
#   docker build -f python/pipelines/Dockerfile -t mlaiops/pipeline-runner .
#
# Python and ML library versions are pinned to the serving image
# (deploy/mlflow/Dockerfile: mlflow v3.1.1 on Python 3.10) — models trained
# here must load there. Unpinned versions are how training-serving skew
# happens.
FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir "prefect>=3.0" "scikit-learn==1.7.0" "mlflow==3.1.1" "httpx>=0.27" "boto3>=1.34"

COPY python/pipelines python/pipelines

ENV PYTHONPATH=/app/python

ENTRYPOINT ["python", "-m", "pipelines.serve"]
