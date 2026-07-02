# Pipeline runner image: serves platform flows as Prefect deployments and
# executes them. Build from the repository root:
#   docker build -f python/pipelines/Dockerfile -t mlaiops/pipeline-runner .
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir "prefect>=3.0" "scikit-learn>=1.4" "mlflow>=2.14" "httpx>=0.27" "boto3>=1.34"

COPY python/pipelines python/pipelines

ENV PYTHONPATH=/app/python

ENTRYPOINT ["python", "-m", "pipelines.serve"]
