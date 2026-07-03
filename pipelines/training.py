"""Training pipeline: validate -> train -> evaluate -> register.

A real Prefect flow executing real scikit-learn training. Deterministic by
construction (fixed random_state everywhere), so every environment produces
identical metrics. When MLflow is configured the run and model are logged
and registered there; when the control plane is configured the model version
is registered against the submitting project.
"""

from __future__ import annotations

import os
from typing import Any

from prefect import flow, task

from .report import reported_step

RANDOM_STATE = 7
MODEL_NAME = "churn-classifier"


@task
def validate_data() -> dict[str, Any]:
    from sklearn.datasets import make_classification
    from sklearn.model_selection import train_test_split

    features, labels = make_classification(
        n_samples=1000,
        n_features=12,
        n_informative=6,
        n_clusters_per_class=1,
        class_sep=1.5,
        random_state=RANDOM_STATE,
    )
    if features.shape[0] != labels.shape[0]:
        raise ValueError("feature/label row mismatch")
    train_x, test_x, train_y, test_y = train_test_split(
        features, labels, test_size=0.25, random_state=RANDOM_STATE
    )
    return {"train_x": train_x, "test_x": test_x, "train_y": train_y, "test_y": test_y}


@task
def train_model(data: dict[str, Any]):
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(max_iter=500, random_state=RANDOM_STATE)
    model.fit(data["train_x"], data["train_y"])
    return model


@task
def evaluate(model: Any, data: dict[str, Any]) -> dict[str, float]:
    from sklearn.metrics import accuracy_score, roc_auc_score

    predictions = model.predict(data["test_x"])
    scores = model.predict_proba(data["test_x"])[:, 1]
    return {
        "accuracy": round(float(accuracy_score(data["test_y"], predictions)), 4),
        "auc": round(float(roc_auc_score(data["test_y"], scores)), 4),
    }


def log_to_mlflow(model: Any, metrics: dict[str, float]) -> tuple[str, str]:
    """Log the trained model and metrics to MLflow; returns (artifact URI,
    registered version). MLflow assigns the next model version, which keeps
    every run's registration unique. Empty strings when MLflow is not
    configured."""
    if not os.environ.get("MLFLOW_TRACKING_URI"):
        return "", ""
    import mlflow

    mlflow.set_experiment("training-pipeline")
    with mlflow.start_run():
        mlflow.log_params({"model": "LogisticRegression", "random_state": RANDOM_STATE})
        mlflow.log_metrics(metrics)
        info = mlflow.sklearn.log_model(
            model, name="model", registered_model_name=MODEL_NAME
        )
        version = str(getattr(info, "registered_model_version", "") or "")
        return info.model_uri, version


def register_with_control_plane(
    project_id: str, artifact_uri: str, metrics: dict[str, float], version: str = ""
) -> None:
    """Register the trained model version in the platform registry. The
    version comes from MLflow when available, otherwise from the run id, so
    repeated runs never collide."""
    gateway = os.environ.get("MLAIOPS_URL")
    if not gateway or not project_id:
        return
    import time as _time

    import httpx

    httpx.post(
        f"{gateway.rstrip('/')}/api/v1/models",
        json={
            "project_id": project_id,
            "name": MODEL_NAME,
            "version": version or os.environ.get("MLAIOPS_MODEL_VERSION") or str(int(_time.time())),
            "artifact_uri": artifact_uri or f"models:/{MODEL_NAME}/latest",
            "metrics": metrics,
        },
        headers={"X-MLAIOps-Actor": "pipeline-engine"},
        timeout=10,
    ).raise_for_status()


@flow(name="training-pipeline")
def training_pipeline(run_id: str = "", project_id: str = "") -> dict[str, float]:
    with reported_step(run_id, "validate-data"):
        data = validate_data()
    with reported_step(run_id, "train-model"):
        model = train_model(data)
    with reported_step(run_id, "evaluate"):
        metrics = evaluate(model, data)
    with reported_step(run_id, "register-model"):
        artifact_uri, version = log_to_mlflow(model, metrics)
        register_with_control_plane(project_id, artifact_uri, metrics, version)
    return metrics
