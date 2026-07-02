"""Gate tests for the training pipeline. Tasks are tested through ``.fn`` so
no Prefect server or event loop is involved; the full flow runs in the
Compose stack and the integration lane."""

import httpx
import pytest

from pipelines.report import report_step, reported_step
from pipelines.training import MODEL_NAME, evaluate, register_with_control_plane, train_model, validate_data


def test_validate_data_deterministic():
    first = validate_data.fn()
    second = validate_data.fn()
    assert (first["train_x"] == second["train_x"]).all()
    assert first["train_x"].shape[0] + first["test_x"].shape[0] == 1000


def test_train_and_evaluate_reproducible():
    data = validate_data.fn()
    metrics = evaluate.fn(train_model.fn(data), data)
    again = evaluate.fn(train_model.fn(data), data)
    assert metrics == again
    assert metrics["accuracy"] > 0.8, f"model quality regressed: {metrics}"
    assert metrics["auc"] > 0.85, f"model quality regressed: {metrics}"


def test_reported_step_brackets_success(monkeypatch):
    calls = []
    monkeypatch.setattr("pipelines.report.report_step", lambda *args: calls.append(args))
    with reported_step("run-1", "train-model"):
        pass
    assert [call[2] for call in calls] == ["running", "succeeded"]


def test_reported_step_reports_failure(monkeypatch):
    calls = []
    monkeypatch.setattr("pipelines.report.report_step", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        with reported_step("run-1", "train-model"):
            raise ValueError("boom")
    assert calls[-1][2] == "failed"
    assert calls[-1][3] == "boom"


def test_report_step_noop_without_gateway(monkeypatch):
    monkeypatch.delenv("MLAIOPS_URL", raising=False)
    report_step("run-1", "x", "running")  # must not raise or call the network


def test_register_with_control_plane(monkeypatch):
    recorded = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        recorded.update(json.loads(request.content))
        return httpx.Response(201, json={"ok": True})

    monkeypatch.setenv("MLAIOPS_URL", "http://gateway")
    monkeypatch.setattr(httpx, "post", lambda url, **kw: httpx.Client(
        transport=httpx.MockTransport(handler)
    ).post(url, **kw))
    register_with_control_plane("prj-1", "models:/churn/3", {"accuracy": 0.9})
    assert recorded["name"] == MODEL_NAME
    assert recorded["artifact_uri"] == "models:/churn/3"
    assert recorded["project_id"] == "prj-1"
