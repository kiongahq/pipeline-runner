"""Definition-backed pipeline tests use a fake Docker client, never a daemon."""

from __future__ import annotations

import json
from typing import Any

import pytest

from pipelines import definition as pipeline_module


class FakeContainer:
    def __init__(self, status: int, logs: list[bytes]):
        self.status = status
        self.log_chunks = logs
        self.started = False
        self.removed = False
        self.wait_timeout = None

    def start(self) -> None:
        self.started = True

    def wait(self, timeout: int) -> dict[str, int]:
        self.wait_timeout = timeout
        return {"StatusCode": self.status}

    def logs(self, **_kwargs):
        return iter(self.log_chunks)

    def remove(self, force: bool = False) -> None:
        assert force is True
        self.removed = True


class FakeContainers:
    def __init__(self, outcomes: list[tuple[int, list[bytes]]]):
        self.outcomes = list(outcomes)
        self.created: list[tuple[dict[str, Any], FakeContainer]] = []

    def create(self, **kwargs) -> FakeContainer:
        status, logs = self.outcomes.pop(0)
        container = FakeContainer(status, logs)
        self.created.append((kwargs, container))
        return container


class FakeImages:
    def __init__(self):
        self.pulled: list[str] = []

    def pull(self, image: str) -> None:
        self.pulled.append(image)


class FakeDockerClient:
    def __init__(self, outcomes: list[tuple[int, list[bytes]]]):
        self.containers = FakeContainers(outcomes)
        self.images = FakeImages()
        self.closed = False

    def close(self) -> None:
        self.closed = True


def container_job(**overrides) -> dict[str, Any]:
    job = {
        "name": "train",
        "kind": "container",
        "image": "registry.example/train@sha256:abc",
        "command": ["python", "train.py"],
        "depends_on": ["features"],
        "environment": {"MODE": "batch"},
        "resources": {"cpu": "500m", "memory": "2Gi", "gpu": 1},
        "retries": 0,
    }
    job.update(overrides)
    return job


def test_definition_layers_preserve_parallel_ready_jobs():
    definition = {
        "jobs": [
            container_job(name="train", depends_on=["features", "validate"]),
            container_job(name="features", depends_on=[]),
            container_job(name="validate", depends_on=[]),
            container_job(name="publish", depends_on=["train"]),
        ]
    }
    assert [[job["name"] for job in layer] for layer in pipeline_module.definition_layers(definition)] == [
        ["features", "validate"],
        ["train"],
        ["publish"],
    ]


def test_definition_layers_reject_cycle():
    definition = {
        "jobs": [
            container_job(name="a", depends_on=["b"]),
            container_job(name="b", depends_on=["a"]),
        ]
    }
    with pytest.raises(ValueError, match="cycle"):
        pipeline_module.definition_layers(definition)


def test_container_job_applies_contract_and_cleans_up(monkeypatch):
    client = FakeDockerClient([(0, [b'{"model_uri":', b'"s3://models/1"}'])])
    reports = []
    monkeypatch.setattr(pipeline_module, "_docker_client", lambda: client)
    monkeypatch.setattr(pipeline_module, "_gpu_device_requests", lambda count: [f"gpu:{count}"])
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args: reports.append(args))
    monkeypatch.setenv("MLAIOPS_PIPELINE_NETWORK", "mlaiops_default")
    monkeypatch.setenv("MLAIOPS_PIPELINE_JOB_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("MLAIOPS_PIPELINE_PIDS_LIMIT", "64")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    output = pipeline_module.run_container_job.fn(
        "run-1",
        "prj-1",
        container_job(),
        {"epochs": 4},
        {"features": {"rows": 12}},
    )

    assert output == {"model_uri": "s3://models/1"}
    kwargs, container = client.containers.created[0]
    assert kwargs["image"].endswith("@sha256:abc")
    assert kwargs["command"] == ["python", "train.py"]
    assert kwargs["nano_cpus"] == 500_000_000
    assert kwargs["mem_limit"] == 2 * (1 << 30)
    assert kwargs["device_requests"] == ["gpu:1"]
    assert kwargs["network"] == "mlaiops_default"
    assert kwargs["cap_drop"] == ["ALL"]
    assert kwargs["security_opt"] == ["no-new-privileges"]
    assert kwargs["pids_limit"] == 64
    assert kwargs["init"] is True
    assert "read_only" not in kwargs
    assert kwargs["environment"]["MODE"] == "batch"
    assert kwargs["environment"]["MLFLOW_TRACKING_URI"] == "http://mlflow:5000"
    assert json.loads(kwargs["environment"]["KIONGA_PARAMETERS"]) == {"epochs": 4}
    assert json.loads(kwargs["environment"]["KIONGA_DEPENDENCY_OUTPUTS"]) == {
        "features": {"rows": 12}
    }
    assert container.started and container.removed and container.wait_timeout == 45
    assert client.closed
    assert [report[2] for report in reports] == ["running", "succeeded"]


def test_container_job_retries_then_succeeds_without_terminal_failure(monkeypatch):
    client = FakeDockerClient([(12, [b"first attempt failed"]), (0, [b'{"ok":true}'])])
    reports = []
    monkeypatch.setattr(pipeline_module, "_docker_client", lambda: client)
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args: reports.append(args))

    output = pipeline_module.run_container_job.fn(
        "run-2",
        "prj-1",
        container_job(resources={}, retries=1),
    )

    assert output == {"ok": True}
    assert len(client.containers.created) == 2
    assert all(container.removed for _, container in client.containers.created)
    assert [report[2] for report in reports] == ["running", "running", "succeeded"]


def test_container_job_reports_final_failure_and_cleans_up(monkeypatch):
    client = FakeDockerClient([(2, [b"bad command"]), (2, [b"still bad"])])
    reports = []
    monkeypatch.setattr(pipeline_module, "_docker_client", lambda: client)
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args: reports.append(args))

    with pytest.raises(pipeline_module.ContainerJobError, match="status 2"):
        pipeline_module.run_container_job.fn(
            "run-3",
            "prj-1",
            container_job(resources={}, retries=1),
        )

    assert len(client.containers.created) == 2
    assert all(container.removed for _, container in client.containers.created)
    assert client.closed
    assert [report[2] for report in reports] == ["running", "running", "failed"]


def test_logs_are_a_bounded_tail():
    container = FakeContainer(0, [b"012345", b"6789"])
    assert pipeline_module._bounded_logs(container, 5) == "56789"


def test_container_kwargs_apply_safe_default_compute_limits(monkeypatch):
    monkeypatch.delenv("MLAIOPS_PIPELINE_NETWORK", raising=False)
    monkeypatch.setenv("MLAIOPS_PIPELINE_PIDS_LIMIT", "999999")
    kwargs = pipeline_module._container_kwargs(
        "run-defaults",
        "prj-1",
        container_job(resources={}),
        {},
        {},
    )
    assert kwargs["nano_cpus"] == 500_000_000
    assert kwargs["mem_limit"] == 1 << 30
    assert kwargs["pids_limit"] == pipeline_module.MAX_PIDS_LIMIT


def test_pipeline_definition_passes_outputs_to_dependent_layer(monkeypatch):
    submissions = []

    class ImmediateResult:
        def __init__(self, value):
            self.value = value

        def result(self):
            return self.value

    def submit(run_id, project_id, job, parameters, dependencies):
        submissions.append((run_id, project_id, job["name"], parameters, dependencies))
        return ImmediateResult({"from": job["name"]})

    monkeypatch.setattr(pipeline_module.run_container_job, "submit", submit)
    definition = {
        "jobs": [
            container_job(name="extract", depends_on=[]),
            container_job(name="train", depends_on=["extract"]),
        ]
    }

    outputs = pipeline_module.pipeline_definition.fn(
        "run-4", "prj-1", {"date": "2026-08-06"}, definition
    )

    assert outputs == {"extract": {"from": "extract"}, "train": {"from": "train"}}
    assert submissions[0][-1] == {}
    assert submissions[1][-1] == {"extract": {"from": "extract"}}
