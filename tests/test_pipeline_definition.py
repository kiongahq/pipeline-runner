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
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: reports.append(args + (facts,)))
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
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: reports.append(args + (facts,)))

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
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: reports.append(args + (facts,)))

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


class ThreadHandle:
    """Runs a job on a real thread so overlap can be measured."""

    def __init__(self, executor, fn):
        self.future = executor.submit(fn)

    def done(self):
        return self.future.done()

    def result(self):
        return self.future.result()


def _dag(*specs):
    return [container_job(name=name, depends_on=list(deps)) for name, deps in specs]


def test_ready_set_runs_independent_branches_concurrently_and_fans_in():
    import threading
    import time as clock
    from concurrent.futures import ThreadPoolExecutor

    spans, reports = {}, []
    lock = threading.Lock()
    release_slow = threading.Event()

    def work(name):
        def run():
            start = clock.monotonic()
            if name == "after-fast":
                release_slow.set()
            if name == "slow":
                # Only a ready-set engine can start after-fast while slow is
                # still running; with layer barriers this wait times out.
                assert release_slow.wait(2), "after-fast never started while slow was running"
            else:
                clock.sleep(0.05)
            with lock:
                spans[name] = (start, clock.monotonic())
            return name
        return run

    jobs = _dag(("extract", ()), ("slow", ("extract",)), ("fast", ("extract",)), ("after-fast", ("fast",)), ("join", ("slow", "after-fast")))
    with ThreadPoolExecutor(max_workers=8) as executor:
        outputs, failures = pipeline_module.execute_ready_set(
            jobs, {}, lambda job, deps: ThreadHandle(executor, work(job["name"])),
            lambda *args: reports.append(args), poll=lambda: clock.sleep(0.005),
        )
    assert failures == {}
    assert set(outputs) == {"extract", "slow", "fast", "after-fast", "join"}
    # slow and fast overlap (parallel branches).
    assert spans["fast"][0] < spans["slow"][1] and spans["slow"][0] < spans["fast"][1]
    # after-fast starts before slow finishes: ready-set, not layer barriers.
    assert spans["after-fast"][0] < spans["slow"][1]
    # fan-in waits for every dependency.
    assert spans["join"][0] >= max(spans["slow"][1], spans["after-fast"][1])


class DoneHandle:
    def __init__(self, value=None, error=None):
        self.value, self.error = value, error

    def done(self):
        return True

    def result(self):
        if self.error:
            raise self.error
        return self.value


def test_ready_set_skips_downstream_of_failures_but_runs_unrelated_branches():
    reports, started = [], []

    def start(job, deps):
        started.append(job["name"])
        if job["name"] == "bad":
            return DoneHandle(error=RuntimeError("exit 3"))
        return DoneHandle(value=job["name"])

    jobs = _dag(("root", ()), ("bad", ("root",)), ("good", ("root",)), ("after-bad", ("bad",)), ("after-good", ("good",)))
    outputs, failures = pipeline_module.execute_ready_set(jobs, {}, start, lambda *args: reports.append(args))
    assert failures == {"bad": "exit 3"}
    assert "after-bad" not in started and "after-good" in started
    assert ("after-bad", "skipped", "not run: upstream bad did not succeed") in reports


def test_ready_set_condition_skips_node_and_dependents_still_run():
    reports, received = [], {}

    def start(job, deps):
        received[job["name"]] = deps
        return DoneHandle(value=job["name"])

    jobs = _dag(("a", ()), ("optional", ("a",)), ("b", ("optional",)))
    jobs[1]["when"] = {"param": "mode", "equals": "full"}
    outputs, failures = pipeline_module.execute_ready_set(jobs, {"mode": "quick"}, start, lambda *args: reports.append(args))
    assert failures == {} and outputs["optional"] is None
    assert received["b"] == {"optional": None}
    assert reports[0][:2] == ("optional", "skipped")


def test_ready_set_respects_max_parallel():
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import time as clock

    active, peak, lock = [0], [0], threading.Lock()

    def work():
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        clock.sleep(0.03)
        with lock:
            active[0] -= 1

    jobs = _dag(*[(f"n{i}", ()) for i in range(8)])
    with ThreadPoolExecutor(max_workers=8) as executor:
        pipeline_module.execute_ready_set(jobs, {}, lambda job, deps: ThreadHandle(executor, work), lambda *a: None, max_parallel=3, poll=lambda: clock.sleep(0.002))
    assert peak[0] == 3


def test_container_job_uses_node_timeout_backoff_and_reports_facts(monkeypatch):
    client = FakeDockerClient([(7, [b"boom"]), (0, [b"done"])])
    reports, sleeps = [], []
    monkeypatch.setattr(pipeline_module, "_docker_client", lambda: client)
    monkeypatch.setattr(pipeline_module, "_sleep", sleeps.append)
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: reports.append((args, facts)))
    pipeline_module.run_container_job.fn("run-9", "prj-1", container_job(resources={}, retries=1, timeout_seconds=90, retry_backoff_seconds=5))
    assert [container.wait_timeout for _, container in client.containers.created] == [90, 90]
    assert sleeps == [5]
    final_args, final_facts = reports[-1]
    assert final_args[2] == "succeeded" and final_facts["attempt"] == 2
    assert final_facts["exit_code"] == 0 and final_facts["workload_kind"] == "docker-container"


def test_flow_reuses_outputs_of_previous_run(monkeypatch):
    submitted, reports = [], []

    class Immediate:
        def __init__(self, value):
            self.value = value

        def result(self):
            return self.value

    def submit(run_id, project_id, job, parameters, dependencies):
        submitted.append((job["name"], dependencies))
        return Immediate(job["name"])

    monkeypatch.setattr(pipeline_module.run_container_job, "submit", submit)
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: reports.append(args))
    definition = {
        "jobs": [container_job(name="extract", depends_on=[]), container_job(name="train", depends_on=["extract"])],
        "reuse": {"extract": {"rows": 12}},
    }
    outputs = pipeline_module.pipeline_definition.fn("run-r", "prj-1", {}, definition)
    assert [name for name, _ in submitted] == ["train"]
    assert submitted[0][1] == {"extract": {"rows": 12}}
    assert outputs["extract"] == {"rows": 12}
    assert reports[0][1:3] == ("extract", "skipped")


def test_container_output_is_streamed_to_the_log_store(monkeypatch):
    from pipelines import report as report_module

    shipped = []
    monkeypatch.setattr(report_module, "_post_logs", lambda run_id, entries: shipped.extend((run_id, e) for e in entries))
    client = FakeDockerClient([(0, [b"epoch 1\nepoch ", b"2\n", b'{"done": true}'])])
    monkeypatch.setattr(pipeline_module, "_docker_client", lambda: client)
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: None)
    pipeline_module.run_container_job.fn("run-s", "prj-1", container_job(resources={}))
    messages = [entry["message"] for _, entry in shipped]
    assert messages == ["epoch 1", "epoch 2", '{"done": true}']
    assert all(run_id == "run-s" for run_id, _ in shipped)
    assert shipped[0][1]["node"] == "train" and shipped[0][1]["attempt"] == 1


def test_log_streaming_is_capped(monkeypatch):
    from pipelines import report as report_module

    shipped = []
    shipper = report_module.LogShipper("run-c", "n", 1, post=lambda run_id, entries: shipped.extend(entries), interval=60)
    container = FakeContainer(0, [b"x" * 40 + b"\n"] * 10)
    pipeline_module._follow_logs(container, shipper, 100)
    shipper.close()
    assert len(shipped) == 4  # three 41-byte lines exceed 100 bytes, then the notice
    assert "truncated" in shipped[-1]["message"]
