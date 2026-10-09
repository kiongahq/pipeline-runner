"""Execute validated container pipeline definitions through Docker and Prefect.

The gateway remains authoritative for definition validation and resource admission.
This Compose-local executor preserves that DAG, starts one Docker container per job,
and passes parameters plus completed dependency outputs through bounded JSON
environment variables. Kubernetes deployments use the platform scale-path executor
instead of this Docker-socket implementation.

Mounting the Docker socket is a deliberate root-equivalent trust boundary for the
local/single-operator profile, not a multi-tenant isolation mechanism. Spawned jobs
never receive the socket, drop Linux capabilities, use no-new-privileges and a PID
limit, but retain a writable container filesystem for checkpoints and generated
artifacts.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from prefect import flow, task

from .report import LogShipper, report_step

DEFAULT_LOG_LIMIT_BYTES = 64 * 1024
MAX_LOG_LIMIT_BYTES = 1024 * 1024
DEFAULT_JOB_TIMEOUT_SECONDS = 60 * 60
DEFAULT_PIDS_LIMIT = 256
MAX_PIDS_LIMIT = 4096
DEFAULT_JOB_CPU = "500m"
DEFAULT_JOB_MEMORY = "1Gi"

PASSTHROUGH_ENVIRONMENT = (
    "MLAIOPS_URL",
    "MLAIOPS_TOKEN",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_S3_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "S3_ENDPOINT",
)


DEFAULT_LOG_STREAM_LIMIT_BYTES = 5 * 1024 * 1024
MAX_LOG_STREAM_LIMIT_BYTES = 100 * 1024 * 1024
DEFAULT_MAX_PARALLEL = 8
MAX_PARALLEL = 64


class ContainerJobError(RuntimeError):
    """A container attempt failed after it was created."""

    def __init__(self, message: str, exit_code: int | None = None, workload_id: str = ""):
        super().__init__(message)
        self.exit_code = exit_code
        self.workload_id = workload_id


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def definition_layers(definition: dict[str, Any]) -> list[list[dict[str, Any]]]:
    """Return deterministic dependency layers after defensive graph validation."""
    jobs = definition.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("pipeline definition requires at least one job")

    ordered: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    for value in jobs:
        if not isinstance(value, dict):
            raise ValueError("pipeline jobs must be objects")
        job = dict(value)
        name = str(job.get("name", "")).strip()
        if not name or name in by_name:
            raise ValueError("pipeline job names must be non-empty and unique")
        if job.get("kind") != "container" or not str(job.get("image", "")).strip():
            raise ValueError(f"pipeline job {name} must be a container with an image")
        dependencies = job.get("depends_on") or []
        if not isinstance(dependencies, list) or any(not isinstance(item, str) for item in dependencies):
            raise ValueError(f"pipeline job {name} has invalid dependencies")
        job["name"], job["depends_on"] = name, dependencies
        ordered.append(job)
        by_name[name] = job

    for job in ordered:
        if any(name == job["name"] or name not in by_name for name in job["depends_on"]):
            raise ValueError(f"pipeline job {job['name']} references an invalid dependency")

    remaining = {job["name"] for job in ordered}
    completed: set[str] = set()
    layers: list[list[dict[str, Any]]] = []
    while remaining:
        layer = [
            job
            for job in ordered
            if job["name"] in remaining and set(job["depends_on"]).issubset(completed)
        ]
        if not layer:
            raise ValueError("pipeline definition contains a dependency cycle")
        layers.append(layer)
        for job in layer:
            remaining.remove(job["name"])
            completed.add(job["name"])
    return layers


def _docker_client():
    # Docker is a runtime-only dependency of the pipeline-runner image. Keeping
    # this import lazy lets SDK/unit-test installations exercise the compiler and
    # orchestration logic without installing or contacting a daemon.
    import docker

    return docker.from_env()


def _gpu_device_requests(count: int) -> list[Any]:
    if count <= 0:
        return []
    from docker.types import DeviceRequest

    return [DeviceRequest(count=count, capabilities=[["gpu"]])]


def _cpu_nanos(value: str) -> int | None:
    if not value:
        return None
    if value.endswith("m") and value[:-1].isdigit():
        return int(value[:-1]) * 1_000_000
    if value.isdigit():
        return int(value) * 1_000_000_000
    raise ValueError(f"invalid CPU quantity {value!r}")


def _memory_bytes(value: str) -> int | None:
    if not value:
        return None
    match = re.fullmatch(r"([1-9][0-9]*)(Mi|Gi)?", value)
    if not match:
        raise ValueError(f"invalid memory quantity {value!r}")
    amount, suffix = int(match.group(1)), match.group(2)
    multiplier = {None: 1, "Mi": 1 << 20, "Gi": 1 << 30}[suffix]
    return amount * multiplier


def _bounded_setting(name: str, default: int, maximum: int) -> int:
    try:
        configured = int(os.environ.get(name, str(default)))
    except ValueError:
        configured = default
    return min(max(configured, 1), maximum)


def _bounded_logs(container: Any, limit: int) -> str:
    stream = container.logs(stdout=True, stderr=True, stream=True, follow=False)
    if isinstance(stream, (bytes, bytearray, str)):
        stream = [stream]
    tail = bytearray()
    for chunk in stream:
        raw = chunk.encode() if isinstance(chunk, str) else bytes(chunk)
        if len(raw) >= limit:
            tail = bytearray(raw[-limit:])
        else:
            tail.extend(raw)
            if len(tail) > limit:
                del tail[: len(tail) - limit]
    return tail.decode("utf-8", errors="replace").strip()


def _dependency_output(logs: str) -> Any:
    if not logs:
        return ""
    for candidate in (logs, logs.splitlines()[-1]):
        try:
            return json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            pass
    return logs


def _container_environment(
    run_id: str,
    project_id: str,
    job: dict[str, Any],
    parameters: dict[str, Any],
    dependency_outputs: dict[str, Any],
) -> dict[str, str]:
    environment = {name: os.environ[name] for name in PASSTHROUGH_ENVIRONMENT if name in os.environ}
    environment.update({str(key): str(value) for key, value in (job.get("environment") or {}).items()})
    environment.update(
        {
            "KIONGA_RUN_ID": run_id,
            "KIONGA_PROJECT_ID": project_id,
            "KIONGA_STEP_NAME": job["name"],
            "KIONGA_PARAMETERS": json.dumps(parameters, separators=(",", ":"), default=str),
            "KIONGA_DEPENDENCY_OUTPUTS": json.dumps(
                dependency_outputs, separators=(",", ":"), default=str
            ),
        }
    )
    return environment


def _container_kwargs(
    run_id: str,
    project_id: str,
    job: dict[str, Any],
    parameters: dict[str, Any],
    dependency_outputs: dict[str, Any],
) -> dict[str, Any]:
    resources = job.get("resources") or {}
    kwargs: dict[str, Any] = {
        "image": job["image"],
        "command": job.get("command") or None,
        "detach": True,
        "environment": _container_environment(
            run_id, project_id, job, parameters, dependency_outputs
        ),
        "labels": {
            "io.kionga.pipeline.run-id": run_id,
            "io.kionga.pipeline.step": job["name"],
            "io.kionga.pipeline.project-id": project_id,
        },
        "cap_drop": ["ALL"],
        "security_opt": ["no-new-privileges"],
        "pids_limit": _bounded_setting(
            "MLAIOPS_PIPELINE_PIDS_LIMIT", DEFAULT_PIDS_LIMIT, MAX_PIDS_LIMIT
        ),
        "init": True,
    }
    if network := os.environ.get("MLAIOPS_PIPELINE_NETWORK"):
        kwargs["network"] = network
    if cpu := _cpu_nanos(str(resources.get("cpu") or DEFAULT_JOB_CPU)):
        kwargs["nano_cpus"] = cpu
    if memory := _memory_bytes(str(resources.get("memory") or DEFAULT_JOB_MEMORY)):
        kwargs["mem_limit"] = memory
    if device_requests := _gpu_device_requests(int(resources.get("gpu", 0) or 0)):
        kwargs["device_requests"] = device_requests
    return kwargs


def _create_container(client: Any, kwargs: dict[str, Any]):
    try:
        return client.containers.create(**kwargs)
    except Exception as error:
        try:
            from docker.errors import ImageNotFound
        except ImportError:
            raise error
        if not isinstance(error, ImageNotFound):
            raise
        client.images.pull(kwargs["image"])
        return client.containers.create(**kwargs)


def _image_digest(container: Any) -> str:
    """Best-effort immutable identity of the image a container ran."""
    image = getattr(container, "image", None)
    attrs = getattr(image, "attrs", None) or {}
    digests = attrs.get("RepoDigests") or []
    if digests:
        return str(digests[0])
    return str(getattr(image, "id", "") or "")


def _follow_logs(container: Any, shipper: LogShipper, limit: int) -> None:
    """Ship container output line by line until it exits or the cap is hit."""
    sent, partial = 0, b""
    try:
        for chunk in container.logs(stdout=True, stderr=True, stream=True, follow=True):
            data = partial + (chunk.encode() if isinstance(chunk, str) else bytes(chunk))
            *lines, partial = data.split(b"\n")
            for line in lines:
                if sent >= limit:
                    shipper.add(f"[log output truncated after {limit} bytes; the full tail is in the step message]")
                    return
                sent += len(line) + 1
                shipper.add(line.decode("utf-8", errors="replace").rstrip("\r"))
        if partial and sent < limit:
            shipper.add(partial.decode("utf-8", errors="replace"))
    except Exception as error:  # streaming is best-effort
        shipper.add(f"[log streaming stopped: {error}]")


def _run_container_attempt(client: Any, kwargs: dict[str, Any], timeout: int, run_id: str = "", node: str = "", attempt: int = 1) -> tuple[str, dict[str, Any]]:
    container = None
    logs = ""
    facts: dict[str, Any] = {"workload_kind": "docker-container"}
    limit = _bounded_setting("MLAIOPS_PIPELINE_LOG_LIMIT_BYTES", DEFAULT_LOG_LIMIT_BYTES, MAX_LOG_LIMIT_BYTES)
    shipper = None
    follower = None
    try:
        container = _create_container(client, kwargs)
        facts["workload_id"] = str(getattr(container, "id", "") or "")[:12]
        container.start()
        facts["image_digest"] = _image_digest(container)
        if run_id:
            shipper = LogShipper(run_id, node, attempt, facts["workload_id"])
            stream_limit = _bounded_setting("MLAIOPS_PIPELINE_LOG_STREAM_LIMIT_BYTES", DEFAULT_LOG_STREAM_LIMIT_BYTES, MAX_LOG_STREAM_LIMIT_BYTES)
            follower = threading.Thread(target=_follow_logs, args=(container, shipper, stream_limit), daemon=True)
            follower.start()
        result = container.wait(timeout=timeout)
        logs = _bounded_logs(container, limit)
        status = int((result or {}).get("StatusCode", 1))
        facts["exit_code"] = status
        if status != 0:
            raise ContainerJobError(f"container exited with status {status}: {logs}", status, facts["workload_id"])
        return logs, facts
    except ContainerJobError:
        raise
    except Exception as error:
        if container is not None and not logs:
            try:
                logs = _bounded_logs(container, limit)
            except Exception:
                logs = ""
        detail = f": {logs}" if logs else ""
        reason = "timed out" if "timed out" in str(error).lower() or "read timeout" in str(error).lower() else "failed"
        raise ContainerJobError(
            f"container execution {reason} after {timeout}s: {error}{detail}" if reason == "timed out" else f"container execution failed: {error}{detail}",
            None,
            facts.get("workload_id", ""),
        ) from error
    finally:
        if follower is not None:
            follower.join(timeout=5)
        if shipper is not None:
            shipper.close()
        if container is not None:
            try:
                container.remove(force=True)
            except Exception as error:
                print(f"container cleanup failed: {error}")


def _job_timeout(job: dict[str, Any]) -> int:
    configured = int(job.get("timeout_seconds") or 0)
    if configured > 0:
        return min(configured, 24 * 60 * 60)
    return _bounded_setting("MLAIOPS_PIPELINE_JOB_TIMEOUT_SECONDS", DEFAULT_JOB_TIMEOUT_SECONDS, 24 * 60 * 60)


@task(name="pipeline-container-job")
def run_container_job(
    run_id: str,
    project_id: str,
    job: dict[str, Any],
    parameters: dict[str, Any] | None = None,
    dependency_outputs: dict[str, Any] | None = None,
) -> Any:
    """Run one definition job with its timeout, retry and backoff policy."""
    parameters = parameters or {}
    dependency_outputs = dependency_outputs or {}
    retries = max(int(job.get("retries", 0) or 0), 0)
    backoff = max(int(job.get("retry_backoff_seconds", 0) or 0), 0)
    attempts = retries + 1
    timeout = _job_timeout(job)
    limit = _bounded_setting("MLAIOPS_PIPELINE_LOG_LIMIT_BYTES", DEFAULT_LOG_LIMIT_BYTES, MAX_LOG_LIMIT_BYTES)
    client = _docker_client()
    last_error: Exception | None = None
    try:
        kwargs = _container_kwargs(run_id, project_id, job, parameters, dependency_outputs)
        for attempt in range(1, attempts + 1):
            report_step(
                run_id,
                job["name"],
                "running",
                f"container {job['image']} attempt {attempt}/{attempts}",
                attempt=attempt,
                at=_now(),
            )
            try:
                logs, facts = _run_container_attempt(client, kwargs, timeout, run_id, job["name"], attempt)
            except ContainerJobError as error:
                last_error = error
                if attempt < attempts:
                    if backoff:
                        _sleep(backoff)
                    continue
                report_step(
                    run_id, job["name"], "failed", str(error)[-limit:],
                    attempt=attempt, exit_code=error.exit_code,
                    workload_kind="docker-container", workload_id=error.workload_id or None, at=_now(),
                )
                raise
            report_step(run_id, job["name"], "succeeded", logs or "container completed", attempt=attempt, at=_now(), **facts)
            return _dependency_output(logs)
    finally:
        client.close()
    raise ContainerJobError(f"container job failed: {last_error}")


class Handle(Protocol):
    def done(self) -> bool: ...
    def result(self) -> Any: ...


def condition_met(job: dict[str, Any], parameters: dict[str, Any]) -> bool:
    """`when: {param, equals}` runs the node only if the parameter matches."""
    condition = job.get("when")
    if not condition:
        return True
    return str(parameters.get(condition.get("param"), "")) == str(condition.get("equals", ""))


def execute_ready_set(
    jobs: list[dict[str, Any]],
    parameters: dict[str, Any],
    start: Callable[[dict[str, Any], dict[str, Any]], Handle],
    report: Callable[..., None],
    max_parallel: int = DEFAULT_MAX_PARALLEL,
    poll: Callable[[], None] = lambda: _sleep(0.2),
) -> tuple[dict[str, Any], dict[str, str]]:
    """Run a validated DAG, starting each node as soon as its own dependencies
    have succeeded (not when a whole layer finishes).

    Skipped nodes (unmet `when`) count as satisfied for their dependents and
    output None. Nodes downstream of a failure are skipped and reported.
    Returns (outputs, failures).
    """
    by_name = {job["name"]: job for job in jobs}
    order = [job["name"] for layer in definition_layers({"jobs": jobs}) for job in layer]
    pending = list(order)
    running: dict[str, Handle] = {}
    outputs: dict[str, Any] = {}
    failures: dict[str, str] = {}
    satisfied: set[str] = set()
    blocked: set[str] = set()

    while pending or running:
        progressed = False
        for name in list(pending):
            job = by_name[name]
            dependencies = job["depends_on"]
            failed_upstream = [dep for dep in dependencies if dep in failures or dep in blocked]
            if failed_upstream:
                pending.remove(name)
                blocked.add(name)
                report(name, "skipped", f"not run: upstream {', '.join(failed_upstream)} did not succeed")
                progressed = True
                continue
            if not all(dep in satisfied for dep in dependencies):
                continue
            if not condition_met(job, parameters):
                pending.remove(name)
                satisfied.add(name)
                outputs[name] = None
                condition = job["when"]
                report(name, "skipped", f"condition not met: {condition['param']} != {condition['equals']}")
                progressed = True
                continue
            if len(running) >= max_parallel:
                break
            pending.remove(name)
            running[name] = start(job, {dep: outputs.get(dep) for dep in dependencies})
            progressed = True
        for name, handle in list(running.items()):
            if not handle.done():
                continue
            del running[name]
            progressed = True
            try:
                outputs[name] = handle.result()
                satisfied.add(name)
            except Exception as error:  # the job already reported its failure
                failures[name] = str(error)
        if not progressed:
            if not running:
                raise ValueError(f"nodes can never start: {', '.join(pending)}")
            poll()
    return outputs, failures


class _ValueHandle:
    """A node whose output was reused from an earlier run."""

    def __init__(self, value: Any):
        self.value = value

    def done(self) -> bool:
        return True

    def result(self) -> Any:
        return self.value


class _PrefectHandle:
    def __init__(self, future: Any):
        self.future = future

    def done(self) -> bool:
        state = getattr(self.future, "state", None)
        if state is None:
            return True
        return bool(state.is_final())

    def result(self) -> Any:
        return self.future.result()


@flow(name="pipeline-definition")
def pipeline_definition(
    run_id: str = "",
    project_id: str = "",
    parameters: dict[str, Any] | None = None,
    definition: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a validated DAG with ready-set scheduling and bounded parallelism."""
    definition = definition or {}
    parameters = parameters or {}
    jobs = [job for layer in definition_layers(definition) for job in layer]
    max_parallel = int(definition.get("max_parallelism") or 0) or _bounded_setting(
        "MLAIOPS_PIPELINE_MAX_PARALLEL", DEFAULT_MAX_PARALLEL, MAX_PARALLEL
    )

    reuse = definition.get("reuse") or {}

    def start(job: dict[str, Any], dependencies: dict[str, Any]) -> Handle:
        if job["name"] in reuse:
            report_step(run_id, job["name"], "skipped", "Reused the output of the previous run", at=_now())
            return _ValueHandle(reuse[job["name"]])
        return _PrefectHandle(run_container_job.submit(run_id, project_id, job, parameters, dependencies))

    def report(name: str, status: str, message: str) -> None:
        report_step(run_id, name, status, message, at=_now())

    outputs, failures = execute_ready_set(jobs, parameters, start, report, min(max_parallel, MAX_PARALLEL))
    if failures:
        raise ContainerJobError("pipeline failed: " + "; ".join(f"{name}: {error}" for name, error in failures.items()))
    return outputs
