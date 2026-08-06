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
from typing import Any

from prefect import flow, task

from .report import report_step

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


class ContainerJobError(RuntimeError):
    """A container attempt failed after it was created."""


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


def _run_container_attempt(client: Any, kwargs: dict[str, Any]) -> str:
    container = None
    logs = ""
    try:
        container = _create_container(client, kwargs)
        container.start()
        timeout = _bounded_setting(
            "MLAIOPS_PIPELINE_JOB_TIMEOUT_SECONDS", DEFAULT_JOB_TIMEOUT_SECONDS, 24 * 60 * 60
        )
        result = container.wait(timeout=timeout)
        logs = _bounded_logs(
            container,
            _bounded_setting(
                "MLAIOPS_PIPELINE_LOG_LIMIT_BYTES",
                DEFAULT_LOG_LIMIT_BYTES,
                MAX_LOG_LIMIT_BYTES,
            ),
        )
        status = int((result or {}).get("StatusCode", 1))
        if status != 0:
            raise ContainerJobError(f"container exited with status {status}: {logs}")
        return logs
    except ContainerJobError:
        raise
    except Exception as error:
        if container is not None and not logs:
            try:
                logs = _bounded_logs(
                    container,
                    _bounded_setting(
                        "MLAIOPS_PIPELINE_LOG_LIMIT_BYTES",
                        DEFAULT_LOG_LIMIT_BYTES,
                        MAX_LOG_LIMIT_BYTES,
                    ),
                )
            except Exception:
                logs = ""
        detail = f": {logs}" if logs else ""
        raise ContainerJobError(f"container execution failed: {error}{detail}") from error
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception as error:
                print(f"container cleanup failed: {error}")


@task(name="pipeline-container-job")
def run_container_job(
    run_id: str,
    project_id: str,
    job: dict[str, Any],
    parameters: dict[str, Any] | None = None,
    dependency_outputs: dict[str, Any] | None = None,
) -> Any:
    """Run one definition job, including its job-specific retry policy."""
    parameters = parameters or {}
    dependency_outputs = dependency_outputs or {}
    retries = max(int(job.get("retries", 0) or 0), 0)
    attempts = retries + 1
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
            )
            try:
                logs = _run_container_attempt(client, kwargs)
            except Exception as error:
                last_error = error
                if attempt < attempts:
                    continue
                message = str(error)[-_bounded_setting(
                    "MLAIOPS_PIPELINE_LOG_LIMIT_BYTES",
                    DEFAULT_LOG_LIMIT_BYTES,
                    MAX_LOG_LIMIT_BYTES,
                ) :]
                report_step(run_id, job["name"], "failed", message)
                raise
            output = _dependency_output(logs)
            report_step(run_id, job["name"], "succeeded", logs or "container completed")
            return output
    finally:
        client.close()
    raise ContainerJobError(f"container job failed: {last_error}")


@flow(name="pipeline-definition")
def pipeline_definition(
    run_id: str = "",
    project_id: str = "",
    parameters: dict[str, Any] | None = None,
    definition: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute all ready jobs in each validated DAG layer concurrently."""
    definition = definition or {}
    parameters = parameters or {}
    layers = definition_layers(definition)
    outputs: dict[str, Any] = {}
    for layer in layers:
        futures = []
        for job in layer:
            dependencies = {name: outputs[name] for name in job["depends_on"]}
            future = run_container_job.submit(
                run_id,
                project_id,
                job,
                parameters,
                dependencies,
            )
            futures.append((job["name"], future))

        failures: list[str] = []
        for name, future in futures:
            try:
                outputs[name] = future.result()
            except Exception as error:
                failures.append(f"{name}: {error}")
        if failures:
            raise ContainerJobError("pipeline layer failed: " + "; ".join(failures))
    return outputs
