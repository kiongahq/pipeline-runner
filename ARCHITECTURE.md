# Architecture

This document describes how the pipeline runner executes a Kionga pipeline and how
it fits into the platform. Read it before you change scheduling, container settings
or step reporting.

## Bird's-eye view

The runner is a Prefect worker process. `python -m pipelines.serve` registers two
Prefect flows as deployments named `mlaiops` and executes their runs in-process:

- `pipeline-definition` runs a user's pipeline: a validated graph of container
  steps, one Docker container per step attempt.
- `training-pipeline` is the built-in scikit-learn example: validate, train,
  evaluate, then log to MLflow and register the model with the gateway.

The gateway is the authority. It validates the definition, checks quotas and
admission, and submits the run to Prefect with the definition in API form (snake
case, `jobs` instead of `nodes`). The runner re-checks the graph defensively,
executes it and reports every transition back to the gateway.

## Code map

### `pipelines/serve.py`

The entry point. Calls Prefect's `serve` with both flows' deployments.

### `pipelines/definition.py`

The container executor.

- `definition_layers` validates the job graph (at least one job, unique names,
  every job a container with an image, no unknown dependencies, no cycles) and
  returns deterministic dependency layers.
- `execute_ready_set` is the scheduler. A job starts as soon as its own
  dependencies succeed, up to `max_parallel` at once. A job whose `when` condition
  is false is skipped and its dependents still run (`condition_met`). Jobs
  downstream of a failure are skipped with the reason, and unrelated branches keep
  running.
- `run_container_job` (Prefect task `pipeline-container-job`) runs one job with its
  retries, backoff and timeout (`_job_timeout`). Each attempt creates a container,
  follows its logs, records the exit code and the image digest that ran, and always
  removes the container.
- `_container_kwargs` builds the Docker settings: CPU and memory limits (defaults
  `500m` and `1Gi`), GPU device requests, all capabilities dropped,
  `no-new-privileges`, a PID limit, labels and the optional network.
- `_container_environment` gives each step `KIONGA_RUN_ID`, `KIONGA_PROJECT_ID`,
  `KIONGA_STEP_NAME`, `KIONGA_PARAMETERS` and `KIONGA_DEPENDENCY_OUTPUTS` (JSON),
  the step's own `environment`, and a fixed passthrough list
  (`PASSTHROUGH_ENVIRONMENT`: gateway URL and token, MLflow and S3 settings).
- `_dependency_output` reads a step's output from its logs: the whole output if it
  is JSON, else the last line if that is JSON, else the raw text.
- `pipeline_definition` is the flow. It reuses outputs listed in the definition's
  `reuse` map, which is how a rerun from failure skips steps that already
  succeeded.

### `pipelines/report.py`

`report_step` posts step transitions to `POST /api/v1/pipelines/runs/{id}/steps`;
the gateway recomputes run status and progress. `reported_step` brackets a block
with running and succeeded or failed reports. `LogShipper` streams container output
to `POST /api/v1/pipelines/runs/{id}/logs` in batches, up to the stream cap.
Reporting is best effort: a gateway outage never fails a run.

### `pipelines/training.py`

The `training-pipeline` flow. Fixed random seeds make its metrics identical in
every environment. MLflow logging and gateway registration run only when
`MLFLOW_TRACKING_URI` and `MLAIOPS_URL` are set.

### `tests/`

`test_pipeline_definition.py` drives the executor with a fake Docker client:
layering and cycles, retries, cleanup, limits, the ready-set rules, output passing,
reuse and log caps. `test_pipelines.py` covers the training tasks and reporting.

## Invariants

- **Same rules as the Go executor.** The platform's `go/internal/execution`
  implements the same ready-set semantics for the Kubernetes path. The two must
  agree on start, skip and failure behavior.
- **Every container is removed** after its attempt, whether it succeeded, failed or
  timed out.
- **Bounded everything.** Parallelism, timeouts, PID limits and log sizes are
  clamped to fixed maximums (64 parallel steps, 24 hours, 4096 processes, 1 MiB
  kept per step, 100 MiB streamed per step) whatever the environment says.
- **Step containers never get the Docker socket.** Only the runner holds it.
- **Reporting never fails a run.** Gateway errors are printed and ignored.

## How it connects to other components

- **platform**: the gateway submits runs to Prefect's `pipeline-definition/mlaiops`
  and `training-pipeline/mlaiops` deployments, and receives step reports and logs.
  `MLAIOPS_TOKEN` carries the platform's internal service token.
- **contracts**: defines the YAML users write. The runner sees only the gateway's
  converted form.
- **deploy**: `compose.yaml` runs the runner as root with the Docker socket
  mounted, next to `prefect-server` and `mlflow`.
- **serving and workspace**: share the MLflow and scikit-learn pins, so models
  trained here load in the model server and in notebooks.

## Cross-cutting concerns

- **Trust boundary.** The Docker socket is root-equivalent. This runner is for a
  single trusted host; multi-tenant installations run steps as Kubernetes Jobs.
- **Security gates.** CI runs ruff, pytest, pip-audit, bandit and gitleaks through
  the shared Python workflow. See [CONTRIBUTING.md](CONTRIBUTING.md).
