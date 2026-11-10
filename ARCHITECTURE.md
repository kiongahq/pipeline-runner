# Architecture

This document describes how the pipeline runner executes a Kionga pipeline and how
it fits into the platform. Read it before you change scheduling, container settings
or step reporting.

## Bird's-eye view

The runner is a Prefect worker process. `python -m pipelines.serve` registers two
Prefect flows as deployments named `kionga` and executes their runs in-process:

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
  the step's own `environment`, the connection settings in
  `PASSTHROUGH_ENVIRONMENT` (gateway, MLflow and S3 endpoints, no credentials)
  and the operator allowlist `KIONGA_PIPELINE_PASSTHROUGH_ENV`
  (`passthrough_names`). `KIONGA_TOKEN` is never forwarded.
- `_dependency_output` reads a step's output from its logs: the whole output if it
  is JSON, else the last line if that is JSON, else the raw text.
- `pipeline_definition` is the flow. It reuses outputs listed in the definition's
  `reuse` map, which is how a rerun from failure skips steps that already
  succeeded.

### `pipelines/report.py`

`report_step` posts step transitions to `POST /api/v1/pipelines/runs/{id}/steps`;
the gateway recomputes run status and progress. Each report carries an
`Idempotency-Key` header (uuid5 of run, step, attempt and status) and an
`X-Kionga-Report-Seq` header that increases strictly per run (seeded from the wall
clock in microseconds, so it also increases across restarts). Both are headers
because the gateway rejects unknown JSON fields. Transitions of one step are sent
one at a time, in order. 5xx, 429 and transport errors are retried with
exponential backoff and full jitter, up to 15 seconds for `running` and
`KIONGA_PIPELINE_REPORT_DEADLINE_SECONDS` (default 120) for terminal reports;
other 4xx answers are not retried. Terminal reports are written to the outbox
(`KIONGA_PIPELINE_REPORT_OUTBOX`, default `~/.kionga/report-outbox`, files mode
0600, no credentials) before sending and removed once the gateway answers.
`serve.py` replays what is left, oldest first, from a background thread at startup
and every minute, so a run finishes once the gateway is back, without a restart. `reported_step` brackets a block
with running and succeeded or failed reports. `LogShipper` streams container output
to `POST /api/v1/pipelines/runs/{id}/logs` in batches, up to the stream cap.
Reporting is best effort: a gateway outage never fails a run.

### `pipelines/redact.py`

`Redactor` masks known secret values with `***`. Each step attempt builds one from
the runner's credentials and the secret-named values of the step environment
(`_step_redactor`). `LogShipper.add`, `_bounded_logs`, the failure messages,
`report_step` and the flow failure exception all pass through it. Text cut at a
length cap also has a dangling fragment of a secret masked.

### `pipelines/training.py`

The `training-pipeline` flow. Fixed random seeds make its metrics identical in
every environment. MLflow logging and gateway registration run only when
`MLFLOW_TRACKING_URI` and `KIONGA_URL` are set.

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
  kept per step, 100 MiB streamed per step) whatever the environment says. The log
  follower buffers at most 16 KiB of an unfinished line (`MAX_LOG_LINE_BYTES`),
  so a container that never writes a newline cannot exhaust runner memory.
- **Step containers never get the Docker socket.** Only the runner holds it.
- **Step containers never get the runner's credentials.** The service token stays
  in the runner; object-store keys need the step's own `env` or the operator
  allowlist.
- **No secret value leaves the runner.** Everything sent to the gateway or kept in
  Prefect state passes through a `Redactor`.
- **Reporting never fails a run.** Gateway errors are retried, then printed; an
  undelivered terminal report waits in the outbox for the next start.

## How it connects to other components

- **platform**: the gateway submits runs to Prefect's `pipeline-definition/kionga`
  and `training-pipeline/kionga` deployments, and receives step reports and logs.
  `KIONGA_TOKEN` carries the platform's internal service token.
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
