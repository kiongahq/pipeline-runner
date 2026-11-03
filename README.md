# pipeline-runner

[![CI](https://github.com/kiongahq/pipeline-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/kiongahq/pipeline-runner/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENCE)

Runs Kionga pipelines as graphs of containers on a single host, through Prefect.

## Background

Kionga is a self-hosted platform for machine learning, data and AI agents. A
pipeline in Kionga is a versioned graph of container steps (see
[contracts](https://github.com/kiongahq/contracts)). When you start a run, the
[platform](https://github.com/kiongahq/platform) gateway validates the definition
and submits it to Prefect; this runner picks the run up and executes it.

Each step starts as soon as its own dependencies succeed, with parallel branches,
conditions, retries with backoff, timeouts and CPU, memory and GPU limits. Step
status, exit codes, image digests and container output stream back to the gateway,
so you follow the run live in the console. The runner also serves the built-in
scikit-learn training pipeline.

The runner starts containers through the Docker socket, which is root-equivalent
on the host, so it is meant for a single trusted host. On Kubernetes the gateway
runs steps as Jobs instead.

## Install

The image is published as `ghcr.io/kiongahq/pipeline-runner`. To build it:

```bash
git clone git@github.com:kiongahq/pipeline-runner.git
cd pipeline-runner
docker build -t ghcr.io/kiongahq/pipeline-runner:dev .
```

## Usage

The [deploy](https://github.com/kiongahq/deploy) repository runs the runner with
Prefect, MLflow and the gateway. To run it on its own, point it at a Prefect server
and a gateway:

```bash
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e PREFECT_API_URL=http://prefect-server:4200/api \
  -e MLAIOPS_URL=http://gateway:8080 \
  -e MLAIOPS_TOKEN="$MLAIOPS_INTERNAL_TOKEN" \
  ghcr.io/kiongahq/pipeline-runner:dev
```

It registers the `pipeline-definition` and `training-pipeline` flows as
deployments named `mlaiops` and executes their runs. Optional limits:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MLAIOPS_PIPELINE_NETWORK` | none | Docker network that step containers join |
| `MLAIOPS_PIPELINE_JOB_TIMEOUT_SECONDS` | `3600` | Step timeout when the step sets none (at most 24 hours) |
| `MLAIOPS_PIPELINE_MAX_PARALLEL` | `8` | Steps running at once in one run (at most `64`) |
| `MLAIOPS_PIPELINE_PIDS_LIMIT` | `256` | Process limit per step container (at most `4096`) |
| `MLAIOPS_PIPELINE_LOG_LIMIT_BYTES` | `65536` | Output tail kept on each step (at most 1 MiB) |
| `MLAIOPS_PIPELINE_LOG_STREAM_LIMIT_BYTES` | 5 MiB | Output streamed to the gateway per step (at most 100 MiB). Lines longer than 16 KiB are cut and marked `[line truncated]`; `\r` progress updates are separate lines |
| `MLAIOPS_PIPELINE_REPORT_DEADLINE_SECONDS` | `120` | How long a terminal step report is retried while the gateway fails (at most 3600) |
| `MLAIOPS_PIPELINE_REPORT_OUTBOX` | `~/.kionga/report-outbox` | Directory for terminal step reports not yet delivered, replayed at startup; empty disables it. Mount a volume here to keep reports across container re-creation |
| `MLAIOPS_PIPELINE_PASSTHROUGH_ENV` | none | Comma-separated runner variables forwarded to every step container, for example `AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY` |

### Step credentials

Step containers run images named in pipeline definitions, so they do not inherit
the runner's credentials. Every step receives `KIONGA_RUN_ID`, `KIONGA_PROJECT_ID`,
`KIONGA_STEP_NAME`, `KIONGA_PARAMETERS`, `KIONGA_DEPENDENCY_OUTPUTS`, its own
`env` from the definition, and these connection settings when the runner has
them: `MLAIOPS_URL`, `MLFLOW_TRACKING_URI`, `MLFLOW_S3_ENDPOINT_URL` and
`S3_ENDPOINT`.

The runner's object-store keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`) reach a step only when it declares them in its own `env`, or
when an operator lists them in `MLAIOPS_PIPELINE_PASSTHROUGH_ENV`. The allowlist
applies to every step on the runner, so use it only on a trusted single-user
host and prefer keys scoped to the buckets steps need. `MLAIOPS_TOKEN`, the
platform's internal service token, is never forwarded, even when listed.

Before step output leaves the runner, every value that must not be stored is
replaced with `***`: in streamed log lines, the output tail, step messages and the
flow failure message. The masked values are the runner's credentials and the
values of step `env` variables whose names look secret (`*TOKEN*`, `*SECRET*`,
`*PASSWORD*`, `*_KEY`, `*_KEY_ID`, `*CREDENTIAL*`, `*PRIVATE*`). Values shorter
than 6 characters are not masked. Redaction matches exact values, so a step that
prints an encoded or transformed secret is not covered; keep secrets out of
output.

## Documentation

- Product documentation: [Pipelines](https://kiongahq.github.io/docs/guides/pipelines/)
- Engineering handbook: [Execution and serving](https://kiongahq.github.io/docs/engineering/components/execution/)
- How this repository is laid out: [ARCHITECTURE.md](ARCHITECTURE.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup, the checks CI
runs and the commit conventions.

## License

[MIT](LICENCE) © Kionga contributors
