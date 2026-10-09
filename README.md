# pipeline-runner

Executes [Kionga](https://github.com/kiongahq) pipelines. It serves Prefect
deployments that run a pipeline definition (`kionga.dev/v1 Pipeline`, see
[contracts](https://github.com/kiongahq/contracts)) as a DAG of containers, plus the
built-in training pipeline.

How a run executes:

- **Ready-set scheduling**: a node starts as soon as its own dependencies succeed;
  independent branches run in parallel (bounded), `when` conditions skip nodes, and
  nodes downstream of a failure are skipped with the reason.
- **Per node**: timeout, retries with backoff, CPU/memory/GPU limits, all
  capabilities dropped, `no-new-privileges` and a PID limit.
- **Reporting**: start and end time, attempt, exit code, container id and the image
  digest that actually ran go to the platform gateway, and container output streams
  to the gateway's log store with a size cap.
- **Rerun from failure** reuses outputs of nodes that already succeeded.

The runner starts containers through the Docker socket, a root-equivalent trust
boundary meant for a trusted single host. On Kubernetes the
[platform](https://github.com/kiongahq/platform) gateway runs nodes as Jobs instead.

## Image

`ghcr.io/kiongahq/pipeline-runner`, entrypoint `python -m pipelines.serve`.
`requirements.txt` is the single dependency list for the image and the tests; its
ML versions match the model server image so trained models load when served.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MLAIOPS_URL` | | Gateway URL for step reports and logs |
| `MLAIOPS_INTERNAL_TOKEN` | | Service credential for those reports |
| `PREFECT_API_URL` | | Prefect server |
| `MLAIOPS_PIPELINE_JOB_TIMEOUT_SECONDS` | `3600` | Default node timeout |
| `MLAIOPS_PIPELINE_LOG_LIMIT_BYTES` | `65536` | Output tail kept on the node |
| `MLAIOPS_PIPELINE_LOG_STREAM_LIMIT_BYTES` | 5 MiB | Streamed output cap per node |
| `MLAIOPS_PIPELINE_PIDS_LIMIT` | `256` | PID limit per node container |

## Develop

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -q && ruff check .
docker build -t ghcr.io/kiongahq/pipeline-runner:dev .
```
