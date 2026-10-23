# Contributing to pipeline-runner

Thank you for helping improve Kionga. Read [ARCHITECTURE.md](ARCHITECTURE.md) for
how the runner executes a pipeline before you change scheduling or container
settings.

## Development setup

Prerequisites:

- Python 3.11, the version of the image and of CI
- Docker with BuildKit, to build the image and to run pipelines for real
- Go, any recent stable release, only to run the gitleaks secret scan locally

```bash
git clone git@github.com:kiongahq/pipeline-runner.git
cd pipeline-runner
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
```

`requirements.txt` is the single dependency list for the image and the tests;
`requirements-dev.txt` adds pytest and ruff. The tests replace the Docker client and
the gateway with fakes, so they need neither Docker nor a running Kionga.

## Run the checks

CI calls the shared Python workflow
([`kiongahq/.github/.github/workflows/python.yml`](https://github.com/kiongahq/.github/blob/main/.github/workflows/python.yml))
with Python 3.11 and `pip install -r requirements-dev.txt`. Run the same steps
locally:

```bash
ruff check .
python -m pytest -q
```

Then the security gates, with the versions CI pins:

```bash
pip install --upgrade pip setuptools "pip-audit==2.10.1"
pip-audit --skip-editable                                    # known vulnerabilities in installed packages
pip install "bandit==1.9.4"
bandit -q -r . -x ./tests,./.venv,./node_modules -ll         # medium and higher static findings
go run github.com/zricethezav/gitleaks/v8@v8.30.1 git --no-banner --redact .   # secrets in the full history
```

## Build the image

```bash
docker build -t ghcr.io/kiongahq/pipeline-runner:dev .
```

On pushes to `main` and on `v*` tags, CI builds the image with the shared image
workflow and publishes `ghcr.io/kiongahq/pipeline-runner` tagged `sha-<short>`,
`latest` (from `main`) and the semantic version (from tags). Pull requests build the
image without publishing it.

To try a change with the whole platform, build the local stack from your checkout
with [deploy](https://github.com/kiongahq/deploy): `make -C ../deploy local-up`
rebuilds the runner image when its sources change.

## Version alignment

MLflow and scikit-learn are pinned in `requirements.txt` to the same versions as
the model server image (`deploy/mlflow/Dockerfile`) and the Jupyter image
(`workspace/jupyter/Dockerfile`), so a model trained here loads where it is served.
Change all three together.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/), as the history
does: `type(scope): summary` in the imperative mood, for example
`feat(pipelines): manual run preflight, structured logs and log export` or
`fix(security): MLflow 3.16.1, Python 3.11`. Common types are `feat`, `fix`,
`docs`, `test`, `ci` and `chore`.

## Pull requests

- Keep one logical change per pull request, with tests for new behavior.
- CI must pass. Do not skip hooks or checks.
- The platform's Go executor (`go/internal/execution` in
  [platform](https://github.com/kiongahq/platform)) implements the same scheduling
  rules. A change to those rules needs a matching platform change; link it.

## Reviewed scanner findings

When bandit flags code that you have reviewed and found safe, annotate the line.
Put the reason in a comment on the line before it, and end the flagged line with
`# nosec <rule>` and nothing after it (bandit reads any text after `nosec` as rule
IDs):

```python
# All interfaces inside the container; an environment variable overrides.
host = os.environ.get("HOST", "0.0.0.0")  # nosec B104
```

If a test needs a fake credential that gitleaks reports, add the finding's
fingerprint (`commit:file:rule:line`, printed by gitleaks) to a `.gitleaksignore`
file at the repository root, with a comment that says why. A fingerprint pins one
exact finding, so any new secret still fails the scan.

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENCE).
