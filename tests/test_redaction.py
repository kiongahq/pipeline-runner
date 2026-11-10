"""Secret values never leave the runner in log entries, step messages or the flow
failure. Fake secrets are assembled at runtime so secret scanners never see a
literal credential."""

from __future__ import annotations

import json

import pytest

from pipelines import definition as pipeline_module
from pipelines import report as report_module
from pipelines.redact import MASK, MIN_SECRET_LENGTH, Redactor, is_secret_name, secret_values
from tests.test_pipeline_definition import FakeContainer, FakeDockerClient, container_job


def fake(label: str) -> str:
    return "".join(["abc123", label, "secret"])


@pytest.mark.parametrize(
    "name",
    ["KIONGA_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "DB_PASSWORD", "PASSWORD",
     "OPENAI_API_KEY", "CLIENT_SECRET_VALUE", "GITHUB_TOKEN", "SERVICE_CREDENTIALS", "KEY"],
)
def test_secret_name_patterns_match(name):
    assert is_secret_name(name)


@pytest.mark.parametrize("name", ["MODE", "MLFLOW_TRACKING_URI", "KEYBOARD_LAYOUT", "EPOCHS", "AUTHOR"])
def test_ordinary_names_are_not_secret(name):
    assert not is_secret_name(name)


def test_short_values_are_never_masked():
    short = "x" * (MIN_SECRET_LENGTH - 1)
    redactor = Redactor([short, fake("long")])
    assert redactor.redact(f"{short} {fake('long')}") == f"{short} {MASK}"
    assert not Redactor([short])


def test_secret_values_reads_secret_names_only():
    values = secret_values({"MODE": "batch-mode", "API_TOKEN": fake("t"), "AWS_ACCESS_KEY_ID": fake("id")})
    assert sorted(values) == sorted([fake("t"), fake("id")])


def test_longer_secret_containing_a_shorter_one_is_masked_whole():
    inner = fake("in")
    redactor = Redactor([inner, inner + "-suffix"])
    assert redactor.redact(f"value={inner}-suffix") == f"value={MASK}"


def test_partial_edges_mask_cut_fragments():
    secret = fake("edge")
    redactor = Redactor([secret])
    assert redactor.redact("line " + secret[:9], partial_end=True) == "line " + MASK
    assert redactor.redact(secret[-9:] + " tail", partial_start=True) == MASK + " tail"
    # Fragments shorter than the minimum are left alone (no false positives).
    assert redactor.redact("line " + secret[:3], partial_end=True) == "line " + secret[:3]


def _run_printing_environment(monkeypatch, status: int, chunks: list[bytes], job=None):
    shipped, reports = [], []
    monkeypatch.setattr(report_module, "_post_logs", lambda run_id, entries: shipped.extend(entries))
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: reports.append((args, facts)))
    client = FakeDockerClient([(status, chunks)])
    monkeypatch.setattr(pipeline_module, "_docker_client", lambda: client)
    error = None
    try:
        pipeline_module.run_container_job.fn("run-red", "prj-1", job or container_job(resources={}))
    except pipeline_module.ContainerJobError as caught:
        error = caught
    return shipped, reports, error


def test_runner_token_printed_by_a_step_is_masked_in_logs_and_success_message(monkeypatch):
    token = fake("runner")
    monkeypatch.setenv("KIONGA_TOKEN", token)
    shipped, reports, error = _run_printing_environment(
        monkeypatch, 0, [f"KIONGA_TOKEN={token}\n".encode(), b"done\n"]
    )
    assert error is None
    messages = [entry["message"] for entry in shipped]
    assert messages == [f"KIONGA_TOKEN={MASK}", "done"]
    final_args, _ = reports[-1]
    assert final_args[2] == "succeeded"
    assert token not in final_args[3] and MASK in final_args[3]


def test_step_environment_secret_is_masked_in_the_failure_message(monkeypatch):
    secret = fake("job")
    job = container_job(resources={}, environment={"DB_PASSWORD": secret, "MODE": "batch-mode"})
    shipped, reports, error = _run_printing_environment(
        monkeypatch, 3, [f"connecting with {secret} in batch-mode\n".encode()], job
    )
    assert error is not None and secret not in str(error)
    final_args, _ = reports[-1]
    assert final_args[2] == "failed"
    assert secret not in final_args[3] and MASK in final_args[3]
    assert "batch-mode" in final_args[3]  # non-secret values stay readable
    assert all(secret not in entry["message"] for entry in shipped)


def test_secret_split_across_two_log_chunks_is_masked(monkeypatch):
    token = fake("split")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", token)
    line = f"key={token}\n".encode()
    middle = len(line) // 2
    shipped, reports, _ = _run_printing_environment(monkeypatch, 0, [line[:middle], line[middle:]])
    assert [entry["message"] for entry in shipped] == [f"key={MASK}"]
    assert token not in reports[-1][0][3]


def test_bounded_tail_masks_a_secret_cut_by_the_front_of_the_tail():
    secret = fake("tail")
    container = FakeContainer(0, [f"x={secret} end".encode()])
    # Keep the last bytes only: the tail starts in the middle of the secret.
    tail = pipeline_module._bounded_logs(container, len(secret) - 4 + len(" end"), Redactor([secret]))
    assert tail == MASK + " end"


def test_flow_failure_exception_is_redacted(monkeypatch):
    secret = fake("flow")
    monkeypatch.setenv("KIONGA_TOKEN", secret)

    class Failed:
        def result(self):
            raise RuntimeError(f"step printed {secret}")

    monkeypatch.setattr(pipeline_module.run_container_job, "submit", lambda *args: Failed())
    monkeypatch.setattr(pipeline_module, "report_step", lambda *args, **facts: None)
    definition = {"jobs": [container_job(name="only", depends_on=[])]}
    with pytest.raises(pipeline_module.ContainerJobError) as caught:
        pipeline_module.pipeline_definition.fn("run-f", "prj-1", {}, definition)
    assert secret not in str(caught.value) and MASK in str(caught.value)


def test_report_step_redacts_runner_credentials_from_any_message(monkeypatch):
    import httpx

    secret = fake("report")
    monkeypatch.setenv("KIONGA_URL", "http://gateway")
    monkeypatch.setenv("AWS_SESSION_TOKEN", secret)
    sent = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return httpx.Response(200)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(report_module, "_client", lambda: client)
    report_module.report_step("run-1", "train-model", "failed", f"boom {secret}")
    assert sent and secret not in sent[0]["message"]
