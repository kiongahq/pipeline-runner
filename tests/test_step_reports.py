"""Step reports are idempotent, ordered, retried and survive a runner restart."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from pipelines import report as report_module


@pytest.fixture
def gateway(monkeypatch):
    """A scripted gateway: `responses` are returned in order, then 200."""
    requests: list[httpx.Request] = []
    responses: list = []
    lock = threading.Lock()

    def handler(request: httpx.Request) -> httpx.Response:
        with lock:
            requests.append(request)
            outcome = responses.pop(0) if responses else 200
        if isinstance(outcome, Exception):
            raise outcome
        return httpx.Response(outcome)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(report_module, "_client", lambda: client)
    monkeypatch.setattr(report_module.time, "sleep", lambda seconds: None)
    monkeypatch.setenv("MLAIOPS_URL", "http://gateway")
    yield requests, responses
    client.close()


def _outbox(monkeypatch):
    return report_module._outbox_dir()


def test_retries_5xx_with_one_idempotency_key(gateway):
    requests, responses = gateway
    responses.extend([503, 503])
    report_module.report_step("run-1", "train", "succeeded", "done", attempt=1)
    assert len(requests) == 3
    keys = {request.headers["Idempotency-Key"] for request in requests}
    assert keys == {report_module.report_key("run-1", "train", 1, "succeeded")}
    assert len({request.headers["X-Kionga-Report-Seq"] for request in requests}) == 1


def test_retries_429_and_transport_errors(gateway):
    requests, responses = gateway
    responses.extend([429, httpx.ConnectError("refused"), httpx.ReadTimeout("slow")])
    report_module.report_step("run-1", "train", "running", attempt=1)
    assert len(requests) == 4


def test_4xx_is_not_retried_and_leaves_no_outbox_entry(gateway, monkeypatch):
    requests, responses = gateway
    responses.append(400)
    report_module.report_step("run-1", "train", "failed", "bad", attempt=1)
    assert len(requests) == 1
    assert not list(_outbox(monkeypatch).glob("*.json"))


def test_body_has_only_fields_the_gateway_accepts(gateway):
    # The gateway decodes with DisallowUnknownFields; key and seq are headers.
    requests, _ = gateway
    report_module.report_step("run-1", "train", "succeeded", "ok", attempt=2, exit_code=0,
                              workload_kind="docker-container", workload_id="abc", image_digest="d", at="t")
    body = json.loads(requests[0].content)
    assert set(body) <= {"step", "status", "message", "attempt", "exit_code", "workload_kind",
                         "workload_id", "image_digest", "at"}
    assert requests[0].headers["X-MLAIOps-Actor"] == "pipeline-engine"


def test_report_key_is_stable_and_distinct_per_transition():
    key = report_module.report_key("run-1", "train", 1, "running")
    assert key == report_module.report_key("run-1", "train", 1, "running")
    assert key != report_module.report_key("run-1", "train", 2, "running")
    assert key != report_module.report_key("run-1", "train", 1, "succeeded")
    assert key != report_module.report_key("run-2", "train", 1, "running")


def test_seq_strictly_increases_across_concurrent_reports(gateway):
    requests, _ = gateway
    with ThreadPoolExecutor(max_workers=16) as pool:
        for index in range(200):
            pool.submit(report_module.report_step, "run-c", f"node-{index % 20}", "running", attempt=index)
    seqs = [int(request.headers["X-Kionga-Report-Seq"]) for request in requests]
    assert len(seqs) == 200 and len(set(seqs)) == 200


def test_sequence_is_strict_when_the_clock_does_not_move():
    sequence = report_module._Sequence(clock=lambda: 5_000)
    assert [sequence.next("run") for _ in range(3)] == [5, 6, 7]
    assert sequence.next("other") == 5


def test_transitions_of_one_step_reach_the_gateway_in_order(gateway, monkeypatch):
    requests, _ = gateway
    first_sent = threading.Event()
    release = threading.Event()
    original = report_module._deliver

    def slow_first(gateway_url, report, deadline, **kwargs):
        if report["payload"]["status"] == "running":
            first_sent.set()
            release.wait(2)
        return original(gateway_url, report, deadline, **kwargs)

    monkeypatch.setattr(report_module, "_deliver", slow_first)
    running = threading.Thread(target=report_module.report_step, args=("run-o", "train", "running"), kwargs={"attempt": 1})
    running.start()
    assert first_sent.wait(2)
    done = threading.Thread(target=report_module.report_step, args=("run-o", "train", "succeeded"), kwargs={"attempt": 1})
    done.start()
    release.set()
    running.join(5)
    done.join(5)
    assert [json.loads(request.content)["status"] for request in requests] == ["running", "succeeded"]


def test_undelivered_terminal_report_is_replayed_on_the_next_start(gateway, monkeypatch):
    requests, responses = gateway
    monkeypatch.setenv(report_module.DEADLINE_VARIABLE, "1")
    clock = iter(range(0, 1000, 5))  # every retry decision sees the deadline passed
    monkeypatch.setattr(report_module.time, "monotonic", lambda: next(clock))
    responses.append(503)  # the deadline has passed after one failure
    report_module.report_step("run-x", "train", "succeeded", "done", attempt=1)
    pending = list(_outbox(monkeypatch).glob("*.json"))
    assert len(pending) == 1
    assert "Authorization" not in pending[0].read_text()  # the outbox never stores a credential
    sent_before = len(requests)

    # Simulated restart: the gateway is back.
    counts = report_module.replay_outbox()
    assert counts == {"delivered": 1, "rejected": 0, "pending": 0}
    replayed = requests[sent_before:]
    assert len(replayed) == 1
    assert replayed[0].headers["Idempotency-Key"] == report_module.report_key("run-x", "train", 1, "succeeded")
    assert json.loads(replayed[0].content)["status"] == "succeeded"
    assert not list(_outbox(monkeypatch).glob("*.json"))


def test_replay_stops_at_the_first_pending_report_to_keep_order(gateway, monkeypatch):
    requests, responses = gateway
    directory = _outbox(monkeypatch)
    directory.mkdir(parents=True)
    for seq, step in ((1, "a"), (2, "b")):
        report = {"run_id": "run-r", "payload": {"step": step, "status": "succeeded", "message": ""},
                  "idempotency_key": report_module.report_key("run-r", step, 0, "succeeded"), "seq": seq}
        (directory / f"{seq:020d}-{report['idempotency_key']}.json").write_text(json.dumps(report))
    (directory / "00000000000000000000-broken.json").write_text("{not json")
    responses.extend([503] * 50)
    counts = report_module.replay_outbox(deadline=0.0)
    assert counts["pending"] == 1 and counts["delivered"] == 0
    assert [json.loads(request.content)["step"] for request in requests] == ["a"]
    assert len(list(directory.glob("*.json"))) == 2  # broken entry discarded, a and b kept


def test_outbox_can_be_disabled(gateway, monkeypatch):
    monkeypatch.setenv(report_module.OUTBOX_VARIABLE, "")
    assert report_module._outbox_dir() is None
    report_module.report_step("run-1", "train", "succeeded", attempt=1)
    assert report_module.replay_outbox() == {"delivered": 0, "rejected": 0, "pending": 0}


def test_outbox_files_are_private(gateway, monkeypatch):
    _, responses = gateway
    monkeypatch.setenv(report_module.DEADLINE_VARIABLE, "1")
    clock = iter(range(0, 1000, 5))
    monkeypatch.setattr(report_module.time, "monotonic", lambda: next(clock))
    responses.append(503)
    report_module.report_step("run-p", "train", "failed", "x", attempt=1)
    (path,) = _outbox(monkeypatch).glob("*.json")
    assert path.stat().st_mode & 0o777 == 0o600


def test_running_runner_retries_the_outbox_without_a_restart(gateway, monkeypatch):
    requests, responses = gateway
    monkeypatch.setenv(report_module.DEADLINE_VARIABLE, "1")
    real_monotonic = report_module.time.monotonic
    ticks = iter(range(0, 1000, 5))
    monkeypatch.setattr(report_module.time, "monotonic", lambda: next(ticks))
    responses.append(503)
    report_module.report_step("run-l", "train", "succeeded", "done", attempt=1)
    monkeypatch.setattr(report_module.time, "monotonic", real_monotonic)
    assert len(list(_outbox(monkeypatch).glob("*.json"))) == 1

    waits = []

    class OneShot:
        def wait(self, interval):
            waits.append(interval)
            return True

    report_module.replay_outbox_forever(OneShot(), interval=7)
    assert waits == [7]
    assert not list(_outbox(monkeypatch).glob("*.json"))
    assert json.loads(requests[-1].content)["status"] == "succeeded"


def test_replay_loop_survives_a_failing_pass(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("disk gone")

    monkeypatch.setattr(report_module, "replay_outbox", broken)

    class OneShot:
        def wait(self, interval):
            return True

    report_module.replay_outbox_forever(OneShot())  # must not raise
