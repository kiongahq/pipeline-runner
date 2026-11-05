"""Keep every test off the network and out of the real report outbox."""

from __future__ import annotations

import httpx
import pytest

from pipelines import report as report_module


def _no_network(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"unexpected step report to {request.url}; give the test a transport")


@pytest.fixture(autouse=True)
def isolated_reporting(monkeypatch, tmp_path):
    monkeypatch.setenv(report_module.OUTBOX_VARIABLE, str(tmp_path / "report-outbox"))
    client = httpx.Client(transport=httpx.MockTransport(_no_network))
    monkeypatch.setattr(report_module, "_client", lambda: client)
    yield
    client.close()
