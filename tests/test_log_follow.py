"""_follow_logs keeps memory bounded whatever a container writes."""

from __future__ import annotations

from pipelines import definition as pipeline_module
from pipelines import report as report_module
from pipelines.redact import MASK, Redactor


class StreamingContainer:
    """Yields chunks lazily and records the largest buffer the follower held."""

    def __init__(self, chunks):
        self.chunks = chunks

    def logs(self, **_kwargs):
        return iter(self.chunks)


def _shipper(shipped, redactor=None):
    return report_module.LogShipper(
        "run-f", "n", 1, post=lambda run_id, entries: shipped.extend(entries), interval=60,
        redactor=redactor if redactor is not None else Redactor(),
    )


def _follow(chunks, limit, max_line=pipeline_module.MAX_LOG_LINE_BYTES, redactor=None):
    shipped = []
    shipper = _shipper(shipped, redactor)
    pipeline_module._follow_logs(StreamingContainer(chunks), shipper, limit, max_line)
    shipper.close()
    return [entry["message"] for entry in shipped]


def test_fifty_mib_without_newlines_stays_bounded(monkeypatch):
    chunk = b"x" * (64 * 1024)
    count = (50 * 1024 * 1024) // len(chunk)
    largest = [0]
    original_finditer = pipeline_module._LINE_BREAK.finditer

    class Spy:
        def finditer(self, data):
            largest[0] = max(largest[0], len(data))
            return original_finditer(data)

    monkeypatch.setattr(pipeline_module, "_LINE_BREAK", Spy())
    limit = 5 * 1024 * 1024
    messages = _follow((chunk for _ in range(count)), limit)
    # Buffer never exceeds one unfinished line cap plus the incoming chunk.
    assert largest[0] <= pipeline_module.MAX_LOG_LINE_BYTES + len(chunk)
    # One truncated line is shipped, the rest of the 50 MiB is dropped.
    assert len(messages) == 1
    assert messages[0].endswith(pipeline_module.LINE_TRUNCATED_MARKER)
    assert len(messages[0]) == pipeline_module.MAX_LOG_LINE_BYTES + len(pipeline_module.LINE_TRUNCATED_MARKER)


def test_line_larger_than_the_stream_limit_is_cut_not_shipped_whole():
    messages = _follow([b"y" * 5000 + b"\n", b"after\n"], limit=1000, max_line=10_000)
    assert messages[0] == "y" * 1000
    assert "truncated after 1000 bytes" in messages[1]
    assert len(messages) == 2
    assert sum(len(message) for message in messages[:-1]) <= 1000


def test_carriage_return_progress_output_yields_separate_entries():
    messages = _follow([b"10%\r20%\r", b"30%\r", b"\ndone\r\n", b"tail"], limit=10_000)
    assert messages == ["10%", "20%", "30%", "done", "tail"]


def test_crlf_split_across_chunks_is_one_line_break():
    messages = _follow([b"first\r", b"\nsecond\n"], limit=10_000)
    assert messages == ["first", "second"]


def test_remainder_of_a_truncated_line_is_dropped_until_the_next_break():
    messages = _follow([b"a" * 30, b"b" * 30, b"c" * 5 + b"\nnext\n"], limit=10_000, max_line=20)
    assert messages == ["a" * 20 + pipeline_module.LINE_TRUNCATED_MARKER, "next"]


def test_secret_cut_by_the_line_cap_is_masked():
    secret = "".join(["abc123", "cut", "secret", "value"])
    redactor = Redactor([secret])
    line = b"z" * 10 + secret.encode() + b"\n"
    messages = _follow([line], limit=10_000, max_line=20, redactor=redactor)
    assert messages == ["z" * 10 + MASK + pipeline_module.LINE_TRUNCATED_MARKER]
    assert not any(secret[:10] in message for message in messages)
