"""Mask secret values before node output leaves the runner.

Step containers print whatever they like, including their own environment. Every
string the runner forwards to the gateway (streamed log lines, the bounded output
tail, step messages and the flow failure message) passes through a `Redactor`
built from the values that must never be stored: the runner's own credentials
and the secret-named variables of the step's environment.
"""

from __future__ import annotations

import os
import re
from typing import Iterable, Mapping

MASK = "***"

# Shorter values (ports, booleans, "local") would mask ordinary output.
MIN_SECRET_LENGTH = 6

# Variable names whose values are treated as secrets.
SECRET_NAME_PATTERN = re.compile(
    r"(TOKEN|SECRET|PASSWORD|PASSWD|_KEY$|^KEY$|_KEY_ID$|CREDENTIAL|PRIVATE)",
    re.IGNORECASE,
)

# Runner variables that are secret whatever the pattern says.
RUNNER_SECRET_NAMES = (
    "MLAIOPS_TOKEN",
    "MLAIOPS_INTERNAL_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)


def is_secret_name(name: str) -> bool:
    return bool(SECRET_NAME_PATTERN.search(name))


def secret_values(environment: Mapping[str, str]) -> list[str]:
    """Values of the secret-named variables in an environment mapping."""
    return [
        str(value)
        for name, value in environment.items()
        if (name in RUNNER_SECRET_NAMES or is_secret_name(str(name))) and value
    ]


class Redactor:
    """Replaces every occurrence of a known secret value with `***`.

    Values shorter than MIN_SECRET_LENGTH are ignored. `redact(text,
    partial_end=True)` also masks a trailing fragment that starts a secret, for
    text cut at a length cap, and `partial_start=True` masks a leading fragment
    that ends one, for a tail cut from the front.
    """

    def __init__(self, values: Iterable[str] = ()):
        unique = {str(value) for value in values if value and len(str(value)) >= MIN_SECRET_LENGTH}
        # Longest first, so a secret that contains another is masked whole.
        self.values = sorted(unique, key=len, reverse=True)
        self._pattern = (
            re.compile("|".join(re.escape(value) for value in self.values)) if self.values else None
        )

    def __bool__(self) -> bool:
        return bool(self.values)

    def extend(self, values: Iterable[str]) -> "Redactor":
        return Redactor([*self.values, *values])

    def redact(self, text: str, *, partial_start: bool = False, partial_end: bool = False) -> str:
        if not text or self._pattern is None:
            return text
        text = self._pattern.sub(MASK, text)
        if partial_end:
            text = self._mask_fragment(text, end=True)
        if partial_start:
            text = self._mask_fragment(text, end=False)
        return text

    def _mask_fragment(self, text: str, end: bool) -> str:
        longest = 0
        for value in self.values:
            for size in range(min(len(value) - 1, len(text)), longest, -1):
                if size < MIN_SECRET_LENGTH:
                    break
                if (end and text.endswith(value[:size])) or (not end and text.startswith(value[-size:])):
                    longest = size
                    break
        if not longest:
            return text
        return text[:-longest] + MASK if end else MASK + text[longest:]


def runner_redactor() -> Redactor:
    """Redactor for the runner's own credentials, read from its environment."""
    return Redactor(secret_values(dict(os.environ)))
