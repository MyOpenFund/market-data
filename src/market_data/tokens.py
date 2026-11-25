"""Private-symbol tokens: opaque, deterministic ``source_symbol`` values for private sources.

A private export's labels (member ids, names, FIGIs) are as of the export date; joined into a past
date they leak the future (who dies, who gets renamed; spec §9.3). The history-file loader
therefore replaces each ``member_id`` by ``token(source_code, member_id, salt)`` in every dated
row and publishes the labels only as identifier observations known at load time.

The salt is a secret kept on the store host (``MOF_MARKET_TOKEN_SALT``): without it, tokens of
guessable member ids (``AAA UW Equity``) could be recomputed by anyone and the guard undone. It is
never logged, never in a repr and never in an error message.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping

__all__ = ["MIN_SALT_BYTES", "SALT_ENV", "TOKEN_PREFIX", "salt_from_env", "token"]

SALT_ENV = "MOF_MARKET_TOKEN_SALT"
TOKEN_PREFIX = "t_"
# Shortest salt accepted from the environment: a short salt is brute-forceable over the small
# space of plausible member ids.
MIN_SALT_BYTES = 16
_HEX_CHARS = 20


def token(source_code: str, member_id: str, salt: bytes) -> str:
    """``"t_" + sha256(salt + source_code + "|" + member_id)`` hex, first 20 characters."""
    digest = hashlib.sha256(salt + source_code.encode() + b"|" + member_id.encode()).hexdigest()
    return TOKEN_PREFIX + digest[:_HEX_CHARS]


def salt_from_env(
    env: Mapping[str, str] | None = None, *, min_bytes: int = MIN_SALT_BYTES
) -> bytes:
    """The salt bytes (UTF-8) from ``MOF_MARKET_TOKEN_SALT``.

    Missing, empty or shorter than ``min_bytes`` bytes: ``ValueError`` (the value is never
    echoed)."""
    raw = (os.environ if env is None else env).get(SALT_ENV, "")
    salt = raw.encode()
    if not salt:
        raise ValueError(
            f"{SALT_ENV} is not set: the history-file loader needs the token salt (a secret kept "
            'on the store host; generate one with `python -c "import secrets; '
            'print(secrets.token_hex(32))"` and keep it: changing it changes every token)'
        )
    if len(salt) < min_bytes:
        raise ValueError(f"{SALT_ENV} is too short: at least {min_bytes} bytes are required")
    return salt
