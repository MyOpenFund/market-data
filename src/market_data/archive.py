"""Immutable raw archive: every fetch is stored with its fetch time, payload bytes untouched.

Contract rows are later derived as a pure function of this archive. Layout under the root::

    <source_code>/<symbol segment>/<kind>/<fetched_at %Y%m%dT%H%M%S%fZ>-<sha256[:12]>.bin
                                                                                    .json (sidecar)

The symbol segment is ``_market`` for the whole-market symbol ``*`` and the percent-encoded symbol
otherwise, so no symbol can escape the root or collide with another. The root comes from
``MOF_MARKET_ARCHIVE`` (no default) and must not sit inside a git checkout.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import quote

from market_data.adapters.base import WHOLE_MARKET_SYMBOL, FetchResult

ARCHIVE_ENV = "MOF_MARKET_ARCHIVE"
MARKET_SEGMENT = "_market"
_TS_FORMAT = "%Y%m%dT%H%M%S%fZ"


def _segment(value: str) -> str:
    """One safe path segment: percent-encoded, with dot-only/empty names neutralised."""
    enc = quote(value, safe="")
    if enc == "":
        return "%00empty"
    if set(enc) == {"."}:
        return enc.replace(".", "%2E")
    return enc


def _symbol_segment(symbol: str) -> str:
    # "_market" cannot collide with a real symbol: quote() leaves "_market" unchanged, so a
    # literal symbol "_market" is stored as "%5Fmarket".
    if symbol == WHOLE_MARKET_SYMBOL:
        return MARKET_SEGMENT
    seg = _segment(symbol)
    return "%5F" + seg[1:] if seg == MARKET_SEGMENT else seg


def _check_not_in_git(root: Path) -> None:
    resolved = root.resolve()
    for parent in (resolved, *resolved.parents):
        if (parent / ".git").exists():
            raise ValueError(f"archive root {resolved} is inside a git checkout ({parent})")


def _write_once(path: Path, data: bytes) -> None:
    """Atomic write (temp file + os.replace); an existing file is never rewritten."""
    if path.exists():
        return
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _iso(value: date | datetime | None) -> str | None:
    return None if value is None else value.isoformat()


class RawArchive:
    def __init__(self, root: Path | str | None = None) -> None:
        if root is None:
            env = os.environ.get(ARCHIVE_ENV)
            if not env:
                raise ValueError(f"no archive root: pass one or set {ARCHIVE_ENV}")
            root = env
        self.root = Path(root)
        _check_not_in_git(self.root)

    def _dir(self, source_code: str, source_symbol: str, kind: str) -> Path:
        return self.root / _segment(source_code) / _symbol_segment(source_symbol) / kind

    def put(self, result: FetchResult) -> str:
        digest = hashlib.sha256(result.payload).hexdigest()
        stamp = result.fetched_at.astimezone(UTC).strftime(_TS_FORMAT)
        directory = self._dir(result.source_code, result.source_symbol, result.kind)
        directory.mkdir(parents=True, exist_ok=True)
        stem = f"{stamp}-{digest[:12]}"
        sidecar = {
            "source_code": result.source_code,
            "source_symbol": result.source_symbol,
            "kind": result.kind,
            "requested_from": _iso(result.requested_from),
            "requested_to": _iso(result.requested_to),
            "fetched_at": result.fetched_at.isoformat(),
            "status": result.status,
            "content_type": result.content_type,
            "payload_sha256": digest,
        }
        # Payload first: a sidecar never points at a missing payload.
        _write_once(directory / f"{stem}.bin", result.payload)
        _write_once(
            directory / f"{stem}.json", json.dumps(sidecar, sort_keys=True, indent=2).encode()
        )
        return (directory / f"{stem}.bin").relative_to(self.root).as_posix()

    def history(self, source_code: str, source_symbol: str, kind: str) -> Iterator[FetchResult]:
        directory = self._dir(source_code, source_symbol, kind)
        if not directory.is_dir():
            return
        entries = []
        for sidecar_path in directory.glob("*.json"):
            meta = json.loads(sidecar_path.read_text())
            fetched_at = datetime.fromisoformat(meta["fetched_at"])
            ref = sidecar_path.with_suffix(".bin").relative_to(self.root).as_posix()
            entries.append((fetched_at, ref, meta))
        entries.sort(key=lambda e: (e[0], e[1]))
        for fetched_at, ref, meta in entries:
            payload = (self.root / ref).read_bytes()
            if hashlib.sha256(payload).hexdigest() != meta["payload_sha256"]:
                raise ValueError(f"corrupted archive: payload sha256 mismatch for {ref}")
            yield FetchResult(
                source_code=meta["source_code"],
                source_symbol=meta["source_symbol"],
                kind=meta["kind"],
                requested_from=_parse_date(meta["requested_from"]),
                requested_to=_parse_date(meta["requested_to"]),
                fetched_at=fetched_at,
                status=meta["status"],
                payload=payload,
                content_type=meta["content_type"],
            )


def _parse_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)
