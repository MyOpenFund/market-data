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
import tempfile
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple
from urllib.parse import quote

from market_data.adapters.base import WHOLE_MARKET_SYMBOL, FetchResult

ARCHIVE_ENV = "MOF_MARKET_ARCHIVE"
MARKET_SEGMENT = "_market"
_TS_FORMAT = "%Y%m%dT%H%M%S%fZ"


def _segment(value: str) -> str:
    """One safe path segment: percent-encoded, with dot-only names neutralised."""
    if value == "":
        raise ValueError("source_code and source_symbol must not be empty")
    enc = quote(value, safe="")
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


def _fsync_dir(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:  # e.g. platforms where directories cannot be opened
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _publish(path: Path, data: bytes) -> None:
    """Write ``data`` at ``path`` atomically and never overwrite.

    A unique temp file is hard-linked into place (``os.link`` fails if the target exists), so
    concurrent writers of the same ref are safe. An existing target must be byte-identical.
    """
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise ValueError(
                    f"archive conflict: {path.name} already exists with different content"
                ) from None
        else:
            _fsync_dir(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


class ArchivedFetch(NamedTuple):
    """One archived fetch and its archive reference (the ``put`` return value)."""

    archive_ref: str
    result: FetchResult


def _iso(value: date | datetime | None) -> str | None:
    return None if value is None else value.isoformat()


class RawArchive:
    def __init__(self, root: Path | str | None = None) -> None:
        if root is None:
            env = os.environ.get(ARCHIVE_ENV)
            if not env:
                raise ValueError(f"no archive root: pass one or set {ARCHIVE_ENV}")
            root = env
        self.root = Path(root).absolute()
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
        _publish(directory / f"{stem}.bin", result.payload)
        _publish(directory / f"{stem}.json", json.dumps(sidecar, sort_keys=True, indent=2).encode())
        return (directory / f"{stem}.bin").relative_to(self.root).as_posix()

    def history(self, source_code: str, source_symbol: str, kind: str) -> Iterator[FetchResult]:
        """Fetches of one series and kind, sorted by (fetched_at, archive_ref)."""
        for entry in self.history_with_refs(source_code, source_symbol, kind):
            yield entry.result

    def history_with_refs(
        self, source_code: str, source_symbol: str, kind: str
    ) -> Iterator[ArchivedFetch]:
        """Like ``history``, each fetch paired with its archive reference."""
        directory = self._dir(source_code, source_symbol, kind)
        if not directory.is_dir():
            return
        entries = []
        for sidecar_path in directory.glob("*.json"):
            meta = json.loads(sidecar_path.read_text())
            if meta["source_code"] != source_code or meta["kind"] != kind:
                raise ValueError(f"archive sidecar identity mismatch: {sidecar_path.name}")
            if meta["source_symbol"] != source_symbol:
                continue  # case-collision neighbour on a case-insensitive filesystem
            fetched_at = datetime.fromisoformat(meta["fetched_at"])
            ref = sidecar_path.with_suffix(".bin").relative_to(self.root).as_posix()
            entries.append((fetched_at, ref, meta))
        entries.sort(key=lambda e: (e[0], e[1]))
        for fetched_at, ref, meta in entries:
            try:
                payload = (self.root / ref).read_bytes()
            except FileNotFoundError:
                raise ValueError(f"corrupted archive: payload missing for {ref}") from None
            if hashlib.sha256(payload).hexdigest() != meta["payload_sha256"]:
                raise ValueError(f"corrupted archive: payload sha256 mismatch for {ref}")
            result = FetchResult(
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
            yield ArchivedFetch(ref, result)

    def symbols(self, source_code: str, kind: str) -> list[str]:
        """Sorted distinct source symbols (``"*"`` included) archived for a source and kind.

        Read from the sidecars, so encoded path segments never need decoding."""
        base = self.root / _segment(source_code)
        if not base.is_dir():
            return []
        found: set[str] = set()
        for sidecar_path in base.glob(f"*/{kind}/*.json"):
            meta = json.loads(sidecar_path.read_text())
            if meta["source_code"] == source_code and meta["kind"] == kind:
                found.add(meta["source_symbol"])
        return sorted(found)


def _parse_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


class MemoryArchive:
    """The ``RawArchive`` reading interface over fetches held in memory: nothing is persisted.

    For the in-memory paths (the API's ``none`` profile, a fetch run's own summary): the builder
    reads it exactly as it reads the on-disk archive, so the same rows come out. References are
    ``memory:<n>`` (n zero-padded) in put order; ``history_with_refs`` and ``symbols`` follow
    ``RawArchive``'s ordering."""

    def __init__(self) -> None:
        self._entries: list[ArchivedFetch] = []

    def put(self, result: FetchResult) -> str:
        ref = f"memory:{len(self._entries):08d}"
        self._entries.append(ArchivedFetch(ref, result))
        return ref

    def history(self, source_code: str, source_symbol: str, kind: str) -> Iterator[FetchResult]:
        for entry in self.history_with_refs(source_code, source_symbol, kind):
            yield entry.result

    def history_with_refs(
        self, source_code: str, source_symbol: str, kind: str
    ) -> Iterator[ArchivedFetch]:
        entries = [
            e
            for e in self._entries
            if (e.result.source_code, e.result.source_symbol, e.result.kind)
            == (source_code, source_symbol, kind)
        ]
        entries.sort(key=lambda e: (e.result.fetched_at.astimezone(UTC), e.archive_ref))
        yield from entries

    def symbols(self, source_code: str, kind: str) -> list[str]:
        return sorted(
            {
                e.result.source_symbol
                for e in self._entries
                if e.result.source_code == source_code and e.result.kind == kind
            }
        )
