"""Provider quota state shared across runs: request times and the month's distinct symbols.

One small JSON file per source, ``<state dir>/<source>-quota.json``, so a second run (another
process, the next day's "Try it") knows what earlier runs already spent of the provider's limits.
It holds request times (seconds of the adapter's clock, the wall clock by default) of the last
24 hours and the distinct symbols requested in the current UTC month: never a key, a URL or a
payload.

State directory: ``MOF_MARKET_STATE_DIR`` when set (``off`` disables persistence), else the
platform user cache directory + ``myopenfund/market-data`` (``~/Library/Caches`` on macOS,
``%LOCALAPPDATA%`` on Windows, ``$XDG_CACHE_HOME`` or ``~/.cache`` elsewhere).

Best effort, never blocking: a missing, unreadable or corrupt file (not JSON, wrong shape, wrong
version) is ignored without a warning and the state starts empty from it (the next save rewrites
it); a failed write (read-only cache) is ignored too, the in-memory state still bounds this
process. Writes are atomic (temp file in the same directory, then ``os.replace``) and the state is
re-read and merged before each request, so runs one after the other share it; two processes
running at the same moment can still race (no lock), as documented by the adapters.
"""

from __future__ import annotations

import json
import math
import os
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

__all__ = ["STATE_DIR_ENV", "STATE_OFF", "QuotaState", "default_state_dir"]

STATE_DIR_ENV = "MOF_MARKET_STATE_DIR"
STATE_OFF = "off"
STATE_VERSION = 1
WINDOW_S = 86400.0  # request times older than this are dropped


def default_state_dir(env: Mapping[str, str] | None = None) -> Path | None:
    """The state directory (see the module docstring); None when persistence is off."""
    env = os.environ if env is None else env
    raw = env.get(STATE_DIR_ENV, "").strip()
    if raw:
        return None if raw.lower() == STATE_OFF else Path(raw).expanduser()
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Caches"
    elif os.name == "nt":
        local = env.get("LOCALAPPDATA", "")
        base = Path(local) if local else Path.home() / "AppData" / "Local"
    else:
        xdg = env.get("XDG_CACHE_HOME", "")
        base = Path(xdg) if xdg and Path(xdg).is_absolute() else Path.home() / ".cache"
    return base / "myopenfund" / "market-data"


class QuotaState:
    """Request times of the last 24 hours and the month's symbols of one source."""

    def __init__(self, source_code: str, directory: Path | None) -> None:
        self.path = None if directory is None else Path(directory) / f"{source_code}-quota.json"
        self.requests: list[float] = []  # sorted clock times
        self.month = ""
        self.symbols: set[str] = set()

    def refresh(self, now_s: float, month: str) -> None:
        """Merge the file into memory, drop request times older than 24 hours and symbols of
        another month."""
        if month != self.month:
            self.month, self.symbols = month, set()
        stored = self._load()
        if stored is not None:
            times, stored_month, symbols = stored
            self.requests = sorted({*self.requests, *times})
            if stored_month == month:
                self.symbols |= symbols
        horizon = now_s - WINDOW_S
        self.requests = [t for t in self.requests if t > horizon]

    def since(self, after_s: float) -> list[float]:
        """Request times strictly after ``after_s``, oldest first."""
        return [t for t in self.requests if t > after_s]

    def record(self, at_s: float, symbol: str, month: str) -> None:
        self.refresh(at_s, month)
        self.requests.append(at_s)
        self.requests.sort()
        self.symbols.add(symbol)
        self._save()

    def _load(self) -> tuple[list[float], str, set[str]] | None:
        if self.path is None:
            return None
        try:
            body: Any = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):  # missing, unreadable, not UTF-8 or not JSON
            return None
        if not isinstance(body, dict) or body.get("version") != STATE_VERSION:
            return None
        times, month, symbols = body.get("requests"), body.get("month"), body.get("symbols")
        if not isinstance(times, list) or not isinstance(month, str):
            return None
        if not isinstance(symbols, list) or not all(isinstance(s, str) for s in symbols):
            return None
        if not all(
            isinstance(t, int | float) and not isinstance(t, bool) and math.isfinite(t)
            for t in times
        ):
            return None
        return [float(t) for t in times], month, set(symbols)

    def _save(self) -> None:
        if self.path is None:
            return
        body = {
            "version": STATE_VERSION,
            "requests": self.requests,
            "month": self.month,
            "symbols": sorted(self.symbols),
        }
        data = json.dumps(body, sort_keys=True).encode()
        tmp: Path | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(
                dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp"
            )
            tmp = Path(name)
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
            tmp = None
        except OSError:
            pass  # best effort: the in-memory state still bounds this process
        finally:
            if tmp is not None:
                tmp.unlink(missing_ok=True)
