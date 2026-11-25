import tomllib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from open_data_store.sources import SourceInfo, discover_sources

from market_data.adapters import base
from market_data.adapters.base import (
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    FetchResult,
    Secret,
    allowed_adapters,
    load_adapters,
)
from market_data.sources import sources

PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"


class FakeEntryPoint:
    def __init__(self, name, factory):
        self.name = name
        self._factory = factory
        self.loaded = False

    def load(self):
        self.loaded = True
        return self._factory


class FakeAdapter:
    def __init__(self, code, api_key=None):
        self.source_code = code
        self.info = SourceInfo(code, "test", "https://example.test", "", "open")
        self._api_key = Secret(api_key) if api_key else None

    def fetch(self, kind, symbols, start, end):
        return iter(())

    def parse(self, result):
        raise NotImplementedError


def patch_entry_points(monkeypatch, eps):
    def fake(*, group):
        assert group == "market_data.adapters"
        return eps

    monkeypatch.setattr(base, "entry_points", fake)


def test_every_shipped_adapter_is_registered_as_a_source():
    declared = tomllib.loads(PYPROJECT.read_text())["project"]["entry-points"]
    shipped = set(declared.get("market_data.adapters", {}))
    assert shipped <= {s.code for s in sources()}


def test_sources_registered_with_expected_access():
    by_code = {s.code: s for s in sources()}
    assert {c for c, s in by_code.items() if s.access == "restricted"} == {
        "massive",
        "tiingo",
        "private_history",
    }
    assert by_code["openfigi"].access == "open"
    assert by_code["openfigi"].attribution == "OpenFIGI"
    assert by_code["massive"].no_public_display and by_code["massive"].no_ml_training
    assert "alpaca" not in by_code


def test_sources_discovered_through_entry_point():
    assert {"massive", "tiingo", "private_history", "openfigi"} <= set(discover_sources())


def test_adapter_not_allowed_is_not_loaded(monkeypatch):
    allowed = FakeEntryPoint("alpha", lambda: FakeAdapter("alpha"))
    denied = FakeEntryPoint("beta", lambda: FakeAdapter("beta"))
    patch_entry_points(monkeypatch, [allowed, denied])
    loaded = load_adapters(["alpha"])
    assert set(loaded) == {"alpha"}
    assert allowed.loaded and not denied.loaded
    assert load_adapters([]) == {}


def test_adapter_code_must_match_entry_point_name(monkeypatch):
    patch_entry_points(monkeypatch, [FakeEntryPoint("alpha", lambda: FakeAdapter("other"))])
    with pytest.raises(ValueError, match="alpha"):
        load_adapters(["alpha"])


def test_allowed_adapters_from_env():
    assert allowed_adapters({"MOF_MARKET_ADAPTERS": " a, b ,,c"}) == ["a", "b", "c"]
    assert allowed_adapters({}) == []


def test_key_never_in_repr(monkeypatch):
    key = "sk-synthetic-secret-123"
    secret = Secret(key)
    assert key not in repr(secret) and key not in str(secret) and key not in f"{secret}"
    assert secret.reveal() == key
    adapter = FakeAdapter("alpha", api_key=key)
    assert key not in repr(adapter._api_key)
    result = FetchResult(
        "alpha", "AAA", "prices", date(2020, 1, 1), None, datetime(2020, 1, 2, tzinfo=UTC),
        "ok", b"{}", "application/json",
    )  # fmt: skip
    assert key not in repr(result)


def test_raw_schemas_have_no_known_at():
    for schema in (RAW_PRICE_SCHEMA, RAW_ACTION_SCHEMA):
        assert "known_at" not in schema.names
        assert {"source_code", "source_symbol", "valid_date"} <= set(schema.names)
