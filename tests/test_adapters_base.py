import tomllib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from open_data_store.contracts.standard.market import CORPORATE_ACTIONS, PRICES
from open_data_store.sources import SourceInfo, discover_sources

from market_data.adapters import base
from market_data.adapters.base import (
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    RAW_SYMBOL_SCHEMA,
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
    def __init__(self, code, api_key=None, info_code=None):
        self.source_code = code
        self.info = SourceInfo(info_code or code, "test", "https://example.test", "", "open")
        self._api_key = Secret(api_key) if api_key else None

    def require_key(self):
        if not self._api_key:
            raise RuntimeError("missing API key for source " + self.source_code)
        return self._api_key.reveal()

    def fetch(self, kind, symbols, start, end):
        return iter(())

    def parse(self, result):
        raise NotImplementedError


def patch_entry_points(monkeypatch, eps):
    def fake(*, group):
        assert group == "market_data.adapters"
        return eps

    monkeypatch.setattr(base, "entry_points", fake)
    registry = {c: SourceInfo(c, "t", "u", "", "open") for c in ("alpha", "beta", "other")}
    monkeypatch.setattr(base, "discover_sources", lambda: registry)


def test_every_shipped_adapter_is_registered_as_a_source():
    declared = tomllib.loads(PYPROJECT.read_text())["project"]["entry-points"]
    shipped = set(declared.get("market_data.adapters", {}))
    assert shipped == {"massive", "tiingo"}
    assert shipped <= {s.code for s in sources()}
    loaded = load_adapters(sorted(shipped))  # installed, loadable, code and registry consistent
    assert set(loaded) == shipped


def test_sources_registered_with_expected_access():
    by_code = {s.code: s for s in sources()}
    assert {c for c, s in by_code.items() if s.access == "restricted"} == {
        "massive",
        "tiingo",
        "private_history",
        "manual",
    }
    assert by_code["manual"].no_public_display and by_code["manual"].no_ml_training
    assert by_code["sec"].access == "open" and by_code["sec"].attribution == "SEC EDGAR"
    assert by_code["openfigi"].access == "open"
    assert by_code["openfigi"].attribution == "OpenFIGI"
    assert by_code["massive"].no_public_display and by_code["massive"].no_ml_training
    assert "alpaca" not in by_code


def test_sources_discovered_through_entry_point():
    assert {"massive", "tiingo", "private_history", "openfigi", "manual", "sec"} <= set(
        discover_sources()
    )


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


def test_missing_allowed_adapter_raises(monkeypatch):
    patch_entry_points(monkeypatch, [FakeEntryPoint("alpha", lambda: FakeAdapter("alpha"))])
    with pytest.raises(ValueError, match="gamma"):
        load_adapters(["alpha", "gamma"])


def test_duplicate_entry_point_names_raise(monkeypatch):
    eps = [FakeEntryPoint("alpha", lambda: FakeAdapter("alpha")) for _ in range(2)]
    patch_entry_points(monkeypatch, eps)
    with pytest.raises(ValueError, match="more than once"):
        load_adapters(["alpha"])


def test_non_adapter_raises(monkeypatch):
    patch_entry_points(monkeypatch, [FakeEntryPoint("alpha", lambda: object())])
    with pytest.raises(ValueError, match="Adapter"):
        load_adapters(["alpha"])


def test_info_code_mismatch_raises(monkeypatch):
    patch_entry_points(
        monkeypatch, [FakeEntryPoint("alpha", lambda: FakeAdapter("alpha", info_code="beta"))]
    )
    with pytest.raises(ValueError, match="info code"):
        load_adapters(["alpha"])


def test_unregistered_source_raises(monkeypatch):
    patch_entry_points(monkeypatch, [FakeEntryPoint("zeta", lambda: FakeAdapter("zeta"))])
    with pytest.raises(ValueError, match="registry"):
        load_adapters(["zeta"])


def test_key_never_in_repr():
    key = "sk-synthetic-secret-123"
    secret = Secret(key)
    assert key not in repr(secret) and key not in str(secret) and key not in f"{secret}"
    assert secret.reveal() == key
    adapter = FakeAdapter("alpha", api_key=key)
    assert key not in repr(adapter)
    assert key not in repr(vars(adapter))
    with pytest.raises(RuntimeError) as err:
        FakeAdapter("alpha").require_key()
    assert key not in str(err.value)
    assert key not in repr(make_result())


def make_result(**kw):
    args = dict(
        source_code="alpha", source_symbol="AAA", kind="prices",
        requested_from=date(2020, 1, 1), requested_to=None,
        fetched_at=datetime(2020, 1, 2, tzinfo=UTC), status="ok",
        payload=b"sk-synthetic-secret-123", content_type="application/json",
    )  # fmt: skip
    args.update(kw)
    return FetchResult(**args)


def test_fetch_result_payload_not_in_repr():
    assert "sk-synthetic" not in repr(make_result())


def test_fetch_result_validation():
    make_result(source_symbol="*")
    with pytest.raises(ValueError, match="kind"):
        make_result(kind="quotes")
    with pytest.raises(ValueError, match="status"):
        make_result(status="weird")
    with pytest.raises(ValueError, match="timezone"):
        make_result(fetched_at=datetime(2020, 1, 2))


def _shape(schema):
    return {f.name: (f.type, f.nullable) for f in schema}


@pytest.mark.parametrize(
    ("raw", "contract"), [(RAW_PRICE_SCHEMA, PRICES), (RAW_ACTION_SCHEMA, CORPORATE_ACTIONS)]
)
def test_raw_schema_matches_contract(raw, contract):
    dropped = {"price_basis", "known_at", "known_at_kind", "known_at_basis", "source_doc_id"}
    expected = {k: v for k, v in _shape(contract.schema).items() if k not in dropped}
    assert _shape(raw) == expected


def test_restricted_price_sources_forbid_display_and_training():
    for s in sources():
        if s.access == "restricted":
            assert s.no_public_display and s.no_ml_training, s.code


def test_raw_symbol_schema():
    assert RAW_SYMBOL_SCHEMA.names == [
        "source_symbol", "id_scheme", "id_value", "valid_date", "valid_to", "name",
    ]  # fmt: skip


def test_raw_schemas_have_no_known_at():
    for schema in (RAW_PRICE_SCHEMA, RAW_ACTION_SCHEMA):
        assert "known_at" not in schema.names
        assert {"source_code", "source_symbol", "valid_date"} <= set(schema.names)
