import json
from datetime import UTC, date, datetime

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from open_data_store.config import StoreConfig
from open_data_store.contracts.standard.market import (
    CORPORATE_ACTIONS,
    INSTRUMENT_IDENTIFIERS,
    MARKET_CONTRACTS,
    PRICES,
)
from open_data_store.market.asof import sessions
from open_data_store.market.returns import resolve_actions, resolve_prices, total_return
from open_data_store.publisher import approve_contract, publish_pending
from open_data_store.reader import Store
from open_data_store.writer.batch import write_batch

from market_data import api
from market_data.adapters._quota import STATE_DIR_ENV
from market_data.adapters.tiingo import TiingoAdapter
from market_data.api import (
    LIVE_PREFIX,
    MissingKeyError,
    RestrictedSourceError,
    UnknownInstrumentError,
)
from market_data.archive import MemoryArchive

KEY = "synthetic-tiingo-key-0000"
NOW = datetime(2025, 6, 10, 12, 0, tzinfo=UTC)
START, END = date(2025, 6, 2), date(2025, 6, 6)
SESS = sessions("XNYS", START, END)
CLOSES = dict(zip(SESS["session_date"].to_pylist(), SESS["close_utc"].to_pylist(), strict=True))
I1 = "mof:" + "1" * 32
I2 = "mof:" + "2" * 32


def bar(day: str, close: float, *, div: float = 0.0, split: float = 1.0) -> dict:
    return {
        "date": f"{day}T00:00:00.000Z",
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": 1000,
        "adjClose": close,
        "divCash": div,
        "splitFactor": split,
    }


PAYLOADS = {
    "AAA": [
        bar("2025-06-02", 100.0),
        bar("2025-06-03", 101.0),
        bar("2025-06-04", 100.0, div=0.5),
        bar("2025-06-05", 51.0, split=2.0),
        bar("2025-06-06", 52.0),
    ],
    "BBB": [bar("2025-06-02", 20.0), bar("2025-06-03", 21.0)],
}


def transport(requests: list | None = None) -> httpx.MockTransport:
    def answer(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(request)
        ticker = request.url.path.split("/")[3]
        if ticker not in PAYLOADS:
            return httpx.Response(404, json={"detail": "Error: Ticker 'X' not found"})
        return httpx.Response(200, content=json.dumps(PAYLOADS[ticker]).encode())

    return httpx.MockTransport(answer)


def fake_tiingo(requests: list | None = None) -> TiingoAdapter:
    return TiingoAdapter(
        KEY,
        transport=transport(requests),
        state_dir=None,
        min_spacing_s=0,
        sleep=lambda s: None,
        now=lambda: NOW,
    )


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv(STATE_DIR_ENV, str(tmp_path / "quota-state"))
    for var in ("TIINGO_API_KEY", "MASSIVE_API_KEY"):
        monkeypatch.delenv(var, raising=False)


# --- none profile ----------------------------------------------------------------------------


def test_none_profile_needs_key():
    with pytest.raises(MissingKeyError, match="TIINGO_API_KEY"):
        api.prices(["AAA"], START, END)
    with pytest.raises(MissingKeyError, match="MASSIVE_API_KEY"):
        api.prices(["AAA"], START, END, source="massive")


def test_none_profile_key_never_in_errors(monkeypatch):
    monkeypatch.setenv("TIINGO_API_KEY", "bad key with spaces")
    with pytest.raises(Exception) as info:
        api.prices(["AAA"], START, END)
    assert "bad key" not in str(info.value)


def test_none_profile_raw_prices_keyed_by_live_id_and_input():
    requests: list = []
    out = api.prices(["AAA", "BBB"], START, END, adapter=fake_tiingo(requests), as_of=NOW)
    assert len(requests) == 2
    assert out.column_names == list(api.RAW_LIVE_SCHEMA.names)
    rows = out.to_pylist()
    assert {r["instrument_id"] for r in rows} == {"live:tiingo:AAA", "live:tiingo:BBB"}
    assert all(r["instrument_id"] == f"{LIVE_PREFIX}tiingo:{r['input']}" for r in rows)
    aaa = [r for r in rows if r["input"] == "AAA"]
    assert [r["close"] for r in aaa] == [100.0, 101.0, 100.0, 51.0, 52.0]
    assert all(r["source_code"] == "tiingo" for r in rows)


def _expected_total_return(symbols: list[str]) -> pa.Table:
    """The recipe on the same rows, built by hand: raw closes, divCash, splitFactor."""
    prices, actions, ids = [], [], []
    for s in symbols:
        ids.append(
            dict(
                instrument_id=f"live:tiingo:{s}",
                id_scheme="source_symbol",
                id_value=f"tiingo:{s}",
                valid_to=None,
                link_reason=None,
                known_at_basis="load_time",
                valid_date=date(1900, 1, 1),
                known_at=NOW,
                known_at_kind="collected",
                source_code="tiingo",
                source_doc_id=None,
            )
        )
        for b in PAYLOADS[s]:
            day = date.fromisoformat(b["date"][:10])
            prices.append(
                dict(
                    source_symbol=s,
                    open=b["open"],
                    high=b["high"],
                    low=b["low"],
                    close=b["close"],
                    volume=b["volume"],
                    currency="USD",
                    price_basis="raw",
                    known_at_basis="session_close",
                    valid_date=day,
                    known_at=CLOSES[day],
                    known_at_kind="published",
                    source_code="tiingo",
                    source_doc_id=None,
                )
            )
            base = dict(
                source_symbol=s,
                action_seq=0,
                announce_date=None,
                record_date=None,
                pay_date=None,
                amount=None,
                currency=None,
                amount_basis=None,
                per_share_basis=None,
                ratio_old=None,
                ratio_new=None,
                spinoff_symbol=None,
                known_at_basis="ex_date_assumed",
                valid_date=day,
                known_at=NOW,
                known_at_kind="published",
                source_code="tiingo",
                source_doc_id=None,
            )
            if b["divCash"]:
                actions.append(
                    {
                        **base,
                        "action_type": "cash_dividend_regular",
                        "source_action_type": "divCash",
                        "amount": b["divCash"],
                        "currency": "USD",
                    }
                )
            if b["splitFactor"] != 1:
                actions.append(
                    {
                        **base,
                        "action_type": "split",
                        "source_action_type": "splitFactor",
                        "ratio_old": 1.0,
                        "ratio_new": b["splitFactor"],
                    }
                )
    p = pa.Table.from_pylist(prices, schema=PRICES.schema)
    a = pa.Table.from_pylist(actions, schema=CORPORATE_ACTIONS.schema)
    i = pa.Table.from_pylist(ids, schema=INSTRUMENT_IDENTIFIERS.schema)
    rp = resolve_prices(p, i, as_of=NOW, precedence=())
    ra = resolve_actions(a, i, as_of=NOW, precedence=())
    return total_return(rp, ra, SESS)


def test_none_profile_total_return_matches_recipe():
    out = api.prices(
        ["AAA", "BBB"], START, END, adjust="total_return", adapter=fake_tiingo(), as_of=NOW
    )
    expected = _expected_total_return(["AAA", "BBB"])
    assert out.column_names == list(api.TOTAL_RETURN_LIVE_SCHEMA.names)
    got = out.drop_columns(["input"])
    assert got.to_pylist() == expected.to_pylist()
    aaa = {r["session_date"]: r["tr_index"] for r in out.to_pylist() if r["input"] == "AAA"}
    # the 0.5 dividend and the 2:1 split are both applied
    assert aaa[date(2025, 6, 4)] / aaa[date(2025, 6, 3)] == pytest.approx(100.5 / 101.0)
    assert aaa[date(2025, 6, 5)] / aaa[date(2025, 6, 4)] == pytest.approx(102.0 / 100.0)


def test_none_profile_unknown_instrument_lists_inputs():
    with pytest.raises(UnknownInstrumentError, match="ZZZ") as info:
        api.prices(["AAA", "ZZZ"], START, END, adapter=fake_tiingo(), as_of=NOW)
    assert info.value.inputs == ("ZZZ",)


def test_none_profile_persists_nothing(tmp_path):
    before = sorted(p for p in tmp_path.rglob("*"))
    api.prices(["AAA"], START, END, adapter=fake_tiingo(), as_of=NOW)
    assert sorted(p for p in tmp_path.rglob("*")) == before


def test_arguments_checked():
    with pytest.raises(ValueError, match="adjust"):
        api.prices(["AAA"], START, END, adjust="adjusted", adapter=fake_tiingo())
    with pytest.raises(ValueError, match="profile"):
        api.prices(["AAA"], START, END, profile="local", adapter=fake_tiingo())
    with pytest.raises(ValueError, match="start"):
        api.prices(["AAA"], END, START, adapter=fake_tiingo())
    with pytest.raises(ValueError, match="instrument"):
        api.prices([], START, END, adapter=fake_tiingo())
    with pytest.raises(ValueError, match="instrument"):
        api.prices("AAA", START, END, adapter=fake_tiingo())
    with pytest.raises(ValueError, match="source"):
        api.prices(["AAA"], START, END, source="yahoo")


def test_memory_archive_reads_like_the_raw_archive():
    from market_data.adapters.base import FetchResult

    mem = MemoryArchive()
    r = FetchResult("tiingo", "AAA", "prices", START, END, NOW, "ok", b"[]", "application/json")
    ref = mem.put(r)
    assert ref.startswith("memory:")
    assert mem.symbols("tiingo", "prices") == ["AAA"]
    assert list(mem.history("tiingo", "AAA", "prices")) == [r]
    assert list(mem.history("tiingo", "AAA", "actions")) == []


# --- server profile --------------------------------------------------------------------------

AS_OF = datetime(2025, 6, 9, 0, 0, tzinfo=UTC)
LOADED = datetime(2025, 6, 8, 0, 0, tzinfo=UTC)


def _price(symbol, day, close, source="tiingo"):
    return dict(
        source_symbol=symbol,
        open=None,
        high=None,
        low=None,
        close=close,
        volume=10,
        currency="USD",
        price_basis="raw",
        known_at_basis="session_close",
        valid_date=day,
        known_at=CLOSES[day],
        known_at_kind="published",
        source_code=source,
        source_doc_id=None,
    )


def _ident(instrument, scheme, value, source, valid=date(2000, 1, 1), valid_to=None):
    return dict(
        instrument_id=instrument,
        id_scheme=scheme,
        id_value=value,
        valid_to=valid_to,
        link_reason=None,
        known_at_basis="load_time",
        valid_date=valid,
        known_at=LOADED,
        known_at_kind="collected",
        source_code=source,
        source_doc_id=None,
    )


@pytest.fixture
def store(tmp_path):
    cfg = StoreConfig(data_root=tmp_path / "store", catalog=str(tmp_path / "meta.ducklake"))
    for c in MARKET_CONTRACTS:
        approve_contract(cfg, c, "test")
    days = SESS["session_date"].to_pylist()
    prices = [_price("AAA", d, 100.0 + i) for i, d in enumerate(days)]
    prices += [_price("BBB", d, 20.0 + i) for i, d in enumerate(days)]
    dividend = dict(
        source_symbol="AAA",
        action_type="cash_dividend_regular",
        action_seq=0,
        source_action_type="divCash",
        announce_date=None,
        record_date=None,
        pay_date=None,
        amount=1.0,
        currency="USD",
        amount_basis=None,
        per_share_basis=None,
        ratio_old=None,
        ratio_new=None,
        spinoff_symbol=None,
        known_at_basis="ex_date_assumed",
        valid_date=days[2],
        known_at=CLOSES[days[1]],
        known_at_kind="published",
        source_code="tiingo",
        source_doc_id=None,
    )
    ids = [
        _ident(I1, "source_symbol", "tiingo:AAA", "tiingo"),
        _ident(I2, "source_symbol", "tiingo:BBB", "tiingo"),
        _ident(I1, "ticker", "AAA", "openfigi"),
        _ident(I1, "composite_figi", "BBG000FAKC01", "openfigi"),
        _ident(I2, "cik", "0000000042", "sec"),
        _ident(I2, "ticker", "PRIV", "private_history"),
    ]
    for contract, rows in (
        (PRICES, prices),
        (CORPORATE_ACTIONS, [dividend]),
        (INSTRUMENT_IDENTIFIERS, ids),
    ):
        write_batch(
            contract,
            pa.Table.from_pylist(rows, schema=contract.schema),
            landing_root=cfg.landing,
            run_id="t",
            producer="test",
        )
    assert publish_pending(cfg).exit_code == 0
    with Store.open(cfg) as s:
        yield s


def test_server_profile_reads_store(store):
    kw = dict(profile="server", store=store, as_of=AS_OF, allowed_restricted={"tiingo"})
    out = api.prices(["AAA", "42", "BBG000FAKC01"], START, END, **kw)
    assert out.column_names == list(api.RAW_SERVER_SCHEMA.names)
    assert "input" not in out.column_names
    assert set(out["instrument_id"].to_pylist()) == {I1, I2}
    aaa = out.filter(pc.equal(out["instrument_id"], I1))
    assert aaa["close"].to_pylist() == [100.0, 101.0, 102.0, 103.0, 104.0]

    tr = api.prices([I1], START, END, adjust="total_return", **kw)
    assert tr.column_names == list(api.TOTAL_RETURN_SERVER_SCHEMA.names)
    level = {r["session_date"]: r["tr_index"] for r in tr.to_pylist()}
    days = SESS["session_date"].to_pylist()
    assert level[days[2]] / level[days[1]] == pytest.approx((102.0 + 1.0) / 101.0)


def test_server_profile_restricted_source_refused(store):
    with pytest.raises(RestrictedSourceError, match="tiingo"):
        api.prices(["AAA"], START, END, profile="server", store=store, as_of=AS_OF, source="tiingo")
    # without the source allowed, the gate leaves no price: the label still resolves (open source)
    out = api.prices(["AAA"], START, END, profile="server", store=store, as_of=AS_OF)
    assert out.num_rows == 0


def test_server_profile_unknown_instrument_never_names_a_restricted_label(store):
    kw = dict(profile="server", store=store, as_of=AS_OF, allowed_restricted={"tiingo"})
    with pytest.raises(UnknownInstrumentError) as info:
        api.prices(["PRIV", "NOPE"], START, END, **kw)
    assert info.value.inputs == ("PRIV", "NOPE")  # PRIV's label is private_history: gated
    message = str(info.value)
    assert I2 not in message
    out = api.prices(
        ["PRIV"],
        START,
        END,
        profile="server",
        store=store,
        as_of=AS_OF,
        allowed_restricted={"tiingo", "private_history"},
    )
    assert set(out["instrument_id"].to_pylist()) == {I2}


def test_server_profile_label_resolved_on_the_as_of_date(store):
    # a ticker closed before as_of names nothing then
    kw = dict(profile="server", store=store, allowed_restricted={"tiingo"})
    with pytest.raises(UnknownInstrumentError):
        api.prices(["AAA"], START, END, as_of=datetime(1999, 1, 1, tzinfo=UTC), **kw)


# --- review fixes: scoped server reads, no-data errors -------------------------------------------


def test_none_profile_symbol_without_rows_is_an_error():
    PAYLOADS["EMPTY"] = []
    try:
        with pytest.raises(api.NoDataError, match="no data for \\['EMPTY'\\] from tiingo") as info:
            api.prices(["AAA", "EMPTY"], START, END, adapter=fake_tiingo(), as_of=NOW)
    finally:
        del PAYLOADS["EMPTY"]
    assert info.value.inputs == ("EMPTY",)


def test_none_profile_massive_symbol_without_rows_is_an_error():
    from test_massive import NOW as MASSIVE_NOW
    from test_massive import Recorder, make

    empty = {"status": "OK", "resultsCount": 0, "ticker": "ZZZZ", "adjusted": False}
    adapter = make(Recorder(answer=lambda request: httpx.Response(200, json=empty)))
    with pytest.raises(api.NoDataError, match="from massive"):
        api.prices(
            ["ZZZZ"], date(2026, 11, 16), date(2026, 11, 20), adapter=adapter, as_of=MASSIVE_NOW
        )


@pytest.fixture
def busy_store(tmp_path):
    """I1 has tiingo and massive rows that differ; I2 has tiingo only; I3 (unrelated) has both."""
    cfg = StoreConfig(data_root=tmp_path / "store", catalog=str(tmp_path / "meta.ducklake"))
    for c in MARKET_CONTRACTS:
        approve_contract(cfg, c, "test")
    days = SESS["session_date"].to_pylist()
    i3 = "mof:" + "3" * 32
    prices = [_price("AAA", d, 100.0 + i) for i, d in enumerate(days)]
    prices += [_price("AAA", d, 200.0 + i, source="massive") for i, d in enumerate(days)]
    prices += [_price("BBB", d, 20.0 + i) for i, d in enumerate(days)]
    prices += [_price("CCC", d, 30.0 + i) for i, d in enumerate(days)]
    prices += [_price("CCC", d, 40.0 + i, source="massive") for i, d in enumerate(days)]
    ids = [
        _ident(I1, "source_symbol", "tiingo:AAA", "tiingo"),
        _ident(I1, "source_symbol", "massive:AAA", "massive"),
        _ident(I2, "source_symbol", "tiingo:BBB", "tiingo"),
        _ident(i3, "source_symbol", "tiingo:CCC", "tiingo"),
        _ident(i3, "source_symbol", "massive:CCC", "massive"),
        _ident(I1, "ticker", "AAA", "openfigi"),
        _ident(I2, "ticker", "BBB", "openfigi"),
    ]
    for contract, rows in ((PRICES, prices), (INSTRUMENT_IDENTIFIERS, ids)):
        write_batch(
            contract,
            pa.Table.from_pylist(rows, schema=contract.schema),
            landing_root=cfg.landing,
            run_id="t",
            producer="test",
        )
    assert publish_pending(cfg).exit_code == 0
    with Store.open(cfg) as s:
        yield s


def test_server_profile_unrelated_conflict_never_fails_a_request(busy_store):
    kw = dict(profile="server", store=busy_store, as_of=AS_OF)
    out = api.prices(["BBB"], START, END, allowed_restricted={"tiingo", "massive"}, **kw)
    assert set(out["instrument_id"].to_pylist()) == {I2}


def test_server_profile_same_instrument_conflict_names_the_input_only(busy_store):
    kw = dict(profile="server", store=busy_store, as_of=AS_OF)
    with pytest.raises(api.ApiError, match="pass source=") as info:
        api.prices(["AAA", "BBB"], START, END, allowed_restricted={"tiingo", "massive"}, **kw)
    message = str(info.value)
    assert "'AAA'" in message and "BBB" not in message
    assert I1 not in message and "massive:" not in message and "tiingo:" not in message
    out = api.prices(
        ["AAA"], START, END, source="massive", allowed_restricted={"tiingo", "massive"}, **kw
    )
    assert out["close"].to_pylist() == [200.0, 201.0, 202.0, 203.0, 204.0]


def test_server_profile_source_leaving_an_input_empty_says_so(busy_store):
    kw = dict(profile="server", store=busy_store, as_of=AS_OF)
    with pytest.raises(api.NoDataError, match="try another source") as info:
        api.prices(
            ["AAA", "BBB"],
            START,
            END,
            source="massive",
            allowed_restricted={"tiingo", "massive"},
            **kw,
        )
    assert info.value.inputs == ("BBB",)
