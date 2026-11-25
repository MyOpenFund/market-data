import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow as pa
import pytest

from market_data.adapters._quota import QuotaState, default_state_dir
from market_data.adapters.base import (
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    Adapter,
    FetchResult,
    Secret,
    load_adapters,
)
from market_data.adapters.tiingo import (
    ACTIONS_FETCH_KIND,
    API_KEY_ENV,
    BAD_RATIO,
    BAD_TIMESTAMP,
    BAD_VOLUME,
    BASE_URL,
    CONFLICTING_DUPLICATE,
    DEFAULT_MIN_SPACING_S,
    DUPLICATE_ROW,
    ERROR_PAYLOAD,
    FREE_PLAN_PER_DAY,
    FREE_PLAN_PER_HOUR,
    FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH,
    KEPT_OUT_REASONS,
    MISSING_FIELD,
    NON_FINITE,
    NON_POSITIVE_AMOUNT,
    NON_POSITIVE_CLOSE,
    NOT_AN_OBJECT,
    STATE_DIR_ENV,
    VENDOR_ADJUSTED_SCHEMA,
    TiingoAdapter,
    TiingoError,
    TiingoKeyError,
    TiingoQuotaError,
    adapter,
)
from market_data.archive import RawArchive
from market_data.build import build_actions, build_fetch_log, build_prices

FIXTURES = Path(__file__).parent / "fixtures" / "tiingo"
FAKE_KEY = "synthetic-tiingo-key-0000"
NOW = datetime(2026, 12, 1, 12, 0, tzinfo=UTC)
DAY = date(2026, 11, 20)
START, END = date(2026, 11, 19), date(2026, 11, 24)


def fixture(name: str = "prices_AAA.json") -> list:
    return json.loads((FIXTURES / name).read_text())


class FakeClock:
    """Time moves only when the adapter sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(round(seconds, 6))
        self.now += seconds


class Recorder:
    """An httpx transport answering from a queue of (status, headers, body), or from a callable
    of the request."""

    def __init__(self, answers=None, *, answer=None) -> None:
        self.answers = list(answers or [])
        self.answer = answer
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.answer is not None:
            return self.answer(request)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        status, headers, body = answer
        if isinstance(body, bytes):
            return httpx.Response(status, headers=headers, content=body)
        return httpx.Response(status, headers=headers, json=body)

    def params(self, i: int) -> dict[str, str]:
        return {k: v[-1] for k, v in parse_qs(urlsplit(str(self.requests[i].url)).query).items()}

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]


@pytest.fixture(autouse=True)
def _state_dir(tmp_path, monkeypatch):
    """Never touch the real user cache: the default state dir points into tmp_path."""
    monkeypatch.setenv(STATE_DIR_ENV, str(tmp_path / "env-state"))


def make(recorder=None, clock=None, key=FAKE_KEY, **kw) -> TiingoAdapter:
    clock = clock or FakeClock()
    kw.setdefault("state_dir", None)  # no persistence unless a test asks for it
    return TiingoAdapter(
        key,
        transport=httpx.MockTransport(recorder or Recorder()),
        clock=clock.clock,
        sleep=clock.sleep,
        now=lambda: NOW,
        **kw,
    )


def ok(body):
    return (200, {}, body)


def always(body):
    return Recorder(answer=lambda request: httpx.Response(200, json=body))


def result(payload, *, symbol="AAA", kind="prices", frm=START, to=END, status="ok"):
    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return FetchResult("tiingo", symbol, kind, frm, to, NOW, status, raw, "application/json")


def nonzero(report) -> dict:
    return {r: keys for r, keys in report.kept_out.items() if keys}


def row(day: str, close=10.0, **kw) -> dict:
    out = {"date": f"{day}T00:00:00.000Z", "close": close, "volume": 100}
    out.update(kw)
    return out


# --- the four plan tests ------------------------------------------------------------------------


def test_raw_close_used_not_adj():
    table = make().parse(result(fixture()))
    assert table.schema == RAW_PRICE_SCHEMA
    rows = table.to_pylist()
    columns = ("valid_date", "open", "high", "low", "close", "volume")
    assert [tuple(r[c] for c in columns) for r in rows] == [
        (date(2026, 11, 19), 99.0, 101.0, 98.0, 100.0, 1000),
        (date(2026, 11, 20), 100.5, 103.0, 100.0, 102.0, 1200),
        (date(2026, 11, 23), 51.0, 52.0, 50.5, 51.5, 2600),
        (date(2026, 11, 24), 51.5, 52.5, 51.0, 52.0, 2400),
    ]
    assert {(r["source_code"], r["source_symbol"], r["currency"]) for r in rows} == {
        ("tiingo", "AAA", "USD")
    }
    # adjusted values only ever reach the control series
    adjusted = make().parse_vendor_adjusted(result(fixture()))
    assert adjusted.schema == VENDOR_ADJUSTED_SCHEMA
    assert adjusted["adj_close"].to_pylist() == [49.0, 50.0, 51.5, 52.0]
    assert adjusted["valid_date"].to_pylist() == table["valid_date"].to_pylist()
    assert set(adjusted["source_symbol"].to_pylist()) == {"AAA"}


def test_divcash_becomes_cash_dividend():
    report = make().parse_actions(result(fixture()))
    assert report.table.schema == RAW_ACTION_SCHEMA
    dividends = [r for r in report.table.to_pylist() if r["action_type"].startswith("cash")]
    assert dividends == [
        {
            "source_code": "tiingo",
            "source_symbol": "AAA",
            "valid_date": DAY,
            "action_type": "cash_dividend_regular",
            "action_seq": 0,
            "source_action_type": "divCash",
            "announce_date": None,
            "record_date": None,
            "pay_date": None,
            "amount": 0.5,
            "currency": "USD",
            "amount_basis": None,
            "per_share_basis": None,
            "ratio_old": None,
            "ratio_new": None,
            "spinoff_symbol": None,
        }
    ]
    assert not nonzero(report)


def test_splitfactor_not_applied_to_volume():
    a = make()
    (split,) = [r for r in a.parse_actions(result(fixture())).table.to_pylist() if r["ratio_new"]]
    assert (split["valid_date"], split["action_type"], split["source_action_type"]) == (
        date(2026, 11, 23),
        "split",
        "splitFactor",
    )
    assert (split["ratio_old"], split["ratio_new"]) == (1.0, 2.0)  # a price factor
    assert split["amount"] is None and split["currency"] is None
    # the raw volume is kept as given on both sides of the split, never scaled by the factor
    volumes = a.parse(result(fixture()))["volume"].to_pylist()
    assert volumes == [1000, 1200, 2600, 2400]
    assert volumes != [r["adjVolume"] for r in fixture()]


def test_missing_key_clear_error():
    rec = Recorder()
    for key in (None, "", "   "):
        with pytest.raises(TiingoKeyError) as exc:
            make(rec, key=key).fetch("prices", ["AAA"], START, END)  # raised by fetch itself
        assert API_KEY_ENV in str(exc.value) and "free" in str(exc.value)
        assert "tiingo.com" in str(exc.value)
    with pytest.raises(TiingoKeyError, match=API_KEY_ENV):
        TiingoAdapter.from_env({}).fetch("prices", ["AAA"], START, END)
    assert rec.requests == []
    # parsing archived payloads needs no key
    assert make(key=None).parse(result(fixture())).num_rows == 4


# --- fetch --------------------------------------------------------------------------------------


def test_end_of_day_endpoint_one_result_per_symbol():
    rec = always(fixture())
    out = list(make(rec).fetch("prices", ["AAA", "BRK-B"], START, END))
    assert rec.paths() == ["/tiingo/daily/AAA/prices", "/tiingo/daily/BRK-B/prices"]
    assert rec.params(0) == {
        "startDate": "2026-11-19",
        "endDate": "2026-11-24",
        "resampleFreq": "daily",
        "format": "json",
    }
    assert [(r.source_symbol, r.kind, r.status) for r in out] == [
        ("AAA", "prices", "ok"),
        ("BRK-B", "prices", "ok"),
    ]
    assert all((r.requested_from, r.requested_to) == (START, END) for r in out)
    assert all(r.fetched_at == NOW and r.source_code == "tiingo" for r in out)
    assert json.loads(out[0].payload) == fixture()  # archived as Tiingo sent it


def test_payload_archived_raw_and_empty_answer():
    raw = b'[ {"date":"2026-11-20T00:00:00.000Z","close":1.0} ]'
    (out,) = make(Recorder([ok(raw)])).fetch("prices", ["AAA"], DAY, DAY)
    assert out.payload == raw and out.status == "ok"
    (out,) = make(Recorder([ok([])])).fetch("prices", ["AAA"], DAY, DAY)
    assert out.status == "empty" and make().parse(out).num_rows == 0


def test_symbol_percent_encoded_never_rewritten():
    rec = always([])
    list(make(rec).fetch("prices", ["A/../B", "brk.b"], DAY, DAY))
    assert rec.requests[0].url.raw_path.startswith(b"/tiingo/daily/A%2F..%2FB/prices")
    assert rec.paths()[1] == "/tiingo/daily/brk.b/prices"


def test_fetch_arguments_validated():
    a = make()
    with pytest.raises(ValueError, match="kind"):
        a.fetch("quotes", ["AAA"], DAY, DAY)
    with pytest.raises(ValueError, match="per symbol"):
        a.fetch("prices", [], DAY, DAY)
    with pytest.raises(ValueError, match="per symbol"):
        a.fetch("prices", ["*"], DAY, DAY)
    with pytest.raises(ValueError, match="symbol"):
        a.fetch("prices", ["AAA", ""], DAY, DAY)
    with pytest.raises(ValueError, match="start"):
        a.fetch("prices", ["AAA"], None, DAY)
    with pytest.raises(ValueError, match="after"):
        a.fetch("prices", ["AAA"], END, START)
    with pytest.raises(ValueError, match="symbols"):
        a.fetch("symbols", [], None, None)


def test_actions_ride_on_the_prices_fetch():
    a = make()
    assert a.actions_fetch_kind == ACTIONS_FETCH_KIND == "prices"
    with pytest.raises(ValueError, match="prices"):
        a.fetch("actions", ["AAA"], START, END)
    # parse of kind "actions" reads the same payload as parse_actions
    prices_kind = a.parse_actions(result(fixture())).table
    actions_kind = a.parse(result(fixture(), kind="actions"))
    assert prices_kind.equals(actions_kind)


def test_end_defaults_to_start_and_duplicate_symbols_fetched_once():
    rec = always([])
    out = list(make(rec).fetch("prices", ["AAA", "AAA"], DAY, None))
    assert len(rec.requests) == 1 and rec.params(0)["endDate"] == "2026-11-20"
    assert [(r.requested_from, r.requested_to) for r in out] == [(DAY, DAY)]


def test_sessions_not_closed_yet_are_never_requested():
    # NOW = 2026-12-01 12:00 UTC: the 2026-12-01 session closes at 21:00 UTC
    rec = always([])
    (out,) = make(rec).fetch("prices", ["AAA"], date(2026, 11, 27), date(2026, 12, 4))
    assert rec.params(0)["endDate"] == "2026-11-30"
    assert (out.requested_from, out.requested_to) == (date(2026, 11, 27), date(2026, 11, 30))
    rec = Recorder()
    assert list(make(rec).fetch("prices", ["AAA"], date(2026, 12, 1), date(2026, 12, 4))) == []
    assert list(make(rec).fetch("prices", ["AAA"], date(2026, 12, 5), date(2026, 12, 6))) == []
    assert rec.requests == []


def test_deep_history_is_one_request():
    rec = always([])
    (out,) = make(rec).fetch("prices", ["AAA"], date(1995, 1, 3), date(2026, 11, 30))
    assert len(rec.requests) == 1 and rec.params(0)["startDate"] == "1995-01-03"
    assert out.requested_from == date(1995, 1, 3)


# --- rate limits and retries ---------------------------------------------------------------------


def test_a_few_symbols_are_fetched_in_seconds():
    clk = FakeClock()
    start = clk.now
    out = list(make(always([]), clk).fetch("prices", ["A", "B", "C", "D", "E"], DAY, DAY))
    assert len(out) == 5
    assert clk.sleeps == [DEFAULT_MIN_SPACING_S] * 4 == [1.5] * 4  # never a burst
    assert clk.now - start == 6.0


def test_rolling_hour_budget_51st_request_waits():
    clk = FakeClock()
    sent: list[float] = []

    def timed(request):
        sent.append(clk.now)
        return httpx.Response(200, json=[])

    names = [f"S{i:02d}" for i in range(FREE_PLAN_PER_HOUR + 1)]
    out = list(make(Recorder(answer=timed), clk).fetch("prices", names, DAY, DAY))
    assert len(out) == len(sent) == 51 and FREE_PLAN_PER_HOUR == 50
    assert {round(b - a, 6) for a, b in zip(sent[:50], sent[1:50], strict=False)} == {1.5}
    assert sent[50] == sent[0] + 3600  # the oldest of the last 50 has just left the hour
    # no trailing hour ever holds more than per_hour requests
    assert all(sum(t - 3600 < u <= t for u in sent) <= 50 for t in sent)
    clk = FakeClock()
    list(make(always([]), clk, per_hour=2).fetch("prices", ["A", "B", "C"], DAY, DAY))
    assert clk.sleeps == [1.5, 3598.5]
    clk = FakeClock()
    list(make(always([]), clk, min_spacing_s=0).fetch("prices", ["A", "B"], DAY, DAY))
    assert clk.sleeps == []


def test_limits_are_parameters_within_the_free_plan():
    for bad in (
        {"per_hour": 0},
        {"per_hour": 51},
        {"per_day": 0},
        {"per_day": 1001},
        {"unique_symbols_per_month": 0},
        {"unique_symbols_per_month": 501},
        {"max_retries": -1},
        {"backoff_s": 0},
        {"max_wait_s": 0},
        {"min_spacing_s": -1},
        {"min_spacing_s": float("nan")},
    ):
        with pytest.raises(ValueError):
            make(**bad)
    assert (FREE_PLAN_PER_DAY, FREE_PLAN_UNIQUE_SYMBOLS_PER_MONTH) == (1000, 500)


def test_daily_cap_refuses_before_and_between_requests():
    clk = FakeClock()
    rec = always([])
    a = make(rec, clk, per_hour=50, per_day=3)
    list(a.fetch("prices", ["AAA", "BBB"], DAY, DAY))
    # more symbols than requests left today: refused before any request
    with pytest.raises(TiingoQuotaError, match="per_day"):
        a.fetch("prices", ["CCC", "DDD"], DAY, DAY)
    assert len(rec.requests) == 2
    list(a.fetch("prices", ["CCC"], DAY, DAY))
    # the cap counts every request of the last 24 hours
    with pytest.raises(TiingoQuotaError, match="per_day"):
        a.fetch("prices", ["AAA"], DAY, DAY)
    clk.now += 24 * 3600
    list(a.fetch("prices", ["AAA"], DAY, DAY))
    assert len(rec.requests) == 4


def test_daily_cap_reached_by_retries_is_an_error_row_then_raised():
    clk = FakeClock()
    rec = Recorder([(503, {}, {}), (503, {}, {})])
    a = make(rec, clk, per_day=2, max_retries=5)
    it = a.fetch("prices", ["AAA"], DAY, DAY)
    failed = next(it)
    assert (failed.status, failed.payload) == ("error", ERROR_PAYLOAD)
    with pytest.raises(TiingoQuotaError, match="per_day"):
        next(it)
    assert len(rec.requests) == 2


def test_unique_symbol_cap_per_month():
    rec = always([])
    a = make(rec, unique_symbols_per_month=2)
    list(a.fetch("prices", ["AAA", "BBB"], DAY, DAY))
    list(a.fetch("prices", ["AAA"], date(2026, 11, 23), date(2026, 11, 23)))  # not a new symbol
    with pytest.raises(TiingoQuotaError, match="unique_symbols_per_month"):
        a.fetch("prices", ["BBB", "CCC"], DAY, DAY)
    assert len(rec.requests) == 3
    # a new UTC month starts a new set
    a._now = lambda: NOW + timedelta(days=31)
    list(a.fetch("prices", ["CCC", "DDD"], DAY, DAY))
    assert len(rec.requests) == 5


def test_429_backs_off_no_retry_storm():
    clk = FakeClock()
    rec = Recorder([(429, {"Retry-After": "100"}, {}), ok([])])
    (out,) = make(rec, clk).fetch("prices", ["AAA"], DAY, DAY)
    assert out.status == "empty" and clk.sleeps == [100.0]
    # no Retry-After: exponential backoff, never shorter than the 72 s spacing, then gives up
    clk = FakeClock()
    rec = Recorder([(429, {}, {})] * 10)
    with pytest.raises(TiingoError, match="429") as exc:
        list(make(rec, clk, max_retries=3, backoff_s=60.0).fetch("prices", ["AAA"], DAY, DAY))
    assert "hourly allocation is probably spent" in str(exc.value)
    assert "about an hour" in str(exc.value)
    assert len(rec.requests) == 4
    assert clk.sleeps == [60.0, 120.0, 240.0]
    # a wait beyond max_wait_s gives up at once (an exhausted hourly allocation)
    clk = FakeClock()
    rec = Recorder([(429, {"Retry-After": "3600"}, {})])
    with pytest.raises(TiingoError, match="max_wait.*about an hour"):
        list(make(rec, clk).fetch("prices", ["AAA"], DAY, DAY))
    assert clk.sleeps == [] and len(rec.requests) == 1


def test_transport_errors_and_5xx_are_retried_bounded():
    clk = FakeClock()
    rec = Recorder([httpx.ReadTimeout("t"), (502, {}, {}), ok(fixture())])
    (out,) = make(rec, clk, backoff_s=100.0).fetch("prices", ["AAA"], START, END)
    assert out.status == "ok" and len(rec.requests) == 3
    assert clk.sleeps == [100.0, 200.0]
    rec = Recorder(answer=lambda r: httpx.Response(503))
    with pytest.raises(TiingoError, match="503; gave up after 2 retries$"):
        list(make(rec, max_retries=2).fetch("prices", ["AAA"], DAY, DAY))
    assert len(rec.requests) == 3


def test_redirect_is_neither_followed_nor_retried():
    rec = Recorder([(302, {"Location": "https://elsewhere.example/"}, {})])
    with pytest.raises(TiingoError, match="302"):
        list(make(rec).fetch("prices", ["AAA"], DAY, DAY))
    assert len(rec.requests) == 1


# --- failures are logged ------------------------------------------------------------------------


def test_mid_range_failure_is_an_error_row_then_raised(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    rec = Recorder([ok(fixture()), (500, {}, {}), (500, {}, {})])
    a = make(rec, max_retries=1)
    with pytest.raises(TiingoError, match="500") as exc:
        for r in a.fetch("prices", ["AAA", "BBB", "CCC"], START, END):
            arc.put(r)
    assert FAKE_KEY not in str(exc.value) and exc.value.__context__ is None
    assert len(rec.requests) == 3  # CCC never requested
    log = build_fetch_log(arc, a, "tiingo").to_pylist()
    columns = ("source_symbol", "status", "requested_from", "requested_to", "row_count")
    assert [tuple(r[c] for c in columns) for r in log] == [
        ("AAA", "ok", START, END, 4),
        ("BBB", "error", START, END, 0),
    ]


@pytest.mark.parametrize(
    ("answer", "match"),
    [
        ((200, {}, b"not json"), "JSON"),
        (ok({"detail": "Error: synthetic"}), "list"),
        (ok([row("2026-11-20"), "x", {"date": 5}]), None),  # bad rows only: still ok
    ],
)
def test_unusable_payload_is_an_error_row_never_ok(answer, match):
    it = make(Recorder([answer])).fetch("prices", ["AAA"], DAY, DAY)
    first = next(it)
    if match is None:
        assert first.status == "ok"
        return
    assert (first.status, first.payload, first.requested_from) == ("error", ERROR_PAYLOAD, DAY)
    with pytest.raises(TiingoError, match=match) as exc:
        next(it)
    assert "synthetic" not in str(exc.value)


def test_unknown_ticker_is_not_found_and_the_fetch_goes_on():
    body = {"detail": "Error: Ticker 'ZZZ' not found"}
    rec = Recorder([(404, {}, body), ok(fixture())])
    out = list(make(rec).fetch("prices", ["ZZZ", "AAA"], START, END))
    assert [(r.source_symbol, r.status) for r in out] == [("ZZZ", "not_found"), ("AAA", "ok")]
    assert out[0].payload == ERROR_PAYLOAD
    # any other 404 (a wrong base_url, a route not found) is an error
    for body in (b"<html>nope</html>", {"detail": "Not found."}, {"detail": "ticker"}):
        it = make(Recorder([(404, {}, body)])).fetch("prices", ["AAA"], DAY, DAY)
        assert next(it).status == "error"
        with pytest.raises(TiingoError, match="404"):
            next(it)


def test_any_exception_in_a_fetch_is_logged_as_error(monkeypatch):
    a = make(Recorder([ok(fixture())]))

    def boom(result):
        raise KeyError(f"payload detail {FAKE_KEY}")

    monkeypatch.setattr(a, "parse_report", boom)
    it = a.fetch("prices", ["AAA"], START, END)
    assert next(it).status == "error"
    with pytest.raises(TiingoError, match="fetch failed: KeyError") as exc:
        next(it)
    assert FAKE_KEY not in str(exc.value)
    assert exc.value.__context__ is None and exc.value.__cause__ is None


# --- key handling -------------------------------------------------------------------------------


def test_key_sent_as_token_header_never_in_a_url():
    rec = always([])
    list(make(rec, key=Secret(FAKE_KEY)).fetch("prices", ["AAA"], DAY, DAY))
    (request,) = rec.requests
    assert request.headers["authorization"] == f"Token {FAKE_KEY}"
    assert FAKE_KEY not in str(request.url) and "token" not in str(request.url).lower()
    assert str(request.url).startswith(BASE_URL + "/")


def test_from_env_reads_the_key():
    rec = always([])
    a = TiingoAdapter.from_env({API_KEY_ENV: f" {FAKE_KEY} "}, transport=httpx.MockTransport(rec))
    list(a.fetch("prices", ["AAA"], DAY, DAY))
    assert rec.requests[0].headers["authorization"] == f"Token {FAKE_KEY}"


@pytest.mark.parametrize("key", ["clé-synthetic", "ab cd", "tab\there", Secret("x\ny")])
def test_key_not_fit_for_a_header_is_a_clear_error(key):
    rec = Recorder()
    with pytest.raises(TiingoKeyError, match="printable ASCII") as exc:
        make(rec, key=key).fetch("prices", ["AAA"], DAY, DAY)
    assert not any(ch in str(exc.value) for ch in "é\t\n") and "ab cd" not in str(exc.value)
    assert rec.requests == []


def test_auth_failure_is_a_clear_error_without_the_answer_text():
    body = {"detail": "synthetic free text"}
    for status in (401, 403):
        it = make(Recorder([(status, {}, body)])).fetch("prices", ["AAA"], DAY, DAY)
        assert next(it).status == "error"
        with pytest.raises(TiingoKeyError, match=API_KEY_ENV) as exc:
            next(it)
        assert str(status) in str(exc.value) and "synthetic free text" not in str(exc.value)
        assert FAKE_KEY not in str(exc.value)


def test_key_never_leaks(tmp_path):
    a = make(Recorder(answer=lambda r: httpx.Response(500, json={"detail": "x"})))
    assert FAKE_KEY not in repr(a) and FAKE_KEY not in str(a)
    assert FAKE_KEY not in repr(vars(a))
    with pytest.raises(TiingoError) as exc:
        list(a.fetch("prices", ["AAA"], DAY, DAY))
    assert "500" in str(exc.value) and FAKE_KEY not in str(exc.value)
    assert exc.value.__cause__ is None and exc.value.__context__ is None

    def broken(request):
        raise httpx.ConnectError(f"synthetic failure {request.headers['authorization']}")

    with pytest.raises(TiingoError, match="ConnectError") as exc:
        list(make(Recorder(answer=broken)).fetch("prices", ["AAA"], DAY, DAY))
    assert FAKE_KEY not in str(exc.value) and exc.value.__context__ is None

    arc = RawArchive(tmp_path / "arc")
    for r in make(always(fixture())).fetch("prices", ["AAA"], START, END):
        assert FAKE_KEY not in repr(r)
        arc.put(r)
    for path in (tmp_path / "arc").rglob("*"):
        if path.is_file():
            assert FAKE_KEY.encode() not in path.read_bytes()


def test_context_manager_closes_the_client():
    with make() as a:
        assert not a._http.is_closed
    assert a._http.is_closed


# --- parse: kept-out rows -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("bad", "reason"),
    [
        ({"close": None}, MISSING_FIELD),
        ({"close": "10"}, MISSING_FIELD),
        ({"close": 0.0}, NON_POSITIVE_CLOSE),
        ({"close": -1.0}, NON_POSITIVE_CLOSE),
        ({"volume": -1}, BAD_VOLUME),
        ({"volume": 1.5}, BAD_VOLUME),
        ({"volume": 2**63}, BAD_VOLUME),
        ({"volume": "1"}, BAD_VOLUME),
        ({"open": "x"}, MISSING_FIELD),
    ],
)
def test_bad_price_row_kept_out_and_counted(bad, reason):
    payload = [row("2026-11-19"), row("2026-11-20", **bad)]
    report = make().parse_report(result(payload))
    assert report.table["valid_date"].to_pylist() == [date(2026, 11, 19)]
    assert nonzero(report) == {reason: (("AAA", DAY),)}
    assert report.row_counts[reason] == 1 and report.counts()[reason] == 1


@pytest.mark.parametrize(
    "raw",
    [
        b'[{"date":"2026-11-20T00:00:00.000Z","close":1e400}]',
        b'[{"date":"2026-11-20T00:00:00.000Z","close":1' + b"0" * 400 + b"}]",
        b'[{"date":"2026-11-20T00:00:00.000Z","close":1,"high":NaN}]',
        b'[{"date":"2026-11-20T00:00:00.000Z","close":1,"volume":Infinity}]',
    ],
)
def test_non_finite_numbers_kept_out(raw):
    report = make().parse_report(result(raw))
    assert report.table.num_rows == 0
    assert nonzero(report) == {NON_FINITE: (("AAA", DAY),)}


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, MISSING_FIELD),
        (20261120, MISSING_FIELD),
        ("20/11/2026", BAD_TIMESTAMP),
        ("2026-13-01T00:00:00.000Z", BAD_TIMESTAMP),
        ("1850-01-02T00:00:00.000Z", BAD_TIMESTAMP),
        ("2026-11-20 garbage", BAD_TIMESTAMP),
    ],
)
def test_bad_dates_kept_out(value, reason):
    report = make().parse_report(result([row("2026-11-19"), {**row("x"), "date": value}]))
    assert report.table.num_rows == 1
    assert nonzero(report) == {reason: (("AAA", None),)}


def test_date_part_taken_as_given():
    payload = [
        {"date": "2026-11-20T00:00:00.000Z", "close": 1.0},
        {"date": "2026-11-23T00:00:00Z", "close": 2.0},
        {"date": "2026-11-24", "close": 3.0},
        {"date": "2026-11-25T00:00:00+00:00", "close": 4.0},
    ]
    table = make().parse(result(payload))
    # midnight UTC is never shifted to the previous New York day
    assert table["valid_date"].to_pylist() == [date(2026, 11, d) for d in (20, 23, 24, 25)]
    assert table["volume"].to_pylist() == [None] * 4 and table["open"].null_count == 4


def test_rows_not_objects_kept_out():
    report = make().parse_report(result([row("2026-11-19"), "x", 3, None]))
    assert report.table.num_rows == 1
    assert nonzero(report) == {NOT_AN_OBJECT: (("AAA", None),)}
    assert report.row_counts[NOT_AN_OBJECT] == 3


def test_duplicate_rows_kept_once_conflicting_kept_out():
    a, b = row("2026-11-19"), row("2026-11-20")
    payload = [a, a, b, {**b, "close": 11.0}]
    report = make().parse_report(result(payload))
    assert report.table["valid_date"].to_pylist() == [date(2026, 11, 19)]
    assert nonzero(report) == {
        DUPLICATE_ROW: (("AAA", date(2026, 11, 19)),),
        CONFLICTING_DUPLICATE: (("AAA", DAY),),
    }
    assert report.row_counts[CONFLICTING_DUPLICATE] == 2


def test_payload_unusable_as_a_whole_is_a_value_error():
    for raw in (b"not json", b'{"detail": "x"}', b"3"):
        with pytest.raises(ValueError):
            make().parse(result(raw))
    with pytest.raises(ValueError, match="symbol"):
        make().parse(result([], symbol="*"))
    with pytest.raises(ValueError, match="symbols"):
        make().parse(result([], kind="symbols"))
    with pytest.raises(ValueError):
        make().parse_vendor_adjusted(result([], kind="actions"))


def test_kept_out_reasons_always_present():
    report = make().parse_report(result(fixture()))
    assert set(report.kept_out) == set(KEPT_OUT_REASONS) == set(report.row_counts)
    assert not nonzero(report)


# --- parse: actions -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fields", "expected", "reason"),
    [
        ({"divCash": 0.0, "splitFactor": 1.0}, [], None),
        ({}, [], None),  # absent: no action
        ({"divCash": None, "splitFactor": None}, [], None),
        ({"divCash": 0.25}, [("cash_dividend_regular", 0.25, None)], None),
        ({"splitFactor": 0.1}, [("split", None, 0.1)], None),  # a reverse split
        ({"divCash": 0.25, "splitFactor": 3.0},
         [("cash_dividend_regular", 0.25, None), ("split", None, 3.0)], None),
        ({"divCash": -0.25}, [], NON_POSITIVE_AMOUNT),
        ({"divCash": "0.25"}, [], MISSING_FIELD),
        ({"splitFactor": 0.0}, [], BAD_RATIO),
        ({"splitFactor": -2.0}, [], BAD_RATIO),
        ({"splitFactor": "2"}, [], BAD_RATIO),
    ],
)  # fmt: skip
def test_actions_mapping_and_kept_out(fields, expected, reason):
    report = make().parse_actions(result([{**row("2026-11-20"), **fields}]))
    got = [(r["action_type"], r["amount"], r["ratio_new"]) for r in report.table.to_pylist()]
    assert got == expected
    assert nonzero(report) == ({} if reason is None else {reason: (("AAA", DAY),)})


@pytest.mark.parametrize(
    "raw",
    [
        b'[{"date":"2026-11-20T00:00:00.000Z","close":1,"splitFactor":NaN}]',
        b'[{"date":"2026-11-20T00:00:00.000Z","close":1,"splitFactor":Infinity}]',
        b'[{"date":"2026-11-20T00:00:00.000Z","close":1,"splitFactor":1e400}]',
    ],
)
def test_non_finite_split_factor_is_a_bad_ratio(raw):
    report = make().parse_actions(result(raw))
    assert report.table.num_rows == 0 and nonzero(report) == {BAD_RATIO: (("AAA", DAY),)}


def test_action_on_a_row_with_a_bad_price_is_still_an_action():
    report = make().parse_actions(result([{**row("2026-11-20", close=-1), "divCash": 0.3}]))
    assert report.table["amount"].to_pylist() == [0.3]
    assert make().parse_report(result([row("2026-11-20", close=-1)])).table.num_rows == 0


def test_action_with_a_bad_date_kept_out_plain_rows_not_counted():
    payload = [
        {**row("x"), "date": "bad", "divCash": 0.3},
        {**row("x"), "date": "bad", "divCash": 0.0, "splitFactor": 1.0},
        "not an object",
    ]
    report = make().parse_actions(result(payload))
    assert report.table.num_rows == 0
    assert nonzero(report) == {BAD_TIMESTAMP: (("AAA", None),), NOT_AN_OBJECT: (("AAA", None),)}
    assert report.row_counts[BAD_TIMESTAMP] == 1


def test_duplicate_and_conflicting_actions():
    a = {**row("2026-11-19"), "divCash": 0.3}
    b = {**row("2026-11-20"), "divCash": 0.3}
    report = make().parse_actions(result([a, a, b, {**b, "divCash": 0.4}]))
    assert [(r["valid_date"], r["action_seq"]) for r in report.table.to_pylist()] == [
        (date(2026, 11, 19), 0)
    ]
    assert nonzero(report) == {
        DUPLICATE_ROW: (("AAA", date(2026, 11, 19)),),
        CONFLICTING_DUPLICATE: (("AAA", DAY),),
    }


def test_vendor_adjusted_control_series():
    payload = [
        {**row("2026-11-19"), "adjClose": 5.0},
        {**row("2026-11-20"), "adjClose": None},
        {**row("2026-11-23"), "adjClose": -1.0},
        {**row("2026-11-24"), "adjClose": 4.0},
        {**row("2026-11-24"), "adjClose": 4.0},
    ]
    a = make()
    report = a.vendor_adjusted_report(result(payload))
    assert report.table.to_pylist() == [
        {"source_symbol": "AAA", "valid_date": date(2026, 11, 19), "adj_close": 5.0},
        {"source_symbol": "AAA", "valid_date": date(2026, 11, 24), "adj_close": 4.0},
    ]
    assert nonzero(report) == {
        MISSING_FIELD: (("AAA", DAY),),
        NON_POSITIVE_CLOSE: (("AAA", date(2026, 11, 23)),),
        DUPLICATE_ROW: (("AAA", date(2026, 11, 24)),),
    }
    assert a.parse_vendor_adjusted(result(payload)).equals(report.table)
    # the control column never enters prices
    assert "adj_close" not in a.parse(result(payload)).column_names


# --- registry and end to end --------------------------------------------------------------------


def test_adapter_protocol_and_registry(monkeypatch):
    a = make()
    assert isinstance(a, Adapter)
    assert a.source_code == a.info.code == "tiingo"
    assert a.info.access == "restricted" and a.info.no_public_display
    monkeypatch.setenv(API_KEY_ENV, FAKE_KEY)
    loaded = load_adapters(["tiingo"])["tiingo"]
    assert isinstance(loaded, TiingoAdapter) and loaded._key.reveal() == FAKE_KEY
    loaded.close()
    monkeypatch.delenv(API_KEY_ENV)
    with adapter() as unkeyed:
        with pytest.raises(TiingoKeyError):
            unkeyed.fetch("prices", ["AAA"], DAY, DAY)


def test_archived_fetches_build_end_to_end(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    bbb = [{**row("2026-11-20", close=20.0, volume=500), "adjClose": 19.0, "divCash": 0.1}]
    a = make(Recorder([ok(fixture()), ok(bbb)]))
    refs = [arc.put(r) for r in a.fetch("prices", ["AAA", "BBB"], START, END)]
    assert len(refs) == 2

    prices = build_prices(arc, a, ["AAA", "BBB"])
    assert prices.counts() == {"non_session": 0, "intraday_partial": 0}
    rows = prices.table.to_pylist()
    assert [(r["source_symbol"], r["valid_date"], r["close"], r["volume"]) for r in rows] == [
        ("AAA", date(2026, 11, 19), 100.0, 1000),
        ("AAA", date(2026, 11, 20), 102.0, 1200),
        ("AAA", date(2026, 11, 23), 51.5, 2600),
        ("AAA", date(2026, 11, 24), 52.0, 2400),
        ("BBB", date(2026, 11, 20), 20.0, 500),
    ]
    assert {(r["price_basis"], r["source_code"], r["known_at_kind"]) for r in rows} == {
        ("raw", "tiingo", "published")
    }
    assert rows[0]["known_at"] == datetime(2026, 11, 19, 21, 0, tzinfo=UTC)

    # the builder reads the actions from the archived prices fetches (adapter.actions_fetch_kind)
    actions = build_actions(arc, a, ["AAA", "BBB"]).table.to_pylist()
    assert [
        (r["source_symbol"], r["valid_date"], r["action_type"], r["amount"], r["ratio_new"])
        for r in actions
    ] == [
        ("AAA", date(2026, 11, 20), "cash_dividend_regular", 0.5, None),
        ("AAA", date(2026, 11, 23), "split", None, 2.0),
        ("BBB", date(2026, 11, 20), "cash_dividend_regular", 0.1, None),
    ]
    # no announce date: known at the ex-date open
    assert actions[0]["known_at"] == datetime(2026, 11, 20, 14, 30, tzinfo=UTC)
    assert actions[0]["known_at_basis"] == "ex_date_assumed"

    log = build_fetch_log(arc, a, "tiingo").to_pylist()
    assert [(r["source_symbol"], r["status"], r["row_count"]) for r in log] == [
        ("AAA", "ok", 4),
        ("BBB", "ok", 1),
    ]


def test_a_later_fetch_changing_history_is_a_collected_row(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    first = [{**row("2026-11-20"), "divCash": 0.3}]
    second = [{**row("2026-11-20", close=10.5), "divCash": 0.35}]
    for at, payload in ((NOW, first), (NOW + timedelta(days=1), second)):
        raw = json.dumps(payload).encode()
        arc.put(FetchResult("tiingo", "AAA", "prices", DAY, DAY, at, "ok", raw, "application/json"))
    a = make()
    prices = build_prices(arc, a, ["AAA"]).table.to_pylist()
    assert [(r["close"], r["known_at_kind"]) for r in prices] == [
        (10.0, "published"),
        (10.5, "collected"),
    ]
    actions = build_actions(arc, a, ["AAA"]).table.to_pylist()
    assert [(r["amount"], r["action_seq"], r["known_at_kind"]) for r in actions] == [
        (0.3, 0, "published"),
        (0.35, 0, "collected"),
    ]


def test_vendor_adjusted_table_is_arrow():
    table = make().parse_vendor_adjusted(result(fixture()))
    assert isinstance(table, pa.Table) and table.num_rows == 4


# --- quota state persisted across runs (ruling 71) ----------------------------------------------


def test_state_survives_a_new_adapter_instance(tmp_path):
    state, clk, rec = tmp_path / "state", FakeClock(), always([])
    list(make(rec, clk, state_dir=state, per_hour=2).fetch("prices", ["AAA", "BBB"], DAY, DAY))
    # a second run sees the first run's two requests in the trailing hour, and its symbols
    later = make(rec, clk, state_dir=state, per_hour=2, unique_symbols_per_month=3)
    clk.sleeps.clear()
    list(later.fetch("prices", ["CCC"], DAY, DAY))
    assert clk.sleeps == [3600 - 1.5]
    with pytest.raises(TiingoQuotaError, match="unique_symbols_per_month"):
        later.fetch("prices", ["DDD"], DAY, DAY)
    with pytest.raises(TiingoQuotaError, match="per_day"):
        make(rec, clk, state_dir=state, per_day=3).fetch("prices", ["AAA"], DAY, DAY)
    body = json.loads((state / "tiingo-quota.json").read_text())
    assert body["symbols"] == ["AAA", "BBB", "CCC"] and len(body["requests"]) == 3
    assert body["month"] == "2026-12"


def test_state_file_never_holds_the_key(tmp_path):
    state = tmp_path / "state"
    list(make(always(fixture()), state_dir=state).fetch("prices", ["AAA"], START, END))
    files = [p for p in state.rglob("*") if p.is_file()]
    assert [p.name for p in files] == ["tiingo-quota.json"]  # no temp file left behind
    assert all(FAKE_KEY.encode() not in p.read_bytes() for p in files)


@pytest.mark.parametrize(
    "content",
    [
        b"not json",
        b"\xff\xfe",
        b"[]",
        b'{"version": 2, "requests": [], "month": "2026-12", "symbols": []}',
        b'{"version": 1, "requests": ["x"], "month": "2026-12", "symbols": []}',
        b'{"version": 1, "requests": [1e400], "month": "2026-12", "symbols": []}',
        b'{"version": 1, "requests": [], "month": 5, "symbols": []}',
        b'{"version": 1, "requests": [], "month": "2026-12", "symbols": [1]}',
    ],
)
def test_corrupt_state_file_is_ignored_without_warning(tmp_path, content):
    state = tmp_path / "state"
    state.mkdir()
    (state / "tiingo-quota.json").write_bytes(content)
    clk = FakeClock()
    out = list(make(always([]), clk, state_dir=state).fetch("prices", ["AAA", "BBB"], DAY, DAY))
    assert len(out) == 2 and clk.sleeps == [1.5]  # an empty budget (pytest -W error: no warning)
    body = json.loads((state / "tiingo-quota.json").read_text())  # rewritten whole
    assert body["version"] == 1 and body["symbols"] == ["AAA", "BBB"]


def test_unwritable_state_dir_is_best_effort(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")  # a file where the directory should be
    out = list(make(always([]), state_dir=blocker / "state").fetch("prices", ["AAA"], DAY, DAY))
    assert [r.status for r in out] == ["empty"]


def test_old_requests_leave_the_state(tmp_path):
    state, clk = tmp_path / "state", FakeClock()
    list(make(always([]), clk, state_dir=state).fetch("prices", ["AAA"], DAY, DAY))
    clk.now += 86400 + 1
    list(make(always([]), clk, state_dir=state).fetch("prices", ["AAA"], DAY, DAY))
    assert len(json.loads((state / "tiingo-quota.json").read_text())["requests"]) == 1


def test_state_dir_from_the_environment(tmp_path, monkeypatch):
    target = tmp_path / "env-dir"
    monkeypatch.setenv(STATE_DIR_ENV, str(target))
    clk = FakeClock()
    a = TiingoAdapter(FAKE_KEY, transport=httpx.MockTransport(always([])), clock=clk.clock,
                      sleep=clk.sleep, now=lambda: NOW)  # fmt: skip
    list(a.fetch("prices", ["AAA"], DAY, DAY))
    assert (target / "tiingo-quota.json").is_file()
    for off in ("off", "OFF"):
        monkeypatch.setenv(STATE_DIR_ENV, off)
        assert default_state_dir() is None
    assert make(state_dir="off")._quota.path is None


def test_default_state_dir_is_the_user_cache(monkeypatch):
    monkeypatch.setattr("sys.platform", "linux")
    env = {"XDG_CACHE_HOME": "/xdg"}
    assert default_state_dir(env) == Path("/xdg/myopenfund/market-data")
    home = Path.home()
    assert default_state_dir({}) == home / ".cache" / "myopenfund" / "market-data"
    assert default_state_dir({"XDG_CACHE_HOME": "relative"}) == default_state_dir({})
    monkeypatch.setattr("sys.platform", "darwin")
    assert default_state_dir({}) == home / "Library" / "Caches" / "myopenfund" / "market-data"
    assert default_state_dir({STATE_DIR_ENV: " /x "}) == Path("/x")


def test_caps_checked_before_waiting():
    clk = FakeClock()
    a = make(always([]), clk, per_hour=1, per_day=1)
    list(a.fetch("prices", ["AAA"], DAY, DAY))
    with pytest.raises(TiingoQuotaError):
        a.fetch("prices", ["AAA"], DAY, DAY)
    # reached between requests (retries): refused at once, never after an hour's wait
    clk = FakeClock()
    rec = Recorder([(503, {}, {})] * 3)
    it = make(rec, clk, per_hour=1, per_day=1, max_retries=3).fetch("prices", ["AAA"], DAY, DAY)
    assert next(it).status == "error"
    with pytest.raises(TiingoQuotaError, match="per_day"):
        next(it)
    assert clk.sleeps == [] and len(rec.requests) == 1


def test_key_not_kept_in_a_headers_dict():
    a = make(always([]))
    assert FAKE_KEY not in repr(vars(a)) and FAKE_KEY not in repr(a._auth)
    assert "headers" not in vars(a)
    assert a._http.headers.get("authorization") is None
    leaks: list[str] = []

    def inspect_frames(request):
        assert request.headers["authorization"] == f"Token {FAKE_KEY}"
        frame = sys._getframe()
        while frame is not None:
            if frame.f_code.co_filename.endswith("tiingo.py"):  # the adapter's own frames
                for name, value in frame.f_locals.items():
                    if FAKE_KEY in repr(value):
                        leaks.append(f"{frame.f_code.co_name}.{name}")
            frame = frame.f_back
        return httpx.Response(200, json=[])

    list(make(Recorder(answer=inspect_frames)).fetch("prices", ["AAA"], DAY, DAY))
    assert leaks == []
    assert QuotaState("tiingo", None).path is None
