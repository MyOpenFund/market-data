import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from market_data.adapters.base import (
    RAW_ACTION_SCHEMA,
    RAW_PRICE_SCHEMA,
    RAW_SYMBOL_SCHEMA,
    Adapter,
    FetchResult,
    Secret,
    load_adapters,
)
from market_data.adapters.massive import (
    AMBIGUOUS_ACTION,
    API_KEY_ENV,
    BAD_CURRENCY,
    BAD_IDENTIFIER,
    BAD_RATIO,
    BAD_TIMESTAMP,
    BAD_VOLUME,
    BASE_URL,
    CONFLICTING_DUPLICATE,
    DUPLICATE_ROW,
    ERROR_PAYLOAD,
    FREE_PLAN_RPM,
    KEPT_OUT_REASONS,
    MISSING_FIELD,
    NON_FINITE,
    NON_POSITIVE_AMOUNT,
    NON_POSITIVE_CLOSE,
    NOT_AN_OBJECT,
    NULL_AMOUNT,
    SEQ_OVERFLOW,
    UNKNOWN_ADJUSTMENT_TYPE,
    UNKNOWN_DISTRIBUTION_TYPE,
    MassiveAdapter,
    MassiveError,
    MassiveKeyError,
    adapter,
)
from market_data.archive import RawArchive
from market_data.build import build_actions, build_fetch_log, build_prices

FIXTURES = Path(__file__).parent / "fixtures" / "massive"
FAKE_KEY = "synthetic-massive-key-0000"
NOW = datetime(2026, 12, 1, 12, 0, tzinfo=UTC)
DAY = date(2026, 11, 20)


def fixture(name: str) -> dict:
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


def make(recorder=None, clock=None, key=FAKE_KEY, **kw) -> MassiveAdapter:
    clock = clock or FakeClock()
    return MassiveAdapter(
        key,
        transport=httpx.MockTransport(recorder or Recorder()),
        clock=clock.clock,
        sleep=clock.sleep,
        now=lambda: NOW,
        **kw,
    )


def ok(body):
    return (200, {}, body)


def result(kind, payload, *, symbol="*", frm=None, to=None, status="ok") -> FetchResult:
    return FetchResult(
        source_code="massive",
        source_symbol=symbol,
        kind=kind,
        requested_from=frm,
        requested_to=to,
        fetched_at=NOW,
        status=status,
        payload=json.dumps(payload).encode(),
        content_type="application/json",
    )


def grouped_on(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json=fixture("grouped_daily.json"))


# --- fetch: prices --------------------------------------------------------------------------------


def test_grouped_daily_one_call_per_session():
    rec = Recorder(answer=grouped_on)
    # Fri 2026-11-20 .. Tue 2026-11-24: three sessions, the weekend is never requested
    out = list(make(rec).fetch("prices", [], date(2026, 11, 20), date(2026, 11, 24)))
    assert rec.paths() == [
        f"/v2/aggs/grouped/locale/us/market/stocks/{d}"
        for d in ("2026-11-20", "2026-11-23", "2026-11-24")
    ]
    assert [(r.requested_from, r.requested_to) for r in out] == [
        (date(2026, 11, d), date(2026, 11, d)) for d in (20, 23, 24)
    ]
    assert {(r.source_code, r.source_symbol, r.kind, r.status) for r in out} == {
        ("massive", "*", "prices", "ok")
    }
    assert all(r.fetched_at == NOW and r.content_type == "application/json" for r in out)
    assert json.loads(out[0].payload) == fixture("grouped_daily.json")
    # Thanksgiving 2026-11-26 is no session: two calls for Wed..Fri, ["*"] same as []
    rec = Recorder(answer=grouped_on)
    list(make(rec).fetch("prices", ["*"], date(2026, 11, 25), date(2026, 11, 27)))
    assert [p.rsplit("/", 1)[1] for p in rec.paths()] == ["2026-11-25", "2026-11-27"]


def test_grouped_payload_is_archived_raw():
    raw = b'{"adjusted":false,"resultsCount":0,"status":"OK"}'
    rec = Recorder(answer=lambda request: httpx.Response(200, content=raw))
    (out,) = make(rec).fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20))
    assert out.payload == raw


def test_requests_unadjusted():
    rec = Recorder(answer=grouped_on)
    list(make(rec).fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert rec.params(0) == {"adjusted": "false", "include_otc": "false"}
    rec = Recorder(answer=grouped_on)
    list(make(rec, include_otc=True).fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert rec.params(0) == {"adjusted": "false", "include_otc": "true"}
    rec = Recorder([ok(fixture("aggs_AAA.json"))])
    (out,) = make(rec).fetch("prices", ["AAA"], date(2026, 11, 19), date(2026, 11, 20))
    assert rec.paths() == ["/v2/aggs/ticker/AAA/range/1/day/2026-11-19/2026-11-20"]
    assert rec.params(0) == {"adjusted": "false", "sort": "asc", "limit": "50000"}
    assert (out.source_symbol, out.requested_from, out.requested_to, out.status) == (
        "AAA",
        date(2026, 11, 19),
        date(2026, 11, 20),
        "ok",
    )


def test_adjusted_answer_is_refused():
    body = {**fixture("grouped_daily.json"), "adjusted": True}
    rec = Recorder([ok(body)])
    with pytest.raises(MassiveError, match="adjusted"):
        list(make(rec).fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    with pytest.raises(ValueError, match="adjusted"):
        make().parse(result("prices", body, frm=date(2026, 11, 20), to=date(2026, 11, 20)))


def test_per_symbol_fetch_one_result_per_symbol():
    rec = Recorder(answer=lambda r: httpx.Response(200, json={"adjusted": False, "status": "OK"}))
    out = list(make(rec).fetch("prices", ["AAA", "BBB"], date(2026, 11, 19), date(2026, 11, 20)))
    assert [r.source_symbol for r in out] == ["AAA", "BBB"]
    assert [r.status for r in out] == ["empty", "empty"]
    assert rec.paths()[1] == "/v2/aggs/ticker/BBB/range/1/day/2026-11-19/2026-11-20"
    # a symbol never alters the path
    rec = Recorder(answer=lambda r: httpx.Response(200, json={"adjusted": False, "status": "OK"}))
    list(make(rec).fetch("prices", ["A/../B"], date(2026, 11, 19), date(2026, 11, 20)))
    assert rec.requests[0].url.raw_path.startswith(b"/v2/aggs/ticker/A%2F..%2FB/range/")


def test_holiday_or_unpublished_session_is_status_empty():
    rec = Recorder([ok(fixture("grouped_daily_empty.json"))])
    (out,) = make(rec).fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20))
    assert out.status == "empty"
    table = make().parse(out)
    assert table.num_rows == 0 and table.schema == RAW_PRICE_SCHEMA


# --- rate limit and retries -----------------------------------------------------------------------


def test_rate_limit_respected():
    rec, clk = Recorder(answer=grouped_on), FakeClock()
    start = clk.now
    sent: list[float] = []

    def timed(request):
        sent.append(clk.now)
        return grouped_on(request)

    rec.answer = timed
    # six sessions: Mon 2026-11-16 .. Mon 2026-11-23
    out = list(make(rec, clk).fetch("prices", [], date(2026, 11, 16), date(2026, 11, 23)))
    assert len(out) == 6 and len(rec.requests) == 6
    assert clk.now - start >= 60
    # evenly spaced: one request every 60 / 5 = 12 s, never a burst
    assert clk.sleeps == [12.0] * 5
    assert [round(b - a, 6) for a, b in zip(sent, sent[1:], strict=False)] == [12.0] * 5
    assert FREE_PLAN_RPM == 5


def test_rpm_is_a_parameter_within_the_free_plan():
    rec, clk = Recorder(answer=grouped_on), FakeClock()
    list(make(rec, clk, rpm=2).fetch("prices", [], date(2026, 11, 19), date(2026, 11, 20)))
    assert clk.sleeps == [30.0]
    with pytest.raises(ValueError, match="rpm"):
        make(rpm=6)
    with pytest.raises(ValueError, match="rpm"):
        make(rpm=0)


def test_no_sleep_when_requests_are_already_spaced():
    clk = FakeClock()
    a = make(Recorder(answer=grouped_on), clk)
    list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    clk.now += 30
    list(a.fetch("prices", [], date(2026, 11, 23), date(2026, 11, 23)))
    assert clk.sleeps == []


def test_429_backs_off_no_retry_storm():
    # Retry-After honoured
    clk = FakeClock()
    rec = Recorder([(429, {"Retry-After": "40"}, {}), ok(fixture("grouped_daily.json"))])
    (out,) = make(rec, clk).fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20))
    assert out.status == "ok" and len(rec.requests) == 2
    assert clk.sleeps == [40.0]
    # no Retry-After: exponential backoff, never shorter than the 12 s spacing, then gives up
    clk = FakeClock()
    rec = Recorder([(429, {}, {})] * 10)
    a = make(rec, clk, max_retries=3, backoff_s=10.0)
    with pytest.raises(MassiveError, match="429"):
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert len(rec.requests) == 4  # one try and three retries
    assert clk.sleeps == [12.0, 20.0, 40.0]
    # a Retry-After beyond max_wait_s gives up at once
    clk = FakeClock()
    rec = Recorder([(429, {"Retry-After": "3600"}, {})])
    with pytest.raises(MassiveError, match="max_wait"):
        list(make(rec, clk, max_wait_s=300).fetch("prices", [], date(2026, 11, 20), None))
    assert clk.sleeps == [] and len(rec.requests) == 1


def test_retry_parameters_validated():
    for bad in ({"max_retries": -1}, {"backoff_s": 0}, {"max_wait_s": 0}):
        with pytest.raises(ValueError):
            make(**bad)


# --- key handling ---------------------------------------------------------------------------------


def test_missing_key_clear_error():
    rec = Recorder()
    for key in (None, "", "   "):
        a = make(rec, key=key)
        with pytest.raises(MassiveKeyError) as exc:
            list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
        assert API_KEY_ENV in str(exc.value) and "free" in str(exc.value)
    with pytest.raises(MassiveKeyError, match=API_KEY_ENV):
        list(MassiveAdapter.from_env({}).fetch("symbols", [], None, None))
    assert rec.requests == []  # nothing sent without a key
    # parsing archived payloads needs no key
    table = make(key=None).parse(result("prices", fixture("grouped_daily.json"), frm=DAY, to=DAY))
    assert table.num_rows == 2


def test_key_sent_as_bearer_header_never_in_a_url():
    rec = Recorder(answer=grouped_on)
    list(make(rec, key=Secret(FAKE_KEY)).fetch("prices", [], date(2026, 11, 20), None))
    (request,) = rec.requests
    assert request.headers["authorization"] == f"Bearer {FAKE_KEY}"
    assert FAKE_KEY not in str(request.url) and "apikey" not in str(request.url).lower()
    assert str(request.url).startswith(BASE_URL + "/")


def test_from_env_reads_the_key():
    rec = Recorder(answer=grouped_on)
    a = MassiveAdapter.from_env({API_KEY_ENV: f" {FAKE_KEY} "}, transport=httpx.MockTransport(rec))
    list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert rec.requests[0].headers["authorization"] == f"Bearer {FAKE_KEY}"


def test_key_never_leaks(tmp_path):
    a = make(Recorder(answer=lambda r: httpx.Response(500, json={"status": "ERROR"})))
    assert FAKE_KEY not in repr(a) and FAKE_KEY not in str(a)
    assert FAKE_KEY not in repr(vars(a))
    with pytest.raises(MassiveError) as exc:
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert "500" in str(exc.value) and FAKE_KEY not in str(exc.value)
    assert exc.value.__cause__ is None and exc.value.__context__ is None

    def broken(request):
        raise httpx.ConnectError(f"synthetic failure {request.headers['authorization']}")

    a = make(Recorder(answer=broken))
    with pytest.raises(MassiveError, match="ConnectError") as exc:
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert FAKE_KEY not in str(exc.value) and exc.value.__cause__ is None
    assert exc.value.__context__ is None

    # archived fetches: neither the payload nor the sidecar holds the key
    arc = RawArchive(tmp_path / "arc")
    rec = Recorder([ok(fixture("dividends_page1.json")), ok(fixture("dividends_page2.json"))])
    rec.answers.append(ok(fixture("splits.json")))
    for r in make(rec).fetch("actions", [], date(2026, 11, 20), date(2026, 11, 23)):
        assert FAKE_KEY not in repr(r)
        arc.put(r)
    for path in (tmp_path / "arc").rglob("*"):
        if path.is_file():
            assert FAKE_KEY.encode() not in path.read_bytes()


def test_auth_failure_is_a_clear_error():
    a = make(Recorder([(401, {}, {"status": "ERROR", "error": "Unknown API Key"})]))
    with pytest.raises(MassiveKeyError, match=API_KEY_ENV) as exc:
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert "401" in str(exc.value) and FAKE_KEY not in str(exc.value)


def test_error_status_in_a_200_body_is_refused():
    a = make(Recorder([ok({"status": "ERROR", "error": "synthetic"})]))
    with pytest.raises(MassiveError, match="ERROR"):
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    a = make(Recorder(answer=lambda r: httpx.Response(200, content=b"not json")))
    with pytest.raises(MassiveError, match="JSON"):
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))


# --- fetch: actions, symbols, pagination ----------------------------------------------------------


def test_actions_follow_next_url_without_key_leakage():
    rec = Recorder(
        [
            ok(fixture("dividends_page1.json")),
            ok(fixture("dividends_page2.json")),
            ok(fixture("splits.json")),
        ]
    )
    out = list(make(rec).fetch("actions", [], date(2026, 11, 20), date(2026, 11, 23)))
    assert rec.paths() == ["/stocks/v1/dividends", "/stocks/v1/dividends", "/stocks/v1/splits"]
    assert rec.params(0) == {
        "ex_dividend_date.gte": "2026-11-20",
        "ex_dividend_date.lte": "2026-11-23",
        "limit": "5000",
    }
    # the next page is fetched exactly as given: nothing appended, the key stays in the header
    assert str(rec.requests[1].url) == fixture("dividends_page1.json")["next_url"]
    assert rec.params(2) == {
        "execution_date.gte": "2026-11-20",
        "execution_date.lte": "2026-11-23",
        "limit": "5000",
    }
    for request in rec.requests:
        assert FAKE_KEY not in str(request.url)
        assert request.headers["authorization"] == f"Bearer {FAKE_KEY}"
    # one FetchResult per complete list (ruling 68): dividends (2 pages merged), then splits
    assert len(out) == 2
    assert {(r.source_symbol, r.kind, r.requested_from, r.requested_to) for r in out} == {
        ("*", "actions", date(2026, 11, 20), date(2026, 11, 23))
    }
    assert [r.status for r in out] == ["ok", "ok"]
    merged = json.loads(out[0].payload)
    assert merged["results"] == (
        fixture("dividends_page1.json")["results"] + fixture("dividends_page2.json")["results"]
    )
    assert "next_url" not in merged


def test_next_url_to_another_host_is_refused():
    page = {**fixture("dividends_page1.json"), "next_url": "https://elsewhere.example/x?c=1"}
    rec = Recorder([ok(page)])
    it = make(rec).fetch("actions", [], date(2026, 11, 20), date(2026, 11, 23))
    failed = next(it)
    assert (failed.status, failed.payload) == ("error", ERROR_PAYLOAD)
    with pytest.raises(MassiveError, match="next_url"):
        next(it)
    assert len(rec.requests) == 1


def test_symbols_fetch_active_and_delisted_with_pagination():
    rec = Recorder(
        [
            ok(fixture("tickers_active.json")),
            ok({"status": "OK", "results": []}),
            ok(fixture("tickers_inactive.json")),
        ]
    )
    out = list(make(rec).fetch("symbols", [], None, None))
    assert rec.params(0) == {"market": "stocks", "active": "true", "limit": "1000"}
    assert str(rec.requests[1].url) == fixture("tickers_active.json")["next_url"]
    assert rec.params(2) == {"market": "stocks", "active": "false", "limit": "1000"}
    assert [(r.kind, r.source_symbol, r.status) for r in out] == [
        ("symbols", "*", "ok"),  # both active pages, merged
        ("symbols", "*", "ok"),
    ]
    assert [r["ticker"] for r in json.loads(out[0].payload)["results"]] == ["AAA", "BBB"]


def test_fetch_arguments_validated():
    a = make(Recorder())
    with pytest.raises(ValueError, match="kind"):
        list(a.fetch("quotes", [], date(2026, 11, 20), date(2026, 11, 20)))
    with pytest.raises(ValueError, match="after"):
        list(a.fetch("prices", [], date(2026, 11, 21), date(2026, 11, 20)))
    with pytest.raises(ValueError, match="start"):
        list(a.fetch("actions", [], None, date(2026, 11, 20)))
    with pytest.raises(ValueError, match="whole-market"):
        list(a.fetch("actions", ["AAA"], date(2026, 11, 20), date(2026, 11, 20)))
    with pytest.raises(ValueError, match="whole-market"):
        list(a.fetch("symbols", ["AAA"], None, None))
    with pytest.raises(ValueError, match=r"\*"):
        list(a.fetch("prices", ["AAA", "*"], date(2026, 11, 20), date(2026, 11, 20)))


def test_history_window_of_the_free_plan():
    rec = Recorder(answer=grouped_on)
    a = make(rec)  # NOW = 2026-12-01; default window 730 days
    with pytest.raises(ValueError, match="max_history_days"):
        list(a.fetch("prices", [], date(2024, 11, 30), date(2024, 12, 2)))
    with pytest.raises(ValueError, match="max_history_days"):
        list(a.fetch("actions", [], date(2020, 1, 1), date(2026, 11, 20)))
    assert rec.requests == []
    list(make(rec, max_history_days=None).fetch("prices", [], date(2020, 1, 2), None))
    assert len(rec.requests) == 1


# --- parse ----------------------------------------------------------------------------------------


def test_parse_grouped_prices():
    day = date(2026, 11, 20)
    table = make().parse(result("prices", fixture("grouped_daily.json"), frm=day, to=day))
    assert table.schema == RAW_PRICE_SCHEMA
    assert table.to_pylist() == [
        {"source_code": "massive", "source_symbol": "AAA", "valid_date": day, "open": 10.0,
         "high": 10.8, "low": 9.9, "close": 10.5, "volume": 120000, "currency": "USD"},
        {"source_code": "massive", "source_symbol": "BBB", "valid_date": day, "open": 20.0,
         "high": 20.2, "low": 19.1, "close": 19.5, "volume": 5300, "currency": "USD"},
    ]  # fmt: skip


def test_parse_per_symbol_dates_in_new_york():
    body = fixture("aggs_AAA.json")
    # 2026-11-19T03:00Z is still 2026-11-18 in New York
    body["results"].append({**body["results"][0], "t": 1795057200000, "c": 11.0})
    table = make().parse(
        result("prices", body, symbol="AAA", frm=date(2026, 11, 19), to=date(2026, 11, 20))
    )
    rows = table.to_pylist()
    assert [(r["source_symbol"], r["valid_date"], r["close"]) for r in rows] == [
        ("AAA", date(2026, 11, 19), 10.0),
        ("AAA", date(2026, 11, 20), 10.5),
        ("AAA", date(2026, 11, 18), 11.0),
    ]
    assert rows[1]["volume"] == 1500


def nonzero(report):
    return {reason: keys for reason, keys in report.kept_out.items() if keys}


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("v", 1500.5, BAD_VOLUME),
        ("v", -1, BAD_VOLUME),
        ("v", "12", BAD_VOLUME),
        ("v", float("nan"), NON_FINITE),
        ("c", float("inf"), NON_FINITE),
        ("o", float("nan"), NON_FINITE),
        ("c", 0.0, NON_POSITIVE_CLOSE),
        ("c", -1.0, NON_POSITIVE_CLOSE),
        ("c", None, MISSING_FIELD),
        ("c", "10.5", MISSING_FIELD),
    ],
)
def test_bad_price_row_kept_out_and_counted(field, value, reason):
    body = fixture("grouped_daily.json")
    body["results"][1][field] = value
    report = make().parse_report(result("prices", body, frm=DAY, to=DAY))
    assert report.table["source_symbol"].to_pylist() == ["AAA"]  # the good row still parses
    assert nonzero(report) == {reason: (("BBB", DAY),)}
    assert report.counts()[reason] == 1 and report.row_counts[reason] == 1


def test_price_rows_without_identity_or_object_kept_out():
    body = {"adjusted": False, "results": ["x", {"c": 1.0}, {"T": "AAA", "c": 1.0}]}
    report = make().parse_report(result("prices", body, frm=DAY, to=DAY))
    assert report.table.num_rows == 1
    assert nonzero(report) == {NOT_AN_OBJECT: (("", None),), MISSING_FIELD: (("", DAY),)}
    aggs = {"results": [{"c": 1.0}, {"c": 1.0, "t": "1795064400000"}]}
    report = make().parse_report(result("prices", aggs, symbol="AAA", frm=DAY, to=DAY))
    assert report.table.num_rows == 0
    assert nonzero(report) == {MISSING_FIELD: (("AAA", None),)}
    assert report.row_counts[MISSING_FIELD] == 2  # two rows, one distinct key


def test_duplicate_bars_kept_once_conflicting_kept_out():
    bar = {"T": "AAA", "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 10}
    body = {"results": [bar, dict(bar), {**bar, "T": "BBB"}, {**bar, "T": "BBB", "c": 2.0}]}
    report = make().parse_report(result("prices", body, frm=DAY, to=DAY))
    assert report.table["source_symbol"].to_pylist() == ["AAA"]
    assert nonzero(report) == {
        DUPLICATE_ROW: (("AAA", DAY),),
        CONFLICTING_DUPLICATE: (("BBB", DAY),),
    }
    assert report.row_counts[CONFLICTING_DUPLICATE] == 2


def test_missing_volume_is_null():
    body = fixture("grouped_daily.json")
    del body["results"][0]["v"]
    table = make().parse(result("prices", body, frm=DAY, to=DAY))
    assert table["volume"].to_pylist() == [None, 5300]


def test_parse_dividends_and_kept_out_counts():
    a = make()
    page1 = result("actions", fixture("dividends_page1.json"), frm=date(2026, 11, 20))
    report = a.parse_report(page1)
    assert report.table.schema == RAW_ACTION_SCHEMA
    rows = report.table.to_pylist()
    assert [
        (r["source_symbol"], r["valid_date"], r["action_type"], r["action_seq"], r["amount"])
        for r in rows
    ] == [
        ("AAA", date(2026, 11, 20), "cash_dividend_regular", 0, 0.25),  # raw, not split-adjusted
        ("BBB", date(2026, 11, 20), "cash_dividend_special", 0, 1.0),
        ("BBB", date(2026, 11, 20), "cash_dividend_special", 1, 0.1),
    ]
    first = rows[0]
    assert first == {
        "source_code": "massive", "source_symbol": "AAA", "valid_date": date(2026, 11, 20),
        "action_type": "cash_dividend_regular", "action_seq": 0, "source_action_type": "recurring",
        "announce_date": date(2026, 11, 5), "record_date": date(2026, 11, 20),
        "pay_date": date(2026, 12, 1), "amount": 0.25, "currency": "USD", "amount_basis": None,
        "per_share_basis": None, "ratio_old": None, "ratio_new": None, "spinoff_symbol": None,
    }  # fmt: skip
    assert [r["source_action_type"] for r in rows[1:]] == ["special", "supplemental"]
    assert nonzero(report) == {UNKNOWN_DISTRIBUTION_TYPE: (("CCC", date(2026, 11, 23)),)}
    assert set(report.kept_out) == set(KEPT_OUT_REASONS)  # every reason present
    page2 = a.parse_report(result("actions", fixture("dividends_page2.json")))
    assert [(r["source_symbol"], r["action_type"]) for r in page2.table.to_pylist()] == [
        ("AAA", "cash_dividend_special")
    ]
    assert {r: n for r, n in page2.counts().items() if n} == {UNKNOWN_DISTRIBUTION_TYPE: 1}
    assert a.parse(page1).equals(report.table)


def test_parse_splits_and_kept_out_counts():
    report = make().parse_report(result("actions", fixture("splits.json")))
    rows = report.table.to_pylist()
    assert [
        (r["source_symbol"], r["valid_date"], r["action_type"], r["ratio_old"], r["ratio_new"],
         r["source_action_type"], r["amount"], r["action_seq"])
        for r in rows
    ] == [
        ("AAA", date(2026, 11, 23), "split", 1.0, 2.0, "forward_split", None, 0),
        ("BBB", date(2026, 11, 20), "split", 10.0, 1.0, "reverse_split", None, 0),
        ("CCC", date(2026, 11, 23), "stock_dividend", 100.0, 105.0, "stock_dividend", None, 0),
    ]  # fmt: skip
    assert nonzero(report) == {UNKNOWN_ADJUSTMENT_TYPE: (("DDD", date(2026, 11, 23)),)}


def div(ticker, amount, kind="special", **kw):
    row = {"ticker": ticker, "cash_amount": amount, "currency": "USD",
           "ex_dividend_date": "2026-11-20", "distribution_type": kind}  # fmt: skip
    return {**row, **kw}


def split(ticker, old, new, kind="forward_split", **kw):
    row = {"ticker": ticker, "execution_date": "2026-11-20", "split_from": old, "split_to": new,
           "adjustment_type": kind}  # fmt: skip
    return {**row, **kw}


@pytest.mark.parametrize(
    ("row", "reason", "key"),
    [
        ("x", NOT_AN_OBJECT, ("", None)),
        ({"cash_amount": 1.0, "ex_dividend_date": "2026-11-20"}, MISSING_FIELD, ("", None)),
        ({"ticker": "AAA", "cash_amount": 1.0}, MISSING_FIELD, ("AAA", None)),
        (div("AAA", 1.0, ex_dividend_date="20/11/2026"), MISSING_FIELD, ("AAA", None)),
        (div("AAA", 1.0, ex_dividend_date=None), MISSING_FIELD, ("AAA", None)),
        (div("AAA", 1.0, currency=5), MISSING_FIELD, ("AAA", DAY)),
        (div("AAA", 1.0, pay_date="soon"), MISSING_FIELD, ("AAA", DAY)),
        (div("AAA", None), NULL_AMOUNT, ("AAA", DAY)),
        (div("AAA", float("nan")), NON_FINITE, ("AAA", DAY)),
        (div("AAA", 1.0, execution_date="2026-11-20"), AMBIGUOUS_ACTION, ("AAA", DAY)),
        (split("AAA", None, 2), BAD_RATIO, ("AAA", DAY)),
        (split("AAA", 0, 2), BAD_RATIO, ("AAA", DAY)),
        (split("AAA", 1, -2), BAD_RATIO, ("AAA", DAY)),
        (split("AAA", 1, float("inf")), NON_FINITE, ("AAA", DAY)),
    ],
)
def test_bad_action_row_kept_out_and_counted(row, reason, key):
    body = {"status": "OK", "results": [row, div("BBB", 1.0)]}
    report = make().parse_report(result("actions", body))
    assert report.table["source_symbol"].to_pylist() == ["BBB"]  # never blocks the payload
    assert nonzero(report) == {reason: (key,)}


def test_action_seq_follows_vendor_ids_not_payload_order():
    rows = [
        div("BBB", 0.1, "supplemental", id="z9"),
        div("BBB", 1.0, "special", id="a1"),
        div("BBB", 0.5, "irregular"),  # no id: after the identified rows
    ]

    def identities(order):
        report = make().parse_report(result("actions", {"results": order}))
        return {(r["action_seq"], r["amount"]) for r in report.table.to_pylist()}

    expected = {(0, 1.0), (1, 0.1), (2, 0.5)}
    assert identities(rows) == expected
    assert identities(rows[::-1]) == expected
    assert identities([rows[2], rows[0], rows[1]]) == expected


def test_parse_symbols():
    a = make()
    table = a.parse(result("symbols", fixture("tickers_active.json")))
    assert table.schema == RAW_SYMBOL_SCHEMA
    assert table.to_pylist() == [
        {"source_symbol": "AAA", "id_scheme": "ticker", "id_value": "AAA",
         "valid_date": date(2001, 2, 3), "valid_to": None, "name": "Synthetic Alpha Inc"},
        {"source_symbol": "AAA", "id_scheme": "composite_figi", "id_value": "BBG000000AA1",
         "valid_date": date(2001, 2, 3), "valid_to": None, "name": "Synthetic Alpha Inc"},
        {"source_symbol": "AAA", "id_scheme": "share_class_figi", "id_value": "BBG000000AA2",
         "valid_date": date(2001, 2, 3), "valid_to": None, "name": "Synthetic Alpha Inc"},
        {"source_symbol": "AAA", "id_scheme": "cik", "id_value": "0000012345",
         "valid_date": date(2001, 2, 3), "valid_to": None, "name": "Synthetic Alpha Inc"},
        {"source_symbol": "BBB", "id_scheme": "ticker", "id_value": "BBB",
         "valid_date": None, "valid_to": None, "name": "Synthetic Beta Corp"},
        {"source_symbol": "BBB", "id_scheme": "composite_figi", "id_value": "BBG000000BB1",
         "valid_date": None, "valid_to": None, "name": "Synthetic Beta Corp"},
        {"source_symbol": "BBB", "id_scheme": "cik", "id_value": "0000067890",
         "valid_date": None, "valid_to": None, "name": "Synthetic Beta Corp"},
    ]  # fmt: skip
    delisted = a.parse(result("symbols", fixture("tickers_inactive.json"))).to_pylist()
    date_only = a.parse(
        result("symbols", {"results": [{"ticker": "D", "delisted_utc": "2025-06-30"}]})
    )
    assert date_only["valid_to"].to_pylist() == [date(2025, 7, 1)]
    assert {r["id_scheme"] for r in delisted} == {"ticker", "composite_figi", "share_class_figi"}
    assert {r["valid_to"] for r in delisted} == {date(2025, 7, 1)}  # last trading day + 1


def test_bad_symbol_rows_kept_out_and_counted():
    rows = [
        "x",
        {"name": "no ticker"},
        {"ticker": "AAA", "list_date": "yesterday"},
        {"ticker": "BBB", "delisted_utc": "soon"},
        {"ticker": "CCC", "cik": "12-34", "composite_figi": 7, "share_class_figi": "BBG0000000C2"},
        {"ticker": "DDD", "cik": 4567},
    ]
    report = make().parse_report(result("symbols", {"results": rows}))
    assert [
        (r["source_symbol"], r["id_scheme"], r["id_value"]) for r in report.table.to_pylist()
    ] == [
        ("CCC", "ticker", "CCC"),
        ("CCC", "share_class_figi", "BBG0000000C2"),
        ("DDD", "ticker", "DDD"),
        ("DDD", "cik", "0000004567"),
    ]
    assert nonzero(report) == {
        NOT_AN_OBJECT: (("", None),),
        MISSING_FIELD: (("", None), ("AAA", None), ("BBB", None)),
        BAD_IDENTIFIER: (("CCC", None),),
    }
    assert report.row_counts[BAD_IDENTIFIER] == 2


def test_parse_rejects_unknown_kind_payload():
    with pytest.raises(ValueError, match="JSON"):
        make().parse(
            FetchResult("massive", "*", "symbols", None, None, NOW, "ok", b"<html>", "text/html")
        )


# --- registration and end to end ------------------------------------------------------------------


def test_adapter_protocol_and_registry():
    a = make()
    assert isinstance(a, Adapter)
    assert a.source_code == "massive" == a.info.code
    assert a.info.access == "restricted"
    assert a.info.no_public_display and a.info.no_ml_training
    assert a.info.terms_url == "https://massive.com/legal/market-data-terms-of-service"


def test_entry_point_loads_the_adapter(monkeypatch):
    monkeypatch.delenv(API_KEY_ENV, raising=False)
    assert isinstance(adapter(), MassiveAdapter)
    loaded = load_adapters(["massive"])
    assert isinstance(loaded["massive"], MassiveAdapter)


def test_archived_fetches_build_end_to_end(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    a = make(
        Recorder(
            [
                ok(fixture("grouped_daily.json")),
                ok(fixture("grouped_daily_empty.json")),
                ok(fixture("dividends_page1.json")),
                ok(fixture("dividends_page2.json")),
                ok(fixture("splits.json")),
            ]
        )
    )
    refs = [arc.put(r) for r in a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 23))]
    refs += [arc.put(r) for r in a.fetch("actions", [], date(2026, 11, 20), date(2026, 11, 23))]
    assert len(refs) == 4

    prices = build_prices(arc, a, ["AAA", "BBB"])
    assert prices.counts() == {"non_session": 0, "intraday_partial": 0}
    rows = prices.table.to_pylist()
    assert [(r["source_symbol"], r["valid_date"], r["close"], r["volume"]) for r in rows] == [
        ("AAA", date(2026, 11, 20), 10.5, 120000),
        ("BBB", date(2026, 11, 20), 19.5, 5300),
    ]
    close = datetime(2026, 11, 20, 21, 0, tzinfo=UTC)
    assert {(r["price_basis"], r["known_at"], r["source_code"]) for r in rows} == {
        ("raw", close, "massive")
    }

    actions = build_actions(arc, a, None).table.to_pylist()
    assert sorted(
        (r["source_symbol"], r["valid_date"], r["action_type"], r["action_seq"]) for r in actions
    ) == [
        ("AAA", date(2026, 11, 20), "cash_dividend_regular", 0),
        ("AAA", date(2026, 11, 23), "cash_dividend_special", 0),
        ("AAA", date(2026, 11, 23), "split", 0),
        ("BBB", date(2026, 11, 20), "cash_dividend_special", 0),
        ("BBB", date(2026, 11, 20), "cash_dividend_special", 1),
        ("BBB", date(2026, 11, 20), "split", 0),
        ("CCC", date(2026, 11, 23), "stock_dividend", 0),
    ]
    assert all(r["source_code"] == "massive" for r in actions)

    log = build_fetch_log(arc, a, "massive").to_pylist()
    assert len(log) == 4
    assert sorted(r["status"] for r in log) == ["empty", "ok", "ok", "ok"]


# --- review fixes: atomic lists, failures logged, retries, clipping (rulings 68, 69) ------------


def test_pair_straddling_a_page_boundary_builds_as_two_actions(tmp_path):
    page1 = {"status": "OK", "next_url": BASE_URL + "/stocks/v1/dividends?cursor=x",
             "results": [div("BBB", 1.0, id="d1")]}  # fmt: skip
    page2 = {"status": "OK", "results": [div("BBB", 0.1, "supplemental", id="d2")]}
    rec = Recorder([ok(page1), ok(page2), ok({"status": "OK", "results": []})])
    arc = RawArchive(tmp_path / "arc")
    for r in make(rec).fetch("actions", [], DAY, DAY):  # every fetch at the same instant
        arc.put(r)
    rows = build_actions(arc, make(), None).table.to_pylist()
    assert [
        (r["source_symbol"], r["action_seq"], r["amount"], r["known_at_kind"]) for r in rows
    ] == [
        ("BBB", 0, 1.0, "published"),
        ("BBB", 1, 0.1, "published"),
    ]


def test_vendor_reordering_between_nights_keeps_identities(tmp_path):
    rows = [div("BBB", 1.0, id="d1"), div("BBB", 0.1, "supplemental", id="d2")]
    arc = RawArchive(tmp_path / "arc")
    for night, order in ((NOW, rows), (NOW + timedelta(days=1), rows[::-1])):
        payload = json.dumps({"status": "OK", "results": order}).encode()
        arc.put(FetchResult("massive", "*", "actions", DAY, DAY, night, "ok", payload, "x/json"))
    built = build_actions(arc, make(), None).table.to_pylist()
    assert len(built) == 2  # no spurious "changed" rows on the second night
    assert {(r["action_seq"], r["amount"]) for r in built} == {(0, 1.0), (1, 0.1)}


def test_mid_list_failure_is_archived_as_error_then_raised(tmp_path):
    page1 = {**fixture("dividends_page1.json")}
    rec = Recorder([ok(page1), (502, {}, {}), (502, {}, {})])
    arc = RawArchive(tmp_path / "arc")
    a = make(rec, max_retries=1)
    with pytest.raises(MassiveError, match="502") as exc:
        for r in a.fetch("actions", [], date(2026, 11, 20), date(2026, 11, 23)):
            arc.put(r)
    assert FAKE_KEY not in str(exc.value) and exc.value.__context__ is None
    assert len(rec.requests) == 3  # first page, then the second page tried twice
    log = build_fetch_log(arc, a, "massive").to_pylist()
    assert [(r["status"], r["requested_from"], r["requested_to"], r["row_count"]) for r in log] == [
        ("error", date(2026, 11, 20), date(2026, 11, 23), 0)
    ]
    assert build_actions(arc, a, None).table.num_rows == 0  # no partial list ever archived


@pytest.mark.parametrize(
    ("answer", "match"),
    [
        (ok({**fixture("grouped_daily.json"), "adjusted": True}), "adjusted"),
        ((200, {}, b"not json"), "JSON"),
        (ok({"status": "ERROR", "error": "synthetic"}), "ERROR"),
        (ok({"status": "OK", "results": {"a": 1}}), "list"),
    ],
)
def test_unusable_payload_is_an_error_row_never_ok(answer, match):
    it = make(Recorder([answer])).fetch("prices", [], DAY, DAY)
    failed = next(it)
    assert (failed.status, failed.payload, failed.requested_from) == ("error", ERROR_PAYLOAD, DAY)
    with pytest.raises(MassiveError, match=match):
        next(it)


def test_transport_errors_and_5xx_are_retried_bounded():
    clk = FakeClock()
    rec = Recorder([httpx.ReadTimeout("t"), (503, {}, {}), ok(fixture("grouped_daily.json"))])
    (out,) = make(rec, clk, backoff_s=15.0).fetch("prices", [], DAY, DAY)
    assert out.status == "ok" and len(rec.requests) == 3
    assert clk.sleeps == [15.0, 30.0]
    rec = Recorder(answer=lambda r: httpx.Response(503))
    with pytest.raises(MassiveError, match="503; gave up after 2 retries"):
        list(make(rec, max_retries=2).fetch("prices", [], DAY, DAY))
    assert len(rec.requests) == 3


def test_redirect_is_neither_followed_nor_retried():
    rec = Recorder([(302, {"Location": "https://elsewhere.example/"}, {})])
    with pytest.raises(MassiveError, match="302"):
        list(make(rec).fetch("prices", [], DAY, DAY))
    assert len(rec.requests) == 1


def test_sessions_not_closed_yet_are_never_requested():
    # NOW = 2026-12-01 12:00 UTC: the 2026-12-01 session closes at 21:00 UTC
    rec = Recorder(answer=grouped_on)
    out = list(make(rec).fetch("prices", [], date(2026, 11, 30), date(2026, 12, 4)))
    assert [r.requested_from for r in out] == [date(2026, 11, 30)]
    rec = Recorder(answer=lambda r: httpx.Response(200, json={"adjusted": False}))
    (out,) = make(rec).fetch("prices", ["AAA"], date(2026, 11, 27), date(2026, 12, 4))
    assert rec.paths() == ["/v2/aggs/ticker/AAA/range/1/day/2026-11-27/2026-11-30"]
    assert (out.requested_from, out.requested_to) == (date(2026, 11, 27), date(2026, 11, 30))
    rec = Recorder()
    assert list(make(rec).fetch("prices", ["AAA"], date(2026, 12, 1), date(2026, 12, 4))) == []
    assert list(make(rec).fetch("prices", [], date(2026, 12, 1), date(2026, 12, 4))) == []
    assert rec.requests == []


def test_per_symbol_aggs_pagination_is_one_result():
    first = {**fixture("aggs_AAA.json"), "next_url": BASE_URL + "/v2/aggs/ticker/AAA/next?c=1"}
    second = {"ticker": "AAA", "adjusted": False, "status": "OK",
              "results": [{**first["results"][0], "t": 1795237200000, "c": 12.0}]}  # fmt: skip
    rec = Recorder([ok(first), ok(second)])
    (out,) = make(rec).fetch("prices", ["AAA"], date(2026, 11, 19), date(2026, 11, 23))
    assert len(rec.requests) == 2 and out.status == "ok"
    table = make().parse(out)
    assert table["valid_date"].to_pylist() == [date(2026, 11, d) for d in (19, 20, 21)]
    # an adjusted later page fails the whole list
    rec = Recorder([ok(first), ok({**second, "adjusted": True})])
    it = make(rec).fetch("prices", ["AAA"], date(2026, 11, 19), date(2026, 11, 23))
    assert next(it).status == "error"
    with pytest.raises(MassiveError, match="adjusted"):
        next(it)


@pytest.mark.parametrize("key", ["clé-synthetic", "ab cd", "tab\there", Secret("x\ny")])
def test_key_not_fit_for_a_header_is_a_clear_error(key):
    rec = Recorder()
    with pytest.raises(MassiveKeyError, match="printable ASCII") as exc:
        make(rec, key=key).fetch("prices", [], DAY, DAY)
    assert not any(ch in str(exc.value) for ch in "é\t\n") and "ab cd" not in str(exc.value)
    assert rec.requests == []


def test_missing_key_raised_when_fetch_is_called():
    rec = Recorder()
    with pytest.raises(MassiveKeyError):
        make(rec, key=None).fetch("prices", [], DAY, DAY)  # not even iterated
    assert rec.requests == []


def test_not_authorized_names_the_status_word_only():
    body = {"status": "NOT_AUTHORIZED", "message": "synthetic free text"}
    it = make(Recorder([(403, {}, body)])).fetch("prices", [], DAY, DAY)
    assert next(it).status == "error"
    with pytest.raises(MassiveKeyError) as exc:
        next(it)
    assert "403" in str(exc.value) and "NOT_AUTHORIZED" in str(exc.value)
    assert "synthetic free text" not in str(exc.value)


def test_context_manager_closes_the_client():
    with make() as a:
        assert not a._http.is_closed
    assert a._http.is_closed


# --- review round 2: out-of-range values never crash; actions deduped by id ---------------------


def grouped_raw(raw: bytes) -> FetchResult:
    return FetchResult("massive", "*", "prices", DAY, DAY, NOW, "ok", raw, "application/json")


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (b'{"results":[{"T":"BBB","c":1,"v":1e30},{"T":"AAA","c":1}]}', BAD_VOLUME),
        (b'{"results":[{"T":"BBB","c":1,"v":9223372036854775808},{"T":"AAA","c":1}]}', BAD_VOLUME),
        (b'{"results":[{"T":"BBB","c":1' + b"0" * 400 + b'},{"T":"AAA","c":1}]}', NON_FINITE),
        (
            b'{"results":[{"T":"BBB","c":1,"o":-1' + b"0" * 400 + b'},{"T":"AAA","c":1}]}',
            NON_FINITE,
        ),
        (b'{"results":[{"T":"BBB","c":1e400},{"T":"AAA","c":1}]}', NON_FINITE),
    ],
)
def test_out_of_range_numbers_are_kept_out_at_parse_and_fetch(raw, reason):
    report = make().parse_report(grouped_raw(raw))
    assert report.table["source_symbol"].to_pylist() == ["AAA"]
    assert nonzero(report) == {reason: (("BBB", DAY),)}
    (out,) = make(Recorder([(200, {}, raw)])).fetch("prices", [], DAY, DAY)
    assert out.status == "ok" and out.payload == raw


def test_volume_at_the_int64_bound_is_kept():
    raw = b'{"results":[{"T":"AAA","c":1,"v":9223372036854775807}]}'
    assert make().parse(grouped_raw(raw))["volume"].to_pylist() == [2**63 - 1]


@pytest.mark.parametrize("stamp", [1e20, -1e20, 9e15, -3e12])
def test_bar_timestamp_out_of_range_is_kept_out(stamp):
    body = {"results": [{"c": 1, "t": stamp}, {"c": 2, "t": 1795064400000}]}
    report = make().parse_report(result("prices", body, symbol="AAA", frm=DAY, to=DAY))
    assert report.table["close"].to_pylist() == [2.0]
    assert nonzero(report) == {BAD_TIMESTAMP: (("AAA", None),)}
    rec = Recorder([ok(body)])
    (out,) = make(rec).fetch("prices", ["AAA"], date(2026, 11, 19), DAY)
    assert out.status == "ok"


@pytest.mark.parametrize(
    "next_url",
    [BASE_URL + "/x\x00y", BASE_URL + "/" + "a" * 70000],
)
def test_unexpected_failure_is_an_error_row_and_a_massive_error(next_url):
    page = {"status": "OK", "results": [div("AAA", 1.0)], "next_url": next_url}
    it = make(Recorder([ok(page)])).fetch("actions", [], DAY, DAY)
    failed = next(it)
    assert (failed.status, failed.payload) == ("error", ERROR_PAYLOAD)
    with pytest.raises(MassiveError, match="InvalidURL") as exc:
        next(it)
    assert exc.value.__context__ is None and exc.value.__cause__ is None


def test_any_exception_in_a_fetch_is_logged_as_error(monkeypatch):
    a = make(Recorder([ok(fixture("grouped_daily.json"))]))

    def boom(result):
        raise KeyError(f"payload detail {FAKE_KEY}")

    monkeypatch.setattr(a, "parse_report", boom)
    it = a.fetch("prices", [], DAY, DAY)
    assert next(it).status == "error"
    with pytest.raises(MassiveError, match="fetch failed: KeyError") as exc:
        next(it)
    assert FAKE_KEY not in str(exc.value)
    assert exc.value.__context__ is None and exc.value.__cause__ is None


def test_rows_sharing_a_vendor_id_are_deduped():
    same = div("AAA", 1.0, "recurring", id="x1")
    report = make().parse_report(result("actions", {"results": [same, dict(same)]}))
    assert [(r["action_seq"], r["amount"]) for r in report.table.to_pylist()] == [(0, 1.0)]
    assert nonzero(report) == {DUPLICATE_ROW: (("AAA", DAY),)}
    rows = [div("AAA", 1.0, id="x1"), div("AAA", 2.0, id="x1"), div("AAA", 3.0, id="x2")]
    report = make().parse_report(result("actions", {"results": rows}))
    # the conflicting id keeps its slot: x2 stays seq 1 whatever the vendor repairs later
    assert [(r["action_seq"], r["amount"]) for r in report.table.to_pylist()] == [(1, 3.0)]
    assert nonzero(report) == {CONFLICTING_DUPLICATE: (("AAA", DAY),)}
    assert report.row_counts[CONFLICTING_DUPLICATE] == 2


def test_kept_out_action_keeps_its_seq_slot(tmp_path):
    bad = div("AAA", None, id="a")
    good = div("AAA", 2.0, id="b")
    report = make().parse_report(result("actions", {"results": [good, bad]}))
    assert [(r["action_seq"], r["amount"]) for r in report.table.to_pylist()] == [(1, 2.0)]
    assert nonzero(report) == {NULL_AMOUNT: (("AAA", DAY),)}
    # the vendor repairs the lower-id row the next night: b keeps its identity
    arc = RawArchive(tmp_path / "arc")
    for night, rows in (
        (NOW, [bad, good]),
        (NOW + timedelta(days=1), [{**bad, "cash_amount": 1.0}, good]),
    ):
        payload = json.dumps({"results": rows}).encode()
        arc.put(FetchResult("massive", "*", "actions", DAY, DAY, night, "ok", payload, "x/json"))
    built = build_actions(arc, make(), None).table.to_pylist()
    assert sorted((r["action_seq"], r["amount"], r["known_at_kind"]) for r in built) == [
        (0, 1.0, "published"),  # first sighting of the repaired row
        (1, 2.0, "published"),  # unchanged: never re-emitted under a new seq
    ]
    assert len(built) == 2


def test_action_seq_overflow_is_kept_out():
    rows = [div("AAA", 1.0, id=f"{i:06d}") for i in range(2**15 + 2)]
    report = make().parse_report(result("actions", {"results": rows}))
    assert report.table.num_rows == 2**15
    assert report.row_counts[SEQ_OVERFLOW] == 2


@pytest.mark.parametrize(
    ("amount", "currency", "reason", "kept"),
    [
        (0, "USD", NON_POSITIVE_AMOUNT, None),
        (-1.0, "USD", NON_POSITIVE_AMOUNT, None),
        (1.0, " usd ", None, "USD"),
        (1.0, "GBp", BAD_CURRENCY, None),
        (1.0, "GBX", BAD_CURRENCY, None),
        (1.0, "US", BAD_CURRENCY, None),
        (1.0, "US D", BAD_CURRENCY, None),
    ],
)
def test_dividend_amount_and_currency(amount, currency, reason, kept):
    row = div("AAA", amount, "recurring", currency=currency)
    report = make().parse_report(result("actions", {"results": [row]}))
    assert report.table["currency"].to_pylist() == ([kept] if kept else [])
    assert nonzero(report) == ({reason: (("AAA", DAY),)} if reason else {})


@pytest.mark.parametrize("value", ["20261120", "2026-11-20T00:00:00Z", "2026-W47-5"])
def test_only_plain_iso_dates_are_dates(value):
    report = make().parse_report(
        result("actions", {"results": [div("AAA", 1.0, ex_dividend_date=value)]})
    )
    assert report.table.num_rows == 0 and nonzero(report) == {MISSING_FIELD: (("AAA", None),)}


def test_null_date_of_the_other_kind_is_not_ambiguous():
    row = split("AAA", 1, 2, ex_dividend_date=None)
    report = make().parse_report(result("actions", {"results": [row]}))
    assert report.table["action_type"].to_pylist() == ["split"] and nonzero(report) == {}


def test_symbols_ticker_stripped_consistently():
    rows = [{"ticker": " BBB ", "cik": "12"}]
    table = make().parse(result("symbols", {"results": rows}))
    assert [(r["source_symbol"], r["id_scheme"], r["id_value"]) for r in table.to_pylist()] == [
        ("BBB", "ticker", "BBB"),
        ("BBB", "cik", "0000000012"),
    ]


def test_delisted_out_of_range_is_kept_out():
    rows = [{"ticker": "A", "delisted_utc": "9999-12-31T00:00:00Z"},
            {"ticker": "B", "delisted_utc": "0001-01-01T00:00:00+05:00"}]  # fmt: skip
    report = make().parse_report(result("symbols", {"results": rows}))
    assert report.table.num_rows == 0
    assert nonzero(report) == {MISSING_FIELD: (("A", None), ("B", None))}


def test_self_referencing_next_url_stops():
    page = {"status": "OK", "results": [div("AAA", 1.0)],
            "next_url": BASE_URL + "/stocks/v1/dividends?cursor=x"}  # fmt: skip
    rec = Recorder(answer=lambda r: httpx.Response(200, json=page))
    it = make(rec).fetch("actions", [], DAY, DAY)
    assert next(it).status == "error"
    with pytest.raises(MassiveError, match="repeats"):
        next(it)
    assert len(rec.requests) == 2


def test_max_pages_bounds_a_list():
    counter = iter(range(100))

    def endless(request):
        nxt = f"{BASE_URL}/stocks/v1/dividends?cursor={next(counter)}"
        return httpx.Response(200, json={"results": [], "next_url": nxt})

    rec = Recorder(answer=endless)
    it = make(rec, max_pages=3).fetch("actions", [], DAY, DAY)
    assert next(it).status == "error"
    with pytest.raises(MassiveError, match="max_pages"):
        next(it)
    assert len(rec.requests) == 3
    with pytest.raises(ValueError, match="max_pages"):
        make(max_pages=0)


@pytest.mark.parametrize("currency", [None, "", "  "])
def test_dividend_without_currency_is_counted_as_such(currency):
    from market_data.adapters.massive import CASH_WITHOUT_CURRENCY

    row = div("AAA", 1.0, "recurring", currency=currency)
    if currency is None:
        del row["currency"]
    report = make().parse_report(result("actions", {"results": [row]}))
    assert report.table.num_rows == 0
    assert nonzero(report) == {CASH_WITHOUT_CURRENCY: (("AAA", DAY),)}
    assert CASH_WITHOUT_CURRENCY in KEPT_OUT_REASONS


def _holds_key(value, key: str, depth: int = 0) -> bool:
    """The key in a string, bytes, or the keys/values of headers, mappings, sequences and
    requests (header values, not their repr)."""
    if depth > 6:
        return False
    if isinstance(value, str | bytes):
        return (key if isinstance(value, str) else key.encode()) in value
    if isinstance(value, httpx.Request):
        return _holds_key(value.headers, key, depth + 1)
    if isinstance(value, httpx.Headers):
        return any(_holds_key(v, key, depth + 1) for _, v in value.raw)
    if isinstance(value, dict):
        return any(
            _holds_key(k, key, depth + 1) or _holds_key(v, key, depth + 1) for k, v in value.items()
        )
    if isinstance(value, list | tuple | set | frozenset):
        return any(_holds_key(v, key, depth + 1) for v in value)
    return False


def test_key_not_a_local_of_the_request_frames():
    import sys

    from market_data.adapters import massive as massive_module

    leaks: list[str] = []

    def inspect_frames(request):
        assert request.headers["authorization"] == f"Bearer {FAKE_KEY}"
        frame = sys._getframe()
        while frame is not None:
            if frame.f_code.co_filename == massive_module.__file__:
                for name, value in frame.f_locals.items():
                    if _holds_key(value, FAKE_KEY) or FAKE_KEY in repr(value):
                        leaks.append(f"{frame.f_code.co_name}.{name}")
            frame = frame.f_back
        return httpx.Response(200, json=fixture("grouped_daily.json"))

    a = make(Recorder(answer=inspect_frames))
    list(a.fetch("prices", [], DAY, DAY))
    assert leaks == []
    assert FAKE_KEY not in repr(vars(a))
