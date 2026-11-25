import json
from datetime import UTC, date, datetime
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
    API_KEY_ENV,
    BASE_URL,
    FREE_PLAN_RPM,
    KEPT_OUT_REASONS,
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
        status, headers, body = self.answers.pop(0)
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
    a = make(Recorder([(500, {}, {"status": "ERROR", "error": "boom"})]))
    assert FAKE_KEY not in repr(a) and FAKE_KEY not in str(a)
    assert FAKE_KEY not in repr(vars(a))
    with pytest.raises(MassiveError) as exc:
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert "500" in str(exc.value) and FAKE_KEY not in str(exc.value)
    assert exc.value.__cause__ is None

    def broken(request):
        raise httpx.ConnectError(f"synthetic failure {request.headers['authorization']}")

    a = make(Recorder(answer=broken))
    with pytest.raises(MassiveError, match="ConnectError") as exc:
        list(a.fetch("prices", [], date(2026, 11, 20), date(2026, 11, 20)))
    assert FAKE_KEY not in str(exc.value) and exc.value.__cause__ is None
    assert exc.value.__context__ is None or FAKE_KEY not in str(exc.value.__context__)

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
    assert len(out) == 3  # one archived FetchResult per page
    assert {(r.source_symbol, r.kind, r.requested_from, r.requested_to) for r in out} == {
        ("*", "actions", date(2026, 11, 20), date(2026, 11, 23))
    }
    assert [r.status for r in out] == ["ok", "ok", "ok"]


def test_next_url_to_another_host_is_refused():
    page = {**fixture("dividends_page1.json"), "next_url": "https://elsewhere.example/x?c=1"}
    rec = Recorder([ok(page)])
    with pytest.raises(MassiveError, match="next_url"):
        list(make(rec).fetch("actions", [], date(2026, 11, 20), date(2026, 11, 23)))
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
        ("symbols", "*", "ok"),
        ("symbols", "*", "empty"),
        ("symbols", "*", "ok"),
    ]


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
    # 2026-11-20T03:00Z is still 2026-11-19 in New York
    body["results"].append({**body["results"][0], "t": 1795143600000, "c": 11.0})
    table = make().parse(
        result("prices", body, symbol="AAA", frm=date(2026, 11, 19), to=date(2026, 11, 20))
    )
    rows = table.to_pylist()
    assert [(r["source_symbol"], r["valid_date"], r["close"]) for r in rows] == [
        ("AAA", date(2026, 11, 19), 10.0),
        ("AAA", date(2026, 11, 20), 10.5),
        ("AAA", date(2026, 11, 19), 11.0),
    ]
    assert rows[1]["volume"] == 1500


@pytest.mark.parametrize("volume", [1500.5, -1, "12"])
def test_volume_non_integral_or_negative_refused(volume):
    body = fixture("grouped_daily.json")
    body["results"][1]["v"] = volume
    day = date(2026, 11, 20)
    with pytest.raises(ValueError, match="volume") as exc:
        make().parse(result("prices", body, frm=day, to=day))
    assert "BBB" in str(exc.value) and "2026-11-20" in str(exc.value)


def test_missing_volume_is_null_missing_close_refused():
    body = fixture("grouped_daily.json")
    del body["results"][0]["v"]
    day = date(2026, 11, 20)
    table = make().parse(result("prices", body, frm=day, to=day))
    assert table["volume"].to_pylist() == [None, 5300]
    del body["results"][1]["c"]
    with pytest.raises(ValueError, match="close") as exc:
        make().parse(result("prices", body, frm=day, to=day))
    assert "BBB" in str(exc.value)


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
    assert report.kept_out == {
        UNKNOWN_DISTRIBUTION_TYPE: (("CCC", date(2026, 11, 23)),),
        UNKNOWN_ADJUSTMENT_TYPE: (),
    }
    page2 = a.parse_report(result("actions", fixture("dividends_page2.json")))
    assert [(r["source_symbol"], r["action_type"]) for r in page2.table.to_pylist()] == [
        ("AAA", "cash_dividend_special")
    ]
    assert page2.counts() == {UNKNOWN_DISTRIBUTION_TYPE: 1, UNKNOWN_ADJUSTMENT_TYPE: 0}
    assert set(KEPT_OUT_REASONS) == {UNKNOWN_DISTRIBUTION_TYPE, UNKNOWN_ADJUSTMENT_TYPE}
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
    assert report.kept_out == {
        UNKNOWN_DISTRIBUTION_TYPE: (),
        UNKNOWN_ADJUSTMENT_TYPE: (("DDD", date(2026, 11, 23)),),
    }


def test_parse_actions_malformed_row_refused():
    body = {"status": "OK", "results": [{"ticker": "AAA", "cash_amount": 1.0}]}
    with pytest.raises(ValueError, match="AAA"):
        make().parse(result("actions", body))


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
    assert {r["id_scheme"] for r in delisted} == {"ticker", "composite_figi", "share_class_figi"}
    assert {r["valid_to"] for r in delisted} == {date(2025, 7, 1)}  # last trading day + 1


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
    assert len(refs) == 5

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
    assert len(log) == 5
    assert sorted(r["status"] for r in log) == ["empty", "ok", "ok", "ok", "ok"]
