import json
from pathlib import Path

import httpx
import pyarrow as pa
import pytest

from market_data.adapters.base import Secret
from market_data.resolve.openfigi import (
    API_KEY_ENV,
    API_KEY_HEADER,
    FIGI_RESULT_SCHEMA,
    KEY_MAX_JOBS,
    KEY_RPM,
    NO_KEY_MAX_JOBS,
    NO_KEY_RPM,
    OPENFIGI_URL,
    OpenFigiClient,
    OpenFigiError,
    figi_results,
    us_ticker_job,
)

RECORDED = Path(__file__).parent / "fixtures" / "openfigi" / "mapping_response.json"
FAKE_KEY = "synthetic-openfigi-key-0000"


class FakeClock:
    """Time moves only when the client sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(round(seconds, 6))
        self.now += seconds


class Recorder:
    """An httpx transport answering from a queue of (status, headers, body) or a callable."""

    def __init__(self, answers=None, *, echo: bool = False) -> None:
        self.answers = list(answers or [])
        self.echo = echo
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.echo:  # one "not found" warning per job
            jobs = json.loads(request.content)
            return httpx.Response(200, json=[{"warning": "No identifier found."} for _ in jobs])
        status, headers, body = self.answers.pop(0)
        return httpx.Response(status, headers=headers, json=body)


def client(recorder: Recorder, clock: FakeClock, key=None, **kw) -> OpenFigiClient:
    return OpenFigiClient(
        key,
        transport=httpx.MockTransport(recorder),
        clock=clock.clock,
        sleep=clock.sleep,
        **kw,
    )


def test_us_ticker_job():
    assert us_ticker_job("ABC") == {
        "idType": "TICKER",
        "idValue": "ABC",
        "exchCode": "US",
        "includeUnlistedEquities": True,
    }


def test_batches_and_spacing_without_key():
    rec, clk = Recorder(echo=True), FakeClock()
    jobs = [us_ticker_job(f"S{i}") for i in range(23)]
    out = client(rec, clk).map(jobs)
    assert len(out) == 23 and all("warning" in r for r in out)
    assert [len(json.loads(r.content)) for r in rec.requests] == [10, 10, 3]
    assert NO_KEY_MAX_JOBS == 10 and NO_KEY_RPM == 25
    for r in rec.requests:
        assert str(r.url) == OPENFIGI_URL and r.method == "POST"
        assert API_KEY_HEADER.lower() not in {k.lower() for k in r.headers}
        assert r.headers["content-type"] == "application/json"
    # 25 requests a minute: one every 2.4 s, never a burst
    assert clk.sleeps == [2.4, 2.4]


def test_batches_and_spacing_with_key():
    rec, clk = Recorder(echo=True), FakeClock()
    jobs = [us_ticker_job(f"S{i}") for i in range(230)]
    out = client(rec, clk, FAKE_KEY).map(jobs)
    assert len(out) == 230
    assert [len(json.loads(r.content)) for r in rec.requests] == [100, 100, 30]
    assert KEY_MAX_JOBS == 100 and KEY_RPM == 250
    assert all(r.headers[API_KEY_HEADER] == FAKE_KEY for r in rec.requests)
    assert clk.sleeps == [0.24, 0.24]


def test_no_sleep_when_requests_are_already_spaced():
    rec, clk = Recorder(echo=True), FakeClock()
    c = client(rec, clk)
    c.map([us_ticker_job("A")])
    clk.now += 10
    c.map([us_ticker_job("B")])
    assert clk.sleeps == []


def test_limits_above_the_documented_quota_are_refused():
    rec, clk = Recorder(), FakeClock()
    with pytest.raises(ValueError, match="25"):
        client(rec, clk, rpm=26)
    with pytest.raises(ValueError, match="10"):
        client(rec, clk, max_jobs=11)
    with pytest.raises(ValueError, match="250"):
        client(rec, clk, FAKE_KEY, rpm=251)
    with pytest.raises(ValueError, match="100"):
        client(rec, clk, FAKE_KEY, max_jobs=101)
    with pytest.raises(ValueError):
        client(rec, clk, rpm=0)
    slower = client(Recorder(echo=True), clk, rpm=6)
    slower.map([us_ticker_job("A")])
    slower.map([us_ticker_job("B")])
    assert clk.sleeps == [10.0]


def test_429_honours_retry_after():
    ok = [{"warning": "No identifier found."}]
    rec = Recorder([(429, {"Retry-After": "7"}, {}), (200, {}, ok)])
    clk = FakeClock()
    assert client(rec, clk).map([us_ticker_job("A")]) == ok
    assert len(rec.requests) == 2
    assert clk.sleeps == [7.0]


def test_429_backs_off_exponentially_then_gives_up():
    rec = Recorder([(429, {}, {})] * 10)
    clk = FakeClock()
    c = client(rec, clk, max_retries=3, backoff_s=2.0)
    with pytest.raises(OpenFigiError, match="429"):
        c.map([us_ticker_job("A")])
    assert len(rec.requests) == 4  # one try and three retries, no storm
    # backoff waits 2, 4, 8, never shorter than the 2.4 s spacing of the quota
    assert clk.sleeps == [2.4, 4.0, 8.0]


def test_429_wait_beyond_max_wait_gives_up():
    rec = Recorder([(429, {"Retry-After": "3600"}, {})])
    clk = FakeClock()
    with pytest.raises(OpenFigiError, match="max_wait"):
        client(rec, clk, max_wait_s=300).map([us_ticker_job("A")])
    assert clk.sleeps == []


def test_key_never_shown():
    rec = Recorder([(500, {}, {"error": "boom"})])
    c = client(rec, FakeClock(), FAKE_KEY)
    assert FAKE_KEY not in repr(c) and FAKE_KEY not in str(c)
    with pytest.raises(OpenFigiError) as exc:
        c.map([us_ticker_job("A")])
    assert "500" in str(exc.value) and FAKE_KEY not in str(exc.value)
    assert exc.value.__cause__ is None


def test_transport_error_wrapped_without_detail():
    def broken(request):
        raise httpx.ConnectError("synthetic failure", request=request)

    c = OpenFigiClient(
        Secret(FAKE_KEY), transport=httpx.MockTransport(broken), sleep=lambda s: None
    )
    with pytest.raises(OpenFigiError, match="ConnectError") as exc:
        c.map([us_ticker_job("A")])
    assert exc.value.__cause__ is None
    assert FAKE_KEY not in str(exc.value)


def test_from_env():
    c = OpenFigiClient.from_env({API_KEY_ENV: FAKE_KEY})
    assert c.rpm == KEY_RPM and c.max_jobs == KEY_MAX_JOBS
    c = OpenFigiClient.from_env({})
    assert c.rpm == NO_KEY_RPM and c.max_jobs == NO_KEY_MAX_JOBS
    c = OpenFigiClient.from_env({API_KEY_ENV: "  "})
    assert c.rpm == NO_KEY_RPM


def test_response_not_aligned_with_jobs_raises():
    rec = Recorder([(200, {}, [])])
    with pytest.raises(OpenFigiError, match="aligned"):
        client(rec, FakeClock()).map([us_ticker_job("A")])


def test_empty_jobs_make_no_request():
    rec = Recorder()
    assert client(rec, FakeClock()).map([]) == []
    assert rec.requests == []


def test_figi_results_from_recorded_response():
    responses = json.loads(RECORDED.read_text())
    series = [("massive", "SYNA"), ("massive", "GONE"), ("massive", "SYNB"), ("tiingo", "SYNC")]
    table = figi_results(series, responses)
    assert table.schema == FIGI_RESULT_SCHEMA
    rows = table.to_pylist()
    assert {(r["source_code"], r["source_symbol"]) for r in rows} == {
        ("massive", "SYNA"),
        ("massive", "SYNB"),
        ("tiingo", "SYNC"),
    }
    syna = [r for r in rows if r["source_symbol"] == "SYNA"]
    assert syna == [
        {
            "source_code": "massive",
            "source_symbol": "SYNA",
            "figi": "BBG000SYNC10",
            "composite_figi": "BBG000SYNC10",
            "share_class_figi": "BBG001SYNS10",
            "ticker": "SYNA",
            "exch_code": "US",
            "security_type": "Common Stock",
            "market_sector": "Equity",
        }
    ]
    assert "name" not in table.column_names  # labels stay with the caller


def test_figi_results_refuses_job_errors_and_misalignment():
    with pytest.raises(OpenFigiError, match="job 0"):
        figi_results([("massive", "A")], [{"error": "Invalid idType."}])
    with pytest.raises(ValueError, match="aligned"):
        figi_results([("massive", "A")], [])
    assert figi_results([], []).num_rows == 0
    assert figi_results([], []).schema == FIGI_RESULT_SCHEMA
    assert isinstance(figi_results([], []), pa.Table)


def test_key_not_a_local_of_the_request_frames():
    import sys

    from market_data.resolve import openfigi as openfigi_module

    leaks: list[str] = []

    def holds(value, depth=0) -> bool:
        if depth > 6:
            return False
        if isinstance(value, str | bytes):
            return (FAKE_KEY if isinstance(value, str) else FAKE_KEY.encode()) in value
        if isinstance(value, httpx.Request):
            return holds(value.headers, depth + 1)
        if isinstance(value, httpx.Headers):
            return any(holds(v, depth + 1) for _, v in value.raw)
        if isinstance(value, dict):
            return any(holds(k, depth + 1) or holds(v, depth + 1) for k, v in value.items())
        if isinstance(value, list | tuple | set | frozenset):
            return any(holds(v, depth + 1) for v in value)
        return False

    class Inspect(Recorder):
        def __call__(self, request):
            assert request.headers[API_KEY_HEADER] == FAKE_KEY
            frame = sys._getframe()
            while frame is not None:
                if frame.f_code.co_filename == openfigi_module.__file__:
                    for name, value in frame.f_locals.items():
                        if holds(value) or FAKE_KEY in repr(value):
                            leaks.append(f"{frame.f_code.co_name}.{name}")
                frame = frame.f_back
            return super().__call__(request)

    out = client(Inspect(echo=True), FakeClock(), FAKE_KEY).map([us_ticker_job("ABC")])
    assert len(out) == 1
    assert leaks == []
