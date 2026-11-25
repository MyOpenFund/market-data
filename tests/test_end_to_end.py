"""End to end through the CLI on synthetic data: a history load, then a Massive night (a fake
transport), the publisher between the steps, the store coverage; the same pass again writes no
new row; a restated second load moves no window. Every name, FIGI and price here is made up.

Dates are relative to today: Massive's free plan only serves recent sessions."""

from __future__ import annotations

import csv
import json
import math
from datetime import UTC, date, datetime, timedelta

import httpx
import pyarrow.parquet as pq
import pytest
from open_data_store.config import StoreConfig
from open_data_store.contracts.standard.market import INSTRUMENT_IDENTIFIERS, MARKET_CONTRACTS
from open_data_store.market.asof import latest, sessions
from open_data_store.market.coverage import delisting_exits
from open_data_store.market.membership import membership_intervals
from open_data_store.market.returns import resolve_prices
from open_data_store.market.store import load_market_inputs
from open_data_store.publisher import approve_contract, publish_pending
from open_data_store.reader import Store

from market_data import cli, pipeline
from market_data.adapters._quota import STATE_DIR_ENV
from market_data.adapters.massive import MassiveAdapter
from market_data.cli import EXIT_DEGRADED, EXIT_ERROR, EXIT_OK, main
from market_data.quality import fetch_coverage

SALT = "synthetic-e2e-salt-0123456789"
KEY = "synthetic-massive-key-e2e"
ALLOWED = frozenset({"private_history", "massive", "manual"})
# ticker -> (composite FIGI, CIK); BBB dies inside the history
FIGI = {
    "AAA": ("BBG000SYNE01", "0000000901"),
    "BBB": ("BBG000SYNE02", "0000000902"),
    "CCC": ("BBG000SYNE03", "0000000903"),
}
BASE = {"AAA": 50.0, "BBB": 8.0, "CCC": 120.0}


def close(symbol: str, day: date) -> float:
    n = day.toordinal()
    return round(BASE[symbol] * (1.0 + 0.03 * math.sin((n + len(symbol)) / 5.0)), 2)


class Calendar:
    """The sessions of the run, relative to today (every one closed days ago)."""

    def __init__(self) -> None:
        today = datetime.now(UTC).date()
        days = sessions("XNYS", today - timedelta(days=120), today - timedelta(days=4))
        self.days: list[date] = days["session_date"].to_pylist()
        self.history = self.days[:60]  # the export
        self.massive = self.days[50:]  # the provider, overlapping the history's last ten
        self.bbb_last = self.history[25]  # BBB's last price
        # month-ends as filed (the last session of each month of the history)
        self.snapshots = [
            d
            for d, nxt in zip(self.history, self.history[1:], strict=False)
            if nxt.month != d.month
        ]
        self.export = datetime.combine(self.history[-1], datetime.min.time(), UTC) + timedelta(
            days=1, hours=2
        )


def write_export(directory, cal: Calendar) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    manifest = {
        "export_time": cal.export.isoformat(),
        "adjustment": "none",
        "dates_timezone": "exchange",
        "dividends": "gross",
        "source_description": "synthetic end-to-end export",
    }
    (directory / "manifest.json").write_text(json.dumps(manifest))
    members = {f"{t} UN Equity": t for t in FIGI}
    with (directory / "securities.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["member_id", "name", "composite_figi", "exchange_code", "last_ticker"])
        for member, t in members.items():
            w.writerow([member, f"{t} Synthetic Corp", FIGI[t][0], "UN", t])
    with (directory / "prices_daily.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["member_id", "date", "close", "volume", "currency"])
        for member, t in members.items():
            for d in cal.history:
                if t == "BBB" and d > cal.bbb_last:
                    break
                w.writerow([member, d.isoformat(), close(t, d), 1000, "USD"])
    with (directory / "index_snapshots.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "index", "member_id", "weight"])
        dead_after = next(d for d in cal.snapshots if d >= cal.bbb_last)
        for d in cal.snapshots:
            for member, t in members.items():
                if t != "BBB" or d <= dead_after:  # BBB is still filed after its last price
                    w.writerow([d.isoformat(), "IDX1", member, ""])
    with (directory / "corporate_actions.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["member_id", "source_action_type", "ex_date", "amount", "currency"])
        w.writerow(["AAA UN Equity", "Regular Cash", cal.history[30].isoformat(), 0.2, "USD"])


def massive_handler(cal: Calendar):
    def tickers(active: bool) -> list[dict]:
        if active:
            return [
                {"ticker": t, "market": "stocks", "active": True, "composite_figi": FIGI[t][0],
                 "cik": FIGI[t][1]}
                for t in ("AAA", "CCC")
            ]  # fmt: skip
        dead = (cal.bbb_last + timedelta(days=1)).isoformat() + "T00:00:00Z"
        return [
            {"ticker": "BBB", "market": "stocks", "active": False,
             "composite_figi": FIGI["BBB"][0], "delisted_utc": dead},
            {"ticker": "OLDX", "market": "stocks", "active": False,
             "composite_figi": "BBG000SYNE99", "delisted_utc": "2015-03-02T00:00:00Z"},
        ]  # fmt: skip

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == f"Bearer {KEY}"
        path = request.url.path
        if "/aggs/grouped/" in path:
            day = date.fromisoformat(path.rsplit("/", 1)[1])
            stamp = int(datetime(day.year, day.month, day.day, 21, tzinfo=UTC).timestamp() * 1000)
            rows = [
                {"T": t, "o": close(t, day), "h": close(t, day), "l": close(t, day),
                 "c": close(t, day), "v": 2000, "t": stamp}
                for t in ("AAA", "CCC")
            ]  # fmt: skip
            body = {"status": "OK", "adjusted": False, "resultsCount": 2, "results": rows}
            return httpx.Response(200, json=body)
        if path.endswith("/reference/tickers"):
            active = request.url.params["active"] == "true"
            return httpx.Response(200, json={"status": "OK", "results": tickers(active)})
        if path.endswith(("/dividends", "/splits")):
            return httpx.Response(200, json={"status": "OK", "results": []})
        return httpx.Response(404, json={"status": "NOT_FOUND"})

    return handler


@pytest.fixture
def world(tmp_path, monkeypatch):
    cal = Calendar()
    paths = {k: tmp_path / k for k in ("archive", "reports", "store", "export")}
    cfg = StoreConfig(data_root=paths["store"], catalog=str(tmp_path / "meta.ducklake"))
    for c in MARKET_CONTRACTS:
        approve_contract(cfg, c, "test")
    for name, value in (
        ("MOF_MARKET_ARCHIVE", paths["archive"]),
        ("MOF_MARKET_REPORTS", paths["reports"]),
        ("OPEN_DATA_STORE_DATA_ROOT", paths["store"]),
        ("OPEN_DATA_STORE_CATALOG", cfg.catalog),
        ("MOF_MARKET_TOKEN_SALT", SALT),
        ("MOF_MARKET_ADAPTERS", "massive"),
        (STATE_DIR_ENV, "off"),
    ):
        monkeypatch.setenv(name, str(value))
    for var in ("MOF_MARKET_MANUAL_MAPPING", "TIINGO_API_KEY", "MASSIVE_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    tick = iter(range(10**9))
    monkeypatch.setattr(
        cli,
        "make_adapter",
        lambda source: MassiveAdapter(
            KEY,
            transport=httpx.MockTransport(massive_handler(cal)),
            sleep=lambda s: None,
            clock=lambda: next(tick) * 100.0,
        ),
    )
    write_export(paths["export"], cal)
    return cal, paths, cfg


def publish(cfg) -> int:
    """Publish the landed batches; the number of new rows they wrote."""
    result = publish_pending(cfg)
    assert result.exit_code == 0 and not result.quarantined
    return sum(p["rows"] for p in result.published)


def run(*argv: str) -> int:
    return main(list(argv))


def nightly(cal: Calendar) -> None:
    for kind, extra in (
        ("symbols", ()),
        ("prices", ("--from", cal.massive[0].isoformat(), "--to", cal.massive[-1].isoformat())),
        ("actions", ("--from", cal.massive[0].isoformat(), "--to", cal.massive[-1].isoformat())),
    ):
        assert run("fetch", "--write", "--source", "massive", "--kind", kind, *extra) == EXIT_OK


def map_build_publish(cfg) -> int:
    """map, publish (build reads the published mapping), build, publish: the new rows."""
    assert run("map", "--write") in (EXIT_OK, EXIT_DEGRADED)
    rows = publish(cfg)
    assert run("build", "--write", "--source", "massive") in (EXIT_OK, EXIT_DEGRADED)
    return rows + publish(cfg)


def report(reports) -> dict:
    """``report --index`` across the junction; the report it wrote."""
    code = run(
        "report", "--write", "--index", "IDX1", "--precedence", "private_history", "massive",
        "--snapshot-precedence", "private_history",
    )  # fmt: skip
    assert code in (EXIT_OK, EXIT_DEGRADED)
    return json.loads(sorted(reports.glob("*-report-report.json"))[-1].read_text())


def open_windows(cfg) -> list[tuple]:
    with Store.open(cfg) as store:
        ids = store.read_asof("instrument_identifiers")
    rows = latest(ids, INSTRUMENT_IDENTIFIERS.natural_key).to_pylist()
    return sorted(
        (r["instrument_id"], r["id_value"], r["valid_date"], r["valid_to"])
        for r in rows
        if r["id_scheme"] == "source_symbol"
        and (r["valid_to"] is None or r["valid_to"] > r["valid_date"])
    )


def test_history_then_massive_twice_then_a_restated_load(world, capsys):
    cal, paths, cfg = world
    # pass 1: the history, then a Massive night
    assert run("load-history", "--dir", str(paths["export"]), "--write") == EXIT_OK
    assert run("map", "--write", "--no-existing") in (EXIT_OK, EXIT_DEGRADED)
    assert publish(cfg) > 0
    nightly(cal)
    assert map_build_publish(cfg) > 0
    first = report(paths["reports"])
    assert first["error"] is None and first["coverage"]["sessions"] > 0
    assert first["mapping"]["listed_not_priced"] == {"massive": {"listed_not_priced": 2}}
    assert "unusable_cash_actions" not in first["reasons"]

    # one instrument per company across the junction, and the history's dead name corroborated
    windows = open_windows(cfg)
    by_value = {v: i for i, v, _, _ in windows}
    assert by_value["massive:AAA"] in {i for i, v, _, _ in windows if v.startswith("private")}
    as_of = datetime.now(UTC)
    with Store.open(cfg) as store:
        coverage = pipeline.store_coverage(
            store, index_code="IDX1", as_of=as_of, allowed_restricted=ALLOWED,
            precedence=("private_history", "massive"), snapshot_precedence=("private_history",),
        )  # fmt: skip
        m = load_market_inputs(store, as_of=as_of, allowed_restricted=ALLOWED)
    assert "delisted" in {r["reason"] for r in coverage.per_member.to_pylist()}
    iv = membership_intervals(m.snapshots, m.identifiers, index_code="IDX1", as_of=as_of)
    rp = resolve_prices(
        m.prices, m.identifiers, as_of=as_of, precedence=("private_history", "massive")
    )
    days = sessions("XNYS", cal.days[0], cal.days[-1])
    covered = fetch_coverage(
        m.fetch_log, m.identifiers, as_of=as_of, sessions=days,
        corroborating_sources=pipeline.corroborating_codes(pipeline.shipped_adapter_classes()),
    )  # fmt: skip
    exits = delisting_exits(iv, rp, days, fetch_coverage=covered)
    assert [r["exit_date"] for r in exits.to_pylist()] == [cal.bbb_last]

    # pass 2, the same inputs: nothing new is written
    assert map_build_publish(cfg) == 0
    second = report(paths["reports"])
    assert second["reasons"] == first["reasons"]
    assert open_windows(cfg) == windows

    # a restated load of the same export: no window moves, the mapping writes nothing
    published = paths["store"].parent / "published_snapshots.parquet"
    with Store.open(cfg) as store:
        pq.write_table(store.read_asof("index_snapshots"), published)
    argv = ("load-history", "--dir", str(paths["export"]), "--previous-snapshots", str(published))
    assert run(*argv, "--write") == EXIT_OK
    capsys.readouterr()
    assert run("map", "--write") in (EXIT_OK, EXIT_DEGRADED)
    assert "identifiers=0 " in capsys.readouterr().out
    publish(cfg)
    assert open_windows(cfg) == windows
    assert run("report", "--index", "IDX1") == EXIT_ERROR  # two sources: --precedence needed
    assert "pass --precedence" in capsys.readouterr().err
