import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pytest
from open_data_store.contracts.rules import conform
from open_data_store.contracts.standard.market import (
    CORPORATE_ACTIONS,
    FETCH_LOG,
    INDEX_SNAPSHOTS,
    PRICES,
    VENDOR_TOTAL_RETURN,
)
from open_data_store.market.asof import asof, latest

from market_data.adapters.base import RAW_SYMBOL_SCHEMA
from market_data.adapters.file import (
    DEFAULT_EXCHANGE_MICS,
    DROP_AFTER_EXPORT,
    DROP_DUPLICATE_ROW,
    DROP_NON_SESSION,
    DROP_UNKNOWN_ACTION_TYPE,
    DROP_UNMAPPED_EXCHANGE,
    MAX_NON_SESSION_SHARE,
    FileHistory,
    HistoryFileError,
)
from market_data.tokens import SALT_ENV, salt_from_env, token

FIXTURE = Path(__file__).parent / "fixtures" / "history_file"
SALT = b"synthetic-test-salt-0123456789"
SRC = "private_history"
LOAD = datetime(2020, 4, 3, 9, 0, tzinfo=UTC)
EXPORT = datetime(2020, 4, 2, 18, 0, tzinfo=UTC)
MEMBERS = {
    "AAA UN Equity": "Alpha Fake Corp",
    "BBB UW Equity": "Beta Fake Inc",
    "CCC LN Equity": "Gamma Fake plc",
    "DDD UQ Equity": "Delta Fake Holdings",
    "EEE ZZ Equity": "Epsilon Fake SA",
    "FFF UN Equity": "Phi Fake Co",
}
WITH_LONDON = {**DEFAULT_EXCHANGE_MICS, "LN": "XLON"}


def tok(member_id: str) -> str:
    return token(SRC, member_id, SALT)


def csv_export(tmp_path: Path) -> Path:
    out = tmp_path / "csv"
    shutil.copytree(FIXTURE, out)
    return out


def parquet_export(tmp_path: Path, *, timestamps: bool = False) -> Path:
    """The fixture's rows as Parquet (types inferred like a typical exporter would write them)."""
    out = tmp_path / "parquet"
    out.mkdir(parents=True)
    shutil.copy(FIXTURE / "manifest.json", out)
    for src in sorted(FIXTURE.glob("*.csv")):
        table = pv.read_csv(src, convert_options=pv.ConvertOptions(null_values=[""]))
        if timestamps:  # pandas-style exporters write dates as naive timestamps
            for i, f in enumerate(table.schema):
                if pa.types.is_date(f.type):
                    table = table.set_column(i, f.name, table[f.name].cast(pa.timestamp("ns")))
        pq.write_table(table, out / f"{src.stem}.parquet")
    return out


def history(path: Path, **kw) -> FileHistory:
    kw.setdefault("salt", SALT)
    # the fixture has one holiday row among few prices; the tripwire has its own tests
    kw.setdefault("max_non_session_share", 0.5)
    return FileHistory(path, **kw)


@pytest.fixture
def fh(tmp_path):
    return history(csv_export(tmp_path))


def rewrite(path: Path, name: str, text: str) -> None:
    (path / name).write_text(text)


def edit_csv(path: Path, name: str, old: str, new: str) -> None:
    text = (path / name).read_text()
    assert old in text
    (path / name).write_text(text.replace(old, new, 1))


def problems_of(fh: FileHistory) -> str:
    return "\n".join(fh.validate())


# --- tokens and salt ---------------------------------------------------------------------------


def test_token_is_deterministic_and_salted():
    t = token(SRC, "AAA UN Equity", SALT)
    assert t == token(SRC, "AAA UN Equity", SALT)
    assert t.startswith("t_") and len(t) == 22
    assert t != token(SRC, "AAA UN Equity", SALT + b"x")
    assert t != token("other_source", "AAA UN Equity", SALT)


def test_missing_salt_refused(tmp_path, monkeypatch):
    monkeypatch.delenv(SALT_ENV, raising=False)
    with pytest.raises(ValueError, match=SALT_ENV):
        FileHistory(csv_export(tmp_path))
    with pytest.raises(ValueError, match="too short"):
        salt_from_env({SALT_ENV: "short"})


def test_salt_from_env_and_never_shown(tmp_path, monkeypatch):
    secret = "env-salt-value-abcdefghijklmnop"
    monkeypatch.setenv(SALT_ENV, secret)
    fh = FileHistory(csv_export(tmp_path), max_non_session_share=0.5)
    assert secret not in repr(fh)
    tokens = set(fh.prices()["source_symbol"].to_pylist())
    assert token(SRC, "AAA UN Equity", secret.encode()) in tokens


# --- manifest ----------------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["split and dividend adjusted", "", "no"])
def test_manifest_must_say_no_adjustment(tmp_path, value):
    path = csv_export(tmp_path)
    manifest = json.loads((path / "manifest.json").read_text())
    manifest["adjustment"] = value
    rewrite(path, "manifest.json", json.dumps(manifest))
    fh = history(path)
    assert "adjustment" in problems_of(fh)
    for method in (fh.prices, fh.snapshots, fh.actions, fh.vendor_total_return):
        with pytest.raises(HistoryFileError, match="adjustment"):
            method()
    with pytest.raises(HistoryFileError):
        fh.observations(LOAD)


def test_manifest_csv_accepted(tmp_path):
    path = csv_export(tmp_path)
    manifest = json.loads((path / "manifest.json").read_text())
    (path / "manifest.json").unlink()
    lines = ["key,value"] + [f'{k},"{v}"' for k, v in manifest.items()]
    rewrite(path, "manifest.csv", "\n".join(lines) + "\n")
    fh = history(path)
    assert fh.validate() == []
    assert fh.manifest.export_time == EXPORT
    assert fh.manifest.dividends == "gross"


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("export_time", "2020-04-02T18:00:00", "offset"),
        ("export_time", None, "export_time"),
        ("dividends", "after tax", "dividends"),
        ("source_description", None, "source_description"),
        ("dates_timezone", None, "dates_timezone"),
    ],
)
def test_manifest_keys_checked(tmp_path, key, value, message):
    path = csv_export(tmp_path)
    manifest = json.loads((path / "manifest.json").read_text())
    if value is None:
        del manifest[key]
    else:
        manifest[key] = value
    rewrite(path, "manifest.json", json.dumps(manifest))
    assert message in problems_of(history(path))


def test_extra_files_columns_and_keys_ignored_and_listed(tmp_path):
    baseline = history(csv_export(tmp_path / "base"))
    path = csv_export(tmp_path)
    manifest = json.loads((path / "manifest.json").read_text())
    manifest["exporter_version"] = "3.1"
    rewrite(path, "manifest.json", json.dumps(manifest))
    lines = (path / "prices_daily.csv").read_text().splitlines()
    lines = [lines[0] + ",adjusted_close"] + [ln + ",-" for ln in lines[1:]]
    rewrite(path, "prices_daily.csv", "\n".join(lines) + "\n")
    rewrite(path, "README.txt", "synthetic\n")
    rewrite(path, "holdings.csv", "a,b\n1,2\n")
    fh = history(path)
    assert fh.validate() == []  # the ignored column is not read: its '-' does not matter
    assert fh.report.ignored.files == ["README.txt", "holdings.csv"]
    assert fh.report.ignored.columns == {"prices_daily.csv": ["adjusted_close"]}
    assert fh.report.ignored.manifest_keys == ["exporter_version"]
    assert fh.prices().equals(baseline.prices())


def test_extra_parquet_column_ignored(tmp_path):
    path = parquet_export(tmp_path)
    table = pq.read_table(path / "securities.parquet")
    pq.write_table(
        table.append_column("sector", pa.array(["x"] * table.num_rows)), path / "securities.parquet"
    )
    fh = history(path)
    assert fh.validate() == []
    assert fh.report.ignored.columns == {"securities.parquet": ["sector"]}


def test_missing_manifest_refused(tmp_path):
    path = csv_export(tmp_path)
    (path / "manifest.json").unlink()
    assert "manifest" in problems_of(history(path))


# --- file format -------------------------------------------------------------------------------


def test_tab_separated_with_row_number_rejected_with_clear_message(tmp_path):
    path = csv_export(tmp_path)
    rewrite(
        path,
        "index_snapshots.csv",
        "\tdate\tindex\tmember_id\tweight\n1\t2020-01-31\tIDX1\tAAA UN Equity\t-\n",
    )
    problems = problems_of(history(path))
    assert "index_snapshots.csv" in problems
    assert "tab-separated" in problems
    assert "comma-separated CSV or Parquet" in problems
    assert "row-number" in problems


@pytest.mark.parametrize("first", ["", "Unnamed: 0"])
def test_leading_row_number_column_rejected(tmp_path, first):
    path = csv_export(tmp_path)
    rewrite(
        path,
        "index_snapshots.csv",
        f"{first},date,index,member_id,weight\n0,2020-01-31,IDX1,AAA UN Equity,\n",
    )
    problems = problems_of(history(path))
    assert "index_snapshots.csv" in problems and "row-number" in problems


def test_parquet_index_column_rejected(tmp_path):
    path = parquet_export(tmp_path)
    table = pq.read_table(path / "prices_daily.parquet")
    table = table.add_column(0, "__index_level_0__", pa.array(range(table.num_rows)))
    pq.write_table(table, path / "prices_daily.parquet")
    problems = problems_of(history(path))
    assert "prices_daily.parquet" in problems and "row-number" in problems


@pytest.mark.parametrize("sentinel", ["-", "N/A", "NA", "#N/A", " n/a "])
@pytest.mark.parametrize(
    ("old", "new", "column", "row"),
    [
        ("BBB UW Equity,2020-01-31,2.9,", "BBB UW Equity,2020-01-31,{s},", "close", 10),
        ("DDD UQ Equity,2020-01-31,40.0,700", "DDD UQ Equity,2020-01-31,40.0,{s}", "volume", 15),
        ("FFF UN Equity,2020-02-03", "FFF UN Equity,{s}", "date", 20),
    ],
)
def test_dash_and_na_rejected(tmp_path, sentinel, old, new, column, row):
    path = csv_export(tmp_path)
    edit_csv(path, "prices_daily.csv", old, new.format(s=sentinel))
    fh = history(path)
    problems = fh.validate()
    hit = [p for p in problems if "prices_daily.csv" in p and f"column {column!r}" in p]
    assert hit, problems
    assert f"data row {row} (file line {row + 1})" in hit[0]
    assert repr(sentinel.strip()) in hit[0]
    with pytest.raises(HistoryFileError):
        fh.prices()


@pytest.mark.parametrize("sentinel", ["-", "N/A"])
def test_dash_rejected_in_parquet_string_column(tmp_path, sentinel):
    path = parquet_export(tmp_path)
    table = pq.read_table(path / "index_snapshots.parquet")
    weights = pa.array([sentinel] * table.num_rows)
    table = table.set_column(table.schema.get_field_index("weight"), "weight", weights)
    pq.write_table(table, path / "index_snapshots.parquet")
    problems = problems_of(history(path))
    assert "index_snapshots.parquet" in problems and "'weight'" in problems and "row 1" in problems


def test_nan_close_rejected(tmp_path):
    path = csv_export(tmp_path)
    edit_csv(
        path, "prices_daily.csv", "DDD UQ Equity,2020-02-28,38.5", "DDD UQ Equity,2020-02-28,NaN"
    )
    assert "NaN" in problems_of(history(path))


def test_required_columns_and_values(tmp_path):
    path = csv_export(tmp_path)
    edit_csv(
        path,
        "prices_daily.csv",
        "member_id,date,close,volume,currency",
        "member_id,date,close,volume,ccy",
    )
    edit_csv(path, "securities.csv", "Beta Fake Inc", "")
    fh = history(path)
    problems = problems_of(fh)
    assert "prices_daily.csv" in problems and "'currency'" in problems and "'ccy'" not in problems
    assert fh.report.ignored.columns == {"prices_daily.csv": ["ccy"]}
    assert "securities.csv" in problems and "'name'" in problems and "row 2" in problems


def test_duplicates_and_unknown_members_refused(tmp_path):
    path = csv_export(tmp_path)
    text = (path / "prices_daily.csv").read_text()
    rewrite(path, "prices_daily.csv", text + "AAA UN Equity,2020-01-30,100.0,1000,USD,,,\n")
    text = (path / "index_snapshots.csv").read_text()
    rewrite(path, "index_snapshots.csv", text + "2020-03-31,IDX2,GGG UN Equity,\n")
    problems = problems_of(history(path))
    assert "duplicate" in problems and "prices_daily" in problems
    assert "not in securities" in problems and "index_snapshots" in problems
    assert "GGG" not in problems  # labels are never echoed


def test_split_files_read_in_order(tmp_path):
    path = csv_export(tmp_path)
    lines = (path / "prices_daily.csv").read_text().splitlines()
    (path / "prices_daily.csv").unlink()
    rewrite(path, "prices_daily_part001.csv", "\n".join(lines[:10]) + "\n")
    rewrite(path, "prices_daily_part002.csv", "\n".join([lines[0], *lines[10:]]) + "\n")
    whole = history(csv_export(tmp_path / "b")).prices()
    assert history(path).prices().equals(whole)


def test_mixed_csv_and_parquet_refused(tmp_path):
    path = csv_export(tmp_path)
    pq.write_table(pv.read_csv(path / "prices_daily.csv"), path / "prices_daily_part002.parquet")
    assert "both CSV and Parquet" in problems_of(history(path))


def test_missing_required_file_refused(tmp_path):
    path = csv_export(tmp_path)
    (path / "securities.csv").unlink()
    assert "securities" in problems_of(history(path))


def test_csv_and_parquet_equivalent(tmp_path):
    a = history(csv_export(tmp_path), exchange_mics=WITH_LONDON)
    for b in (
        history(parquet_export(tmp_path), exchange_mics=WITH_LONDON),
        history(parquet_export(tmp_path / "ts", timestamps=True), exchange_mics=WITH_LONDON),
    ):
        assert b.validate() == []
        assert a.prices().equals(b.prices())
        assert a.actions().equals(b.actions())
        assert a.snapshots().equals(b.snapshots())
        assert a.vendor_total_return().equals(b.vendor_total_return())
        assert a.observations(LOAD).equals(b.observations(LOAD))
        assert a.fetch_log(LOAD).equals(b.fetch_log(LOAD))
        assert a.report == b.report


# --- look-ahead guard --------------------------------------------------------------------------


def _strings(table: pa.Table) -> set[str]:
    out = set()
    for col in table.columns:
        if pa.types.is_string(col.type):
            out.update(v for v in col.to_pylist() if v is not None)
    return out


def test_tokens_hide_labels(tmp_path):
    fh = history(csv_export(tmp_path), exchange_mics=WITH_LONDON)
    tables = [
        fh.prices(),
        fh.actions(),
        fh.vendor_total_return(),
        fh.snapshots(),
        fh.fetch_log(LOAD),
    ]
    labels = set(MEMBERS) | set(MEMBERS.values()) | {"BBBQ", "BBG000FAKC01", "XS0000000AA7"}
    roots = {m.split()[0] for m in MEMBERS}
    for table in tables:
        values = _strings(table)
        assert not values & labels
        for v in values:
            assert not any(label in v for label in labels | roots), v
    assert set(fh.prices()["source_symbol"].to_pylist()) <= {tok(m) for m in MEMBERS}
    spin = fh.actions().filter(pc.equal(fh.actions()["action_type"], "spin_off"))
    assert spin["spinoff_symbol"].to_pylist() == [tok("FFF UN Equity")]


def test_identifiers_known_at_is_load_time(fh):
    obs = fh.observations(LOAD)
    assert obs.select(RAW_SYMBOL_SCHEMA.names).schema == RAW_SYMBOL_SCHEMA
    assert set(obs["known_at"].to_pylist()) == {LOAD}
    assert set(obs["known_at_kind"].to_pylist()) == {"collected"}
    assert set(obs["known_at_basis"].to_pylist()) == {"load_time"}
    assert set(obs["source_code"].to_pylist()) == {SRC}
    assert set(obs["valid_date"].to_pylist()) == {date(2020, 4, 2)}  # labels describe the export
    rows = {(r["source_symbol"], r["id_scheme"]): r for r in obs.to_pylist()}
    a = tok("AAA UN Equity")
    assert rows[(a, "export_member_id")]["id_value"] == "AAA UN Equity"
    assert rows[(a, "export_name")]["id_value"] == "Alpha Fake Corp"
    assert rows[(a, "composite_figi")]["id_value"] == "BBG000FAKC01"
    assert rows[(a, "share_class_figi")]["id_value"] == "BBG001FAKS01"
    assert rows[(a, "isin")]["id_value"] == "XS0000000AA7"
    assert rows[(tok("BBB UW Equity"), "ticker")]["id_value"] == "BBBQ"
    assert (tok("BBB UW Equity"), "isin") not in rows  # empty = unknown, no row
    assert (tok("EEE ZZ Equity"), "export_member_id") in rows  # labels for every member
    assert {r["name"] for r in obs.to_pylist() if r["source_symbol"] == a} == {"Alpha Fake Corp"}


def test_load_time_must_be_aware_and_after_export(fh):
    with pytest.raises(ValueError, match="timezone"):
        fh.observations(datetime(2020, 4, 3, 9))
    with pytest.raises(ValueError, match="export_time"):
        fh.observations(EXPORT - timedelta(seconds=1))
    with pytest.raises(ValueError, match="export_time"):
        fh.fetch_log(EXPORT - timedelta(seconds=1))


# --- prices, total return ----------------------------------------------------------------------


def test_prices_known_at_session_close(fh):
    rows = {(r["source_symbol"], r["valid_date"]): r for r in fh.prices().to_pylist()}
    first = rows[(tok("AAA UN Equity"), date(2020, 1, 30))]
    assert first["known_at"] == datetime(2020, 1, 30, 21, 0, tzinfo=UTC)  # 16:00 EST
    assert (first["known_at_kind"], first["known_at_basis"]) == ("published", "session_close")
    assert first["price_basis"] == "raw" and first["source_code"] == SRC
    assert (first["open"], first["high"], first["low"], first["volume"]) == (
        99.0,
        101.0,
        98.5,
        1000,
    )
    summer = rows[(tok("AAA UN Equity"), date(2020, 3, 31))]
    assert summer["known_at"] == datetime(2020, 3, 31, 20, 0, tzinfo=UTC)  # 16:00 EDT
    assert summer["volume"] is None
    # BBB (dead name) keeps its whole listed life; DDD's exchange comes from its member_id
    assert (tok("BBB UW Equity"), date(2020, 2, 14)) in rows
    assert (tok("DDD UQ Equity"), date(2020, 3, 31)) in rows


def test_non_session_dropped(fh):
    keys = {(r["source_symbol"], r["valid_date"]) for r in fh.prices().to_pylist()}
    assert (tok("AAA UN Equity"), date(2020, 2, 17)) not in keys  # Presidents' Day
    assert fh.report.dropped["prices"][DROP_NON_SESSION] == 1
    tr = {(r["source_symbol"], r["valid_date"]) for r in fh.vendor_total_return().to_pylist()}
    assert (tok("AAA UN Equity"), date(2020, 2, 17)) not in tr
    assert fh.report.dropped["vendor_total_return"][DROP_NON_SESSION] == 1


def test_price_after_export_dropped(fh):
    keys = {(r["source_symbol"], r["valid_date"]) for r in fh.prices().to_pylist()}
    assert (tok("AAA UN Equity"), date(2020, 4, 2)) not in keys  # closes after the export time
    assert fh.report.dropped["prices"][DROP_AFTER_EXPORT] == 1


def test_unmapped_exchange_counted(fh):
    prices = fh.prices()
    symbols = set(prices["source_symbol"].to_pylist())
    assert tok("EEE ZZ Equity") not in symbols and tok("CCC LN Equity") not in symbols
    assert fh.report.unmapped_exchanges["prices"] == {"LN": 3, "ZZ": 2}
    assert fh.report.dropped["prices"][DROP_UNMAPPED_EXCHANGE] == 5
    # compositions need no calendar: unmapped members stay in the snapshots
    snapped = set(fh.snapshots()["source_symbol"].to_pylist())
    assert {tok("EEE ZZ Equity"), tok("CCC LN Equity")} <= snapped
    assert "index_snapshots" not in fh.report.unmapped_exchanges
    assert fh.report.dropped["index_snapshots"] == {}
    fh.actions()
    assert fh.report.unmapped_exchanges["corporate_actions"] == {"LN": 1, "ZZ": 1}


def test_gbp_pence_preserved(tmp_path):
    fh = history(csv_export(tmp_path), exchange_mics=WITH_LONDON)
    ccc = tok("CCC LN Equity")
    prices = [r for r in fh.prices().to_pylist() if r["source_symbol"] == ccc]
    assert [r["currency"] for r in prices] == ["GBp"] * 3
    assert prices[0]["close"] == 1520.5
    assert prices[0]["known_at"] == datetime(2020, 1, 31, 16, 30, tzinfo=UTC)  # London close
    actions = [r for r in fh.actions().to_pylist() if r["source_symbol"] == ccc]
    assert [(a["currency"], a["amount"]) for a in actions] == [("GBp", 12.5)]
    assert fh.report.unmapped_exchanges["prices"] == {"ZZ": 2}


def test_vendor_total_return(fh):
    tr = conform(VENDOR_TOTAL_RETURN, fh.vendor_total_return(), now=LOAD)
    assert tr.num_rows == 5
    row = tr.filter(pc.equal(tr["valid_date"], date(2020, 3, 31))).to_pylist()
    assert {(r["source_symbol"], r["tr_index"]) for r in row} == {
        (tok("AAA UN Equity"), 1027.5),
        (tok("DDD UQ Equity"), 412.0),
    }
    assert {r["known_at_basis"] for r in tr.to_pylist()} == {"session_close"}


# --- snapshots ---------------------------------------------------------------------------------


def test_snapshots_as_filed(fh):
    snaps = fh.snapshots()
    rows = snaps.to_pylist()
    saturday = [r for r in rows if r["valid_date"] == date(2020, 2, 29)]
    assert saturday  # a weekend month-end is kept as filed
    eod = datetime(2020, 2, 29, 23, 59, 59, 999999, tzinfo=UTC)
    assert {r["known_at"] for r in saturday} == {eod}
    assert {(r["known_at_kind"], r["known_at_basis"]) for r in rows} == {
        ("published", "snapshot_date")
    }
    assert all(r["is_member"] for r in rows)
    idx1 = [r for r in rows if r["index_code"] == "IDX1" and r["valid_date"] == date(2020, 1, 31)]
    # EEE (unmapped exchange) is kept: a composition needs no calendar
    assert {r["snapshot_size"] for r in idx1} == {4} and len(idx1) == 4
    assert tok("EEE ZZ Equity") in {r["source_symbol"] for r in idx1}
    idx2 = [r for r in rows if r["index_code"] == "IDX2" and r["valid_date"] == date(2020, 3, 31)]
    assert {r["source_symbol"]: r["weight"] for r in idx2} == {
        tok("AAA UN Equity"): 0.55,
        tok("CCC LN Equity"): 0.25,
        tok("FFF UN Equity"): 0.2,
    }
    assert [r["weight"] for r in idx1] == [None] * 4


def test_restatement_gives_is_member_false(tmp_path):
    path = csv_export(tmp_path)
    previous = history(path).snapshots()
    # the vendor restates IDX1 at 2020-03-31: FFF removed, BBB added
    edit_csv(
        path,
        "index_snapshots.csv",
        "2020-03-31,IDX1,FFF UN Equity,",
        "2020-03-31,IDX1,BBB UW Equity,",
    )
    fh = history(path)
    snaps = fh.snapshots(previous_snapshots=previous, load_time=LOAD)
    conform(INDEX_SNAPSHOTS, snaps, now=LOAD + timedelta(days=1))
    mar = snaps.filter(
        pc.and_(
            pc.equal(snaps["index_code"], "IDX1"), pc.equal(snaps["valid_date"], date(2020, 3, 31))
        )
    )
    by = {r["source_symbol"]: r for r in mar.to_pylist()}
    gone, new = by[tok("FFF UN Equity")], by[tok("BBB UW Equity")]
    assert gone["is_member"] is False
    assert gone["snapshot_size"] == 4 and gone["weight"] is None
    assert (gone["known_at"], gone["known_at_kind"], gone["known_at_basis"]) == (
        LOAD,
        "collected",
        "load_time",
    )
    # a member added by a restatement is known at load time, never at the snapshot date
    assert new["is_member"] is True and new["known_at"] == LOAD
    # unchanged pairs are not re-emitted with a conflicting value: published rows only once
    combined = pa.concat_tables([previous, snaps])
    key = ["source_code", "index_code", "source_symbol", "valid_date"]
    now = asof(combined, key, LOAD + timedelta(hours=1))
    members = now.filter(
        pc.and_(pc.equal(now["index_code"], "IDX1"), pc.equal(now["valid_date"], date(2020, 3, 31)))
    )
    current = {r["source_symbol"] for r in members.to_pylist() if r["is_member"]}
    assert current == {
        tok(m) for m in ("AAA UN Equity", "DDD UQ Equity", "EEE ZZ Equity", "BBB UW Equity")
    }
    before = asof(combined, key, LOAD - timedelta(hours=1))
    old = before.filter(
        pc.and_(
            pc.equal(before["index_code"], "IDX1"),
            pc.equal(before["valid_date"], date(2020, 3, 31)),
        )
    )
    assert {r["source_symbol"] for r in old.to_pylist() if r["is_member"]} == {
        tok(m) for m in ("AAA UN Equity", "DDD UQ Equity", "EEE ZZ Equity", "FFF UN Equity")
    }
    latest(combined, key)  # no tie on (key, known_at)
    dup = combined.group_by([*key, "known_at"]).aggregate([([], "count_all")])
    assert pc.max(dup["count_all"]).as_py() == 1


def test_reload_with_same_previous_is_quiet(tmp_path):
    path = csv_export(tmp_path)
    previous = history(path).snapshots()
    assert history(path).snapshots(previous_snapshots=previous, load_time=LOAD).num_rows == 0


# --- corporate actions -------------------------------------------------------------------------


def test_actions(fh):
    rows = fh.actions().to_pylist()
    aaa = [r for r in rows if r["source_symbol"] == tok("AAA UN Equity")]
    same_day = sorted(
        (r["action_type"], r["action_seq"]) for r in aaa if r["valid_date"] == date(2020, 3, 2)
    )
    assert same_day == [("cash_dividend_regular", 0), ("split", 0)]
    split = next(r for r in aaa if r["action_type"] == "split")
    assert (split["ratio_old"], split["ratio_new"], split["amount_basis"]) == (1.0, 2.0, None)
    assert split["source_action_type"] == "Stock Split"
    # announced 2020-02-10: end of day New York
    assert split["known_at"] == datetime(2020, 2, 11, 4, 59, 59, 999999, tzinfo=UTC)
    assert split["known_at_basis"] == "announce_date"
    div = next(r for r in aaa if r["valid_date"] == date(2020, 3, 2) and r["amount"])
    assert (div["amount"], div["currency"], div["amount_basis"]) == (0.25, "USD", "gross")
    # future ex-date without announce date: capped at the export time
    capped = next(r for r in aaa if r["valid_date"] == date(2020, 5, 15))
    assert (capped["known_at"], capped["known_at_kind"], capped["known_at_basis"]) == (
        EXPORT,
        "collected",
        "fetch_time",
    )
    # two regular dividends of DDD on one ex-date (case-insensitive type): file order
    ddd = [
        (r["action_seq"], r["amount"])
        for r in rows
        if r["source_symbol"] == tok("DDD UQ Equity")
        and r["action_type"] == "cash_dividend_regular"
    ]
    assert sorted(ddd) == [(0, 0.10), (1, 0.05)]


def test_unknown_action_type_counted(fh):
    rows = fh.actions().to_pylist()
    assert tok("FFF UN Equity") not in {r["source_symbol"] for r in rows}
    assert fh.report.unknown_action_types == {"Mystery Distribution": 1}
    assert fh.report.dropped["corporate_actions"][DROP_UNKNOWN_ACTION_TYPE] == 1


def test_action_type_column_must_agree(tmp_path):
    path = csv_export(tmp_path)
    edit_csv(
        path,
        "corporate_actions.csv",
        "AAA UN Equity,,Stock Split",
        "AAA UN Equity,rights,Stock Split",
    )
    assert "action_type" in problems_of(history(path))
    path2 = csv_export(tmp_path / "b")
    edit_csv(
        path2,
        "corporate_actions.csv",
        "FFF UN Equity,,Mystery Distribution",
        "FFF UN Equity,cash_dividend_special,Mystery Distribution",
    )
    fh = history(path2)
    special = [r for r in fh.actions().to_pylist() if r["action_type"] == "cash_dividend_special"]
    assert [r["source_action_type"] for r in special] == ["Mystery Distribution"]
    assert fh.report.unknown_action_types == {}


def test_action_types_parameter(tmp_path):
    fh = history(csv_export(tmp_path), action_types={"mystery distribution": "return_of_capital"})
    assert {r["action_type"] for r in fh.actions().to_pylist()} == {"return_of_capital"}
    with pytest.raises(ValueError, match="not an action type"):
        history(csv_export(tmp_path / "b"), action_types={"x": "bonus"})


# --- fetch log, contracts ----------------------------------------------------------------------


def test_fetch_log_rows(fh):
    log = fh.fetch_log(LOAD)
    rows = {r["source_symbol"]: r for r in log.to_pylist()}
    assert set(rows) == {tok(m) for m in MEMBERS}  # unmapped exchanges included
    bbb = rows[tok("BBB UW Equity")]
    assert bbb["fetch_id"] == f"2020-04-02T18:00:00+00:00:{tok('BBB UW Equity')}"
    assert (bbb["requested_from"], bbb["requested_to"]) == (date(2020, 1, 30), date(2020, 4, 2))
    assert (bbb["returned_from"], bbb["returned_to"]) == (date(2020, 1, 30), date(2020, 2, 14))
    assert (bbb["status"], bbb["row_count"]) == ("ok", 3)
    assert (bbb["valid_date"], bbb["known_at"]) == (date(2020, 4, 2), LOAD)
    assert (bbb["known_at_kind"], bbb["known_at_basis"]) == ("collected", "fetch_time")
    aaa = rows[tok("AAA UN Equity")]
    assert aaa["row_count"] == 6 and aaa["returned_to"] == date(2020, 3, 31)
    fff = rows[tok("FFF UN Equity")]
    assert fff["requested_from"] == date(2020, 1, 31)  # earliest snapshot before its prices
    # unmapped exchange: prices filed but not loaded -> empty, counted in the report
    eee = rows[tok("EEE ZZ Equity")]
    assert (eee["status"], eee["row_count"], eee["returned_from"], eee["returned_to"]) == (
        "empty",
        0,
        None,
        None,
    )
    assert eee["requested_from"] == date(2020, 1, 31)
    assert fh.report.fetch_log == {
        "members": 6,
        "ok": 4,
        "empty": 2,
        "empty_with_unloaded_prices": 2,
        "unloaded_price_rows": 5,
    }


def test_fetch_log_empty_member(tmp_path):
    path = csv_export(tmp_path)
    lines = [
        ln
        for ln in (path / "prices_daily.csv").read_text().splitlines()
        if not ln.startswith("FFF")
    ]
    rewrite(path, "prices_daily.csv", "\n".join(lines) + "\n")
    rows = {r["source_symbol"]: r for r in history(path).fetch_log(LOAD).to_pylist()}
    fff = rows[tok("FFF UN Equity")]
    assert (fff["status"], fff["row_count"], fff["returned_from"], fff["returned_to"]) == (
        "empty",
        0,
        None,
        None,
    )


def test_every_table_conforms(tmp_path):
    fh = history(csv_export(tmp_path), exchange_mics=WITH_LONDON)
    now = LOAD + timedelta(minutes=1)
    for contract, table in (
        (PRICES, fh.prices()),
        (CORPORATE_ACTIONS, fh.actions()),
        (VENDOR_TOTAL_RETURN, fh.vendor_total_return()),
        (INDEX_SNAPSHOTS, fh.snapshots()),
        (FETCH_LOG, fh.fetch_log(LOAD)),
    ):
        assert table.schema == contract.schema, contract.name
        conform(contract, table, now=now)


def test_rebuild_is_identical(tmp_path):
    a = history(csv_export(tmp_path))
    b = history(csv_export(tmp_path / "again"))
    assert a.prices().equals(b.prices())
    assert a.actions().equals(b.actions())
    assert a.snapshots().equals(b.snapshots())


# --- review fixes: no echo, text sentinels, formats, tripwire, minors ---------------------------


@pytest.mark.parametrize(
    "sentinel", ["-", "--", " n/a ", "NULL", "NaN", "#N/A", "#N/A Field Not Applicable"]
)
def test_text_sentinels_rejected(tmp_path, sentinel):
    path = csv_export(tmp_path)
    edit_csv(
        path,
        "prices_daily.csv",
        "CCC LN Equity,2020-02-28,1490.0,250,GBp",
        f"CCC LN Equity,2020-02-28,1490.0,250,{sentinel}",
    )
    problems = problems_of(history(path))
    assert "prices_daily.csv: column 'currency'" in problems
    assert "data row 13 (file line 14)" in problems
    assert "Field Not Applicable" not in problems


def test_na_is_a_value_in_text_columns(tmp_path):
    path = csv_export(tmp_path)
    edit_csv(path, "securities.csv", "Epsilon Fake SA,,,,,ZZ,ZZ,", "Epsilon Fake SA,,,,,NA,NA,")
    fh = history(path)
    assert fh.validate() == []
    eee = tok("EEE ZZ Equity")
    assert eee in set(fh.snapshots()["source_symbol"].to_pylist())
    fh.prices()
    assert fh.report.unmapped_exchanges["prices"] == {"LN": 3, "NA": 2}
    assert fh.report.dropped["prices"][DROP_UNMAPPED_EXCHANGE] == 5


def test_malformed_csv_line_never_echoed(tmp_path):
    path = csv_export(tmp_path)
    edit_csv(
        path,
        "prices_daily.csv",
        "BBB UW Equity,2020-01-31,2.9,60000,USD,,,",
        "BBB UW Equity,2020-01-31,2.9,60000,USD,,,,SECRET EXTRA",
    )
    problems = problems_of(history(path))
    assert "prices_daily.csv: malformed line 11: 9 field(s) where the header has 8" in problems
    assert "BBB" not in problems and "SECRET" not in problems


def test_invalid_utf8_never_echoed(tmp_path):
    path = csv_export(tmp_path)
    data = (path / "prices_daily.csv").read_bytes()
    data = data.replace(b"BBB UW Equity,2020-01-31", b"BBB\xff\xfeSECRET UW Equity,2020-01-31")
    (path / "prices_daily.csv").write_bytes(data)
    problems = problems_of(history(path))
    assert "column 'member_id' is not valid UTF-8 at data row 10 (file line 11)" in problems
    assert "SECRET" not in problems and "BBB" not in problems


def test_unparsable_number_never_echoed(tmp_path):
    path = csv_export(tmp_path)
    edit_csv(
        path,
        "prices_daily.csv",
        "BBB UW Equity,2020-01-31,2.9,",
        "BBB UW Equity,2020-01-31,SECRETX,",
    )
    problems = problems_of(history(path))
    assert "column 'close' cannot be read as a number at data row 10 (file line 11)" in problems
    assert "SECRETX" not in problems


def test_unparsable_parquet_number_never_echoed(tmp_path):
    path = parquet_export(tmp_path)
    table = pq.read_table(path / "total_return_index.parquet")
    values = [str(v) for v in table["tr_index"].to_pylist()]
    values[2] = "SECRETX"
    i = table.schema.get_field_index("tr_index")
    table = table.set_column(i, "tr_index", pa.array(values))
    pq.write_table(table, path / "total_return_index.parquet")
    problems = problems_of(history(path))
    assert "column 'tr_index' cannot be read as a number at row 3" in problems
    assert "SECRETX" not in problems


@pytest.mark.parametrize(
    ("column", "old", "new"),
    [
        ("isin", "XS0000000AA7", "XS0000000AA1"),  # bad check digit
        ("isin", "XS0000000AA7", "xs0000000AA7"),
        ("figi", "BBG000FAKE01", "BBG000FAKE0"),
        ("composite_figi", "BBG000FAKC01", "BBX000FAKC01"),
        ("share_class_figi", "BBG001FAKS01", "BBG001fakS01"),
    ],
)
def test_identifier_formats_checked_without_echo(tmp_path, column, old, new):
    path = csv_export(tmp_path)
    edit_csv(path, "securities.csv", old, new)
    problems = problems_of(history(path))
    assert f"securities.csv: column {column!r} holds an invalid" in problems
    assert "data row 1 (file line 2)" in problems
    assert new not in problems


def test_dates_timezone_must_be_exchange(tmp_path):
    path = csv_export(tmp_path)
    manifest = json.loads((path / "manifest.json").read_text())
    manifest["dates_timezone"] = "America/New_York"
    rewrite(path, "manifest.json", json.dumps(manifest))
    assert "dates_timezone must be 'exchange'" in problems_of(history(path))
    manifest["dates_timezone"] = "Exchange"
    rewrite(path, "manifest.json", json.dumps(manifest))
    assert history(path).manifest.dates_timezone == "exchange"


def test_non_session_tripwire(tmp_path):
    fh = history(csv_export(tmp_path), max_non_session_share=MAX_NON_SESSION_SHARE)
    with pytest.raises(HistoryFileError, match=r"prices: 1 row\(s\) .* for 15 loaded"):
        fh.prices()
    with pytest.raises(HistoryFileError, match="vendor_total_return: 1 row"):
        fh.vendor_total_return()
    assert history(csv_export(tmp_path / "b"), max_non_session_share=1 / 15).prices().num_rows
    with pytest.raises(ValueError, match="max_non_session_share"):
        history(csv_export(tmp_path / "c"), max_non_session_share=2)


def test_trailing_comma_and_semicolon_named(tmp_path):
    path = csv_export(tmp_path)
    rewrite(
        path, "total_return_index.csv", "member_id,date,tr_index,\nAAA UN Equity,2020-01-31,1,\n"
    )
    rewrite(
        path, "index_snapshots.csv", "date;index;member_id;weight\n2020-01-31;IDX1;AAA UN Equity;\n"
    )
    problems = problems_of(history(path))
    assert "total_return_index.csv: the header ends with a trailing comma" in problems
    assert "index_snapshots.csv: the file is semicolon-separated" in problems


def test_part_files_in_natural_order(tmp_path):
    path = csv_export(tmp_path)
    lines = (path / "corporate_actions.csv").read_text().splitlines()
    header, rows = lines[0], lines[1:]
    first = [r for r in rows if ",0.10," in r]
    second = [r for r in rows if ",0.05," in r]
    rest = [r for r in rows if r not in first + second]
    (path / "corporate_actions.csv").unlink()
    rewrite(path, "corporate_actions_part2.csv", "\n".join([header, *rest, *first]) + "\n")
    rewrite(path, "corporate_actions_part10.csv", "\n".join([header, *second]) + "\n")
    rows = history(path).actions().to_pylist()
    ddd = sorted(
        (r["action_seq"], r["amount"])
        for r in rows
        if r["source_symbol"] == tok("DDD UQ Equity")
        and r["action_type"] == "cash_dividend_regular"
    )
    assert ddd == [(0, 0.10), (1, 0.05)]  # part2 is read before part10


def test_exact_duplicate_action_kept_once(tmp_path):
    path = csv_export(tmp_path)
    text = (path / "corporate_actions.csv").read_text()
    dup = next(ln for ln in text.splitlines() if "Stock Split" in ln)
    rewrite(path, "corporate_actions.csv", text + dup + "\n")
    fh = history(path)
    baseline = history(csv_export(tmp_path / "b")).actions()
    assert fh.actions().equals(baseline)
    assert fh.report.dropped["corporate_actions"][DROP_DUPLICATE_ROW] == 1


def test_unlisted_spinoff_target_counted(tmp_path):
    path = csv_export(tmp_path)
    edit_csv(path, "corporate_actions.csv", ",1,1,FFF UN Equity", ",1,1,ZZZ UN Equity")
    fh = history(path)
    spin = [r for r in fh.actions().to_pylist() if r["action_type"] == "spin_off"]
    assert spin[0]["spinoff_symbol"] == tok("ZZZ UN Equity")
    assert fh.report.unlisted_spinoff_targets == 1


def test_restatement_load_time_after_previous(tmp_path):
    fh = history(csv_export(tmp_path))
    previous = fh.snapshots()
    later = previous.set_column(
        previous.schema.get_field_index("known_at"),
        "known_at",
        pa.array([LOAD] * previous.num_rows, previous.schema.field("known_at").type),
    )
    with pytest.raises(ValueError, match="strictly after"):
        fh.snapshots(later, load_time=LOAD)
    assert fh.snapshots(later, load_time=LOAD + timedelta(seconds=1)).num_rows == 0


def test_build_all(tmp_path):
    fh = history(csv_export(tmp_path))
    result = fh.build_all(LOAD)
    now = LOAD + timedelta(minutes=1)
    for contract, table in (
        (PRICES, result.prices),
        (CORPORATE_ACTIONS, result.corporate_actions),
        (VENDOR_TOTAL_RETURN, result.vendor_total_return),
        (INDEX_SNAPSHOTS, result.index_snapshots),
        (FETCH_LOG, result.fetch_log),
    ):
        conform(contract, table, now=now)
    assert result.observations.equals(fh.observations(LOAD))
    assert result.report is fh.report
    assert set(result.report.dropped) == {
        "prices",
        "corporate_actions",
        "vendor_total_return",
        "index_snapshots",
    }
    assert result.report.fetch_log["members"] == 6
    with pytest.raises(ValueError, match="export_time"):
        fh.build_all(EXPORT - timedelta(seconds=1))
