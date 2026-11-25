import json
import threading
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from market_data.adapters.base import WHOLE_MARKET_SYMBOL, FetchResult
from market_data.archive import ARCHIVE_ENV, RawArchive

T0 = datetime(2026, 1, 5, 10, 0, 0, 123456, tzinfo=UTC)


def make(symbol="AAA", payload=b"x,1\n", at=T0, kind="prices", **kw) -> FetchResult:
    fields = dict(
        source_code="synth",
        source_symbol=symbol,
        kind=kind,
        requested_from=date(2026, 1, 1),
        requested_to=date(2026, 1, 4),
        fetched_at=at,
        status="ok",
        payload=payload,
        content_type="text/csv",
    )
    fields.update(kw)
    return FetchResult(**fields)


def test_put_is_immutable(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    ref = arc.put(make())
    path = tmp_path / "arc" / ref
    mtime = path.stat().st_mtime_ns
    assert arc.put(make()) == ref
    assert path.stat().st_mtime_ns == mtime
    files = [p for p in (tmp_path / "arc").rglob("*") if p.is_file()]
    assert sorted(p.suffix for p in files) == [".bin", ".json"]
    assert ref == ref.replace("\\", "/") and not ref.startswith("/")
    assert ref.startswith("synth/AAA/prices/20260105T100000123456Z-")
    assert path.read_bytes() == b"x,1\n"
    side = path.with_suffix(".json")
    side_before = (side.read_bytes(), side.stat().st_mtime_ns)
    arc.put(make())
    assert (side.read_bytes(), side.stat().st_mtime_ns) == side_before


def test_two_fetches_same_second_do_not_collide(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    a = arc.put(make(payload=b"a"))
    b = arc.put(make(payload=b"a", at=T0 + timedelta(microseconds=1)))
    c = arc.put(make(payload=b"b"))
    assert len({a, b, c}) == 3


def test_history_ordered_and_roundtrip(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    late = make(payload=b"late", at=T0 + timedelta(days=1), status="ok")
    early = make(payload=b"early", at=T0, requested_from=None, requested_to=None)
    arc.put(late)
    arc.put(early)
    arc.put(make(symbol="BBB"))
    got = list(arc.history("synth", "AAA", "prices"))
    assert got == [early, late]
    assert list(arc.history("synth", "AAA", "actions")) == []
    assert list(arc.history("other", "AAA", "prices")) == []


def test_whole_market_symbol(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    r = make(symbol=WHOLE_MARKET_SYMBOL)
    ref = arc.put(r)
    assert ref.startswith("synth/_market/prices/")
    assert list(arc.history("synth", WHOLE_MARKET_SYMBOL, "prices")) == [r]


def test_symbol_encoding_never_collides_or_escapes(tmp_path):
    root = tmp_path / "arc"
    arc = RawArchive(root)
    a = make(symbol="BRK/B", payload=b"1")
    b = make(symbol="BRK%2FB", payload=b"1")
    c = make(symbol="../x", payload=b"1")
    d = make(symbol="..", payload=b"1")
    refs = [arc.put(r) for r in (a, b, c, d)]
    assert len({r.split("/")[1] for r in refs}) == 4
    for ref in refs:
        assert (root / ref).resolve().is_relative_to(root.resolve())
    assert list(arc.history("synth", "BRK/B", "prices")) == [a]
    assert list(arc.history("synth", "BRK%2FB", "prices")) == [b]
    assert list(arc.history("synth", "../x", "prices")) == [c]


def test_history_detects_corruption(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    ref = arc.put(make())
    (tmp_path / "arc" / ref).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="sha256"):
        list(arc.history("synth", "AAA", "prices"))


def test_refuses_root_inside_git_checkout(tmp_path):
    (tmp_path / ".git").mkdir()
    with pytest.raises(ValueError, match="git"):
        RawArchive(tmp_path / "deep" / "arc")


def test_root_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv(ARCHIVE_ENV, str(tmp_path / "envarc"))
    arc = RawArchive()
    assert arc.root == tmp_path / "envarc"
    monkeypatch.delenv(ARCHIVE_ENV)
    with pytest.raises(ValueError, match=ARCHIVE_ENV):
        RawArchive()


def test_no_temp_files_left(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    arc.put(make())
    assert not [p for p in (tmp_path / "arc").rglob("*") if p.name.endswith(".tmp")]
    assert isinstance(arc.root, Path)


def test_conflicting_rewrite_refused(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    arc.put(make())
    with pytest.raises(ValueError, match="conflict"):
        arc.put(make(status="error"))
    with pytest.raises(ValueError, match="conflict"):
        arc.put(make(content_type="application/json"))
    with pytest.raises(ValueError, match="conflict"):
        arc.put(make(requested_from=date(2025, 1, 1)))


def test_corrupted_existing_payload_refused_on_put(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    ref = arc.put(make())
    (tmp_path / "arc" / ref).write_bytes(b"bad")
    with pytest.raises(ValueError, match="conflict"):
        arc.put(make())


def test_concurrent_puts_same_refs(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    errors, refs = [], []

    def work():
        try:
            refs.append([arc.put(make(payload=bytes([i]))) for i in range(5)])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors
    assert all(r == refs[0] for r in refs)
    files = [p for p in (tmp_path / "arc").rglob("*") if p.is_file()]
    assert len(files) == 10


def test_history_rejects_foreign_sidecar(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    ref = arc.put(make())
    side = (tmp_path / "arc" / ref).with_suffix(".json")
    meta = json.loads(side.read_text())
    side.write_text(json.dumps({**meta, "source_symbol": "aaa"}))
    assert list(arc.history("synth", "AAA", "prices")) == []
    side.write_text(json.dumps({**meta, "source_code": "evil"}))
    with pytest.raises(ValueError, match="identity"):
        list(arc.history("synth", "AAA", "prices"))
    side.write_text(json.dumps({**meta, "kind": "actions"}))
    with pytest.raises(ValueError, match="identity"):
        list(arc.history("synth", "AAA", "prices"))


def test_missing_payload_is_corruption(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    ref = arc.put(make())
    (tmp_path / "arc" / ref).unlink()
    with pytest.raises(ValueError, match="corrupted"):
        list(arc.history("synth", "AAA", "prices"))


def test_crash_heal_orphan_payload(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    ref = arc.put(make())
    (tmp_path / "arc" / ref).with_suffix(".json").unlink()  # crash between payload and sidecar
    assert arc.put(make()) == ref
    assert list(arc.history("synth", "AAA", "prices")) == [make()]


def test_empty_names_rejected(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    with pytest.raises(ValueError, match="empty"):
        arc.put(make(symbol=""))
    with pytest.raises(ValueError, match="empty"):
        arc.put(make(source_code=""))
    with pytest.raises(ValueError, match="empty"):
        list(arc.history("synth", "", "prices"))


def test_root_is_absolute(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert RawArchive("rel").root.is_absolute()


def test_history_with_refs_matches_put_refs(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    late = make(payload=b"late", at=T0 + timedelta(days=1))
    early = make(payload=b"early")
    ref_late, ref_early = arc.put(late), arc.put(early)
    got = list(arc.history_with_refs("synth", "AAA", "prices"))
    assert [(g.archive_ref, g.result) for g in got] == [(ref_early, early), (ref_late, late)]
    assert list(arc.history("synth", "AAA", "prices")) == [early, late]
    assert list(arc.history_with_refs("synth", "AAA", "actions")) == []


def test_symbols_lists_stored_symbols_per_kind(tmp_path):
    arc = RawArchive(tmp_path / "arc")
    for sym in ("BBB", "AAA", WHOLE_MARKET_SYMBOL, "BRK/B", "_market"):
        arc.put(make(symbol=sym))
    arc.put(make(symbol="CCC", kind="actions"))
    arc.put(make(symbol="DDD", source_code="other"))
    assert arc.symbols("synth", "prices") == sorted(
        ["AAA", "BBB", "BRK/B", "_market", WHOLE_MARKET_SYMBOL]
    )
    assert arc.symbols("synth", "actions") == ["CCC"]
    assert arc.symbols("nothing", "prices") == []
