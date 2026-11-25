# market-data

Market-data producer for [MyOpenFund](https://github.com/MyOpenFund). It fetches daily prices,
corporate actions and identifiers through pluggable source adapters, keeps every raw answer in an
immutable archive, maps provider symbols to opaque instruments, and writes look-ahead-free
batches into [open-data-store](https://github.com/MyOpenFund/open-data-store), whose recipes then
serve point-in-time index membership, prices and total returns for backtests. It also loads a
history export in a public file format, and ships the acceptance computations that check a
backtest universe for look-ahead and survivorship bias.

## Principles

- **Bring your own source.** The repo ships code: adapters, a source registry, the history-file
  loader. It ships no data. You use your own provider accounts and keys (environment variables,
  never logged, never in an error message).
- **Never redistribute.** Prices are fetched for your own use. Provider sources are registered
  `restricted`: open-data-store readers deny them by default, and a deployment must allow them
  explicitly. Returning to you what was fetched with your own key (the `none` profile below) is
  not redistribution; serving stored restricted data to someone else is.
- **Look-ahead-free.** Every fact carries the time it became known (`known_at`), from the exchange
  calendar (session close in UTC) or the fetch time of a correction. Dated rows from a private
  export are keyed by opaque tokens; their labels are published only as identifier observations
  known at load time.

## Install

Not on PyPI yet. Install from the repository (Python 3.11 or later):

```
pip install "myopenfund-market-data @ git+https://github.com/MyOpenFund/market-data"
```

This pulls `myopenfund-open-data-store[writer,recipes]`, pinned to a commit until its 0.2.0 is on
PyPI.

## Try it: prices in a notebook (`none` profile)

With your own free [Tiingo](https://www.tiingo.com) key in `TIINGO_API_KEY`:

```python
from datetime import date
from market_data.api import prices

prices(["AAPL", "MSFT"], date(2024, 1, 2), date(2024, 3, 28))  # raw daily closes
prices(["MSFT"], date(2024, 1, 2), date(2024, 3, 28), adjust="total_return")
```

Nothing is stored: the data is fetched with your key, built in memory and returned to you (only
Tiingo's quota state is kept, see `MOF_MARKET_STATE_DIR`). Inputs are the source's own symbols
(Tiingo writes a class share with a hyphen, as `XXX-B`). Rows are keyed by an in-memory id
(`live:tiingo:MSFT`, not a store instrument id) and your input. `source="massive"` uses
`MASSIVE_API_KEY` instead, within that plan's history window; `api_key=` passes a key directly.

With `adjust="total_return"`, the store's recipes build a total-return index (base 1.0 on the
first session of the range with a price) from raw closes and corporate actions, with the recipe's
`flags`. Vendor-adjusted prices are never used.

On a store (`profile="server"`), inputs may be tickers, CIKs, FIGIs or instrument ids
(`mof:...`), resolved through the latest mapping; the answer is keyed by `instrument_id` only and
read as known at `as_of` (only the requested instruments' series are read). Restricted sources
are read only when listed in `allowed_restricted`. Where two sources hold one instrument (the
history file, then a daily provider), pass `precedence=["private_history", "massive"]` (the first
source with a row wins per session); `source=` keeps one source only. Errors are `ApiError` subclasses
(`MissingKeyError`, `UnknownInstrumentError`, `AmbiguousInstrumentError`,
`RestrictedSourceError`, `NoDataError`). See the docstring of `market_data.api`.

## Environment variables

| Variable | Used by | Meaning |
|---|---|---|
| `TIINGO_API_KEY` | Tiingo adapter, `prices()` | Your Tiingo key. Required for Tiingo. |
| `MASSIVE_API_KEY` | Massive adapter, `prices(source="massive")` | Your Massive key. Required for Massive. |
| `OPENFIGI_API_KEY` | `map --openfigi` | Optional. Without it the resolver stays within OpenFIGI's keyless quota (25 requests a minute, 10 jobs per request); with it, 250 and 100. |
| `MOF_MARKET_ADAPTERS` | CLI | Comma-separated source codes of the adapters allowed (e.g. `massive,tiingo`). Unset: none. Others are never imported. |
| `MOF_MARKET_ARCHIVE` | CLI | Root of the raw archive (default for `--archive`). No default; must not be inside a git checkout. |
| `MOF_MARKET_REPORTS` | CLI | Private directory for run records and reports (default for `--reports`). Required by every `--write` and by `report`. Must not be inside a git checkout. |
| `MOF_MARKET_TOKEN_SALT` | `load-history` | Secret salt (at least 16 bytes) for the opaque tokens of a history file. Keep it on the store host. |
| `MOF_MARKET_MANUAL_MAPPING` | `map` | Optional path to a reviewed CSV (`source_code`, `source_symbol`, `instrument_id` or `composite_figi`, `valid_date`, `valid_to`, `link_reason`) that overrides the automatic mapping for the series it names. Private. |
| `MOF_MARKET_STATE_DIR` | Tiingo adapter | Where the provider quota state is kept across runs (`off` disables it). Default: the user cache directory + `myopenfund/market-data`. |
| `OPEN_DATA_STORE_DATA_ROOT` | CLI, `prices(profile="server")` | The store's data root; batches are dropped in its `landing/` (default for `--landing`). |
| `OPEN_DATA_STORE_CATALOG` | CLI, `prices(profile="server")` | The store's catalogue (published identifiers, coverage reads). |

Other open-data-store variables (publisher, catalogue tuning) are documented in its README.

## Operator commands (`server` profile)

`market-data <command>` runs on the store host. Every command is a **dry run** unless `--write`:
it computes what it can and writes nothing (`fetch` lists the requests it would make and the quota
they would spend). Each prints one summary line (counts, never a label) and exits 0 (`ok`), 1
(`degraded`; 0 with `--allow-degraded`) or 2 (`error`, or a usage error refused before any
work). Common flags: `--write`, `--allow-degraded`, `--archive`, `--reports`, and `--landing` for
the commands that land batches. `--source` takes the adapters shipped here (`massive`,
`tiingo`); an adapter from your own package is driven through the Python API
(`market_data.pipeline`).

The order of a run, the publisher (open-data-store's `publish_pending`) landing the batches
between the steps that read the store:

1. `load-history --write` (once per export), `map --write --no-existing` (the first mapping),
   publish;
2. `fetch --write` (symbols, prices, actions: as many as the plan allows), `map --write` (reads
   the published identifiers), `build --source ... --write`, publish;
3. `report --index CODE --write` (with `--precedence private_history massive` once a provider
   overlaps the history: two sources for one instrument and session refuse a coverage without
   it).

The raw archive (`MOF_MARKET_ARCHIVE`) must sit on a local filesystem that supports hard links
(each answer is written to a temp file, then linked into place, never overwritten): not a share
mounted without hard-link support. Fetch a session once the provider has published it: a
session fetched right after its close may come back empty and is logged as fetched; while no
member has a price that day the store's coverage counts the session `session_not_published`,
never a delisting; fetch it again later (`report --index` without `--as-of` leaves out a
session not closed yet).

Load a history file (format below); `--previous-snapshots` restates snapshots already published,
`--max-non-session-share` sets the tripwire for dates that are not exchange-local:

```
market-data load-history --dir /private/exports/2026-11 --write
```

Fetch into the raw archive (`--kind prices|actions|symbols`; `--date D` or `--from D --to D`;
`--symbols` for a per-symbol fetch, else the whole market where the source offers it):

```
market-data fetch --source massive --kind prices --date 2026-11-20 --write
```

Map provider symbols to instruments (defaults: the newest history load's observations and spans,
plus the archived symbols and prices of every allowed adapter, and the identifiers already
published; `--no-existing` is required for a first mapping, `--openfigi` asks OpenFIGI):

```
market-data map --openfigi --write
```

Build `prices`, `corporate_actions` and `fetch_log` batches from the archive of one source,
restricted to its mapped universe (`--identifiers PARQUET` instead of the store's;
`--all-symbols` for every archived symbol):

```
market-data build --source massive --write
```

Merge the newest piece of each step into `<run_id>-report.json` / `.md`, with the store's
coverage of one index (`--as-of`, `--precedence`, `--snapshot-precedence`,
`--allowed-restricted`, `--staleness-days`, `--max-missing-share`,
`--min-delisting-gap-sessions`; defaults are the store recipes'). Without `--index` the report is
degraded (`coverage_not_run`):

```
market-data report --index sp500 --as-of 2026-11-20T22:00:00+00:00 --write
```

See `market-data <command> --help` and the docstrings of `market_data.cli` and
`market_data.pipeline`.

## History-file format

The door for anyone with their own data: a directory loaded by `load-history`
(`market_data.adapters.file`). Tables are Parquet (preferred) or CSV (UTF-8, comma separator,
header line), possibly split into parts (`prices_daily_part001.parquet`); dates `YYYY-MM-DD`;
`.` as decimal separator; empty means unknown (never `-`, `N/A` or `#N/A`, which are refused).
Prices must be **raw, unadjusted**.

| File | Columns | Required |
|---|---|---|
| `manifest.json` or `manifest.csv` (`key,value`) | `export_time` (ISO 8601 with offset), `adjustment` (must be `none`), `dates_timezone` (must be `exchange`), `dividends` (`gross` or `net`), `source_description`; optional `fields`, `notes`, `per_share_basis_default` (`old_share` or `new_share`: the basis of a cash amount whose row leaves `per_share_basis` empty) | yes, every key but the optional ones |
| `securities` (one row per `member_id`) | `member_id`, `name`, `figi`, `composite_figi`, `share_class_figi`, `isin`, `country`, `exchange_code`, `security_type`, `price_currency`, `status`, `delisting_date`, `delisting_reason`, `last_ticker` | yes; `member_id`, `name` (the rest best effort) |
| `index_snapshots` | `date`, `index`, `member_id`, `weight` | `date`, `index`, `member_id` |
| `prices_daily` | `member_id`, `date`, `close`, `volume`, `currency`, optional `open`, `high`, `low` | `member_id`, `date`, `close`, `volume` (values may be empty), `currency`; for every member over its whole listed life |
| `corporate_actions` | `member_id`, `action_type`, `source_action_type`, `announce_date`, `ex_date`, `record_date`, `pay_date`, `amount`, `currency`, `ratio_old`, `ratio_new`, `spinoff_member_id`, optional `per_share_basis` (`old_share` or `new_share`: is a cash amount per share before or after a split on the same ex-date) | optional file; `member_id`, `source_action_type`, `ex_date` |
| `total_return_index` | `member_id`, `date`, `tr_index` | optional file (a vendor control series) |

At least one of `index_snapshots` and `prices_daily` is required. An exchange code (from
`securities.exchange_code`, else the second word of `member_id`) picks the calendar of a member;
US codes map to XNYS by default. A cash action the total-return recipe could never apply (no
`currency`, or no `per_share_basis` on the ex-date of a split or stock dividend of the same
member) is still loaded but counted, and degrades the run (`unusable_cash_actions`). The loader replaces every `member_id` by an opaque token
(`MOF_MARKET_TOKEN_SALT`) in every dated row, and refuses a malformed file with a list of every
problem (file, column, row; never a cell's content).

## Acceptance computations

`market_data.acceptance` holds pure functions over the store recipes' outputs
(`membership_intervals` of one index, `total_return`, `sessions`), to check a backtest universe on
your data. None of them reports a clean result on missing evidence:

- `equal_weight_index(intervals, tr, sessions, rebalance="quarterly", company_groups=...)`: an
  equal-weight index (per company when `company_groups` [instrument_id, company_id] groups share
  lines), rebalanced monthly or quarterly (`rebalance_months`, default January, April, July,
  October) on membership as known at each date. Members leaving between rebalances are sold at
  their last total-return level; `exit_proceeds` keeps the proceeds in cash (default) or
  reinvests them pro rata. A member with no usable level at a rebalance (none yet, or older than
  `max_stale_sessions`) is skipped and counted (`on_missing="skip"`, default) or refused
  (`"raise"`). The result holds `levels` and one `rebalances` row per date (members, bought,
  skipped, stale, carried). `annual_fee` for an ETF comparison.
- `truncation_check(build_fn, as_of)`: your `build_fn(as_of, truncate=...)` builds the series
  twice, once after removing every fact known after `as_of` from its inputs (`truncate=True`) and
  once from everything available now; the check lists the sessions up to `as_of` that differ
  (empty: no look-ahead). It proves only what your `build_fn` does: one that ignores `truncate`
  always passes. Take `as_of` at or after a session close; comparing no session raises.
- `compare_series(a, b, tolerance=...)`: our series vs a vendor's (maximum difference, sessions
  over tolerance, annualised tracking difference); it does not pass on fewer than `min_sessions`
  (default 2) common sessions, and says why in `reason`.
- `survivors_only_delta(...)`: the same index on today's members only, and the annualised return
  that bias would add. The point-in-time index raises on a member without total return
  (`full_on_missing="raise"`, default: skipping a dead name would hide the bias measured); the
  survivors index skips (`survivors_on_missing="skip"`); both skipped counts are returned.

Their docstrings state every rule and parameter.

## Adapters

What `--source` and `load-history` read:

| Adapter | What | Command |
|---|---|---|
| `massive` | Massive (formerly Polygon.io) daily US prices, dividends, splits, tickers; free Stocks Basic plan, 5 requests a minute, 2 years of history | `fetch` / `build --source massive` |
| `tiingo` | Tiingo end-of-day prices with dividends and splits, per symbol; free plan (50 requests an hour, 1,000 a day, 500 symbols a month) | `fetch` / `build --source tiingo` |
| history file | the format above (source code `private_history`) | `load-history` |

Quotas are respected, never worked around: requests are spaced, retries are bounded, and a limit
may be set lower, never higher.

Sources this package registers (licence, terms, access class) under the `open_data_store.sources`
entry point:

| Source code | What | Access |
|---|---|---|
| `massive` | the Massive adapter's rows | restricted |
| `tiingo` | the Tiingo adapter's rows | restricted |
| `private_history` | rows loaded from a history file | restricted |
| `openfigi` | identifiers from OpenFIGI (a resolver, not a price source; `map --openfigi`) | open |
| `sec` | issuer links (CIK) from the SEC ticker map | open |
| `manual` | your reviewed manual mapping (`MOF_MARKET_MANUAL_MAPPING`) | restricted |

### Writing an adapter in your own package

Implement the `market_data.adapters.base.Adapter` protocol (`source_code`, `info`, `fetch`,
`parse`), register a `SourceInfo` for your source under the `open_data_store.sources` entry
point, and declare the adapter under the `market_data.adapters` entry point (name = source code,
value = a zero-argument callable returning the adapter). Then add its code to
`MOF_MARKET_ADAPTERS`.

```toml
[project.entry-points."market_data.adapters"]
my_source = "my_package.adapter:make_adapter"

[project.entry-points."open_data_store.sources"]
my_package = "my_package.sources:sources"
```

### Compliance checklist (adapters merged in this repo)

- Link to the source's terms
- Official or documented endpoint
- Quota respected (rate limiter, backoff)
- Licence declared in the source registry
- No header or browser spoofing
- Personal-use sources flagged `restricted`

Anything else lives in your own package, under your responsibility.

## Licences of sources

The provider sources (`massive`, `tiingo`) and a private export are for personal use with your
own key or rights: registered `restricted`, with **no public display** and **no ML training**
(`no_public_display`, `no_ml_training` in the registry). Never share their data with another
person, never publish it, never train a model on it. Check each provider's current terms
yourself before your first fetch. OpenFIGI identifiers and SEC EDGAR data are open (attribution:
OpenFIGI, SEC EDGAR). The code in this repository is MIT licensed; the data you fetch is not.

## What never goes in this repository

- A private export, or anything derived from it: member lists, dead-name lists, their FIGIs,
  manual mapping rows, tokens, mapping reports.
- Run records and reports (`MOF_MARKET_REPORTS`), the raw archive (`MOF_MARKET_ARCHIVE`), store
  data: all on private paths outside any git checkout.
- Keys and the token salt.

Tests use synthetic fixtures only.

## Development

```
python3 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
ruff check src tests && ruff format --check src tests
pytest -q -W error
```

Releases: the `release` workflow publishes to PyPI by trusted publishing (tag `vX.Y.Z`, or Run
workflow with target `pypi` or `testpypi`) once the `pypi` environment's reviewer approves. Its
guard (`scripts/check-no-direct-references.sh`) fails the build while `pyproject.toml` pins a
dependency by git URL, and checks the built wheel's `Requires-Dist` again: the first release
waits for open-data-store 0.2.0 on PyPI.

MIT licensed.
