# market-data

Producer (work in progress) that fetches market data (prices, corporate actions, identifiers) through pluggable
source adapters and writes it into [open-data-store](https://github.com/MyOpenFund/open-data-store).

## Principles

- **Bring your own source.** The repo ships the adapter interface and a source registry, not data.
  You supply your own provider accounts and keys (environment variables, never logged).
- **Never redistribute.** Prices are fetched for your own use. Restricted sources are denied by
  default by open-data-store readers; nothing here republishes provider data.
- Only adapters you enable are loaded: set `MOF_MARKET_ADAPTERS` to a comma-separated list of
  source codes.

## Identifier mapping

Provider symbols are mapped to opaque instruments (`mof:<uuid>`) by `market_data.resolve.mapping`,
one instrument per composite FIGI. Two optional settings, both environment variables:

- `OPENFIGI_API_KEY`: your OpenFIGI key. Without it the resolver stays within the keyless quota
  (25 requests a minute, 10 jobs per request); with it, 250 a minute and 100 jobs. Never logged.
- `MOF_MARKET_MANUAL_MAPPING`: path to a reviewed CSV (`source_code`, `source_symbol`,
  `instrument_id` or `composite_figi`, `valid_date`, `valid_to`, `link_reason`) that overrides
  the automatic mapping for the series it names. Keep it on a private path: it is never
  published.

## Writing an adapter in your own package

Implement the `market_data.adapters.Adapter` protocol (`source_code`, `info`, `fetch`, `parse`),
register a `SourceInfo` for your source under the `open_data_store.sources` entry point, and
declare the adapter under the `market_data.adapters` entry point (name = source code, value = a
zero-argument callable returning the adapter):

```toml
[project.entry-points."market_data.adapters"]
my_source = "my_package.adapter:make_adapter"

[project.entry-points."open_data_store.sources"]
my_package = "my_package.sources:sources"
```

## Compliance checklist (adapters merged in this repo)

- Link to the source's terms
- Official or documented endpoint
- Quota respected (rate limiter, backoff)
- Licence declared in the source registry
- No header or browser spoofing
- Personal-use sources flagged `restricted`

Anything else lives in your own package, under your responsibility.

## Development

```
python3 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
ruff check src tests && ruff format --check src tests
pytest -q -W error
```

MIT licensed.
