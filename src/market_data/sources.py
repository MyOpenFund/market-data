"""Sources this package registers under the ``open_data_store.sources`` entry point.

Restricted sources are denied by default by open-data-store readers (market-data spec §13).
"""

from open_data_store.sources import SourceInfo

__all__ = ["sources"]


def sources() -> list[SourceInfo]:
    return [
        SourceInfo(
            code="massive",
            licence="Massive market data terms of service; personal use, no redistribution",
            terms_url="https://massive.com/legal/market-data-terms-of-service",
            attribution="",
            access="restricted",
            no_public_display=True,
            no_ml_training=True,
        ),
        SourceInfo(
            code="tiingo",
            # TODO: verify the terms URL and the redistribution terms before the first fetch.
            licence="Tiingo terms of service; personal use, no redistribution (to verify)",
            terms_url="https://www.tiingo.com/about/terms",
            attribution="",
            access="restricted",
        ),
        SourceInfo(
            code="private_history",
            licence="personal one-shot export; never redistributed",
            terms_url="",
            attribution="",
            access="restricted",
        ),
        SourceInfo(
            code="openfigi",
            licence="OpenFIGI terms of service",
            terms_url="https://www.openfigi.com/docs/terms-of-service",
            attribution="OpenFIGI",
            access="open",
        ),
    ]
