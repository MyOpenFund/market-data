"""Exchange-calendar helpers shared by the provider builder and the history-file adapter.

Session opens and the exchange time zone come from ``exchange_calendars`` directly (the version
open-data-store's ``recipes`` extra pins), since ``open_data_store.market.asof.sessions`` returns
closes only. Calendar failures are translated into ``ValueError`` naming the mic and the range.
"""

from __future__ import annotations

import warnings
from datetime import UTC, date, datetime, time, timedelta

# Default calendar margin past the last ex-date, so a non-session ex-date finds its next session.
NEXT_SESSION_MARGIN = timedelta(days=14)


def calendar_errors() -> tuple[type[Exception], ...]:
    from exchange_calendars import errors as xerrors
    from pandas.errors import OutOfBoundsDatetime

    return (xerrors.NoSessionsError, xerrors.DateOutOfBounds, OutOfBoundsDatetime)


def calendar(mic: str, start: date, end: date):
    """The ``exchange_calendars`` calendar of ``mic`` over ``[start, end]``."""
    import exchange_calendars as xcals

    try:
        with warnings.catch_warnings():
            # Upstream: exchange_calendars 4.13.2 calls Series.values on tz-aware data while
            # building a calendar, which pandas 3.1 flags (same filter as asof.sessions).
            warnings.filterwarnings(
                "ignore", message="Series.values returning an ndarray", category=Warning
            )
            return xcals.get_calendar(mic, start=start.isoformat(), end=end.isoformat())
    except calendar_errors() as exc:
        raise ValueError(
            f"exchange calendar {mic!r} cannot be built over {start.isoformat()}.."
            f"{end.isoformat()}: {exc}"
        ) from exc


def actions_calendar(mic: str, ex_dates: list[date], margin: timedelta):
    """Calendar covering every ex-date with ``margin`` on both sides (``margin`` > 0)."""
    return calendar(mic, min(ex_dates) - margin, max(ex_dates) + margin)


def check_margin(margin: timedelta) -> None:
    if margin <= timedelta(0):
        raise ValueError(f"next_session_margin must be positive, got {margin}")


def ex_date_open(cal, mic: str, ex: date) -> datetime:
    """Open in UTC of the ex-date session, or of the next session when ex is not a session."""
    import pandas as pd

    try:
        session = cal.date_to_session(pd.Timestamp(ex), direction="next")
        return cal.session_open(session).to_pydatetime().astimezone(UTC)
    except calendar_errors() as exc:
        raise ValueError(
            f"exchange calendar {mic!r} has no session on or after ex-date {ex.isoformat()} "
            f"within the built range (raise next_session_margin): {exc}"
        ) from exc


def action_known_at(
    cal, mic: str, ex: date, announced: date | None, cap: datetime
) -> tuple[datetime, str, str]:
    """First ``(known_at, known_at_kind, known_at_basis)`` of a corporate action (spec §7.3).

    The announce date at 23:59:59.999999 in the exchange time zone, in UTC, when that instant is
    not after the ex-date session open (``published`` / ``announce_date``); otherwise the ex-date
    session open (``published`` / ``ex_date_assumed``; a non-session ex-date uses the next
    session). Then capped at ``cap`` (the first time the action was seen: a fetch time or an
    export time): when the cap binds the row is ``collected`` / ``fetch_time``."""
    opened = ex_date_open(cal, mic, ex)
    first, basis = opened, "ex_date_assumed"
    if announced is not None:
        eod = datetime.combine(announced, time.max, tzinfo=cal.tz).astimezone(UTC)
        if eod <= opened:
            first, basis = eod, "announce_date"
    if cap < first:
        return cap, "collected", "fetch_time"
    return first, "published", basis
