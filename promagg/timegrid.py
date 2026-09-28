"""Time zones and time buckets.

SQL sees sample times as naive timestamps in the connection time zone (like osagg), so
`DATE_TRUNC('day', ts)` means local days. A local bucket is converted into the UTC
intervals it covers: days around a daylight-saving change last 23 or 25 hours, and in the
autumn repeated hour one local minute covers two separate UTC minutes.
"""

from __future__ import annotations

import calendar
import datetime as dt
from dataclasses import dataclass
from zoneinfo import ZoneInfo

EPOCH = dt.datetime(1970, 1, 1)
EPOCH_UTC = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
SECOND, MINUTE, HOUR, DAY = 1000, 60_000, 3_600_000, 86_400_000
ONE_MS = dt.timedelta(milliseconds=1)
DUCKDB_ORIGIN = dt.datetime(2000, 1, 3)          # time_bucket origin (a Monday)
DUCKDB_MONTH_ORIGIN = dt.datetime(2000, 1, 1)


def to_ms(naive: dt.datetime) -> int:
    """Naive datetime taken as UTC -> epoch ms (floor)."""
    return (naive - EPOCH) // ONE_MS


def from_ms(ms: int) -> dt.datetime:
    return EPOCH + dt.timedelta(milliseconds=ms)


class Zone:
    def __init__(self, name: str) -> None:
        self.name = name
        self.tz = ZoneInfo(name)

    def offset_ms(self, t_ms: int) -> int:
        aware = EPOCH_UTC + dt.timedelta(milliseconds=t_ms)
        return int(aware.astimezone(self.tz).utcoffset() // ONE_MS)

    def local(self, t_ms: int) -> dt.datetime:
        """UTC epoch ms -> naive local wall-clock time."""
        return from_ms(t_ms + self.offset_ms(t_ms))

    def utc_ms(self, local: dt.datetime, fold: int = 0) -> int:
        """Naive local time -> UTC epoch ms (first occurrence of a repeated time; a time in
        the spring gap maps to the instant after the change, like most databases)."""
        aware = local.replace(tzinfo=self.tz, fold=fold)
        return (aware.astimezone(dt.timezone.utc) - EPOCH_UTC) // ONE_MS

    def segments(self, t0: int, t1: int) -> list[tuple[int, int, int]]:
        """[t0, t1) split where the UTC offset changes: [(start, end, offset_ms)]."""
        out: list[tuple[int, int, int]] = []
        step = 6 * HOUR
        a, off = t0, self.offset_ms(t0)
        t = t0
        while t < t1:
            nxt = min(t + step, t1)
            o = self.offset_ms(nxt - 1)
            if o != off:                          # a change inside (t, nxt]: bisect it
                lo, hi = t, nxt - 1               # offset(lo) == off, offset(hi) != off
                while hi - lo > 1:
                    mid = (lo + hi) // 2
                    if self.offset_ms(mid) == off:
                        lo = mid
                    else:
                        hi = mid
                out.append((a, hi, off))
                a, off = hi, self.offset_ms(hi)
                t = hi
                continue
            t = nxt
        out.append((a, t1, off))
        return out


def _add_months(d: dt.datetime, n: int) -> dt.datetime:
    y, m = divmod(d.month - 1 + n, 12)
    y += d.year
    day = min(d.day, calendar.monthrange(y, m + 1)[1])
    return d.replace(year=y, month=m + 1, day=day)


UNITS = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")
FIXED = {"second": SECOND, "minute": MINUTE, "hour": HOUR, "day": DAY, "week": 7 * DAY}


@dataclass(frozen=True)
class Grain:
    """A time bucket expression: DATE_TRUNC(unit, ts + shift_in) + shift_out, or
    TIME_BUCKET(width, ts, origin) (fixed width in ms, or a number of months)."""

    unit: str                                # a UNITS member, "fixed" or "months"
    width_ms: int = 0
    months: int = 0
    origin: dt.datetime = DUCKDB_ORIGIN
    shift_in: dt.timedelta = dt.timedelta(0)
    shift_out: dt.timedelta = dt.timedelta(0)
    as_date: bool = False

    # keys live in the shifted space: key = trunc(local + shift_in)
    def key_floor(self, local: dt.datetime) -> dt.datetime:
        x = local + self.shift_in
        u = self.unit
        if u == "fixed":
            n = (to_ms(x) - to_ms(self.origin)) // self.width_ms
            return from_ms(to_ms(self.origin) + n * self.width_ms)
        if u == "months":
            months = (x.year - self.origin.year) * 12 + (x.month - self.origin.month)
            k = _add_months(self.origin, (months // self.months) * self.months)
            if k > x:
                k = _add_months(k, -self.months)
            return k
        if u == "second":
            return x.replace(microsecond=0)
        if u == "minute":
            return x.replace(second=0, microsecond=0)
        if u == "hour":
            return x.replace(minute=0, second=0, microsecond=0)
        d = x.replace(hour=0, minute=0, second=0, microsecond=0)
        if u == "day":
            return d
        if u == "week":
            return d - dt.timedelta(days=d.weekday())
        if u == "month":
            return d.replace(day=1)
        if u == "quarter":
            return d.replace(day=1, month=(d.month - 1) // 3 * 3 + 1)
        if u == "year":
            return d.replace(day=1, month=1)
        raise ValueError(u)

    def next_key(self, k: dt.datetime) -> dt.datetime:
        u = self.unit
        if u == "fixed":
            return k + dt.timedelta(milliseconds=self.width_ms)
        if u == "months":
            return _add_months(k, self.months)
        if u in FIXED:
            return k + dt.timedelta(milliseconds=FIXED[u])
        return _add_months(k, {"month": 1, "quarter": 3, "year": 12}[u])

    def local_range(self, k: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
        return k - self.shift_in, self.next_key(k) - self.shift_in

    def label(self, k: dt.datetime):
        v = k + self.shift_out
        return v.date() if self.as_date else v

    @property
    def nominal_ms(self) -> int:
        """Typical bucket length (for sizing, not for windows)."""
        if self.unit == "fixed":
            return self.width_ms
        if self.unit == "months":
            return self.months * 30 * DAY
        return FIXED.get(self.unit) or {"month": 30, "quarter": 91, "year": 365}[self.unit] * DAY


@dataclass
class Bucket:
    label: object                        # value of the bucket expression (naive local / date)
    pieces: list[tuple[int, int]]        # UTC [start, end) ms, sorted, merged
    local_start: dt.datetime | None = None   # local wall-clock start of the whole bucket

    @property
    def start(self) -> int:
        return self.pieces[0][0]


def buckets(zone: Zone, grain: Grain | None, t0: int, t1: int, limit: int = 2_000_000) -> list[Bucket]:
    """Buckets of `grain` that meet [t0, t1), each with the UTC intervals it covers inside
    [t0, t1). grain None: one bucket (label None) for the whole range."""
    if t1 <= t0:
        return []
    if grain is None:
        return [Bucket(None, [(t0, t1)], None)]
    pieces: dict[dt.datetime, list[tuple[int, int]]] = {}
    for a, b, off in zone.segments(t0, t1):
        la, lb = from_ms(a + off), from_ms(b + off)
        k = grain.key_floor(la)
        while True:
            ks, ke = grain.local_range(k)
            s, e = max(ks, la), min(ke, lb)
            if s < e:
                pieces.setdefault(k, []).append((to_ms(s) - off, to_ms(e) - off))
                if len(pieces) > limit:
                    raise ValueError(f"more than {limit:,} time buckets: use a coarser time grain")
            if ke >= lb:
                break
            k = grain.next_key(k)
    out: list[Bucket] = []
    for k, ps in pieces.items():
        ps.sort()
        merged = [ps[0]]
        for s, e in ps[1:]:
            if s <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], e))
            else:
                merged.append((s, e))
        out.append(Bucket(grain.label(k), merged, grain.local_range(k)[0]))
    out.sort(key=lambda b: b.start)
    return out


def duration(ms: int) -> str:
    """PromQL duration literal."""
    if ms <= 0:
        raise ValueError("empty duration")
    for unit, size in (("d", DAY), ("h", HOUR), ("m", MINUTE), ("s", SECOND)):
        if ms % size == 0:
            return f"{ms // size}{unit}"
    return f"{ms}ms"


def parse_duration(s: str) -> int:
    """'5m', '1h30m', '90s', '250ms', '2d', '1w' -> ms."""
    import re

    s = s.strip().lower()
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|s|m|h|d|w|y)", s)
    if not parts or "".join(n + u for n, u in parts) != s:
        raise ValueError(f"invalid duration {s!r} (examples: 30s, 5m, 1h, 1d)")
    size = {"ms": 1, "s": SECOND, "m": MINUTE, "h": HOUR, "d": DAY, "w": 7 * DAY, "y": 365 * DAY}
    return int(round(sum(float(n) * size[u] for n, u in parts)))
