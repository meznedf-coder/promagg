"""Time buckets: labels agree with DuckDB's DATE_TRUNC / TIME_BUCKET on naive local times,
and the UTC pieces of a bucket cover exactly the instants whose local time falls in it."""

from __future__ import annotations

import datetime as dt
import random

import duckdb
import pytest

from promagg.timegrid import DAY, HOUR, MINUTE, Grain, Zone, buckets, from_ms, parse_duration, duration

PARIS = Zone("Europe/Paris")
GRAINS = {
    "DATE_TRUNC('second', ts)": Grain("second"),
    "DATE_TRUNC('minute', ts)": Grain("minute"),
    "DATE_TRUNC('hour', ts)": Grain("hour"),
    "DATE_TRUNC('day', ts)": Grain("day"),
    "DATE_TRUNC('week', ts)": Grain("week"),
    "DATE_TRUNC('month', ts)": Grain("month"),
    "DATE_TRUNC('quarter', ts)": Grain("quarter"),
    "DATE_TRUNC('year', ts)": Grain("year"),
    "TIME_BUCKET(INTERVAL '5 minutes', ts)": Grain("fixed", width_ms=5 * MINUTE),
    "TIME_BUCKET(INTERVAL '7 minutes', ts)": Grain("fixed", width_ms=7 * MINUTE),
    "TIME_BUCKET(INTERVAL '6 hours', ts)": Grain("fixed", width_ms=6 * HOUR),
    "TIME_BUCKET(INTERVAL '3 days', ts)": Grain("fixed", width_ms=3 * DAY),
    "TIME_BUCKET(INTERVAL '2 months', ts)": Grain("months", months=2, origin=dt.datetime(2000, 1, 1)),
    "DATE_TRUNC('week', ts + INTERVAL '1 day') - INTERVAL '1 day'":
        Grain("week", shift_in=dt.timedelta(days=1), shift_out=dt.timedelta(days=-1)),
    "DATE_TRUNC('week', ts) + INTERVAL '6 day'": Grain("week", shift_out=dt.timedelta(days=6)),
}


@pytest.mark.parametrize("sql", list(GRAINS))
def test_labels_match_duckdb(sql):
    rnd = random.Random(sql)
    grain = GRAINS[sql]
    base = dt.datetime(2025, 10, 20)
    stamps = [base + dt.timedelta(milliseconds=rnd.randrange(0, 200 * DAY)) for _ in range(300)]
    con = duckdb.connect()
    con.execute("CREATE TABLE t (ts TIMESTAMP)")
    con.executemany("INSERT INTO t VALUES (?)", [(s,) for s in stamps])
    got = con.execute(f"SELECT ts, {sql} FROM t").fetchall()
    for ts, want in got:
        mine = grain.label(grain.key_floor(ts))
        assert mine == want, (sql, ts, mine, want)


def _local_label(grain, t_ms):
    return grain.label(grain.key_floor(PARIS.local(t_ms)))


@pytest.mark.parametrize("sql", ["DATE_TRUNC('minute', ts)", "DATE_TRUNC('hour', ts)", "DATE_TRUNC('day', ts)",
                                 "DATE_TRUNC('week', ts)", "TIME_BUCKET(INTERVAL '6 hours', ts)"])
@pytest.mark.parametrize("window", [("2025-10-25 20:00", "2025-10-27 03:00"), ("2026-03-28 20:00", "2026-03-30 03:00")])
def test_pieces_cover_exactly_the_local_bucket(sql, window):
    grain = GRAINS[sql]
    t0 = PARIS.utc_ms(dt.datetime.fromisoformat(window[0]))
    t1 = PARIS.utc_ms(dt.datetime.fromisoformat(window[1]))
    bs = buckets(PARIS, grain, t0, t1)
    labels = [b.label for b in bs]
    assert len(labels) == len(set(labels))
    covered = sorted(p for b in bs for p in b.pieces)
    assert covered[0][0] == t0 and covered[-1][1] == t1
    for (a, b), (c, _d) in zip(covered, covered[1:]):
        assert b == c                                           # no gap, no overlap
    rnd = random.Random(sql + window[0])
    by_label = {b.label: b for b in bs}
    for _ in range(2000):
        t = rnd.randrange(t0, t1)
        b = by_label[_local_label(grain, t)]
        assert any(s <= t < e for s, e in b.pieces)


def test_daylight_saving_days_and_repeated_hour():
    day = GRAINS["DATE_TRUNC('day', ts)"]
    t0 = PARIS.utc_ms(dt.datetime(2025, 10, 25))
    t1 = PARIS.utc_ms(dt.datetime(2025, 10, 28))
    lengths = [b.pieces[0][1] - b.pieces[0][0] for b in buckets(PARIS, day, t0, t1)]
    assert lengths == [24 * HOUR, 25 * HOUR, 24 * HOUR]
    t0 = PARIS.utc_ms(dt.datetime(2026, 3, 28))
    t1 = PARIS.utc_ms(dt.datetime(2026, 3, 31))
    assert [b.pieces[0][1] - b.pieces[0][0] for b in buckets(PARIS, day, t0, t1)] == [24 * HOUR, 23 * HOUR, 24 * HOUR]
    # autumn: local hour 02:00 lasts two hours (one contiguous piece), minute 02:10 is two pieces
    hour = buckets(PARIS, GRAINS["DATE_TRUNC('hour', ts)"], PARIS.utc_ms(dt.datetime(2025, 10, 26, 1)),
                   PARIS.utc_ms(dt.datetime(2025, 10, 26, 4)))
    two = [b for b in hour if b.label == dt.datetime(2025, 10, 26, 2)][0]
    assert two.pieces == [(two.pieces[0][0], two.pieces[0][0] + 2 * HOUR)]
    minute = buckets(PARIS, GRAINS["DATE_TRUNC('minute', ts)"], PARIS.utc_ms(dt.datetime(2025, 10, 26, 1, 50)),
                     PARIS.utc_ms(dt.datetime(2025, 10, 26, 3, 10)))
    m = [b for b in minute if b.label == dt.datetime(2025, 10, 26, 2, 10)][0]
    assert len(m.pieces) == 2 and m.pieces[1][0] - m.pieces[0][0] == HOUR
    # spring: no local 02:xx hour
    spring = buckets(PARIS, GRAINS["DATE_TRUNC('hour', ts)"], PARIS.utc_ms(dt.datetime(2026, 3, 29, 0)),
                     PARIS.utc_ms(dt.datetime(2026, 3, 29, 5)))
    assert [b.label.hour for b in spring] == [0, 1, 3, 4]


def test_durations():
    assert duration(3 * HOUR) == "3h" and duration(90 * 1000) == "90s" and duration(1500) == "1500ms"
    assert parse_duration("1h30m") == 90 * MINUTE and parse_duration("250ms") == 250
    assert from_ms(0) == dt.datetime(1970, 1, 1)
