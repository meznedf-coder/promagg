"""Offline planner tests (no Mimir): the PromQL generated for typical Superset queries, and
the refusals (with their advice) for what Prometheus cannot compute exactly."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest
import sqlglot

from promagg.errors import ProgrammingError, PushdownError
from promagg.executor import Executor
from promagg.planner import AggScan, Planner, RowScan, Settings
from promagg.schema import MetricMeta
from promagg.timegrid import HOUR, Zone

ZONE = Zone("Europe/Paris")
METRICS = {
    "node_cpu_seconds_total": MetricMeta("node_cpu_seconds_total", "counter", ["cpu", "instance", "job", "mode", "node", "region"]).build(),
    "node_load1": MetricMeta("node_load1", "gauge", ["instance", "job", "node", "region"]).build(),
    "http_requests_total": MetricMeta("http_requests_total", "counter", ["application", "code", "method"]).build(),
    "job_duration_seconds_bucket": MetricMeta("job_duration_seconds_bucket", "counter", ["application", "env", "le"]).build(),
    "job:errors:rate5m": MetricMeta("job:errors:rate5m", "gauge", ["job"]).build(),
}
T = "ts >= TIMESTAMP '2026-09-24 00:00:00' AND ts < TIMESTAMP '2026-09-25 00:00:00'"


def plan(sql: str):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'Europe/Paris'")

    def const_eval(node):
        return con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()[0]

    s = Settings(zone=ZONE, now_ms=ZONE.utc_ms(dt.datetime(2026, 9, 25, 6)))
    p = Planner(METRICS.get, s, const_eval).plan(sqlglot.parse_one(sql, read="duckdb"))
    ex = Executor(client=None, settings=s)
    ex._left_open = True
    return p, ex


def exprs(sql: str, width: int = HOUR) -> list[str]:
    p, ex = plan(sql)
    scan = p.scans[0]
    return [ex.atom_expr(scan, a, width) for a in scan.atoms]


def test_superset_timeseries_is_one_sum_by():
    sql = (f"SELECT DATE_TRUNC('hour', ts) AS ts, node AS node, SUM(rate) AS \"SUM(rate)\" FROM node_cpu_seconds_total "
           f"WHERE {T} AND mode <> 'idle' GROUP BY DATE_TRUNC('hour', ts), node ORDER BY \"SUM(rate)\" DESC LIMIT 10000")
    p, _ex = plan(sql)
    assert p.scans[0].mode == "A" and p.scans[0].grain.unit == "hour"
    assert exprs(sql) == ['sum by (node) (rate(node_cpu_seconds_total{mode!="idle", mode!=""}[1h] offset 1ms))']


def test_matchers_keep_sql_null_semantics():
    e = exprs(f"SELECT COUNT(*) FROM node_load1 WHERE {T} AND node IN ('a', 'b.c') AND region NOT IN ('x') "
              f"AND node LIKE 'srv_%' AND job IS NOT NULL")[0]
    assert 'node=~"a|b\\\\.c"' in e and 'region!="x", region!=""' in e and 'node=~"(?s)srv..*"' in e and 'job!=""' in e


def test_or_of_different_labels_is_a_union():
    e = exprs(f"SELECT SUM(value) FROM node_load1 WHERE {T} AND (node = 'a' OR region = 'apac')")[0]
    assert e == 'sum by () ((sum_over_time(node_load1{node="a"}[1h] offset 1ms)) or (sum_over_time(node_load1{region="apac"}[1h] offset 1ms)))'


def test_topk_without_time_bucket():
    p, ex = plan(f"SELECT node AS node__, SUM(value) AS mme_inner__ FROM node_load1 WHERE {T} GROUP BY node "
                 "ORDER BY mme_inner__ DESC LIMIT 5")
    assert p.scans[0].topk[:2] == (5, "topk")


def test_histogram_quantile():
    e = exprs(f"SELECT DATE_TRUNC('hour', ts), application, HISTOGRAM_QUANTILE(0.95, SUM(RATE(value))) "
              f"FROM job_duration_seconds_bucket WHERE {T} AND env = 'PROD' GROUP BY 1, 2")
    assert e == ['histogram_quantile(0.95, sum by (application, le) (rate(job_duration_seconds_bucket{env="PROD"}[1h] offset 1ms)))']


def test_metric_names_with_colons_and_explicit_ranges():
    e = exprs(f"SELECT MAX(AVG_OVER_TIME(value, '10m')) FROM \"job:errors:rate5m\" WHERE {T}")[0]
    assert e == 'max by () (avg_over_time(job:errors:rate5m[10m] offset 1ms))'


def test_partial_pushdown_for_label_expressions():
    p, ex = plan(f"SELECT UPPER(region), SUM(RATE(value)), AVG(RATE(value)) FROM node_cpu_seconds_total WHERE {T} GROUP BY 1")
    scan = p.scans[0]
    assert scan.mode == "B" and scan.labels == ["region"]
    assert sorted(a.outer for a in scan.atoms) == ["count", "sum"]


def test_rows_have_null_virtual_columns():
    p, _ex = plan(f"SELECT ts, node, rate, value FROM node_cpu_seconds_total WHERE {T} ORDER BY ts DESC LIMIT 10")
    assert isinstance(p.scans[0], RowScan) and p.scans[0].limit == 10 and p.scans[0].order == "desc"
    assert "CAST(NULL AS DOUBLE)" in p.statement.sql(dialect="duckdb")


@pytest.mark.parametrize("sql, message", [
    (f"SELECT EXTRACT(epoch FROM ts), SUM(value) FROM node_load1 WHERE {T} GROUP BY 1", "time bucket"),
    (f"SELECT SUM(value) FROM node_load1 WHERE {T} AND value > 1", "conditions on sample values"),
    (f"SELECT MEDIAN(value) FROM node_load1 WHERE {T}", "quantiles of raw samples"),
    (f"SELECT a.node FROM node_load1 a JOIN node_cpu_seconds_total b ON a.node = b.node WHERE {T}", "aggregate each metric"),
    (f"SELECT COUNT(DISTINCT UPPER(node)) FROM node_load1 WHERE {T}", "one label"),
    (f"SELECT HISTOGRAM_QUANTILE(0.9, SUM(RATE(value))) FROM node_load1 WHERE {T}", '"le"'),
    (f"SELECT SUM(nope) FROM node_load1 WHERE {T}", 'column "nope"'),
    (f"SELECT SUM(RATE(node)) FROM node_load1 WHERE {T}", "value column"),
    (f"SELECT SUM(value) FILTER (WHERE value > 1) FROM node_load1 WHERE {T}", "only conditions on labels"),
    (f"SELECT value, COUNT(*) FROM node_load1 WHERE {T} GROUP BY value", "sample values"),
    (f"SELECT EXTRACT(hour FROM ts), AVG(RATE(value)) FROM node_cpu_seconds_total WHERE {T} GROUP BY 1", "several time buckets"),
])
def test_refusals_explain_why(sql, message):
    with pytest.raises((PushdownError, ProgrammingError)) as ex:
        plan(sql)
    assert message in str(ex.value)


def test_default_range_note():
    p, _ex = plan("SELECT SUM(value) FROM node_load1")
    scan = p.scans[0]
    assert isinstance(scan, AggScan) and scan.t1 - scan.t0 == 24 * HOUR and "no time filter" in p.notes[0]


class _LimitedClient:
    """Refuses query_range with more than 4 points (like a samples limit), answers 1 per bucket."""

    def __init__(self):
        self.calls = []

    def query_range(self, expr, start, end, step):
        from promagg.client import Series
        from promagg.errors import LimitError

        n = (end - start) // step + 1
        self.calls.append(n)
        if n > 4:
            raise LimitError("the query exceeded the maximum number of samples")
        return [Series({"node": "a"}, [(t, 1.0) for t in range(start, end + 1, step)])]

    def query(self, expr, t):
        from promagg.client import Series

        self.calls.append(1)
        return [Series({"node": "a"}, [(t, 1.0)])]


def test_runs_are_split_when_the_backend_refuses_them():
    p, ex = plan("SELECT DATE_TRUNC('hour', ts), node, SUM(value) FROM node_load1 "
                 "WHERE ts >= TIMESTAMP '2026-09-24 00:00:00' AND ts < TIMESTAMP '2026-09-24 12:00:00' GROUP BY 1, 2")
    ex.client = _LimitedClient()
    res = ex.run_agg(p.scans[0])
    assert res.rows == 12 and all(v == 1.0 for v in res.data["__a0"])
    assert max(n for n in ex.client.calls if n <= 4) <= 4 and ex.client.calls[0] == 12


def test_or_chains_of_one_label_become_one_regex():
    e = exprs(f"SELECT SUM(value) FROM node_load1 WHERE {T} AND (node = 'a' OR node = 'b' OR node = 'c.d')")[0]
    assert e == 'sum by () (sum_over_time(node_load1{node=~"a|b|c\\\\.d"}[1h] offset 1ms))'


def test_filter_lists_read_the_label_index():
    from promagg.planner import LabelScan

    p, _ex = plan("SELECT DISTINCT node FROM node_load1 WHERE region = 'amer' LIMIT 1000")
    assert isinstance(p.scans[0], LabelScan) and p.scans[0].labels == ["node"]
    p, _ex = plan(f"SELECT DISTINCT node FROM node_load1 WHERE {T}")      # the index of that time range
    assert isinstance(p.scans[0], LabelScan) and p.scans[0].t0 is not None
    p, _ex = plan(f"SELECT node, COUNT(*) FROM node_load1 WHERE {T} GROUP BY node")   # counts: samples
    assert not isinstance(p.scans[0], LabelScan)


def test_long_ranges_without_bucket_are_split_by_day_when_exact():
    p, _ex = plan("SELECT node, COUNT(*), SUM(value), MAX(value) FROM node_load1 "
                  "WHERE ts >= TIMESTAMP '2026-09-01 00:00:00' AND ts < TIMESTAMP '2026-09-25 00:00:00' GROUP BY node")
    assert p.scans[0].grain.unit == "day" and p.scans[0].mode == "B"
    p, _ex = plan("SELECT node, SUM(RATE(value)) FROM node_cpu_seconds_total "
                  "WHERE ts >= TIMESTAMP '2026-09-01 00:00:00' AND ts < TIMESTAMP '2026-09-25 00:00:00' GROUP BY node")
    assert p.scans[0].grain is None          # a rate over the whole range stays one window


def test_group_by_raw_time_uses_automatic_buckets():
    p, _ex = plan(f"SELECT ts, node, SUM(value) FROM node_load1 WHERE {T} GROUP BY ts, node")
    scan = p.scans[0]
    assert scan.grain.unit == "fixed" and scan.grain.width_ms == 300_000 and "automatic" in scan.notes[0]
