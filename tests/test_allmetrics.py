"""all_metrics: one table (one Superset dataset) for every metric. Queries on it become the
same queries on the metrics' own tables, so the PromQL is the one of the metric's table."""

from __future__ import annotations

import pytest
import sqlglot

from promagg.allmetrics import AllMetrics, build_meta
from promagg.errors import PushdownError
from test_planner import METRICS, T, exprs, plan

LABELS = sorted({lb for m in METRICS.values() for lb in m.labels})


def table(max_metrics: int = 50) -> AllMetrics:
    return AllMetrics("all_metrics", lambda: build_meta("all_metrics", LABELS), lambda: sorted(METRICS),
                      METRICS.get, lambda label: [f"{label}-1", f"{label}-2"], max_metrics)


def sql_of(sql: str, max_metrics: int = 50) -> str:
    return table(max_metrics).rewrite(sqlglot.parse_one(sql, read="duckdb")).sql(dialect="duckdb")


def test_columns_are_every_label_of_every_metric():
    meta = build_meta("all_metrics", LABELS + ["value", "__name__"])
    names = list(meta.columns)
    assert names[:2] == ["ts", "metric_name"] and names[-3:] == ["value", "rate", "increase"]
    assert "label_value" in names and "node" in names and "__name__" not in names
    assert list(build_meta("all_metrics", LABELS, only=["node", "job"]).columns)[2:4] == ["job", "node"]
    assert len(build_meta("all_metrics", LABELS, max_labels=3).columns) == 3 + 5


def test_one_metric_gives_the_promql_of_its_own_table():
    superset = ("SELECT DATE_TRUNC('hour', ts) AS ts, node AS node, SUM(rate) AS \"SUM(rate)\" FROM all_metrics "
                f"WHERE {T} AND metric_name IN ('node_cpu_seconds_total') AND mode <> 'idle' "
                "GROUP BY DATE_TRUNC('hour', ts), node ORDER BY \"SUM(rate)\" DESC LIMIT 10000")
    assert exprs(sql_of(superset)) == [
        'sum by (node) (rate(node_cpu_seconds_total{mode!="idle", mode!=""}[1h] offset 1ms))']
    series_limit = ("SELECT node AS node__, SUM(rate) AS mme_inner__ FROM all_metrics "
                    f"WHERE {T} AND metric_name = 'node_cpu_seconds_total' GROUP BY node ORDER BY mme_inner__ DESC "
                    "LIMIT 5")
    p, _ex = plan(sql_of(series_limit))
    assert p.scans[0].topk[:2] == (5, "topk")


def test_a_label_the_metric_lacks_is_null():
    assert exprs(sql_of(f"SELECT SUM(value) FROM all_metrics WHERE {T} AND metric_name = 'node_load1' "
                        "AND code = '500'")) == [None]                   # never true: nothing is read
    rows = sql_of(f"SELECT * FROM all_metrics WHERE {T} AND metric_name = 'node_load1' LIMIT 10")
    assert "CAST(NULL AS TEXT) AS \"code\"" in rows and "'node_load1' AS \"metric_name\"" in rows
    assert "CAST(NULL AS DOUBLE) AS \"rate\"" in rows                   # a gauge: no rate on raw samples
    p, _ex = plan(rows)
    assert len(p.scans) == 1


def test_several_metrics_are_a_union_grouped_by_metric():
    sql = sql_of(f"SELECT metric_name, node, AVG(value) AS v FROM all_metrics WHERE {T} "
                 "AND metric_name LIKE 'node_%' GROUP BY 1, 2 ORDER BY v DESC LIMIT 20")
    assert sql.count("UNION ALL") == 1 and "'node_cpu_seconds_total' AS \"metric_name\"" in sql
    assert sql.endswith("ORDER BY v DESC LIMIT 20")
    p, _ex = plan(sql)
    assert len(p.scans) == 2
    with pytest.raises(PushdownError, match="GROUP BY metric_name"):
        sql_of(f"SELECT node, AVG(value) FROM all_metrics WHERE {T} AND metric_name LIKE 'node_%' GROUP BY node")
    with pytest.raises(PushdownError, match="at most 1 per query"):
        sql_of(f"SELECT metric_name, AVG(value) FROM all_metrics WHERE {T} AND metric_name LIKE 'node_%' "
               "GROUP BY 1", max_metrics=1)


def test_filter_boxes_without_a_metric_filter_read_the_label_index():
    metrics = sql_of("SELECT metric_name AS metric_name FROM all_metrics GROUP BY metric_name ORDER BY 1 LIMIT 1000")
    assert "VALUES" in metrics and "'node_load1'" in metrics
    assert len(plan(metrics)[0].scans) == 0
    nodes = sql_of(f"SELECT DISTINCT node FROM all_metrics WHERE {T} LIMIT 1000")
    assert "'node-1'" in nodes and "ts" not in nodes                   # the label index: no time filter
    with pytest.raises(PushdownError, match="add a filter on metric_name"):
        sql_of(f"SELECT DATE_TRUNC('hour', ts), AVG(value) FROM all_metrics WHERE {T} GROUP BY 1")
    with pytest.raises(PushdownError, match="add a filter on metric_name"):
        sql_of("SELECT metric_name, COUNT(*) FROM all_metrics GROUP BY 1")


def test_counters_only_columns_and_mixed_conditions_are_refused_with_the_reason():
    with pytest.raises(PushdownError, match="node_load1 is a gauge: rate exists for counters only"):
        sql_of(f"SELECT SUM(rate) FROM all_metrics WHERE {T} AND metric_name = 'node_load1'")
    with pytest.raises(PushdownError, match="mixes metric_name with other columns"):
        sql_of(f"SELECT AVG(value) FROM all_metrics WHERE {T} AND (metric_name = 'node_load1' OR node = 'a')")
    with pytest.raises(PushdownError, match="cannot be joined"):
        sql_of("SELECT * FROM all_metrics a JOIN all_metrics b ON a.node = b.node")


def test_no_metric_matches_and_names_with_colons():
    empty = sql_of(f"SELECT DATE_TRUNC('hour', ts), AVG(value) FROM all_metrics WHERE {T} AND "
                   "metric_name = 'nothing_like_it' GROUP BY 1")
    assert "WHERE FALSE" in empty and len(plan(empty)[0].scans) == 0
    colons = exprs(sql_of(f"SELECT AVG(value) FROM all_metrics WHERE {T} AND metric_name = 'job:errors:rate5m'"))
    assert colons and all("job:errors:rate5m" in e for e in colons)


def test_a_virtual_dataset_on_it_is_still_pushed_down():
    sql = sql_of(f"SELECT DATE_TRUNC('hour', ts) AS t, SUM(value) FROM (SELECT * FROM all_metrics WHERE "
                 f"metric_name = 'node_load1') AS virtual_table WHERE {T} GROUP BY 1")
    p, _ex = plan(sql)
    assert len(p.scans) == 1 and p.scans[0].mode == "A"                # aggregated in Prometheus
