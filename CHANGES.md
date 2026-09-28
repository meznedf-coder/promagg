# Changes

## 0.2.0 — 28 Sep 2026

* **One dataset for every metric**: the table `all_metrics` (schema `default`, listed first)
  holds every metric, with `metric_name`, one column per label of the database, `value`,
  `rate` and `increase`. A query filters the metric (`metric_name = 'x'`, `IN`, `LIKE`) and
  becomes the same query on each metric's own table (the same PromQL); several metrics are
  one query each, `UNION ALL`, grouped by `metric_name`. Filter lists (`metric_name`, one
  label) come from the label index; a query without a metric filter, or `rate` of a gauge, is
  refused with the reason. Connection parameters `all_metrics` (its name, or none),
  `all_metrics_labels` (a list, or at most N label columns, 300) and `all_metrics_max` (50).
  The metric tables and the DB-API `list_tables()` are unchanged.
* Superset 6.0 / 5.0 (sqlglot 27 / 26): queries whose time expressions or label filters went
  through `HOUR()` / `MINUTE()` / `SECOND()` or a regular expression failed with
  `AttributeError: module 'sqlglot.expressions' has no attribute ...`; fixed (the test suite
  now runs on sqlglot 26, 27 and 28).

## 0.1.0 — 27 Sep 2026

First version: Prometheus / Grafana Mimir as a SQL database for Superset.

* One table per metric (`ts`, labels, `value`, and `rate` / `increase` for counters).
* SQL -> PromQL: aggregates over raw samples (exact), functions over time with every
  PromQL aggregation across series (sum, avg, min, max, count, stddev, stdvar, quantile,
  topk / bottomk), math functions, histogram_quantile, FILTER / CASE on labels,
  COUNT(DISTINCT label), label filters with SQL NULL semantics, time buckets in the
  connection time zone (daylight saving: 23 / 25 hour days, the repeated autumn hour).
* Partial pushdown (by finer labels) for label expressions and hour-of-day groupings;
  DuckDB finishes on the small result.
* `promql('...')` table function with Grafana variables; `EXPLAIN [ANALYZE]`.
* Row queries read raw samples in growing windows up to the LIMIT; dashboard filter lists
  read the label index.
* Superset engine spec and SQLAlchemy dialect (`promagg://`, `promagg+https://`, tenant,
  basic auth or Bearer token, TLS options); LIMIT 0 column probes read nothing.
* Retries on busy answers (429 / 5xx); a query refused for a backend limit is split in time.
* Data edges: row queries and `MIN(ts)` / `MAX(ts)` start at the first / last sample
  (`timestamp()` probes) instead of scanning empty windows.
* `SELECT DISTINCT label` of a time range reads the label index of that range (no samples).
* `GROUP BY ts` (raw time): automatic buckets of about 500 points.
* Ranges over two days without time bucket are computed per day (bounded memory in Mimir).
* Schema prefixes `promagg.`, `prometheus.`, `mimir.`, `metrics.` accepted; unknown qualifiers
  get a clear error.
* Several tenants in one connection (`tenant=a|b`, Mimir tenant federation): `__tenant_id__`
  column, tenant filters pushed down, tenant values read from the series, alerts and rules
  asked per tenant; a clear error when Mimir has federation disabled.
* Gateways: TLS trust, client certificates (mTLS) and gateway error pages explained (no retry
  loop on TLS errors), TLS files checked up front; table metadata cached per credentials;
  alerts and rules when the gateway sets the tenants; `promagg+https` dialect uses SQLAlchemy's
  statement cache; Superset shows promagg's own message in "Test connection".
* Tenant values (`SELECT DISTINCT __tenant_id__`, dashboard filters) read from at most 20,000
  series, else from one series asked per tenant (16 at a time), instead of every series of the
  metric (520,000 series: 0.8 s instead of 4 s).
