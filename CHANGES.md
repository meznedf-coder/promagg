# Changes

## 0.2.2 — 1 Oct 2026

Silent errors in counts and sums, found while reviewing osagg's (see its 0.2.8):

* **Exact increases.** Prometheus extrapolates `rate`, `increase` and `delta` to the window's
  edges, so the increase of a bucket is a little off: the lab's hourly failed jobs per status
  were wrong in 82 of 240 hours even after rounding (284.18 for 285). promagg now asks for
  anchored ranges (no extrapolation, the sample before the bucket included) when the backend
  allows them for the tenant. Increases per bucket are then the counter's increments, which add
  up exactly. New parameter `increase` (`auto` by default, `exact`, `prometheus`); EXPLAIN says
  which is used. In Mimir 3, enable them with
  `-query-frontend.enabled-promql-extended-range-selectors=anchored`.
* **A warning is an error.** The backend answers "success" with a warning when part of the data
  is missing (a store or remote read that failed) or a function dropped series (a malformed
  `le`, histograms mixed with floats). The warning was only logged, and the incomplete result
  was returned. It is now an error that quotes the warning. PromQL infos and the truncation of
  lists promagg asks for are still only logged.
* **Native histogram samples** have no single value and were left out of the rows without a
  word. A query that meets them is now an error that points to promql() and the histogram
  functions.

## 0.2.1 — 28 Sep 2026

* all_metrics filter lists (metric names, the values of a label) keep only what their LIMIT
  shows before the list is built: 10,000 metrics or 50,000 label values take about 0.05 s
  instead of about 2 s.

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
