# promagg

Prometheus / Grafana Mimir as a SQL database for Apache Superset (DB-API 2.0, SQLAlchemy dialect
and Superset engine spec). Superset sends plain SQL; promagg translates it into PromQL, so the
metrics backend aggregates over billions of samples and only the (small) result travels back.
An embedded, locked-down DuckDB evaluates what is left (ORDER BY, LIMIT, HAVING, arithmetic on
aggregates, joins of aggregated subqueries).

Companion of osagg (the same approach for OpenSearch).

## Install (pip, nothing in superset_config.py)

    pip install promagg-0.1.0-py3-none-any.whl      # into Superset's virtualenv, restart Superset

Dependencies are already in Superset: sqlglot, duckdb, pyarrow, urllib3, SQLAlchemy.

## Connect

Settings > Database Connections > + Database > **Prometheus / Mimir (PromQL pushdown)**:

    promagg://mimir-query-frontend:8080/prometheus?tenant=<org id>&timezone=Europe/Paris
    promagg+https://user:password@mimir.example.com/prometheus?tenant=...
    promagg+https://bearer:<token>@mimir.example.com:443/prometheus?tenant=...   (Bearer token)
    promagg://prometheus:9090/?timezone=Europe/Paris                              (plain Prometheus)

| option | default | meaning |
|---|---|---|
| URL path | `prometheus` | API prefix: Mimir `/prometheus`, Prometheus empty |
| tenant | none | `X-Scope-OrgID` (Mimir tenant); `a\|b` reads several tenants (see below) |
| timezone | UTC | time zone of the naive timestamps Superset sends and receives |
| default_range | 24h | window of queries that have no time filter |
| schema_window | 7d | where metric names, labels and filter values are looked up (`all` = no limit) |
| tables | all | metric name patterns to expose (`node_*,batch_*`, `~regex`) |
| counters | none | patterns of counters without `_total` suffix or metadata (`node_vmstat_*`) |
| request_timeout | 120 | seconds per HTTP request |
| concurrency | 4 | PromQL queries run in parallel per SQL query |
| max_points | 2,000,000 | rows (groups x buckets) a query may return |
| max_samples | 1,000,000 | raw samples a row query may read |
| scrape_interval | 15s | for `$__rate_interval` in promql() |
| allow_promql | true | allow the promql() table function (it needs database access in Superset) |
| increase | auto | `rate`, `increase` and `delta` of a time bucket: `auto` exact (anchored ranges) when the backend allows them, else Prometheus' extrapolated values; `exact` anchored or an error; `prometheus` extrapolated, as Grafana shows them (see Exact increases) |
| all_metrics | `all_metrics` | name of the table that holds every metric (see below); empty: no such table |
| all_metrics_labels | 300 | its label columns: a comma-separated list (`job,instance,node`), or how many at most |
| all_metrics_max | 50 | metrics one query on it may read |
| verify_certs, ca_certs, client_cert, client_key | | TLS |

### Several tenants (Mimir tenant federation)

    promagg://mimir-query-frontend:8080/prometheus?tenant=team-a|team-b&timezone=Europe/Paris
    (the same with team-a%7Cteam-b)

With tenant federation enabled in Mimir (`-tenant-federation.enabled=true` on the
query-frontends and queriers), one connection reads several tenants. Mimir merges them and adds
the label `__tenant_id__` to every series; promagg shows it as a column like any label:

* aggregates add up across tenants (`SUM(rate) GROUP BY node`); `GROUP BY __tenant_id__`
  splits them;
* `WHERE __tenant_id__ = 'team-a'` (`IN`, `<>`, `LIKE`) selects tenants inside Mimir (a matcher
  on `__tenant_id__`);
* the tables are the metrics of all the tenants; `SELECT DISTINCT __tenant_id__ FROM m` lists
  the tenants that have series of m (read from the series, since Mimir's label index lists every
  tenant of the request: at most 20,000 series at once, else one series asked per tenant; 0.8 s
  for 52 tenants and 520,000 series in the lab, instead of 4 s to read them all);
* the ruler API (alerts, rules) takes one tenant: promagg asks each one and labels the alerts
  with `__tenant_id__`.

Without federation Mimir refuses the request ("too many tenant IDs") and promagg says how to
enable it. Tested on Mimir 3.2.1 with three tenants.

### Through a gateway (TLS, passwords, tokens, client certificates)

promagg reads what Superset can reach: Mimir's query-frontend, or the gateway in front of it
(nginx, an API gateway...). Tested with nginx 1.26 in five set-ups (the same rows as a direct
connection, and each refusal explained):

| gateway | URI |
|---|---|
| TLS + user / password; tenant header passed through, checked against the account | `promagg+https://user:pw@gw:443/prometheus?tenant=a\|b&ca_certs=/etc/ssl/gw-ca.pem` |
| TLS + Bearer token | `promagg+https://bearer:<token>@gw:443/prometheus?tenant=a\|b&ca_certs=...` |
| TLS + client certificate (mTLS) | `promagg+https://gw:443/prometheus?tenant=a\|b&client_cert=/p/client.pem&client_key=/p/client.key&ca_certs=...` |
| the gateway sets the account's tenants (when the client sends none, or always) | `promagg+https://user:pw@gw:443/prometheus` |

What the gateway must allow: `X-Scope-OrgID` passed unchanged (with `|`) and every tenant of
the list allowed for the account (else HTTP 403); GET and POST (form bodies) on
`/prometheus/api/v1/` `query`, `query_range`, `series`, `labels`, `label/<name>/values`,
`metadata`, `status/buildinfo`, and `alerts`, `rules` for the agent; a read time-out above
Mimir's query time-out. The ruler API (alerts, rules) takes one tenant: promagg asks each tenant
on its own, which a gateway that replaces the client's tenant header cannot serve (the error says
so; queries still work). The certificate and key files must be readable by the Superset
processes (web server and workers).

Refusals say why and are not retried: certificate not trusted (set `ca_certs`), HTTP 401
(credentials), HTTP 403 (the tenants refused), client certificate missing or refused, TLS file
not found. Table metadata is cached per credentials, so a wrong password never answers from
what a right one read.

## Tables and columns

One table per metric name (also as `promagg.<metric>`, `prometheus.<metric>`, `mimir.<metric>` or
`metrics.<metric>`: tools that prefix a schema work). Columns:

| column | type | |
|---|---|---|
| `ts` | TIMESTAMP | sample time (connection time zone); in aggregates, the start of the time bucket |
| one per label | VARCHAR | NULL when the series lacks the label |
| `value` | DOUBLE | sample value |
| `rate`, `increase` | DOUBLE | counters only: per-second rate / increase **per series and time bucket** |

### One dataset for every metric: all_metrics

With thousands of metrics, one Superset dataset per metric is not practical. The table
`all_metrics` (schema `default`, listed first) holds them all: create **one dataset** on it and
choose the metric with a filter (a dashboard native filter on `metric_name`, or a chart filter).

| column | type | |
|---|---|---|
| `ts` | TIMESTAMP | as in a metric table |
| `metric_name` | VARCHAR | the metric: **filter it** (`= 'x'`, `IN (...)`, `LIKE 'node_%'`) |
| one per label | VARCHAR | every label name of the database (`all_metrics_labels` lists or caps them); NULL for the metrics without it |
| `value`, `rate`, `increase` | DOUBLE | as in a metric table (`rate`, `increase`: counters only) |

A query names its metrics in WHERE, with conditions on `metric_name` alone; it becomes the same
query on each metric's own table, so the PromQL (and the result) is exactly the one of the
metric's table:

* one metric: that query;
* several metrics (at most `all_metrics_max`): one query per metric, `UNION ALL`; an aggregate
  must then `GROUP BY metric_name` (charts: add *metric_name* to the dimensions);
* no metric filter: the filter lists only (`metric_name` values, or the values of one label,
  from the label index); anything else is refused, since it would read every metric;
* `SUM(rate)` / `SUM(increase)` of a gauge is refused with the reason; a virtual dataset
  `SELECT * FROM all_metrics WHERE ...` is still pushed down.

Tools that check SQL per metric table (for example supagent's refusal of `SUM(value)` on a
counter) do not see through `all_metrics`: they apply to the metric tables.

## SQL -> PromQL

Aggregates over `value` are exact over the raw samples of each group and time bucket
(`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VARIANCE`, and
linear expressions like `AVG(value / 1024)`):

    SELECT DATE_TRUNC('hour', ts), node, AVG(value)            -- sum by (node) (sum_over_time(m[1h]))
    FROM node_load1 WHERE ts >= ... AND ts < ... GROUP BY 1, 2  -- / sum by (node) (count_over_time(m[1h]))

Functions over time give one value per series and time bucket, and the SQL aggregate around
them aggregates across series, as in Grafana:

| SQL | PromQL |
|---|---|
| `SUM(rate)`, `SUM(RATE(value))` ... `GROUP BY node` | `sum by (node) (rate(m[bucket]))` |
| `SUM(INCREASE(value))`, `MAX(IRATE(value))`, `AVG(DERIV(value))` | increase, irate, deriv |
| `SUM(DELTA(value))`, `MIN(IDELTA(value))`, `SUM(CHANGES(value))`, `SUM(RESETS(value))` | delta, idelta, changes, resets |
| `AVG_OVER_TIME`, `MIN_`, `MAX_`, `SUM_`, `COUNT_`, `LAST_`, `FIRST_`, `PRESENT_`, `STDDEV_`, `STDVAR_`, `MAD_OVER_TIME(value)` | `*_over_time` |
| `QUANTILE_OVER_TIME(0.95, value)`, `PREDICT_LINEAR(value, 3600)`, `HOLT_WINTERS(value, 0.5, 0.5)` (`DOUBLE_EXPONENTIAL_SMOOTHING`) | same |
| `RATE(value, '5m')` | explicit window (sliding, evaluated at each bucket end) |
| outer `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `STDDEV_POP`, `VAR_POP`, `STDDEV_SAMP`, `VARIANCE`, `MEDIAN`, `QUANTILE_CONT(x, q)`, `PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x)` | sum, avg, min, max, count, stddev, stdvar, quantile by (...) |
| `ABS`, `CEIL`, `FLOOR`, `ROUND(x, 2)`, `EXP`, `LN`, `LOG2`, `LOG10`, `SQRT`, `SIGN`, `GREATEST(x, c)`, `LEAST(x, c)`, arithmetic with constants | per series: abs, ceil, ..., clamp_min, clamp_max |
| `HISTOGRAM_QUANTILE(0.95, SUM(RATE(value)))` on a `_bucket` table | `histogram_quantile(0.95, sum by (..., le) (rate(m[bucket])))` |
| `SUM(rate) FILTER (WHERE code LIKE '5%')`, `SUM(CASE WHEN status = 'FAILED' THEN increase ELSE 0 END)` | extra label matchers for that aggregate |
| `COUNT(DISTINCT node)` | `count by (...) (count by (..., node) (...))` |
| `ORDER BY <aggregate> DESC LIMIT k` (no time bucket) | `topk(k, ...)` / `bottomk` |

`MAD_OVER_TIME` (and any experimental PromQL function) needs Mimir's per-tenant setting
`-query-frontend.enabled-promql-experimental-functions=mad_over_time` (or `all`); otherwise Mimir
refuses the query and the error says so. Tested on Mimir 3.2.1: `HOLT_WINTERS`,
`DOUBLE_EXPONENTIAL_SMOOTHING` and `FIRST_OVER_TIME` run without it.

Label filters become matchers, keeping SQL NULL semantics: `=`, `<>` (`node!="x", node!=""`),
`IN`, `NOT IN`, `LIKE`, `ILIKE`, `NOT LIKE`, `regexp_matches`, `~`, `IS [NOT] NULL`, `AND`, `OR`
(same label: one regex; several labels: PromQL `or` union), `NOT`. Other label expressions
(`UPPER(node)`, `SPLIT_PART(instance, ':', 1)`, `CASE`...) in WHERE or GROUP BY are computed by
DuckDB on partial aggregates by finer labels (exact).

Time buckets: `DATE_TRUNC('second'...'year', ts)`, `TIME_BUCKET(INTERVAL '5 minutes', ts [, origin])`,
Superset's week variants, `CAST(ts AS DATE)`, all in the connection time zone: days around
daylight-saving changes last 23 / 25 hours, the autumn repeated hour is one 2-hour bucket.
`EXTRACT(hour FROM ts)` / `DATE_PART('dow', ts)` in GROUP BY or WHERE (business hours) work for
raw-value aggregates and for sums of increases / counts (they add up across buckets).

A bucket `[start, end)` is evaluated at `end` with `offset 1ms`: a sample on a boundary belongs to
one bucket only, and evaluation times stay aligned on the step. Ranges are left-open in Prometheus 3 /
Mimir 3; on older backends (closed ranges, probed once per connection) the window is 1 ms
shorter, with the same result (tested on Mimir 3.2.1). Runs of equal buckets are one
`query_range` (at most 10,000 points; longer runs are split); irregular buckets (partial first /
last bucket, 23 / 25 hour days, months) are instant queries; queries run in parallel; a query
refused for a backend limit (samples, chunks, time) is split in time and retried.

Row queries (`SELECT ts, node, value ... ORDER BY ts DESC LIMIT 100`) read raw samples, from the
end (or the start for ascending order) in growing windows until the LIMIT is reached; the first
window starts at the data edge (located with `timestamp()` probes), so a series that stopped days
ago answers as fast as a live one. `rate` / `increase` are NULL on raw samples.
`SELECT MIN(ts), MAX(ts) FROM m WHERE ...` returns the time of the first / last sample the same way.
`SELECT DISTINCT label` (label columns only: dashboard filter lists) reads the label index of the
time range, no samples, like a Grafana variable: at index granularity (storage blocks), a series
that stopped earlier in the same block is listed. For an exact "which series have samples in the
range", aggregate: `SELECT node, COUNT(*) FROM m WHERE ... GROUP BY node` (or `COUNT(DISTINCT node)`).

`GROUP BY ts` (Superset's "original value" time grain, compile checks of generated charts)
uses automatic buckets of about 500 points over the time range (15 s ... 1 day), since samples
are not grouped one by one. Queries over more than two days without a time bucket
(`SUM(increase)` over a month) are computed per day and added up (for aggregates that add up),
so the backend never holds a month of chunks for a single evaluation.

### Exact increases

Prometheus computes `rate`, `increase` and `delta` from the samples inside the window and
extrapolates them to its edges: the hourly increase of a job counter reads 284.18 failed jobs
for 285, and the increments between the last sample of one bucket and the first of the next
belong to neither. In the lab, per hour and status, 82 of 240 values were wrong even after
rounding. With **anchored ranges** (an experimental extended range selector of Prometheus 3 and
Mimir 3), the sample just before the bucket is included and nothing is extrapolated: the
increase of a bucket is the sum of the counter's increments in it, resets included, and the
buckets add up exactly to the day.

promagg uses them by default (`increase=auto`) for `rate`, `increase` and `delta` of a time
bucket whenever the backend allows them for the tenant, and says which in EXPLAIN. Enable them
in Mimir with `-query-frontend.enabled-promql-extended-range-selectors=anchored` (or the
tenant's `enabled_promql_extended_range_selectors` limit). Tested on Mimir 3.2.1: in the lab, 4
counters on the days around both daylight-saving changes matched the raw samples' increments
exactly, per hour and label.

One difference remains. When a counter comes back after an interruption of more than 5 minutes
(Prometheus' lookback) that spans the start of the bucket, the bucket counts from the counter's
first value after the interruption: what it counted between its restart and its first scrape is
not included. `increase=exact` refuses to run without anchored ranges, and `increase=prometheus`
keeps the extrapolated values. Explicit windows (`RATE(value, '5m')`) keep Prometheus' sliding
semantics.

### promql(): any PromQL, as a table

    SELECT DATE_TRUNC('hour', ts), node, AVG(value)
    FROM promql('100 * (1 - avg_over_time(node_memory_MemAvailable_bytes[$__interval] offset 1ms)
                          / avg_over_time(node_memory_MemTotal_bytes[$__interval] offset 1ms))')
    WHERE ts >= ... AND ts < ... GROUP BY 1, 2

Columns: `ts`, the labels of the result, `value`. With a time bucket, the expression is evaluated
at the end of each bucket (`$__interval` = bucket length); otherwise every `step` (second
argument, e.g. `promql('up', '5m')`, or automatic). Variables: `$__interval`, `$__interval_ms`,
`$__rate_interval`, `$__range`, `$__range_s`, `$__range_ms`. As a Superset virtual dataset, the
chart's time range and grain are applied to the evaluation. Covers what SQL does not express:
arithmetic between metrics, `label_replace`, subqueries, `absent`, offsets...

Native histograms (Prometheus 2.40+ / Mimir) were not in the lab data and are untested as SQL
tables; query them with promql(), whose functions return floats:
`promql('histogram_quantile(0.95, sum by (application) (rate(m[$__interval] offset 1ms)))')`,
`histogram_count`, `histogram_sum`, `histogram_fraction`. Classic histograms (`*_bucket` tables)
work in SQL with `HISTOGRAM_QUANTILE`.

### Not possible (refused with the reason)

* quantiles / medians of raw samples (use `QUANTILE_OVER_TIME` per series, or a histogram);
* grouping by, or filtering on, sample values in aggregates (use HAVING, or promql('m > 90'));
* joining metric tables row by row (aggregate each in a subquery and join the subqueries, or promql());
* functions over time that do not add up (rate, avg_over_time...) across several buckets of one
  group (hour-of-day grouping): use one time bucket per group.

`EXPLAIN <sql>` shows the PromQL; `EXPLAIN ANALYZE <sql>` runs it and lists the queries sent.

## Performance notes

* The work is done by Mimir (query-frontend splitting by day and sharding, store-gateways);
  promagg sends one PromQL query per aggregate and run of buckets, in parallel, and receives
  groups x buckets rows.
* Mimir's results cache (memcached) only keeps queries whose times are multiples of the step:
  minute, hour and 5-minute buckets are cached; local-time days (Europe/Paris days end at
  22:00 / 23:00 UTC) are not. Superset's own chart cache still applies.
* Long ranges over many series read many chunks: size the chunks cache (memcached) and, for
  dashboards over months, use recording rules (pre-aggregated series are tables like any metric).

## Tests

    pytest tests/          # offline: generated PromQL, refusals, run splitting, time buckets

* `test_timegrid.py`: time buckets identical to DuckDB's DATE_TRUNC / TIME_BUCKET;
* `test_planner.py`: generated PromQL, refusals, run splitting, label index lookups.

The integration tests (every SQL query through a real Mimir compared with DuckDB over the same
raw samples, across both daylight-saving changes; functions compared with a Python port of the
Prometheus ones; gateways; tenant federation) run on an internal lab and are not published.
