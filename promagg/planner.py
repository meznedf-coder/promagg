"""Query planner: SQL over metric tables -> PromQL scans + a DuckDB residual.

A SELECT on a metric table becomes one *scan*, evaluated by Prometheus / Mimir, and the
SELECT is rewritten to read the scan's (small) result in an embedded DuckDB, which does what
is left: ORDER BY, LIMIT, HAVING, arithmetic on aggregates, joins between scans...

Semantics (exact, testable against the raw samples):
  * `value` aggregates (COUNT, SUM, MIN, MAX, AVG, STDDEV, VARIANCE) are over the raw samples
    of the group: PromQL `sum by (L) (sum_over_time(m{..}[bucket]))` etc.
  * functions over time (RATE(value), INCREASE(value), AVG_OVER_TIME(value), ... and the
    `rate` / `increase` columns of counters) give one value per series and time bucket (the
    bucket is the window, or an explicit range: RATE(value, '5m')); the SQL aggregate around
    them aggregates across series, like `sum by (L) (rate(m[..]))` in Grafana.
  * a bucket [start, end) is evaluated at `end` with `offset 1ms` (left-open ranges), so a
    sample exactly on a boundary belongs to one bucket only, and evaluation times stay
    aligned on the step (Mimir's results cache).

Scan modes for aggregates:
  A  direct      GROUP BY labels + one time bucket: PromQL aggregates `by (the labels)`.
  B  partial     extra labels are needed in DuckDB (COUNT(DISTINCT label), expressions over
                 labels, label filters PromQL cannot express, several time buckets per group):
                 decomposable partials (sum, count, min, max, sum of squares) by finer labels
                 or buckets, recombined exactly by DuckDB.
  C  per series  quantiles across series with extra labels: one row per series and bucket.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable

from sqlglot import exp

from promagg import promql as pq
from promagg.errors import ProgrammingError, PushdownError
from promagg.schema import MetricMeta
from promagg.timegrid import (DAY, DUCKDB_MONTH_ORIGIN, DUCKDB_ORIGIN, HOUR, MINUTE, SECOND, Grain, Zone,
                              parse_duration)

SCAN_PREFIX = "__promagg_scan_"
SPLIT_WHOLE_RANGE_MS = 2 * DAY          # longer ranges without time bucket: computed per day
FROM_KEY = "from_" if "from_" in exp.Select.arg_types else "from"
# functions that take one part of a timestamp (sqlglot 26 / 27, Superset 5.0 / 6.0, have no Hour,
# Minute, Second: HOUR(ts) is then an anonymous function, read like DATE_PART)
TIME_PART_UNITS = {getattr(exp, name): unit for name, unit in (
    ("Hour", "hour"), ("Minute", "minute"), ("Second", "second"), ("Day", "day"), ("Month", "month"),
    ("Year", "year"), ("Quarter", "quarter"), ("DayOfWeek", "dow"), ("DayOfMonth", "day"), ("DayOfYear", "doy"),
    ("Week", "week")) if hasattr(exp, name)}
TIME_PARTS = tuple(TIME_PART_UNITS)
REGEXPS = tuple(getattr(exp, n) for n in ("RegexpLike", "RegexpFullMatch") if hasattr(exp, n))   # 26 / 27: no FullMatch

# SQL name -> (PromQL function, scalar args before the range vector, scalar args after)
RANGE_FUNCS: dict[str, tuple[str, int, int]] = {
    "rate": ("rate", 0, 0), "irate": ("irate", 0, 0), "increase": ("increase", 0, 0),
    "delta": ("delta", 0, 0), "idelta": ("idelta", 0, 0), "deriv": ("deriv", 0, 0),
    "changes": ("changes", 0, 0), "resets": ("resets", 0, 0),
    "avg_over_time": ("avg_over_time", 0, 0), "min_over_time": ("min_over_time", 0, 0),
    "max_over_time": ("max_over_time", 0, 0), "sum_over_time": ("sum_over_time", 0, 0),
    "count_over_time": ("count_over_time", 0, 0), "last_over_time": ("last_over_time", 0, 0),
    "first_over_time": ("first_over_time", 0, 0), "present_over_time": ("present_over_time", 0, 0),
    "stddev_over_time": ("stddev_over_time", 0, 0), "stdvar_over_time": ("stdvar_over_time", 0, 0),
    "mad_over_time": ("mad_over_time", 0, 0), "quantile_over_time": ("quantile_over_time", 1, 0),
    "predict_linear": ("predict_linear", 0, 1), "holt_winters": ("holt_winters", 0, 2),
    "double_exponential_smoothing": ("double_exponential_smoothing", 0, 2),
    "ts_of_max_over_time": ("ts_of_max_over_time", 0, 0), "ts_of_min_over_time": ("ts_of_min_over_time", 0, 0),
    "ts_of_last_over_time": ("ts_of_last_over_time", 0, 0),
}
# functions whose values add up over consecutive windows (a SQL group spanning several buckets)
ADDITIVE = {"increase", "count_over_time", "sum_over_time", "changes", "resets"}
MATH1 = {exp.Abs: "abs", exp.Ceil: "ceil", exp.Floor: "floor", exp.Exp: "exp", exp.Ln: "ln",
         exp.Sqrt: "sqrt", exp.Sign: "sgn"}
# aggregate -> operator name used below
AGG_OPS: dict[type, str] = {exp.Sum: "sum", exp.Avg: "avg", exp.Min: "min", exp.Max: "max", exp.Count: "count",
                            exp.StddevPop: "stddev_pop", exp.VariancePop: "var_pop", exp.Stddev: "stddev_samp",
                            exp.StddevSamp: "stddev_samp", exp.Variance: "var_samp", exp.Median: "median",
                            exp.PercentileCont: "quantile", exp.ApproxQuantile: "quantile"}
EXTRACT_GRAIN = {"second": "second", "minute": "minute", "hour": "hour", "day": "day", "dow": "day",
                 "isodow": "day", "doy": "day", "dayofweek": "day", "dayofmonth": "day", "dayofyear": "day",
                 "week": "day", "isoweek": "day", "yearweek": "day", "weekday": "day", "month": "month",
                 "quarter": "month", "year": "month", "isoyear": "day", "decade": "month", "century": "month"}
UNIT_ORDER = ["second", "minute", "hour", "day", "week", "month", "quarter", "year"]
UNIT_ALIASES = {"seconds": "second", "minutes": "minute", "hours": "hour", "days": "day", "weeks": "week",
                "months": "month", "quarters": "quarter", "years": "year", "isoweek": "week"}
INTERVAL_MS = {"millisecond": 1, "milliseconds": 1, "second": SECOND, "seconds": SECOND, "minute": MINUTE,
               "minutes": MINUTE, "hour": HOUR, "hours": HOUR, "day": DAY, "days": DAY, "week": 7 * DAY,
               "weeks": 7 * DAY}


# --------------------------------------------------------------------------- #
# settings and plan objects
# --------------------------------------------------------------------------- #
@dataclass
class Settings:
    zone: Zone
    now_ms: int
    default_range_ms: int = DAY
    max_points: int = 2_000_000            # rows a scan may return (buckets x groups)
    max_samples: int = 1_000_000           # raw samples a row query may read
    scrape_interval_ms: int = 15_000       # $__rate_interval of promql()
    allow_promql: bool = True
    schema_window_ms: int | None = None    # where label values are looked up


@dataclass(frozen=True)
class FnDesc:
    """Per-series function of the samples of a window, with per-series math around it."""

    func: str
    lead: tuple = ()
    trail: tuple = ()
    range_ms: int | None = None
    math: tuple = ()                       # innermost first: ("call", "abs") ("bin", "*", 8.0, False) ...

    def text(self, rng: str) -> str:
        args = [_num(a) for a in self.lead] + [rng] + [_num(a) for a in self.trail]
        s = f"{self.func}({', '.join(args)})"
        for m in self.math:
            s = _apply_math(m, s)
        return s


def _num(v: float) -> str:
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e15:
        return str(int(v))
    return repr(float(v))


def _apply_math(m: tuple, s: str) -> str:
    kind = m[0]
    if kind == "call":
        return f"{m[1]}({s})"
    if kind == "call2":                     # clamp_min(v, c), round(v, c)
        return f"{m[1]}({s}, {_num(m[2])})"
    if kind == "clamp":
        return f"clamp({s}, {_num(m[1])}, {_num(m[2])})"
    if kind == "bin":                       # ("bin", op, const, const_on_left)
        return f"({_num(m[2])} {m[1]} {s})" if m[3] else f"({s} {m[1]} {_num(m[2])})"
    if kind == "neg":
        return f"(-{s})"
    raise ValueError(m)


@dataclass(frozen=True)
class Atom:
    """One pushed aggregate: a PromQL expression evaluated per (bucket, group)."""

    kind: str                      # raw_count raw_sum raw_min raw_max raw_sumsq fn fn_sq distinct hq exists
    fn: FnDesc | None = None
    outer: str | None = None       # PromQL aggregation at the pushed level (None: per series)
    q: float | None = None         # quantile level (outer quantile / histogram_quantile)
    cond: tuple = ()               # extra matchers (FILTER / CASE WHEN), DNF as tuple of tuples
    label: str | None = None       # distinct: the label counted
    merge: str = "sum"             # how the two pieces of an autumn repeated-hour bucket combine


@dataclass
class AggScan:
    table: str
    metric: str
    alias: str
    cond: pq.Cond | None
    t0: int
    t1: int
    grain: Grain | None
    labels: list[str]               # pushed group labels (Prometheus names); mode C: all labels
    per_series: bool
    atoms: dict[Atom, str]          # atom -> column
    columns: list[tuple[str, str, str | None]]   # (SQL name, type, Prometheus label or None)
    mode: str
    notes: list[str] = field(default_factory=list)
    topk: tuple | None = None       # (k, "topk"|"bottomk", atom) for a single-atom ORDER BY .. LIMIT

    kind = "aggregation"


@dataclass
class RowScan:
    table: str
    metric: str
    alias: str
    cond: pq.Cond | None
    t0: int
    t1: int
    columns: list[tuple[str, str, str | None]]
    order: str | None               # "asc" | "desc" | None (any rows)
    limit: int | None               # samples needed (None: all, capped by max_samples)
    notes: list[str] = field(default_factory=list)

    kind = "samples"


@dataclass
class LabelScan:
    """Label values only (DISTINCT, dashboard filter lists): an index lookup over the time range
    (or the schema window without time filter), no samples read."""

    table: str
    metric: str
    alias: str
    cond: pq.Cond | None
    labels: list[str]
    columns: list[tuple[str, str, str | None]]
    notes: list[str] = field(default_factory=list)
    t0: int | None = None           # None: the schema window
    t1: int | None = None

    kind = "labels"


@dataclass
class PromqlScan:
    table: str
    alias: str
    expr: str
    t0: int
    t1: int
    grain: Grain | None
    step_ms: int | None
    columns: list[tuple[str, str, str | None]] = field(default_factory=list)   # filled by the executor
    notes: list[str] = field(default_factory=list)

    kind = "promql"


@dataclass
class Plan:
    statement: exp.Expression
    scans: list[Any]
    notes: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# SQL helpers
# --------------------------------------------------------------------------- #
def _strip(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _key(node: exp.Expression) -> str:
    node = _strip(node).copy()
    for col in node.find_all(exp.Column):
        col.set("table", None)
        col.set("db", None)
        col.set("catalog", None)
    return node.sql(dialect="duckdb", normalize=True)


def conjuncts(node: exp.Expression | None) -> list[exp.Expression]:
    if node is None:
        return []
    node = _strip(node)
    if isinstance(node, exp.And):
        return conjuncts(node.this) + conjuncts(node.expression)
    return [node]


def and_all(parts: list[exp.Expression]) -> exp.Expression | None:
    out = None
    for p in parts:
        out = p if out is None else exp.and_(out, p, copy=False)
    return out


def is_hq(node: exp.Expression) -> bool:
    return isinstance(node, exp.Anonymous) and node.name.lower() == "histogram_quantile"


def is_aggregate(node: exp.Expression) -> bool:
    return (isinstance(node, (exp.AggFunc, exp.Filter, exp.WithinGroup)) and not isinstance(node, exp.Window)) \
        or is_hq(node)


def aggs_in(node: exp.Expression) -> list[exp.Expression]:
    """Outermost aggregate calls (window functions skipped, their arguments searched)."""
    out: list[exp.Expression] = []

    def visit(n: exp.Expression) -> None:
        if isinstance(n, (exp.Subquery, exp.Select)):
            return
        if isinstance(n, exp.Window):
            if n.this is not None:
                for child in n.this.iter_expressions():
                    visit(child)
            return
        if is_aggregate(n):
            out.append(n)
            return
        for child in n.iter_expressions():
            visit(child)

    visit(node)
    return out


def contains_agg(node: exp.Expression) -> bool:
    return bool(aggs_in(node))


def is_constant(node: exp.Expression) -> bool:
    for n in node.walk():
        if isinstance(n, (exp.Column, exp.Subquery, exp.Select, exp.AggFunc, exp.Window, exp.Star,
                          exp.Placeholder, exp.Parameter)):
            return False
        if isinstance(n, exp.Anonymous) and n.name.lower() in ("random", "uuid", "gen_random_uuid"):
            return False
    return True


def _own_columns(select: exp.Select) -> list[exp.Column]:
    """Columns of this SELECT only (not of nested subqueries)."""
    out = []
    for col in select.find_all(exp.Column):
        if isinstance(col.this, exp.Star):
            continue
        if col.find_ancestor(exp.Select) is select:
            out.append(col)
    return out


def _lit(v: Any) -> exp.Expression:
    if v is None:
        return exp.Null()
    if isinstance(v, bool):
        return exp.Boolean(this=v)
    if isinstance(v, (int, float)):
        return exp.Literal.number(v)
    return exp.Literal.string(str(v))


def _col(name: str, table: str | None = None) -> exp.Column:
    return exp.column(name, table=table, quoted=True)


def _fn(name: str, *args: exp.Expression) -> exp.Expression:
    return exp.Anonymous(this=name, expressions=list(args))


# --------------------------------------------------------------------------- #
# aggregate description
# --------------------------------------------------------------------------- #
@dataclass
class AggDesc:
    node: exp.Expression
    op: str                               # sum avg min max count stddev_pop ... quantile median
    src: str                              # raw | fn | label | star | const | distinct | hq
    q: float | None = None
    lin: tuple = (1.0, 0.0)               # raw: a * value + b
    fn: FnDesc | None = None
    label: str | None = None
    cond: tuple = ()                      # extra matcher DNF (FILTER / CASE)
    coalesce0: bool = False


class Ctx:
    """Columns of one metric table in one SELECT."""

    def __init__(self, meta: MetricMeta, alias: str) -> None:
        self.meta = meta
        self.alias = alias

    def column(self, col: exp.Column):
        if col.table and col.table not in (self.alias, self.meta.name):
            return None
        return self.meta.columns.get(col.name)

    def role(self, node: exp.Expression) -> str | None:
        node = _strip(node)
        if isinstance(node, exp.Column):
            c = self.column(node)
            return c.role if c else None
        return None

    def label_of(self, node: exp.Expression) -> str | None:
        node = _strip(node)
        if isinstance(node, exp.Column):
            c = self.column(node)
            if c and c.role == "label":
                return c.label
        return None


# --------------------------------------------------------------------------- #
# planner
# --------------------------------------------------------------------------- #
class Planner:
    def __init__(self, lookup: Callable[[str], MetricMeta | None], settings: Settings,
                 const_eval: Callable[[exp.Expression], Any]) -> None:
        self.lookup = lookup
        self.s = settings
        self.const_eval = const_eval
        self.n = 0
        self.scans: list[Any] = []
        self.notes: list[str] = []

    # ------------------------------------------------------------------ #
    def plan(self, stmt: exp.Expression) -> Plan:
        self.ctes = {c.alias_or_name for c in stmt.find_all(exp.CTE)}
        self._flatten(stmt)
        # innermost SELECTs first: an outer query then reads the (registered) scans
        selects = list(stmt.find_all(exp.Select))
        selects.sort(key=lambda s: -_depth(s))
        for select in selects:
            target = self._source(select)
            if target is None:
                continue
            kind, table, meta = target
            if kind == "promql":
                self._plan_promql(select, table)
            elif self._is_aggregate_query(select):
                self._plan_agg(select, table, meta)
            else:
                self._plan_rows(select, table, meta)
        for t in stmt.find_all(exp.Table):
            if isinstance(t.this, exp.Identifier) and not t.name.startswith(SCAN_PREFIX) and \
                    t.name not in self.ctes and self._meta(t) is not None and not t.args.get("db"):
                raise PushdownError(f'"{t.name}" is used in a way promagg cannot compute (joined directly '
                                    "with another table?). Aggregate each metric in a subquery and join the "
                                    "subqueries, or use promql('a / b') for arithmetic between metrics.")
        return Plan(stmt, self.scans, self.notes)

    def _meta(self, table: exp.Table) -> MetricMeta | None:
        if not isinstance(table.this, exp.Identifier):
            return None
        db = table.args.get("db")
        if db is not None and db.name.lower() not in ("", "default", "metrics", "promagg", "prometheus", "mimir"):
            return None
        if table.name in getattr(self, "ctes", ()):
            return None
        return self.lookup(table.name)

    def _source(self, select: exp.Select):
        from_ = select.args.get(FROM_KEY)
        if from_ is None or not isinstance(from_.this, exp.Table):
            if select.args.get("joins"):
                for j in select.args["joins"]:
                    if isinstance(j.this, exp.Table) and self._meta(j.this) is not None:
                        raise PushdownError(_JOIN_MSG)
            return None
        table = from_.this
        if isinstance(table.this, exp.Anonymous) and table.this.name.lower() == "promql":
            if select.args.get("joins"):
                raise PushdownError(_JOIN_MSG)
            return "promql", table, None
        meta = self._meta(table)
        if meta is None:
            for j in select.args.get("joins") or []:
                if isinstance(j.this, exp.Table) and self._meta(j.this) is not None:
                    raise PushdownError(_JOIN_MSG)
            return None
        if select.args.get("joins"):
            raise PushdownError(_JOIN_MSG)
        return "metric", table, meta

    def _new_table(self) -> str:
        self.n += 1
        return f"{SCAN_PREFIX}{self.n}"

    def _is_aggregate_query(self, select: exp.Select) -> bool:
        if select.args.get("group") or select.args.get("distinct"):
            return True
        for e in select.expressions + [select.args.get("having")] + ([select.args["order"]] if select.args.get("order") else []):
            if e is not None and contains_agg(e):
                return True
        return False

    # ------------------------------------------------------------------ #
    # derived tables: SELECT .. FROM (SELECT * FROM metric WHERE ..) t GROUP BY ..
    # ------------------------------------------------------------------ #
    def _flatten(self, stmt: exp.Expression) -> None:
        changed = True
        while changed:
            changed = False
            for outer in list(stmt.find_all(exp.Select)):
                from_ = outer.args.get(FROM_KEY)
                if from_ is None or outer.args.get("joins") or outer.args.get("laterals"):
                    continue
                sub = from_.this
                if not isinstance(sub, exp.Subquery) or not isinstance(sub.this, exp.Select):
                    continue
                inner = sub.this
                if not self._simple_inner(inner):
                    continue
                if _merge(outer, sub, inner):
                    changed = True
                    break

    def _simple_inner(self, inner: exp.Select) -> bool:
        from_ = inner.args.get(FROM_KEY)
        if from_ is None or not isinstance(from_.this, exp.Table):
            return False
        t = from_.this
        is_promql = isinstance(t.this, exp.Anonymous) and t.this.name.lower() == "promql"
        if not is_promql and self._meta(t) is None:
            return False
        for arg in ("joins", "laterals", "group", "having", "limit", "offset", "distinct", "qualify",
                    "with", "windows", "order"):
            if inner.args.get(arg):
                return False
        return not any(contains_agg(e) or e.find(exp.Window) for e in inner.expressions)

    # ------------------------------------------------------------------ #
    # WHERE: time range, label conditions, residual
    # ------------------------------------------------------------------ #
    def _split_where(self, select: exp.Select, ctx: Ctx):
        t0, t1 = None, None
        conds: list[pq.Cond] = []
        residual: list[exp.Expression] = []
        for c in conjuncts(select.args["where"].this if select.args.get("where") else None):
            bound = self._time_bound(c, ctx)
            if bound is not None:
                a, b = bound
                t0 = a if t0 is None or (a is not None and a > t0) else t0
                t1 = b if t1 is None or (b is not None and b < t1) else t1
                continue
            lc = self._label_cond(c, ctx)
            if lc is not None:
                conds.append(lc)
                continue
            if is_constant(c):
                v = self.const_eval(c)
                if v is True:
                    continue
                conds.append(pq.Const(False))
                continue
            residual.append(c)
        self._had_time = t0 is not None or t1 is not None
        if t1 is None:
            t1 = self.s.now_ms
        if t0 is None:
            t0 = t1 - self.s.default_range_ms
            self._default_note = (f"no time filter: the last {self.s.default_range_ms // HOUR} h are used "
                                  "(connection option default_range)")
        else:
            self._default_note = None
        cond = pq.And(conds) if len(conds) > 1 else (conds[0] if conds else None)
        return t0, t1, cond, residual

    def _ts_micro_exact(self, node: exp.Expression) -> tuple[int, int] | None:
        """(ms floor, sub-ms remainder in microseconds) of a constant timestamp."""
        if not is_constant(node):
            return None
        v = self.const_eval(node)
        if isinstance(v, str):
            try:
                v = dt.datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
            except ValueError:
                raise ProgrammingError(f"not a timestamp: {v!r}") from None
        if isinstance(v, dt.datetime):
            if v.tzinfo is not None:
                us = (v - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)) // dt.timedelta(microseconds=1)
                return us // 1000, us % 1000
            ms = self.s.zone.utc_ms(v.replace(microsecond=0)) + v.microsecond // 1000
            return ms, v.microsecond % 1000
        if isinstance(v, dt.date):
            return self.s.zone.utc_ms(dt.datetime(v.year, v.month, v.day)), 0
        return None

    def _time_bound(self, c: exp.Expression, ctx: Ctx) -> tuple[int | None, int | None] | None:
        """ts <op> constant (or CAST(ts AS DATE) <op> date) -> [t0, t1) in UTC ms."""
        c = _strip(c)
        if isinstance(c, exp.Between):
            if ctx.role(c.this) != "ts" and not self._is_ts_date(c.this, ctx):
                return None
            lo = self._time_bound(exp.GTE(this=c.this.copy(), expression=c.args["low"].copy()), ctx)
            hi = self._time_bound(exp.LTE(this=c.this.copy(), expression=c.args["high"].copy()), ctx)
            if lo is None or hi is None:
                return None
            return lo[0], hi[1]
        ops = {exp.GTE: ">=", exp.GT: ">", exp.LTE: "<=", exp.LT: "<", exp.EQ: "="}
        op = ops.get(type(c))
        if op is None:
            return None
        left, right = _strip(c.this), _strip(c.expression)
        if ctx.role(right) == "ts" or self._is_ts_date(right, ctx):
            left, right = right, left
            op = {">=": "<=", ">": "<", "<=": ">=", "<": ">", "=": "="}[op]
        is_date = self._is_ts_date(left, ctx)
        if ctx.role(left) != "ts" and not is_date:
            return None
        v = self._ts_micro_exact(right)
        if v is None:
            return None
        ms, sub = v
        if is_date:                       # CAST(ts AS DATE) <op> d: whole local days
            day = self.const_eval(right)
            if isinstance(day, dt.datetime):
                day = day.date()
            elif isinstance(day, str):
                day = dt.date.fromisoformat(day[:10])
            start = self.s.zone.utc_ms(dt.datetime(day.year, day.month, day.day))
            nxt = self.s.zone.utc_ms(dt.datetime(day.year, day.month, day.day) + dt.timedelta(days=1))
            return {">=": (start, None), ">": (nxt, None), "<": (None, start), "<=": (None, nxt),
                    "=": (start, nxt)}[op]
        # samples have millisecond times: ts > x  <=>  ts >= floor(x) + 1ms
        if op == ">=":
            return (ms + (1 if sub else 0), None)
        if op == ">":
            return (ms + 1, None)
        if op == "<":
            return (None, ms + (1 if sub else 0))
        if op == "<=":
            return (None, ms + 1)
        return (ms, ms + 1) if not sub else (ms + 1, ms + 1)

    def _is_ts_date(self, node: exp.Expression, ctx: Ctx) -> bool:
        node = _strip(node)
        return isinstance(node, exp.Cast) and node.to.this == exp.DataType.Type.DATE and ctx.role(node.this) == "ts"

    def _label_cond(self, c: exp.Expression, ctx: Ctx) -> pq.Cond | None:
        """A condition over labels that PromQL matchers express exactly, else None."""
        c = _strip(c)
        if isinstance(c, (exp.And, exp.Or)):
            a, b = self._label_cond(c.this, ctx), self._label_cond(c.expression, ctx)
            if a is None or b is None:
                return None
            return pq.And([a, b]) if isinstance(c, exp.And) else pq.Or([a, b])
        if isinstance(c, exp.Not):
            inner = self._label_cond(c.this, ctx)
            if inner is None:
                return None
            try:
                return inner.negate()
            except ValueError:
                return None
        if isinstance(c, exp.Boolean):
            return pq.Const(bool(c.this))
        if isinstance(c, (exp.EQ, exp.NEQ)):
            left, right = _strip(c.this), _strip(c.expression)
            if ctx.label_of(right) and is_constant(left):
                left, right = right, left
            label = ctx.label_of(left)
            if label is None or not is_constant(right):
                return None
            v = self.const_eval(right)
            if v is not None and not isinstance(v, str):
                v = _str_value(v)
            return pq.eq(label, v) if isinstance(c, exp.EQ) else pq.ne(label, v)
        if isinstance(c, exp.In):
            label = ctx.label_of(c.this)
            if label is None or c.args.get("query") is not None:
                return None
            vals = []
            for e in c.expressions:
                if not is_constant(e):
                    return None
                v = self.const_eval(e)
                vals.append(v if v is None or isinstance(v, str) else _str_value(v))
            return pq.in_(label, vals)
        if isinstance(c, exp.Is):
            label = ctx.label_of(c.this)
            if label is None or not isinstance(c.expression, exp.Null):
                return None
            return pq.is_null(label)
        if isinstance(c, (exp.Like, exp.ILike)):
            label = ctx.label_of(c.this)
            pat = c.expression
            escape = None
            if isinstance(pat, exp.Escape):
                escape = self.const_eval(pat.expression)
                pat = pat.this
            if label is None or not is_constant(pat):
                return None
            p = self.const_eval(pat)
            if p is None:
                return pq.Never()
            rx = pq.like_regex(str(p), escape)
            if isinstance(c, exp.ILike):
                rx = "(?i)" + rx
            return pq.regex(label, rx)
        if isinstance(c, exp.Escape) and isinstance(c.this, (exp.Like, exp.ILike)):
            return None
        if isinstance(c, REGEXPS):
            label = ctx.label_of(c.this)
            if label is None or not is_constant(c.expression) or c.args.get("flag") is not None:
                return None
            rx = str(self.const_eval(c.expression))
            if isinstance(c, exp.RegexpLike):           # search anywhere (DuckDB regexp_matches)
                rx = f"(?s).*(?:{rx}).*"
            return pq.regex(label, rx)
        return None

    # ------------------------------------------------------------------ #
    # time buckets
    # ------------------------------------------------------------------ #
    def _interval_ms(self, node: exp.Expression) -> tuple[int, int] | None:
        """INTERVAL literal -> (ms, months)."""
        node = _strip(node)
        if not isinstance(node, exp.Interval):
            if is_constant(node):
                v = self.const_eval(node)
                if isinstance(v, dt.timedelta):
                    return int(v // dt.timedelta(milliseconds=1)), 0
            return None
        unit = (node.args.get("unit").name if node.args.get("unit") is not None else "").lower()
        raw = node.this.name if node.this is not None else ""
        if not unit:                                 # INTERVAL '5 minutes'
            m = re.fullmatch(r"\s*([\d.]+)\s*([a-z]+)\s*", raw.lower())
            if not m:
                return None
            raw, unit = m.group(1), m.group(2)
        try:
            n = float(raw)
        except ValueError:
            return None
        if unit in ("month", "months", "mon", "mons"):
            return 0, int(n)
        if unit in ("year", "years"):
            return 0, int(n * 12)
        if unit in ("quarter", "quarters"):
            return 0, int(n * 3)
        size = INTERVAL_MS.get(unit)
        return (int(round(n * size)), 0) if size else None

    def grain_of(self, node: exp.Expression, ctx: Ctx) -> Grain | None:
        """Recognized time bucket expression over ts -> Grain."""
        node = _strip(node)
        shift_out = dt.timedelta(0)
        if isinstance(node, (exp.Add, exp.Sub)) and self._interval_ms(node.expression) and \
                not self._interval_ms(node.expression)[1]:
            ms = self._interval_ms(node.expression)[0]
            shift_out = dt.timedelta(milliseconds=ms if isinstance(node, exp.Add) else -ms)
            node = _strip(node.this)
        if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
            unit = node.args.get("unit")
            unit = (unit.name if unit is not None else "").lower().strip("'")
            unit = UNIT_ALIASES.get(unit, unit)
            if unit not in UNIT_ORDER:
                return None
            arg = _strip(node.this)
            shift_in = dt.timedelta(0)
            if isinstance(arg, (exp.Add, exp.Sub)) and self._interval_ms(arg.expression) and \
                    not self._interval_ms(arg.expression)[1]:
                ms = self._interval_ms(arg.expression)[0]
                shift_in = dt.timedelta(milliseconds=ms if isinstance(arg, exp.Add) else -ms)
                arg = _strip(arg.this)
            if ctx.role(arg) != "ts":
                return None
            return Grain(unit, shift_in=shift_in, shift_out=shift_out)
        if isinstance(node, exp.DateBin):                        # TIME_BUCKET(width, ts [, origin])
            width = self._interval_ms(node.this)
            if width is None or ctx.role(node.expression) != "ts" or shift_out:
                return None
            origin_node = node.args.get("unit")
            if width[1]:
                origin = DUCKDB_MONTH_ORIGIN
            else:
                origin = DUCKDB_ORIGIN
            if origin_node is not None:
                v = self.const_eval(origin_node)
                if isinstance(v, str):
                    v = dt.datetime.fromisoformat(v)
                if not isinstance(v, dt.datetime):
                    return None
                origin = v.replace(tzinfo=None)
            if width[1]:
                return Grain("months", months=width[1], origin=origin)
            if width[0] <= 0:
                return None
            return Grain("fixed", width_ms=width[0], origin=origin)
        if isinstance(node, exp.Cast) and node.to.this == exp.DataType.Type.DATE and ctx.role(node.this) == "ts" \
                and not shift_out:
            return Grain("day", as_date=True)
        return None

    def _required_grain(self, node: exp.Expression, ctx: Ctx) -> Grain | None:
        """ts expression constant within buckets of the returned grain (EXTRACT(hour FROM ts)...)."""
        node = _strip(node)
        g = self.grain_of(node, ctx)
        if g is not None:
            return g
        unit = None
        arg = None
        if isinstance(node, exp.Extract):
            unit, arg = node.this.name.lower(), node.expression
        elif isinstance(node, exp.Anonymous) and node.name.lower() in ("date_part", "datepart") and len(node.expressions) == 2:
            u = node.expressions[0]
            unit = (self.const_eval(u) if is_constant(u) else "")
            unit, arg = str(unit).lower(), node.expressions[1]
        elif isinstance(node, TIME_PARTS):
            unit, arg = TIME_PART_UNITS[type(node)], node.this
        elif isinstance(node, exp.Anonymous) and node.name.lower() in ("hour", "minute", "second") \
                and len(node.expressions) == 1:
            unit, arg = node.name.lower(), node.expressions[0]           # sqlglot 26 / 27
        if unit is None or arg is None:
            return None
        g_arg = self.grain_of(arg, ctx)
        if ctx.role(arg) != "ts" and g_arg is None:
            return None
        grain = EXTRACT_GRAIN.get(unit)
        if grain is None:
            return None
        return Grain(grain) if g_arg is None else g_arg

    # ------------------------------------------------------------------ #
    # aggregate queries
    # ------------------------------------------------------------------ #
    def _group_exprs(self, select: exp.Select) -> list[exp.Expression]:
        group = select.args.get("group")
        exprs = list(group.expressions) if group is not None else []
        if select.args.get("distinct") and not exprs:
            exprs = [e.this if isinstance(e, exp.Alias) else e for e in select.expressions]
        out = []
        aliases = {e.alias: e.this for e in select.expressions if isinstance(e, exp.Alias)}
        for g in exprs:
            g = _strip(g)
            if isinstance(g, exp.Literal) and not g.is_string:
                idx = int(g.this) - 1
                if not 0 <= idx < len(select.expressions):
                    raise ProgrammingError(f"GROUP BY position {idx + 1} is not in the select list")
                e = select.expressions[idx]
                out.append(_strip(e.this if isinstance(e, exp.Alias) else e))
            elif isinstance(g, exp.Column) and not g.table and g.name in aliases and \
                    g.name not in getattr(self, "_cur_columns", {}):
                out.append(_strip(aliases[g.name]))
            else:
                out.append(g)
        return out

    def _plan_extent(self, select: exp.Select, meta: MetricMeta, ctx: Ctx, alias: str) -> bool:
        """SELECT MIN(ts), MAX(ts) FROM m [WHERE labels / time]: time of the first / last sample,
        read from the start / the end in growing windows (no GROUP BY)."""
        if select.args.get("group") or select.args.get("distinct") or select.args.get("having"):
            return False
        nodes = []
        for part in select.expressions:
            nodes.extend(aggs_in(part))
        if not nodes or any(not isinstance(n, (exp.Min, exp.Max)) or ctx.role(n.this) != "ts" for n in nodes):
            return False
        t0, t1, cond, residual = self._split_where(select, ctx)
        if residual:
            return False
        if not self._had_time and self.s.schema_window_ms:
            t0 = t1 - self.s.schema_window_ms
        cols = [("ts", "TIMESTAMP", None)] + [(_label_col(lb, meta.columns), "VARCHAR", lb) for lb in meta.labels] + \
            [("value", "DOUBLE", None)]
        for n in nodes:
            name = self._new_table()
            self.scans.append(RowScan(name, meta.name, alias, cond, t0, t1, cols,
                                      "desc" if isinstance(n, exp.Max) else "asc", 1,
                                      ["first / last sample time, read from the edge of the range"]))
            sub = exp.select(type(n)(this=_col("ts"))).from_(exp.Table(this=exp.to_identifier(name, quoted=True)))
            n.replace(exp.Subquery(this=sub))
        select.set(FROM_KEY, None)
        select.set("where", None)
        return True

    def _plan_agg(self, select: exp.Select, table: exp.Table, meta: MetricMeta) -> None:
        alias = table.alias_or_name
        ctx = Ctx(meta, alias)
        self._cur_columns = meta.columns
        self._check_columns(select, ctx)
        if self._plan_extent(select, meta, ctx, alias):
            return
        t0, t1, cond, residual = self._split_where(select, ctx)
        notes: list[str] = []

        # ---- group keys -------------------------------------------------
        group_labels: list[str] = []
        extra_labels: set[str] = set()
        bucket_grains: list[Grain] = []
        required: list[Grain] = []
        for g in self._group_exprs(select):
            if is_constant(g):
                continue
            lb = ctx.label_of(g)
            if lb is not None:
                if lb not in group_labels:
                    group_labels.append(lb)
                continue
            gr = self.grain_of(g, ctx)
            if gr is not None:
                bucket_grains.append(gr)
                continue
            used = {ctx.column(c).role if ctx.column(c) else None for c in g.find_all(exp.Column)}
            if "value" in used or "rate" in used or "increase" in used:
                raise PushdownError(f"GROUP BY {g.sql(dialect='duckdb')}: grouping by sample values is not "
                                    "possible inside Prometheus / Mimir (group by labels and time).")
            if "ts" in used:
                if ctx.role(g) == "ts":
                    # the raw sample time (Superset's "original value" grain, MCP compile checks):
                    # samples are not grouped one by one; automatic buckets of about 500 points
                    auto = _auto_grain(t1 - t0)
                    bucket_grains.append(auto)
                    notes.append(f"GROUP BY ts: automatic time buckets of {auto.width_ms // 1000} s "
                                 "(choose a time grain for other buckets)")
                    continue
                rg = self._required_grain(g, ctx)
                if rg is None:
                    raise PushdownError(f"GROUP BY {g.sql(dialect='duckdb')}: group by a time bucket "
                                        "(DATE_TRUNC('hour', ts), TIME_BUCKET(INTERVAL '5 minutes', ts), ...), "
                                        "not by an expression of the raw sample time.")
                required.append(rg)
            for c in g.find_all(exp.Column):
                lb2 = ctx.label_of(c)
                if lb2:
                    extra_labels.add(lb2)
        # ts used outside aggregates elsewhere (SELECT / HAVING / ORDER BY) must be bucketed
        raw_ts_grouped = any(g.unit == "fixed" and g in bucket_grains and any(
            ctx.role(k) == "ts" for k in self._group_exprs(select)) for g in bucket_grains)
        for part in self._outside_aggs(select):
            for c in part.find_all(exp.Column):
                col = ctx.column(c)
                if col is None or col.role != "ts" or _inside_agg(c, part):
                    continue
                top = _time_expr_top(c)
                if raw_ts_grouped and top is c:
                    continue
                if self.grain_of(top, ctx) is None and self._required_grain(top, ctx) is None:
                    raise PushdownError("the raw sample time (ts) can only be used through a time bucket "
                                        "(DATE_TRUNC, TIME_BUCKET) or EXTRACT in an aggregate query.")
        # residual WHERE: label expressions (DuckDB), time expressions (finer buckets)
        for r in residual:
            roles = {ctx.column(c).role for c in r.find_all(exp.Column) if ctx.column(c)}
            if roles & {"value", "rate", "increase"}:
                raise PushdownError(f"WHERE {r.sql(dialect='duckdb')}: conditions on sample values cannot run "
                                    "inside Prometheus / Mimir in an aggregate query; filter aggregates with "
                                    "HAVING, or use a promql() dataset (e.g. m > 90).")
            if "ts" in roles:
                for c in r.find_all(exp.Column):
                    if ctx.role(c) == "ts":
                        top = _time_expr_top(c)
                        rg = self._required_grain(top, ctx)
                        if rg is None:
                            raise PushdownError(f"WHERE {r.sql(dialect='duckdb')}: time conditions must compare "
                                                "ts with constants, or use EXTRACT / DATE_TRUNC.")
                        required.append(rg)
            for c in r.find_all(exp.Column):
                lb2 = ctx.label_of(c)
                if lb2:
                    extra_labels.add(lb2)

        # a long range without time bucket: decomposable aggregates are computed per day and added
        # up by DuckDB (the same result; the backend splits range queries by day, and a single
        # evaluation over weeks of many series can exhaust its memory)
        if not bucket_grains and not required and t1 - t0 > SPLIT_WHOLE_RANGE_MS:
            probe = []
            for part in [*select.expressions, select.args.get("having"), select.args.get("order")]:
                if part is not None:
                    probe.extend(aggs_in(part))
            if probe and all(self._describe_agg(n.copy(), ctx).src in ("raw", "star", "distinct", "label")
                             for n in probe):
                required.append(Grain("day"))
        grain, fine = self._pushed_grain(bucket_grains, required)
        if grain is None and not bucket_grains and not required and not residual and not extra_labels and \
                group_labels and not any(aggs_in(p) for p in [*select.expressions, select.args.get("having"),
                                                              select.args.get("order")] if p is not None):
            # label values only (dashboard filter lists, DISTINCT): the label index of the time
            # range, like Grafana variables; no samples read
            name = self._new_table()
            cols = [("ts", "TIMESTAMP", None)] + [(_label_col(lb, meta.columns), "VARCHAR", lb) for lb in group_labels]
            span = (t0, t1) if self._had_time else (None, None)
            self.scans.append(LabelScan(name, meta.name, alias, cond, list(group_labels), cols,
                                        ["label values from the index of the time range, no samples read"],
                                        *span))
            _replace_from(select, name, alias)
            select.set("where", None)
            return
        if self._default_note:
            self.notes.append(self._default_note)

        # ---- aggregates -------------------------------------------------
        agg_nodes = []
        for part in [*select.expressions, select.args.get("having"), select.args.get("order")]:
            if part is not None:
                agg_nodes.extend(aggs_in(part))
        descs = [self._describe_agg(n, ctx) for n in agg_nodes]
        for d in descs:
            if d.src in ("distinct", "label"):
                extra_labels.add(d.label)
        extra_labels -= set(group_labels)

        # ---- mode ---------------------------------------------------------
        need_partial = bool(extra_labels) or fine
        mode = "A"
        if need_partial:
            mode = "B" if all(self._decomposable(d, fine) for d in descs) else "C"
            if mode == "C":
                for d in descs:
                    if d.src == "hq":
                        raise PushdownError("HISTOGRAM_QUANTILE needs a plain GROUP BY on labels and one time bucket "
                                            "(it cannot be combined with label expressions or several buckets).")
                    if fine and d.src == "fn" and not self._decomposable(d, fine):
                        raise PushdownError(
                            f"{d.node.sql(dialect='duckdb')}: a function over time cannot be combined across several "
                            "time buckets of one group (hour-of-day, WHERE on EXTRACT...). Group by one time bucket, "
                            "or use SUM(INCREASE(value)) / MAX(MAX_OVER_TIME(value)) which add up across buckets.")
        if mode == "A":
            push_labels = list(group_labels)
        elif mode == "B":
            push_labels = list(group_labels) + sorted(extra_labels)
        else:
            push_labels = list(meta.labels)

        scan_name = self._new_table()
        atoms: dict[Atom, str] = {}

        def atom(a: Atom) -> exp.Expression:
            if a not in atoms:
                atoms[a] = f"__a{len(atoms)}"
            return _col(atoms[a], alias)

        repl: dict[int, exp.Expression] = {}
        for d in descs:
            repl[id(d.node)] = self._recombine(d, mode, atom)
        if not atoms:                          # GROUP BY without aggregates: rows must exist
            atom(Atom("raw_count", outer="sum"))

        # ---- rewrite --------------------------------------------------------
        for d in descs:
            new = repl[id(d.node)]
            if d.node is select:
                continue
            d.node.replace(new)
        cols = self._scan_columns(meta, push_labels, grain, atoms)
        scan = AggScan(scan_name, meta.name, alias, cond, t0, t1, grain, push_labels, mode == "C",
                       atoms, cols, mode, notes)
        if mode == "A" and grain is None:
            scan.topk = self._topk(select, descs, atoms, repl)
        self.scans.append(scan)
        _replace_from(select, scan_name, alias)
        select.set("where", exp.Where(this=and_all(residual)) if residual else None)

    def _outside_aggs(self, select: exp.Select) -> list[exp.Expression]:
        parts = list(select.expressions)
        for k in ("having", "order"):
            if select.args.get(k) is not None:
                parts.append(select.args[k])
        return parts

    def _pushed_grain(self, buckets: list[Grain], required: list[Grain]) -> tuple[Grain | None, bool]:
        distinct = []
        for g in buckets:
            if g not in distinct:
                distinct.append(g)
        if not required and len(distinct) <= 1:
            return (distinct[0] if distinct else None), False
        cands = distinct + [r for r in required if r not in distinct]
        cands.sort(key=lambda g: g.nominal_ms)
        options = [cands[0]] + [Grain(u) for u in ("day", "hour", "minute", "second")
                                if Grain(u).nominal_ms <= cands[0].nominal_ms]
        for g in options:
            if all(_divides(g, other) for other in cands):
                fine = not (len(distinct) >= 1 and g in distinct)
                return g, fine
        raise PushdownError("these time buckets cannot be computed together; use one time grain per query.")

    def _check_columns(self, select: exp.Select, ctx: Ctx) -> None:
        aliases = {e.alias for e in select.expressions if isinstance(e, exp.Alias)}
        for col in _own_columns(select):
            if ctx.column(col) is not None:
                continue
            if not col.table and col.name in aliases:
                continue
            known = ", ".join(ctx.meta.columns)
            if col.table and col.table not in (ctx.alias, ctx.meta.name):
                raise ProgrammingError(f'"{col.table}.{col.name}": "{col.table}" is not a table of this query '
                                       f'(metric "{ctx.meta.name}", alias "{ctx.alias}"); correlated subqueries '
                                       "are not possible on metrics, aggregate in one query")
            raise ProgrammingError(f'column "{col.name}" does not exist in metric "{ctx.meta.name}" (columns: {known})')

    # ------------------------------------------------------------------ #
    def _describe_agg(self, node: exp.Expression, ctx: Ctx) -> AggDesc:
        orig = node
        cond: tuple = ()
        if is_hq(node):
            args = node.expressions
            if len(args) != 2 or not is_constant(args[0]):
                raise ProgrammingError("HISTOGRAM_QUANTILE(q, SUM(RATE(value))) expects a constant level and SUM(...)")
            q = float(self.const_eval(args[0]))
            inner = _strip(args[1])
            if isinstance(inner, exp.Filter):
                cond = self._filter_cond(inner.expression, ctx)
                inner = inner.this
            if not isinstance(inner, exp.Sum):
                raise ProgrammingError("HISTOGRAM_QUANTILE(q, SUM(RATE(value))): the second argument must be SUM(...)")
            what = self._value_expr(inner.this, ctx)
            if what[0] != "fn":
                raise ProgrammingError("HISTOGRAM_QUANTILE needs a function over time of the buckets, e.g. "
                                       "HISTOGRAM_QUANTILE(0.95, SUM(RATE(value)))")
            if "le" not in ctx.meta.labels:
                raise ProgrammingError(f'HISTOGRAM_QUANTILE needs the "le" label ({ctx.meta.name} has none; use the '
                                       "_bucket metric of the histogram)")
            return AggDesc(orig, "hq", "hq", q=q, fn=what[1], cond=cond)
        if isinstance(node, exp.Filter):
            cond = self._filter_cond(node.expression, ctx)
            node = node.this
        q = None
        if isinstance(node, exp.WithinGroup):
            inner = node.this
            order = node.expression
            if not isinstance(inner, exp.PercentileCont) or not isinstance(order, exp.Order) or \
                    len(order.expressions) != 1:
                raise PushdownError(f"{orig.sql(dialect='duckdb')} is not supported")
            q = float(self.const_eval(inner.this))
            arg = order.expressions[0].this
            op = "quantile"
        else:
            op = AGG_OPS.get(type(node))
            if op is None:
                raise PushdownError(f"{orig.sql(dialect='duckdb')}: aggregate not supported on metrics "
                                    "(SUM, AVG, MIN, MAX, COUNT, STDDEV, VARIANCE, MEDIAN, QUANTILE_CONT)")
            arg = node.this
            if op == "quantile":
                lvl = node.args.get("expression") if isinstance(node, exp.PercentileCont) else node.args.get("quantile")
                if lvl is None or not is_constant(lvl):
                    raise ProgrammingError(f"{orig.sql(dialect='duckdb')}: the quantile level must be a constant")
                q = float(self.const_eval(lvl))
            if op == "median":
                op, q = "quantile", 0.5
        if op == "count":
            if isinstance(arg, exp.Star):
                return AggDesc(orig, "count", "star", cond=cond)
            if isinstance(arg, exp.Distinct):
                if len(arg.expressions) != 1 or ctx.label_of(arg.expressions[0]) is None:
                    raise PushdownError("COUNT(DISTINCT ...) is possible on one label only")
                return AggDesc(orig, "count", "distinct", label=ctx.label_of(arg.expressions[0]), cond=cond)
        # CASE WHEN <labels> THEN x [ELSE 0 | NULL] END  ==  x FILTER (WHERE <labels>)
        arg = _strip(arg)
        coalesce0 = False
        if isinstance(arg, exp.Case) and len(arg.args.get("ifs") or []) == 1 and not arg.this:
            when = arg.args["ifs"][0]
            default = arg.args.get("default")
            lc = self._label_cond(when.this, ctx)
            zero = default is not None and is_constant(default) and self.const_eval(default) == 0
            if lc is not None and (default is None or isinstance(default, exp.Null) or (zero and op in ("sum", "count"))):
                cond = _and_dnf(cond, _dnf(lc))
                arg = _strip(when.args["true"])
                coalesce0 = zero and op == "sum"
                if zero and op == "count":
                    raise PushdownError("COUNT(CASE WHEN .. THEN .. ELSE 0 END) counts every sample; use "
                                        "COUNT(CASE WHEN .. THEN 1 END) or SUM(CASE WHEN .. THEN 1 ELSE 0 END)")
        what = self._value_expr(arg, ctx)
        kind = what[0]
        if kind == "raw":
            if op == "quantile":
                raise PushdownError(f"{orig.sql(dialect='duckdb')}: quantiles of raw samples cannot be computed "
                                    "inside Prometheus / Mimir. Use QUANTILE_CONT(QUANTILE_OVER_TIME(0.95, value), 0.5), "
                                    "MAX(QUANTILE_OVER_TIME(0.95, value)) per series, or a histogram.")
            return AggDesc(orig, op, "raw", q=q, lin=what[1], cond=cond, coalesce0=coalesce0)
        if kind == "const":
            if op == "count":
                return AggDesc(orig, "count", "star", cond=cond)
            if op == "sum":
                return AggDesc(orig, "sum", "raw", lin=(0.0, float(what[1])), cond=cond, coalesce0=coalesce0)
            raise PushdownError(f"{orig.sql(dialect='duckdb')}: aggregate of a constant is not supported")
        if kind == "fn":
            return AggDesc(orig, op, "fn", q=q, fn=what[1], cond=cond, coalesce0=coalesce0)
        if kind == "label":
            if op not in ("min", "max", "count"):
                raise PushdownError(f"{orig.sql(dialect='duckdb')}: only MIN, MAX and COUNT apply to labels")
            return AggDesc(orig, op, "label", label=what[1], cond=cond)
        if kind == "ts":
            raise PushdownError(f"{orig.sql(dialect='duckdb')}: the first / last sample time is available alone "
                                "(SELECT MIN(ts), MAX(ts) FROM metric WHERE <labels>), not per group; per series "
                                "use TS_OF_LAST_OVER_TIME(value) (Mimir 3).")
        raise PushdownError(f"{orig.sql(dialect='duckdb')}: cannot be computed inside Prometheus / Mimir")

    def _filter_cond(self, where: exp.Expression, ctx: Ctx) -> tuple:
        w = where.this if isinstance(where, exp.Where) else where
        lc = self._label_cond(w, ctx)
        if lc is None:
            raise PushdownError(f"FILTER (WHERE {w.sql(dialect='duckdb')}): only conditions on labels are possible")
        return _dnf(lc)

    def _value_expr(self, node: exp.Expression, ctx: Ctx):
        """Classify an aggregate argument:
        ("raw", (a, b))  a * value + b      ("fn", FnDesc)   per-series function (+ math)
        ("label", name)  ("const", c)       ("ts", None)     ("other", None)"""
        node = _strip(node)
        role = ctx.role(node)
        if role == "value":
            return "raw", (1.0, 0.0)
        if role in ("rate", "increase"):
            return "fn", FnDesc(role)
        if role == "label":
            return "label", ctx.label_of(node)
        if role == "ts":
            return "ts", None
        if is_constant(node):
            v = self.const_eval(node)
            return ("const", v) if isinstance(v, (int, float)) else ("other", None)
        # range function call
        if isinstance(node, exp.Anonymous) and node.name.lower() in RANGE_FUNCS:
            return "fn", self._range_call(node, ctx)
        # math around a per-series function, or linear in value
        if isinstance(node, exp.Neg):
            inner = self._value_expr(node.this, ctx)
            if inner[0] == "raw":
                a, b = inner[1]
                return "raw", (-a, -b)
            if inner[0] == "fn":
                return "fn", _with_math(inner[1], ("neg",))
            return "other", None
        if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.Pow)):
            left, right = _strip(node.this), _strip(node.expression)
            lc, rc = is_constant(left), is_constant(right)
            if lc == rc:
                return "other", None
            const = float(self.const_eval(left if lc else right))
            inner = self._value_expr(right if lc else left, ctx)
            op = {exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/", exp.Mod: "%", exp.Pow: "^"}[type(node)]
            if inner[0] == "raw" and op in "+-*/":
                a, b = inner[1]
                if op == "+":
                    return "raw", (a, b + const)
                if op == "-":
                    return ("raw", (-a, const - b)) if lc else ("raw", (a, b - const))
                if op == "*":
                    return "raw", (a * const, b * const)
                if op == "/" and not lc and const != 0:
                    return "raw", (a / const, b / const)
                return "other", None
            if inner[0] == "fn":
                return "fn", _with_math(inner[1], ("bin", op, const, lc))
            return "other", None
        for cls, name in MATH1.items():
            if isinstance(node, cls):
                inner = self._value_expr(node.this, ctx)
                if inner[0] == "fn":
                    return "fn", _with_math(inner[1], ("call", name))
                return "other", None
        if isinstance(node, exp.Log):
            base = node.this if node.expression is not None else None
            arg = node.expression if node.expression is not None else node.this
            inner = self._value_expr(arg, ctx)
            if inner[0] != "fn":
                return "other", None
            b = float(self.const_eval(base)) if base is not None else 10.0   # DuckDB LOG(x) is base 10
            if b == 2:
                return "fn", _with_math(inner[1], ("call", "log2"))
            if b == 10:
                return "fn", _with_math(inner[1], ("call", "log10"))
            return "fn", _with_math(_with_math(inner[1], ("call", "ln")), ("bin", "/", math.log(b), False))
        if isinstance(node, exp.Round):
            inner = self._value_expr(node.this, ctx)
            if inner[0] != "fn":
                return "other", None
            dec = node.args.get("decimals")
            d = int(self.const_eval(dec)) if dec is not None else 0
            return "fn", _with_math(inner[1], ("call2", "round", 10.0 ** (-d)))
        if isinstance(node, (exp.Greatest, exp.Least)):
            args = [node.this] + list(node.expressions)
            consts = [a for a in args if is_constant(a)]
            others = [a for a in args if not is_constant(a)]
            if len(others) != 1 or not consts:
                return "other", None
            inner = self._value_expr(others[0], ctx)
            if inner[0] != "fn":
                return "other", None
            vals = [float(self.const_eval(c)) for c in consts]
            if isinstance(node, exp.Greatest):
                return "fn", _with_math(inner[1], ("call2", "clamp_min", max(vals)))
            return "fn", _with_math(inner[1], ("call2", "clamp_max", min(vals)))
        return "other", None

    def _range_call(self, node: exp.Anonymous, ctx: Ctx) -> FnDesc:
        name = node.name.lower()
        func, lead, trail = RANGE_FUNCS[name]
        args = list(node.expressions)
        usage = f"{name.upper()}(" + ", ".join(["q"] * lead + ["value"] + ["x"] * trail) + " [, '5m'])"
        if len(args) not in (lead + 1 + trail, lead + 2 + trail):
            raise ProgrammingError(f"{node.sql(dialect='duckdb')}: expected {usage}")
        vals = args[lead]
        if ctx.role(vals) != "value":
            raise ProgrammingError(f"{node.sql(dialect='duckdb')}: the argument must be the value column ({usage})")
        lead_v = tuple(float(self.const_eval(a)) for a in args[:lead])
        trail_v = tuple(float(self.const_eval(a)) for a in args[lead + 1: lead + 1 + trail])
        rng = None
        if len(args) == lead + 2 + trail:
            r = args[-1]
            iv = self._interval_ms(r)
            if iv is not None and not iv[1]:
                rng = iv[0]
            else:
                if not is_constant(r):
                    raise ProgrammingError(f"{node.sql(dialect='duckdb')}: the range must be a constant like '5m'")
                try:
                    rng = parse_duration(str(self.const_eval(r)))
                except ValueError as ex:
                    raise ProgrammingError(str(ex)) from None
        return FnDesc(func, lead_v, trail_v, rng)

    def _decomposable(self, d: AggDesc, fine: bool) -> bool:
        if d.src in ("raw", "star", "distinct", "label"):
            return True
        if d.src == "hq":
            return False
        fn = d.fn
        if d.op == "quantile":
            return False
        if not fine:
            return True
        if fn.math or fn.range_ms:
            return False
        if d.op == "sum" and fn.func in ADDITIVE:
            return True
        if d.op == "min" and fn.func in ("min_over_time",):
            return True
        if d.op == "max" and fn.func in ("max_over_time", "present_over_time"):
            return True
        return False

    # ------------------------------------------------------------------ #
    def _recombine(self, d: AggDesc, mode: str, atom: Callable[[Atom], exp.Expression]) -> exp.Expression:
        """SQL over the scan columns that equals the original aggregate."""
        cond = d.cond

        def raw(kind: str, outer: str) -> exp.Expression:
            return atom(Atom(kind, outer=None if mode == "C" else outer, cond=cond, merge=_merge_rule(kind)))

        def SUM(x):
            return exp.Sum(this=x)

        def COUNT0(x):
            return exp.Cast(this=exp.Coalesce(this=SUM(x), expressions=[_lit(0)]), to=exp.DataType.build("BIGINT"))

        def num(v):
            return _lit(v)

        if d.src == "star" or (d.src == "raw" and d.op == "count"):
            return COUNT0(raw("raw_count", "sum"))
        if d.src == "distinct":
            if mode == "A":
                return exp.Coalesce(this=exp.Min(this=atom(Atom("distinct", outer="count", label=d.label, cond=cond,
                                                                   merge="max"))), expressions=[_lit(0)])
            cnt = raw("raw_count", "sum")
            lab = _col(_label_col(d.label, self._cur_columns))
            return exp.Count(this=exp.Distinct(expressions=[exp.Case(
                ifs=[exp.If(this=exp.GT(this=cnt, expression=_lit(0)), true=lab)])]))
        if d.src == "label":
            cnt = raw("raw_count", "sum")
            lab = _col(_label_col(d.label, self._cur_columns))
            guarded = exp.Case(ifs=[exp.If(this=exp.GT(this=cnt, expression=_lit(0)), true=lab)])
            if d.op == "count":
                return exp.Coalesce(this=SUM(exp.Case(ifs=[exp.If(this=exp.Not(this=exp.Is(this=lab, expression=exp.Null())),
                                                                  true=cnt)], default=_lit(0))), expressions=[_lit(0)])
            return exp.Min(this=guarded) if d.op == "min" else exp.Max(this=guarded)
        if d.src == "raw":
            a, b = d.lin
            if d.op in ("min", "max"):
                use_max = (d.op == "max") == (a >= 0)
                m = exp.Max(this=raw("raw_max", "max")) if use_max else exp.Min(this=raw("raw_min", "min"))
                return m if (a, b) == (1.0, 0.0) else _p(exp.Add(this=exp.Mul(this=num(a), expression=m), expression=num(b)))
            S = SUM(raw("raw_sum", "sum")) if (d.op != "sum" or a != 0) else None
            C = SUM(raw("raw_count", "sum")) if (d.op != "sum" or b != 0) else None
            if d.op == "sum":
                if (a, b) == (1.0, 0.0):
                    out = S
                elif a == 0:
                    out = _p(exp.Mul(this=num(b), expression=C))
                elif b == 0:
                    out = _p(exp.Mul(this=num(a), expression=S))
                else:
                    out = _p(exp.Add(this=exp.Mul(this=num(a), expression=S), expression=exp.Mul(this=num(b), expression=C)))
                return exp.Coalesce(this=out, expressions=[_lit(0)]) if d.coalesce0 else out
            if d.op == "avg":
                avg = exp.Div(this=S, expression=exp.Nullif(this=C, expression=_lit(0)))
                return _p(avg) if (a, b) == (1.0, 0.0) else _p(exp.Add(this=exp.Mul(this=num(a), expression=_p(avg)),
                                                                     expression=num(b)))
            if d.op in ("stddev_pop", "var_pop", "stddev_samp", "var_samp"):
                Q = SUM(raw("raw_sumsq", "sum"))
                return _variance(S, Q, C, d.op, a)
            raise PushdownError(f"{d.node.sql(dialect='duckdb')} is not supported")
        # functions over time
        fn = d.fn
        if d.src == "hq":
            return exp.Min(this=atom(Atom("hq", fn=fn, outer="sum", q=d.q, cond=cond, merge="avg")))
        if mode == "C":
            v = atom(Atom("fn", fn=fn, outer=None, cond=cond, merge=_fn_merge(fn)))
            return _sql_agg(d.op, v, d.q, d.coalesce0)
        if mode == "A":
            single = {"sum": "sum", "avg": "avg", "min": "min", "max": "max", "count": "count",
                      "stddev_pop": "stddev", "var_pop": "stdvar", "quantile": "quantile"}
            if d.op in single:
                v = exp.Min(this=atom(Atom("fn", fn=fn, outer=single[d.op], q=d.q, cond=cond,
                                           merge=_outer_merge(single[d.op], fn))))
                if d.op == "count" or d.coalesce0:
                    return exp.Coalesce(this=v, expressions=[_lit(0)])
                return v
            # sample variance / stddev: from the population value and the count
            pop = exp.Min(this=atom(Atom("fn", fn=fn, outer="stdvar", cond=cond, merge="avg")))
            n = exp.Min(this=atom(Atom("fn", fn=fn, outer="count", cond=cond, merge="sum")))
            var = _p(exp.Div(this=_p(exp.Mul(this=pop, expression=n)),
                             expression=exp.Nullif(this=exp.Sub(this=n.copy(), expression=_lit(1)), expression=_lit(0))))
            return var if d.op == "var_samp" else _fn("SQRT", var)
        # mode B: decomposable partials at finer labels / buckets
        S = SUM(atom(Atom("fn", fn=fn, outer="sum", cond=cond, merge=_outer_merge("sum", fn))))
        if d.op == "sum":
            return exp.Coalesce(this=S, expressions=[_lit(0)]) if d.coalesce0 else S
        if d.op == "min":
            return exp.Min(this=atom(Atom("fn", fn=fn, outer="min", cond=cond, merge="min")))
        if d.op == "max":
            return exp.Max(this=atom(Atom("fn", fn=fn, outer="max", cond=cond, merge="max")))
        C = SUM(atom(Atom("fn", fn=fn, outer="count", cond=cond, merge="sum")))
        if d.op == "count":
            return exp.Coalesce(this=C, expressions=[_lit(0)])
        if d.op == "avg":
            return _p(exp.Div(this=S, expression=exp.Nullif(this=C, expression=_lit(0))))
        Q = SUM(atom(Atom("fn_sq", fn=fn, outer="sum", cond=cond, merge="sum")))
        return _variance(S, Q, C, d.op, 1.0)

    def _topk(self, select: exp.Select, descs, atoms, repl) -> tuple | None:
        """ORDER BY <one aggregate> DESC|ASC LIMIT k without time buckets: topk/bottomk in PromQL."""
        limit = select.args.get("limit")
        order = select.args.get("order")
        if limit is None or order is None or len(order.expressions) != 1 or len(atoms) != 1 or \
                select.args.get("offset") or select.args.get("having"):
            return None
        try:
            k = int(self.const_eval(limit.expression))
        except Exception:  # pylint: disable=broad-except
            return None
        (a,) = atoms
        if a.outer not in ("sum", "max", "min", "count") or a.kind not in ("raw_sum", "raw_count", "raw_max", "raw_min", "fn"):
            return None
        o = order.expressions[0]
        target = _strip(o.this)
        aliases = {e.alias: e.this for e in select.expressions if isinstance(e, exp.Alias)}
        if isinstance(target, exp.Column) and target.name in aliases:
            target = _strip(aliases[target.name])
        cols = list(target.find_all(exp.Column))
        if not cols or any(c.name != atoms[a] for c in cols) or not isinstance(target, (exp.Min, exp.Max, exp.Sum, exp.Coalesce)):
            return None
        return (k, "bottomk" if not o.args.get("desc") else "topk", a)

    def _scan_columns(self, meta: MetricMeta, labels: list[str], grain: Grain | None,
                      atoms: dict[Atom, str]) -> list[tuple[str, str, str | None]]:
        cols = [("ts", "TIMESTAMP", None)]          # local start of the bucket
        for lb in labels:
            cols.append((_label_col(lb, meta.columns), "VARCHAR", lb))
        for _a, name in atoms.items():
            cols.append((name, "DOUBLE", None))
        return cols

    # ------------------------------------------------------------------ #
    # raw samples
    # ------------------------------------------------------------------ #
    def _plan_rows(self, select: exp.Select, table: exp.Table, meta: MetricMeta) -> None:
        alias = table.alias_or_name
        ctx = Ctx(meta, alias)
        self._cur_columns = meta.columns
        self._check_columns(select, ctx)
        t0, t1, cond, residual = self._split_where(select, ctx)
        if self._default_note:
            self.notes.append(self._default_note)
        order_dir = None
        limit = None
        lim = select.args.get("limit")
        off = select.args.get("offset")
        if lim is not None and is_constant(lim.expression):
            limit = int(self.const_eval(lim.expression)) + (int(self.const_eval(off.expression)) if off else 0)
        order = select.args.get("order")
        if order is not None:
            if len(order.expressions) == 1 and ctx.role(order.expressions[0].this) == "ts":
                order_dir = "desc" if order.expressions[0].args.get("desc") else "asc"
            else:
                limit = None                            # sorted by something else: read all
        elif limit is not None:
            order_dir = "desc"                          # any rows: the most recent ones
        if residual:
            limit = None
        if not self._had_time and limit is not None and self.s.schema_window_ms:
            # no time filter: look back as far as the schema window (stops at the LIMIT)
            t0 = t1 - self.s.schema_window_ms
            if self.notes and self.notes[-1] == self._default_note:
                self.notes[-1] = "no time filter: the most recent samples of the schema window are read"
        name = self._new_table()
        cols = [("ts", "TIMESTAMP", None)] + [(_label_col(lb, meta.columns), "VARCHAR", lb) for lb in meta.labels] + \
            [("value", "DOUBLE", None)]
        self.scans.append(RowScan(name, meta.name, alias, cond, t0, t1, cols, order_dir, limit))
        # virtual columns are per bucket: NULL on samples
        for col in _own_columns(select):
            c = ctx.column(col)
            if c is not None and c.role in ("rate", "increase"):
                col.replace(exp.Cast(this=exp.Null(), to=exp.DataType.build("DOUBLE")))
        stars = [e for e in select.expressions if isinstance(e, exp.Star)]
        if stars and any(c.role in ("rate", "increase") for c in meta.columns.values()):
            new = []
            for e in select.expressions:
                if isinstance(e, exp.Star):
                    for c in meta.columns.values():
                        if c.role in ("rate", "increase"):
                            new.append(exp.alias_(exp.Cast(this=exp.Null(), to=exp.DataType.build("DOUBLE")), c.name))
                        else:
                            new.append(_col(c.name))
                else:
                    new.append(e)
            select.set("expressions", new)
        _replace_from(select, name, alias)
        select.set("where", exp.Where(this=and_all(residual)) if residual else None)

    # ------------------------------------------------------------------ #
    # promql('<expr>' [, '<step>'])
    # ------------------------------------------------------------------ #
    def _plan_promql(self, select: exp.Select, table: exp.Table) -> None:
        if not self.s.allow_promql:
            raise ProgrammingError("promql() is disabled on this connection (allow_promql=false)")
        call = table.this
        args = call.expressions
        if not args or not is_constant(args[0]):
            raise ProgrammingError("promql('<PromQL expression>' [, '<step>'])")
        expr = str(self.const_eval(args[0]))
        step = None
        if len(args) > 1:
            step = parse_duration(str(self.const_eval(args[1])))
        alias = table.alias_or_name or "promql"
        fake = MetricMeta("promql", "gauge", []).build()
        ctx = Ctx(fake, alias)
        # time range from the WHERE; everything else is applied by DuckDB on the result
        t0 = t1 = None
        rest = []
        for c in conjuncts(select.args["where"].this if select.args.get("where") else None):
            b = self._time_bound(c, ctx)
            if b is None:
                rest.append(c)
                continue
            t0 = b[0] if t0 is None or (b[0] is not None and b[0] > t0) else t0
            t1 = b[1] if t1 is None or (b[1] is not None and b[1] < t1) else t1
        if t1 is None:
            t1 = self.s.now_ms
        if t0 is None:
            t0 = t1 - self.s.default_range_ms
        grains = []
        for g in self._group_exprs(select) if self._is_aggregate_query(select) else []:
            gr = self.grain_of(g, ctx)
            if gr is not None and gr not in grains:
                grains.append(gr)
        if len(grains) > 1:
            raise PushdownError("promql(): use one time bucket per query")
        name = self._new_table()
        self.scans.append(PromqlScan(name, alias, expr, t0, t1, grains[0] if grains else None, step))
        _replace_from(select, name, alias)
        select.set("where", exp.Where(this=and_all(rest)) if rest else None)


_JOIN_MSG = ("joins of metric tables are not computed row by row: aggregate each metric in a subquery "
             "(SELECT node, AVG(value) v FROM m GROUP BY node) and join the subqueries, or use "
             "promql('a / on(node) b') for arithmetic between metrics.")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _depth(node: exp.Expression) -> int:
    d = 0
    p = node.parent
    while p is not None:
        d += 1
        p = p.parent
    return d


def _inside_agg(node: exp.Expression, top: exp.Expression) -> bool:
    p = node.parent
    while p is not None and p is not top.parent:
        if is_aggregate(p):
            return True
        p = p.parent
    return False


def _time_expr_top(col: exp.Column) -> exp.Expression:
    """Largest expression around ts made of functions, casts and constant arithmetic."""
    node: exp.Expression = col
    while node.parent is not None:
        p = node.parent
        if isinstance(p, (exp.Select, exp.Where, exp.Group, exp.Order, exp.Ordered, exp.Having, exp.Alias,
                          exp.Predicate, exp.Connector, exp.Not, exp.AggFunc, exp.Filter, exp.Case, exp.If)):
            break
        if isinstance(p, (exp.Paren, exp.TimestampTrunc, exp.DateTrunc, exp.DateBin, exp.Cast, exp.Extract,
                          exp.Anonymous, exp.Add, exp.Sub, exp.Interval) + TIME_PARTS):
            others = [c for c in p.find_all(exp.Column) if c is not col]
            if others:
                break
            node = p
            continue
        break
    return node


def _auto_grain(range_ms: int) -> Grain:
    """About 500 buckets over the range, on a round width (1 min .. 1 day)."""
    target = max(1, range_ms // 500)
    for w in (SECOND * 15, SECOND * 30, MINUTE, 2 * MINUTE, 5 * MINUTE, 10 * MINUTE, 15 * MINUTE, 30 * MINUTE, HOUR,
              2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR, DAY):
        if w >= target:
            return Grain("fixed", width_ms=w)
    return Grain("fixed", width_ms=DAY)


def _divides(g: Grain, other: Grain) -> bool:
    """Is every bucket of `other` a union of buckets of `g` (and constant for EXTRACT grains)?"""
    if g == other:
        return True
    if g.shift_in or g.shift_out:
        return False
    if g.unit == "fixed":
        if (DAY % g.width_ms) or ((g.origin - DUCKDB_ORIGIN) // dt.timedelta(milliseconds=1)) % g.width_ms:
            return other.unit == "fixed" and other.width_ms % g.width_ms == 0 and \
                ((other.origin - g.origin) // dt.timedelta(milliseconds=1)) % g.width_ms == 0
        if other.unit == "fixed":
            return other.width_ms % g.width_ms == 0 and \
                ((other.origin - g.origin) // dt.timedelta(milliseconds=1)) % g.width_ms == 0
        if other.unit == "months" or other.unit in ("day", "week", "month", "quarter", "year"):
            return _shift_ok(other, g.width_ms)
        size = {"second": SECOND, "minute": MINUTE, "hour": HOUR}[other.unit]
        return size % g.width_ms == 0 and _shift_ok(other, g.width_ms)
    if g.unit in ("second", "minute", "hour", "day"):
        size = {"second": SECOND, "minute": MINUTE, "hour": HOUR, "day": DAY}[g.unit]
        if other.unit == "fixed":
            return other.width_ms % size == 0 and ((other.origin - DUCKDB_ORIGIN) // dt.timedelta(milliseconds=1)) % size == 0
        if other.unit == "months":
            return True
        if UNIT_ORDER.index(other.unit) < UNIT_ORDER.index(g.unit):
            return False
        return _shift_ok(other, size)
    if g.unit == "month":
        return other.unit in ("month", "quarter", "year") and not other.shift_in
    return False


def _shift_ok(other: Grain, size: int) -> bool:
    return (other.shift_in // dt.timedelta(milliseconds=1)) % size == 0


def _replace_from(select: exp.Select, name: str, alias: str) -> None:
    select.set(FROM_KEY, exp.From(this=exp.Table(this=exp.to_identifier(name, quoted=True),
                                                 alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))))


def _label_col(label: str, columns: dict) -> str:
    for c in columns.values():
        if c.role == "label" and c.label == label:
            return c.name
    return label


def _str_value(v: Any) -> str:
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _dnf(c: pq.Cond) -> tuple:
    return tuple(tuple(conj) for conj in c.dnf())


def _and_dnf(a: tuple, b: tuple) -> tuple:
    if not a:
        return b
    if not b:
        return a
    return tuple(x + y for x in a for y in b)


def _with_math(fn: FnDesc, m: tuple) -> FnDesc:
    return FnDesc(fn.func, fn.lead, fn.trail, fn.range_ms, fn.math + (m,))


def _merge_rule(kind: str) -> str:
    return {"raw_count": "sum", "raw_sum": "sum", "raw_sumsq": "sum", "raw_min": "min", "raw_max": "max"}.get(kind, "sum")


def _fn_merge(fn: FnDesc) -> str:
    if fn.func in ADDITIVE:
        return "sum"
    if fn.func in ("min_over_time",):
        return "min"
    if fn.func in ("max_over_time", "present_over_time"):
        return "max"
    if fn.func in ("last_over_time",):
        return "last"
    return "avg"


def _outer_merge(outer: str, fn: FnDesc) -> str:
    if outer == "count":
        return "max"
    if outer in ("min", "max"):
        return outer if _fn_merge(fn) in ("min", "max", "sum") else "avg"
    if outer == "sum":
        return _fn_merge(fn)
    return "avg"


def _sql_agg(op: str, v: exp.Expression, q: float | None, coalesce0: bool) -> exp.Expression:
    if op == "sum":
        s = exp.Sum(this=v)
        return exp.Coalesce(this=s, expressions=[_lit(0)]) if coalesce0 else s
    if op == "avg":
        return exp.Avg(this=v)
    if op == "min":
        return exp.Min(this=v)
    if op == "max":
        return exp.Max(this=v)
    if op == "count":
        return exp.Count(this=v)
    if op == "stddev_pop":
        return exp.StddevPop(this=v)
    if op == "var_pop":
        return exp.VariancePop(this=v)
    if op == "stddev_samp":
        return exp.StddevSamp(this=v)
    if op == "var_samp":
        return exp.Variance(this=v)
    if op == "quantile":
        return exp.PercentileCont(this=v, expression=_lit(q))
    raise PushdownError(f"aggregate {op} not supported")


def _p(x: exp.Expression) -> exp.Expression:
    """Parenthesize composite expressions (trees built by hand are printed without them)."""
    return exp.Paren(this=x) if isinstance(x, (exp.Binary, exp.Connector, exp.Not, exp.Neg)) else x


def _variance(S, Q, C, op: str, a: float) -> exp.Expression:
    """Variance family from sum, sum of squares and count (scaled by a^2 for a * value)."""
    mean = exp.Div(this=S.copy(), expression=exp.Nullif(this=C.copy(), expression=_lit(0)))
    ss = exp.Sub(this=Q, expression=exp.Mul(this=_p(mean), expression=S.copy()))       # sum (x - mean)^2
    if op in ("var_pop", "stddev_pop"):
        var = exp.Div(this=_p(ss), expression=exp.Nullif(this=C.copy(), expression=_lit(0)))
    else:
        var = exp.Div(this=_p(ss), expression=exp.Nullif(this=exp.Sub(this=C.copy(), expression=_lit(1)),
                                                         expression=_lit(0)))
    var = _fn("GREATEST", var, _lit(0.0))
    if a != 1.0:
        var = exp.Mul(this=_lit(a * a), expression=var)
    return _fn("SQRT", var) if op.startswith("stddev") else var


def _merge(outer: exp.Select, sub: exp.Subquery, inner: exp.Select) -> bool:
    """Inline `SELECT .. FROM (SELECT cols FROM metric WHERE ..) alias` into the outer query."""
    alias = sub.alias_or_name
    star = any(isinstance(e, exp.Star) for e in inner.expressions)
    proj: dict[str, exp.Expression] = {}
    order: list[str] = []
    for e in inner.expressions:
        if isinstance(e, exp.Star):
            continue
        name = e.alias_or_name
        if not name:
            return False
        proj[name] = e.this if isinstance(e, exp.Alias) else e
        order.append(name)
    out_aliases = {e.alias for e in outer.expressions if isinstance(e, exp.Alias)}

    def alias_ref(col: exp.Column) -> bool:
        return (not col.table and col.name in out_aliases
                and col.find_ancestor(exp.Order, exp.Having, exp.Qualify) is not None)

    for col in outer.find_all(exp.Column):
        if col.find_ancestor(exp.Select) is not outer:
            continue
        if isinstance(col.this, exp.Star):
            continue
        if col.table and col.table != alias:
            return False
        if col.name not in proj and not star and not alias_ref(col):
            return False

    inner_table = inner.args[FROM_KEY].this
    inner_alias = inner_table.alias_or_name

    def subst(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star) \
                and (not node.table or node.table == alias) \
                and node.find_ancestor(exp.Select) is outer and not alias_ref(node):
            repl = proj.get(node.name)
            if repl is not None:
                new = repl.copy()
                if isinstance(new, (exp.Binary, exp.Connector, exp.Not, exp.Case)):
                    new = exp.Paren(this=new)
                return new
            new = node.copy()
            new.set("table", None)
            return new
        return node

    new_exprs: list[exp.Expression] = []
    for e in list(outer.expressions):
        if isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)):
            if star:
                new_exprs.append(exp.Star())
            for name in order:
                new_exprs.append(exp.alias_(proj[name].copy(), name, quoted=True))
            continue
        out_name = e.alias_or_name if isinstance(e, (exp.Alias, exp.Column)) else None
        e2 = e.transform(subst, copy=False)
        if isinstance(e, exp.Column) and out_name and not (isinstance(e2, exp.Column) and e2.name == out_name):
            e2 = exp.alias_(e2, out_name, quoted=True)
        new_exprs.append(e2)
    outer.set("expressions", new_exprs)
    for key in ("where", "group", "having", "order", "qualify"):
        val = outer.args.get(key)
        if val is None:
            continue
        outer.set(key, val.transform(subst, copy=False))
    t = inner_table.copy()
    if not inner_table.alias:
        t.set("alias", exp.TableAlias(this=exp.to_identifier(inner_alias)))
    outer.set(FROM_KEY, exp.From(this=t))
    w = [c for c in [inner.args.get("where"), outer.args.get("where")] if c is not None]
    merged = and_all([c.this.copy() if isinstance(c, exp.Where) else c.copy() for c in w])
    outer.set("where", exp.Where(this=merged) if merged is not None else None)
    # qualifiers of the inner alias now point at the metric table
    for col in outer.find_all(exp.Column):
        if col.table == alias:
            col.set("table", None)
    return True
