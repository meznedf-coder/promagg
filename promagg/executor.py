"""Run the scans of a plan against the Prometheus API.

Aggregation scans: every pushed atom is evaluated once per run of equal, contiguous time
buckets with one query_range (step = bucket length, at most 10,000 points per query), or
with an instant query for irregular buckets (partial first / last bucket, 23 / 25 hour
days, months). Queries run in parallel.
"""

from __future__ import annotations

import math
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from promagg import promql as pq
from promagg.client import TENANT_LABEL, PromClient, Series
from promagg.errors import LimitError, OperationalError, ProgrammingError
from promagg.planner import AggScan, Atom, LabelScan, PromqlScan, RowScan, Settings
from promagg.timegrid import HOUR, MINUTE, Bucket, buckets, duration, from_ms

MAX_POINTS_PER_QUERY = 10_000        # Prometheus / Mimir refuse more than 11,000 per series
PROBE_CONCURRENCY = 16                # per-tenant probes of the label index, at a time
SERIES_AT_ONCE = 20_000               # tenant values read from the series up to this many, else probed


@dataclass
class Result:
    columns: list[tuple[str, str]]   # (name, SQL type)
    data: dict[str, list]            # columnar
    rows: int
    queries: list[str]


def _nan_none(v: float) -> float | None:
    return None if v is None or math.isnan(v) else v


class Executor:
    def __init__(self, client: PromClient, settings: Settings, concurrency: int = 4) -> None:
        self.client = client
        self.s = settings
        self.concurrency = max(1, concurrency)
        self._left_open: bool | None = None
        self.log: list[str] = []
        self._lock = threading.Lock()

    @property
    def left_open(self) -> bool:
        if self._left_open is None:
            self._left_open = self.client.range_left_open()
        return self._left_open

    def range_sel(self, sel: pq.Selector, window_ms: int) -> str:
        """Samples in [end - window, end) when evaluated at `end`."""
        w = window_ms if self.left_open else window_ms - 1
        return f"{sel.text()}[{duration(w)}] offset 1ms"

    # ------------------------------------------------------------------ #
    def run(self, scan) -> Result:
        if isinstance(scan, AggScan):
            return self.run_agg(scan)
        if isinstance(scan, RowScan):
            return self.run_rows(scan)
        if isinstance(scan, PromqlScan):
            return self.run_promql(scan)
        if isinstance(scan, LabelScan):
            return self.run_labels(scan)
        raise TypeError(scan)

    def run_labels(self, scan: LabelScan) -> Result:
        sels = pq.selectors(scan.metric, scan.cond)
        matches = [s.text() for s in sels]
        end = scan.t1 if scan.t1 is not None else self.s.now_ms
        start = scan.t0 if scan.t0 is not None else end - (self.s.schema_window_ms or 7 * 86_400_000)
        label_cols = [(c, lb) for c, _t, lb in scan.columns if lb is not None]
        rows: set[tuple] = set()
        if matches and any(lb == TENANT_LABEL for _c, lb in label_cols):
            rows = self._tenant_labels(sels, [lb for _c, lb in label_cols], start, end)
        elif matches and len(label_cols) == 1:
            lb = label_cols[0][1]
            rows = {(v,) for v in self.client.label_values(lb, matches, start, end) if v != ""}
        elif matches:
            for series in self.client.series(matches, start, end):
                rows.add(tuple(series.get(lb) or None for _c, lb in label_cols))
        data: dict[str, list] = {c: [] for c, _t, _l in scan.columns}
        for r in sorted(rows, key=lambda r: tuple(x or "" for x in r)):
            data["ts"].append(None)
            for (c, _lb), v in zip(label_cols, r):
                data[c].append(v)
        q = [f"labels {', '.join(lb for _c, lb in label_cols)} of {' or '.join(matches)}"]
        return Result([(c, t) for c, t, _l in scan.columns], data, len(rows), q)

    def _tenant_labels(self, sels: list, labels: list[str], start: int, end: int) -> set[tuple]:
        """Label values with __tenant_id__ (tenant federation). Mimir's label index lists every
        tenant of the request for __tenant_id__, with or without matching series, so the answer
        comes from the series: read at once while there are at most SERIES_AT_ONCE of them,
        else from one probe per tenant (a matcher on __tenant_id__ selects it), several at a
        time: one series (does it have any?), the values of the other label, or its series."""
        rows = lambda series: {tuple(s.get(lb) or None for lb in labels) for s in series}  # noqa: E731
        series = self.client.series([x.text() for x in sels], start, end, limit=SERIES_AT_ONCE + 1)
        if len(series) <= SERIES_AT_ONCE:
            return rows(series)
        tenants = self.client.tenants()
        if len(tenants) < 2:                 # tenants set by a gateway: the index lists them
            tenants = [t for t in self.client.label_values(TENANT_LABEL, None, start, end) if t]
        others = [lb for lb in labels if lb != TENANT_LABEL]

        def one(t: str) -> set[tuple]:
            ms = [x.with_([pq.Matcher(TENANT_LABEL, "=", t)]).text() for x in sels]
            if not others:
                vals = [{}] if self.client.series(ms, start, end, limit=1) else []
            elif len(others) == 1:
                vals = [{others[0]: v} for v in self.client.label_values(others[0], ms, start, end) if v != ""]
            else:
                vals = self.client.series(ms, start, end)
            return {tuple(t if lb == TENANT_LABEL else (v.get(lb) or None) for lb in labels) for v in vals}

        out: set[tuple] = set()
        with ThreadPoolExecutor(max_workers=max(1, min(PROBE_CONCURRENCY, len(tenants)))) as pool:
            for part in pool.map(one, tenants):
                out |= part
        return out

    def _map(self, fn, items: list) -> list:
        if len(items) <= 1 or self.concurrency == 1:
            return [fn(i) for i in items]
        with ThreadPoolExecutor(max_workers=min(self.concurrency, len(items))) as pool:
            return list(pool.map(fn, items))

    # ------------------------------------------------------------------ #
    # aggregations
    # ------------------------------------------------------------------ #
    def atom_expr(self, scan: AggScan, atom: Atom, window_ms: int) -> str | None:
        cond = scan.cond
        extra = [list(c) for c in atom.cond] if atom.cond else None
        sels = pq.selectors(scan.metric, cond)
        if extra is not None:
            sels = [s.with_(conj) for s in sels for conj in extra]
            sels = [s for s in sels if not pq._contradiction(s.matchers)]
        if not sels:
            return None
        labels = ", ".join(scan.labels)

        def one(sel: pq.Selector) -> str:
            if atom.kind == "distinct":
                sel = sel.with_([pq.Matcher(atom.label, "!=", "")])
            fn_window = atom.fn.range_ms if atom.fn is not None and atom.fn.range_ms else window_ms
            if atom.kind in ("fn", "fn_sq", "hq"):
                r = self.range_sel(sel, fn_window)
            else:
                r = self.range_sel(sel, window_ms)
            k = atom.kind
            if k in ("raw_count", "distinct"):
                return f"count_over_time({r})"
            if k == "raw_sum":
                return f"sum_over_time({r})"
            if k == "raw_min":
                return f"min_over_time({r})"
            if k == "raw_max":
                return f"max_over_time({r})"
            if k == "raw_sumsq":
                return f"(count_over_time({r}) * (stdvar_over_time({r}) + avg_over_time({r}) ^ 2))"
            if k in ("fn", "hq"):
                return atom.fn.text(r)
            if k == "fn_sq":
                return f"({atom.fn.text(r)}) ^ 2"
            raise ValueError(k)

        body = pq.union([one(s) for s in sels])
        if atom.kind == "distinct":
            inner_labels = ", ".join(scan.labels + [atom.label])
            return f"count by ({labels}) (count by ({inner_labels}) ({body}))"
        if atom.kind == "hq":
            return f"histogram_quantile({_q(atom.q)}, sum by ({', '.join(scan.labels + ['le'])}) ({body}))"
        if atom.outer is None:
            return body
        if atom.outer == "quantile":
            return f"quantile by ({labels}) ({_q(atom.q)}, {body})"
        return f"{atom.outer} by ({labels}) ({body})"

    def runs(self, bks: list[Bucket]) -> list[tuple[int, list[tuple[int, int]]]]:
        """[(width, [(bucket index, piece end)...])]: contiguous pieces of equal width."""
        out: list[tuple[int, list[tuple[int, int]]]] = []
        cur: tuple[int, list] | None = None
        last_end = None
        for bi, b in enumerate(bks):
            for (s, e) in b.pieces:
                w = e - s
                if len(b.pieces) == 1 and cur is not None and cur[0] == w and last_end == s and \
                        len(cur[1]) < MAX_POINTS_PER_QUERY:
                    cur[1].append((bi, e))
                else:
                    cur = (w, [(bi, e)])
                    out.append(cur)
                    if len(b.pieces) > 1:
                        cur = None            # pieces of a split bucket are evaluated alone
                last_end = e
        return out

    def evaluate(self, expr: str, pts: list[tuple[int, int]], width: int) -> list[Series]:
        """One run of buckets; halves of the run when the backend refuses it for a limit."""
        try:
            if len(pts) == 1:
                return self.client.query(expr, pts[0][1])
            return self.client.query_range(expr, pts[0][1], pts[-1][1], width)
        except LimitError:
            if len(pts) == 1:
                raise
            mid = len(pts) // 2
            with self._lock:
                self.log.append(f"split: {len(pts)} buckets -> {mid} + {len(pts) - mid}")
            return self.evaluate(expr, pts[:mid], width) + self.evaluate(expr, pts[mid:], width)

    def run_agg(self, scan: AggScan) -> Result:
        bks = buckets(self.s.zone, scan.grain, scan.t0, scan.t1)
        runs = self.runs(bks)
        atoms = list(scan.atoms.items())
        label_cols = [(c, lb) for c, t, lb in scan.columns if lb is not None]
        tasks = [(a, col, run) for a, col in atoms for run in runs]
        queries: list[str] = []

        def one(task):
            a, col, (width, pts) = task
            expr = self.atom_expr(scan, a, width)
            if expr is None:
                return a, col, [], pts, None
            if scan.topk is not None and scan.topk[2] == a:
                expr = f"{scan.topk[1]}({scan.topk[0]}, {expr})"
            return a, col, self.evaluate(expr, pts, width), pts, expr

        results = self._map(one, tasks)
        rows: dict[tuple, dict[str, Any]] = {}
        merges = {col: a.merge for a, col in atoms}
        for a, col, res, pts, expr in results:
            if expr:
                queries.append(expr)
            end_to_b = {e: bi for bi, e in pts}
            for series in res:
                key = tuple((series.labels.get(lb) or None) for _c, lb in label_cols)
                for t, v in series.points:
                    bi = end_to_b.get(t)
                    if bi is None:
                        continue
                    r = rows.setdefault((bi, key), {})
                    v = _nan_none(v)
                    if col in r and r[col] is not None and v is not None:
                        r[col] = _combine(merges[col], r[col], v)
                    elif col not in r or r[col] is None:
                        r[col] = v
        if len(rows) > self.s.max_points:
            raise OperationalError(f"the query returns {len(rows):,} groups x buckets (limit {self.s.max_points:,} "
                                   "- connection option max_points): use a coarser time grain or fewer labels")
        data: dict[str, list] = {c: [] for c, _t, _l in scan.columns}
        for (bi, key), vals in sorted(rows.items(), key=lambda kv: (kv[0][0], tuple(x or "" for x in kv[0][1]))):
            b = bks[bi]
            data["ts"].append(b.local_start)
            for (c, _lb), v in zip(label_cols, key):
                data[c].append(v)
            for _a, col in atoms:
                data[col].append(vals.get(col))
        with self._lock:
            self.log.extend(queries)
        return Result([(c, t) for c, t, _l in scan.columns], data, len(rows), queries)

    # ------------------------------------------------------------------ #
    # raw samples
    # ------------------------------------------------------------------ #
    def edge(self, sels: list[pq.Selector], t0: int, t1: int, last: bool, queries: list[str]) -> int | None:
        """Time (ms) of the last (or the first) sample in [t0, t1) of the selected series, without
        moving samples: `max(timestamp(m))` / `count(timestamp(m))` every 4 minutes (the 5-minute
        lookback sees every sample), over windows growing away from the edge of the range."""
        step, lookback = 4 * MINUTE, 5 * MINUTE
        stamps = pq.union([f"timestamp({s.text()})" for s in sels])
        expr = f"max({stamps})" if last else f"count({stamps})"
        span = HOUR
        a, b = t0, t1
        while a < b:
            lo, hi = (max(a, b - span), b) if last else (a, min(b, a + span))
            if last:        # steps end exactly at hi - 1 ms: the lookback of the last one reaches hi
                end_step = hi - 1
                k = min((end_step - lo) // step, MAX_POINTS_PER_QUERY - 1)
                first_step = end_step - k * step
            else:           # the first lookback starts exactly at lo: (lo - 1 ms, lo + 5 min - 1 ms]
                first_step = lo + lookback - 1
                k = min(max(0, (hi - lo) // step), MAX_POINTS_PER_QUERY - 1)
                end_step = first_step + k * step
            queries.append(f"{expr} @ [{first_step}, {end_step}] step 4m")
            res = self.client.query_range(expr, first_step, end_step, step)
            if last:
                vals = [int(round(v * 1000)) for sr in res for _t, v in sr.points if v == v]
                vals = [v for v in vals if t0 <= v < t1]
                if vals:
                    return max(vals)
                b = lo
            else:
                steps = sorted(t for sr in res for t, v in sr.points if v > 0)
                if steps:
                    tf = steps[0]
                    win_a, win_b = max(t0, tf - lookback), min(t1, tf + 1)
                    firsts = []
                    for sel in sels:
                        rexpr = f"{sel.text()}[{duration(win_b - win_a if self.left_open else win_b - win_a - 1)}] offset 1ms"
                        queries.append(f"{rexpr} @ {win_b}")
                        for sr in self.client.query(rexpr, win_b):
                            firsts += [t for t, _v in sr.points if win_a <= t < win_b]
                    if firsts:
                        return min(firsts)
                a = hi
            span *= 4
        return None

    def run_rows(self, scan: RowScan) -> Result:
        sels = pq.selectors(scan.metric, scan.cond)
        label_cols = [(c, lb) for c, t, lb in scan.columns if lb is not None]
        data: dict[str, list] = {c: [] for c, _t, _l in scan.columns}
        queries: list[str] = []
        total = 0
        if not sels or scan.t1 <= scan.t0:
            return Result([(c, t) for c, t, _l in scan.columns], data, 0, queries)

        def samples(sel, a: int, b: int) -> list[Series]:
            expr = f"{sel.text()}[{duration(b - a if self.left_open else b - a - 1)}] offset 1ms"
            try:
                queries.append(f"{expr} @ {b}")
                return self.client.query(expr, b)
            except LimitError:
                if b - a <= 60_000:
                    raise
                mid = a + (b - a) // 2
                return samples(sel, a, mid) + samples(sel, mid, b)

        def fetch(a: int, b: int) -> int:
            nonlocal total
            n = 0
            for sel in sels:
                res = samples(sel, a, b)
                for series in res:
                    key = tuple((series.labels.get(lb) or None) for _c, lb in label_cols)
                    for t, v in series.points:
                        if not (a <= t < b):
                            continue
                        data["ts"].append(self.s.zone.local(t))
                        for (c, _lb), kv in zip(label_cols, key):
                            data[c].append(kv)
                        data["value"].append(v)
                        n += 1
            total += n
            if total > self.s.max_samples:
                raise OperationalError(f"more than {self.s.max_samples:,} samples (connection option max_samples): "
                                       "add a LIMIT, narrow the time range or filter on labels, or aggregate")
            return n

        if scan.limit is not None and len(sels) == 1:
            got, w = 0, 15 * MINUTE
            edge = self.edge(sels, scan.t0, scan.t1, scan.order != "asc", queries)
            if edge is None:
                return Result([(c, t) for c, t, _l in scan.columns], data, 0, queries)
            if scan.order == "asc":
                a = edge
                while a < scan.t1 and got < scan.limit:
                    b = min(scan.t1, a + w)
                    got += fetch(a, b)
                    a, w = b, w * 4
            else:
                b = edge + 1
                while b > scan.t0 and got < scan.limit:
                    a = max(scan.t0, b - w)
                    got += fetch(a, b)
                    b, w = a, w * 4
        else:
            step = 6 * HOUR
            a = scan.t0
            while a < scan.t1:
                b = min(scan.t1, a + step)
                fetch(a, b)
                a = b
        return Result([(c, t) for c, t, _l in scan.columns], data, total, queries)

    # ------------------------------------------------------------------ #
    # promql()
    # ------------------------------------------------------------------ #
    def promql_text(self, scan: PromqlScan, interval_ms: int) -> str:
        rng = scan.t1 - scan.t0
        scrape = self.s.scrape_interval_ms
        rate_iv = max(interval_ms + scrape, 4 * scrape)
        subs = {
            "__interval_ms": str(interval_ms), "__interval": duration(interval_ms),
            "__rate_interval_ms": str(rate_iv), "__rate_interval": duration(rate_iv),
            "__range_ms": str(rng), "__range_s": str(rng // 1000), "__range": duration(rng),
        }

        def rep(m: re.Match) -> str:
            name = m.group(1) or m.group(2)
            if name not in subs:
                raise ProgrammingError(f"promql(): unknown variable ${name}")
            return subs[name]

        return re.sub(r"\$\{(__[a-z_]+)\}|\$(__[a-z_]+)", rep, scan.expr)

    def run_promql(self, scan: PromqlScan, probe: bool = False) -> Result:
        queries: list[str] = []
        rows: list[tuple[Any, dict, float]] = []
        if probe:
            # column names only: the labels of the result at the end of the range, else over
            # the range (coarse), else the by (...) clause of the expression
            interval = scan.step_ms or max(self.s.scrape_interval_ms, 60_000)
            expr = self.promql_text(scan, interval)
            res = self.client.query(expr, scan.t1)
            queries.append(expr)
            if not res:
                step = max(interval, (scan.t1 - scan.t0) // 50)
                res = self.client.query_range(expr, scan.t1 - 50 * step, scan.t1, step)
            if not res:
                m = re.search(r"\bby\s*\(([^)]*)\)", scan.expr)
                if m:
                    res = [Series({k.strip(): "x" for k in m.group(1).split(",") if k.strip()}, [])]
        elif scan.grain is not None:
            bks = buckets(self.s.zone, scan.grain, scan.t0, scan.t1)
            for width, pts in self.runs(bks):
                expr = self.promql_text(scan, width)
                queries.append(expr)
                res = self.client.query(expr, pts[0][1]) if len(pts) == 1 else \
                    self.client.query_range(expr, pts[0][1], pts[-1][1], width)
                end_to_b = {e: bi for bi, e in pts}
                for series in res:
                    for t, v in series.points:
                        bi = end_to_b.get(t)
                        if bi is not None:
                            rows.append((bks[bi].local_start, series.labels, _nan_none(v)))
            res = None
        else:
            step = scan.step_ms or _auto_step(scan.t1 - scan.t0, self.s.scrape_interval_ms)
            expr = self.promql_text(scan, step)
            queries.append(expr)
            first = scan.t0 + step
            n = max(1, (scan.t1 - first) // step + 1)
            if n > MAX_POINTS_PER_QUERY:
                raise ProgrammingError(f"promql(): {n:,} points per series; give a larger step, e.g. promql('..', '5m')")
            res = self.client.query_range(expr, first, first + (n - 1) * step, step)
        names: list[str] = []
        if res is not None:
            for series in res:
                for k in series.labels:
                    if k not in names:
                        names.append(k)
                if probe:
                    continue
                for t, v in series.points:
                    rows.append((self.s.zone.local(t), series.labels, _nan_none(v)))
        for _t, labels, _v in rows:
            for k in labels:
                if k not in names:
                    names.append(k)
        cols = [("ts", "TIMESTAMP")] + [(_promql_col(k), "VARCHAR") for k in sorted(names)] + [("value", "DOUBLE")]
        data: dict[str, list] = {c: [] for c, _t in cols}
        for t, labels, v in rows:
            data["ts"].append(t)
            for k in sorted(names):
                data[_promql_col(k)].append(labels.get(k) or None)
            data["value"].append(v)
        scan.columns = [(c, t, None) for c, t in cols]
        return Result(cols, data, len(rows), queries)


def _promql_col(label: str) -> str:
    return "metric_name" if label == "__name__" else ("label_" + label if label in ("ts", "value") else label)


def _auto_step(range_ms: int, scrape_ms: int) -> int:
    target = max(scrape_ms, range_ms // 1000)
    for nice in (15_000, 30_000, MINUTE, 2 * MINUTE, 5 * MINUTE, 10 * MINUTE, 15 * MINUTE, 30 * MINUTE, HOUR,
                 2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR, 24 * HOUR):
        if nice >= target:
            return nice
    return 24 * HOUR


def _q(v: float) -> str:
    return repr(float(v))


def _combine(rule: str, a: float, b: float) -> float:
    if rule == "sum":
        return a + b
    if rule == "min":
        return min(a, b)
    if rule == "max":
        return max(a, b)
    if rule == "last":
        return b
    return (a + b) / 2
