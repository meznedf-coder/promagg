"""all_metrics: one table for every metric, so that Superset needs one dataset, not one per
metric.

Columns: ts, metric_name (the metric), one column per label name of the database (the labels of
every metric seen in the schema window; the connection parameter `all_metrics_labels` lists or
caps them), value, rate and increase. A query names its metrics in WHERE, with conditions on
metric_name alone (metric_name = 'x', IN (...), LIKE 'node_%', regexp_matches(...), NOT ...).
Before planning, it becomes the same query on each metric's own table, where a label that the
metric does not have is NULL, so the PromQL is the one of the metric's table:

* one metric: that query;
* several metrics (at most `all_metrics_max`, 50): one query per metric, UNION ALL, with the
  ORDER BY / LIMIT on the union. An aggregate must then GROUP BY metric_name (adding up metrics
  of different kinds is rarely meant; filter one metric, or group by it);
* no metric filter: only the list of metrics (SELECT metric_name ... GROUP BY metric_name, or
  DISTINCT) and the values of one label (a filter box) are answered, from the label index;
  anything else is refused with the reason, since it would read every metric.

rate and increase exist for counters: on a gauge they are refused in aggregates and NULL in row
lists (as they are on raw samples). `SELECT * FROM (SELECT * FROM all_metrics WHERE ...) t`
(a virtual dataset) is read as the inner table with its filter.
"""

from __future__ import annotations

from typing import Callable

import pyarrow as pa
from sqlglot import exp

from promagg import duck
from promagg.errors import PushdownError
from promagg.planner import FROM_KEY, _depth, contains_agg
from promagg.schema import VIRTUAL, Column, MetricMeta

METRIC = "metric_name"
SCHEMAS = ("", "default", "metrics", "promagg", "prometheus", "mimir")
RESERVED = ("ts", "value", METRIC) + VIRTUAL


def column_of(label: str) -> str:
    """The column of a label in all_metrics (label_<name> for the names of other columns)."""
    return f"label_{label}" if label in RESERVED else label


def build_meta(name: str, labels: list[str], only: list[str] | None = None, max_labels: int = 300) -> MetricMeta:
    """The table's columns: ts, metric_name, the labels (`only`, or the first `max_labels` in
    alphabetical order), value, rate, increase."""
    seen = sorted({lb for lb in labels if lb and lb != "__name__"})
    chosen = list(dict.fromkeys(lb for lb in (only or []) if lb and lb != "__name__")) or seen[:max_labels]
    meta = MetricMeta(name, "all", chosen)
    cols = [Column("ts", "TIMESTAMP", "ts", comment="Sample time (in aggregates: start of the time bucket)"),
            Column(METRIC, "VARCHAR", "metric", comment="The metric. Filter it: metric_name = '...', IN (...), "
                   "LIKE '...'")]
    cols += [Column(column_of(lb), "VARCHAR", "label", lb, f"label {lb} (NULL for the metrics without it)")
             for lb in sorted(chosen)]
    cols += [Column("value", "DOUBLE", "value", comment="raw sample value"),
             Column("rate", "DOUBLE", "rate", comment="counters only: per-second rate over the time bucket, per "
                    "series (SUM(rate) = total per second)"),
             Column("increase", "DOUBLE", "increase", comment="counters only: increase over the time bucket, per "
                    "series (SUM(increase) = total count)")]
    meta.columns = {c.name: c for c in cols}
    return meta


# --------------------------------------------------------------------------- #
# small helpers on sqlglot trees
# --------------------------------------------------------------------------- #
def _conjuncts(node: exp.Expression | None) -> list[exp.Expression]:
    if node is None:
        return []
    node = node.unnest() if isinstance(node, exp.Paren) and isinstance(node.unnest(), exp.And) else node
    if isinstance(node, exp.And):
        return _conjuncts(node.left) + _conjuncts(node.right)
    return [node]


def _own_nodes(select: exp.Select, kind: type) -> list[exp.Expression]:
    """Nodes of `select` itself, not of the subqueries it contains."""
    out: list[exp.Expression] = []

    def visit(node: exp.Expression) -> None:
        for child in node.iter_expressions():
            if isinstance(child, (exp.Select, exp.Subquery, exp.Union)) and child is not select:
                continue
            if isinstance(child, kind):
                out.append(child)
            visit(child)

    visit(select)
    return out


def _null(sql_type: str) -> exp.Expression:
    return exp.Cast(this=exp.Null(), to=exp.DataType.build(sql_type))


def _is_const(node: exp.Expression) -> bool:
    node = node.this if isinstance(node, exp.Alias) else node
    return isinstance(node, (exp.Literal, exp.Null)) or (isinstance(node, exp.Cast) and isinstance(node.this, exp.Null))


def _aggregating(select: exp.Select) -> bool:
    if select.args.get("group"):
        return True
    items = list(select.expressions) + [select.args.get("having")]
    return any(e is not None and contains_agg(e) for e in items)


def _output_name(node: exp.Expression) -> str:
    return node.alias_or_name if isinstance(node, (exp.Alias, exp.Column)) else ""


class AllMetrics:
    """Rewrites the queries on the all_metrics table into queries on the metrics' own tables."""

    def __init__(self, name: str, meta: Callable[[], MetricMeta], metric_names: Callable[[], list[str]],
                 lookup: Callable[[str], MetricMeta | None], label_values: Callable[[str], list[str]],
                 max_metrics: int = 50) -> None:
        self.name = name
        self.meta = meta
        self.metric_names = metric_names
        self.lookup = lookup
        self.label_values = label_values
        self.max_metrics = max_metrics

    def is_table(self, t: exp.Expression) -> bool:
        if not isinstance(t, exp.Table) or not isinstance(t.this, exp.Identifier) or t.name != self.name:
            return False
        db = t.args.get("db")
        return db is None or db.name.lower() in SCHEMAS

    # ------------------------------------------------------------------ #
    def rewrite(self, stmt: exp.Expression) -> exp.Expression:
        if not self.name or not any(self.is_table(t) for t in stmt.find_all(exp.Table)):
            return stmt
        if self.name in {c.alias_or_name for c in stmt.find_all(exp.CTE)}:
            return stmt                                     # a CTE of that name hides the table
        self._flatten(stmt)
        for select in sorted(stmt.find_all(exp.Select), key=lambda s: -_depth(s)):
            src = select.args.get(FROM_KEY)
            table = src.this if src is not None else None
            joins = select.args.get("joins") or []
            if any(self.is_table(j.this) for j in joins) or (joins and self.is_table(table)):
                raise PushdownError(f"{self.name} cannot be joined. Aggregate each metric in its own subquery "
                                    "and join the subqueries, or use promql() for arithmetic between metrics.")
            if self.is_table(table):
                new = self._select(select, table)
                if new is not select:
                    if select is stmt:
                        stmt = new
                    else:
                        select.replace(new)
        return stmt

    def _flatten(self, stmt: exp.Expression) -> None:
        """SELECT ... FROM (SELECT * FROM all_metrics WHERE c) t  ->  SELECT ... FROM all_metrics t WHERE c."""
        for sub in list(stmt.find_all(exp.Subquery)):
            inner, holder = sub.this, sub.parent
            if not (isinstance(inner, exp.Select) and isinstance(holder, exp.From)
                    and isinstance(holder.parent, exp.Select)):
                continue
            src = inner.args.get(FROM_KEY)
            if src is None or not self.is_table(src.this) or len(inner.expressions) != 1 \
                    or not isinstance(inner.expressions[0], exp.Star) or any(
                        inner.args.get(k) for k in ("joins", "group", "having", "order", "limit", "offset",
                                                    "distinct", "qualify")):
                continue
            outer = holder.parent
            table = src.this.copy()
            table.set("alias", exp.TableAlias(this=exp.to_identifier(sub.alias or self.name)))
            holder.set("this", table)
            if inner.args.get("where") is not None:
                outer.where(inner.args["where"].this.copy(), copy=False)

    # ------------------------------------------------------------------ #
    def _select(self, select: exp.Select, table: exp.Table) -> exp.Expression:
        quals = {q for q in (table.alias, table.name) if q}
        alias = table.alias or table.name

        def mine(col: exp.Column) -> bool:
            return not col.table or col.table in quals

        metric_conds = []
        for c in _conjuncts(select.args["where"].this if select.args.get("where") else None):
            names = {col.name for col in c.find_all(exp.Column) if mine(col)}
            if METRIC in names:
                if names - {METRIC}:
                    raise PushdownError(f"a condition mixes {METRIC} with other columns ({c.sql(dialect='duckdb')}): "
                                        f"filter {METRIC} on its own, joined to the rest with AND")
                metric_conds.append(c)
        if not metric_conds:
            return self._without_metric(select, alias, mine)
        metrics = self._resolve(metric_conds)
        if not metrics:
            return self._empty(select, table, alias)
        if len(metrics) > self.max_metrics:
            raise PushdownError(f"{len(metrics)} metrics match the {METRIC} filter ({', '.join(metrics[:5])}...): "
                                f"at most {self.max_metrics} per query, narrow the filter")
        agg = _aggregating(select)
        dedupe = bool(select.args.get("distinct")) or (bool(select.args.get("group")) and not any(
            contains_agg(e) for e in list(select.expressions) + [select.args.get("having")] if e is not None))
        if len(metrics) > 1 and agg and not dedupe and not self._grouped_by_metric(select, mine):
            shown = ", ".join(metrics[:5]) + ("..." if len(metrics) > 5 else "")
            raise PushdownError(f"several metrics match the {METRIC} filter ({shown}): GROUP BY {METRIC} to see "
                                "each one, or filter a single metric (adding different metrics up is refused)")
        branches = [self._branch(select, table, alias, m, agg, mine) for m in metrics]
        if len(branches) == 1:
            return branches[0]
        return self._union(select, branches, alias, dedupe)

    def _resolve(self, conds: list[exp.Expression]) -> list[str]:
        """The metrics (of the schema window) that satisfy the conditions on metric_name."""
        names = self.metric_names()
        cond = exp.and_(*[c.copy() for c in conds])
        for col in cond.find_all(exp.Column):
            col.set("table", None)
        con = duck.locked_session("UTC")
        try:
            con.register("__promagg_metrics", pa.table({METRIC: pa.array(names, pa.string())}))
            rows = con.execute(f"SELECT {METRIC} FROM __promagg_metrics WHERE {cond.sql(dialect='duckdb')} "
                               f"ORDER BY 1").fetchall()
        except Exception as ex:  # pylint: disable=broad-except
            raise PushdownError(f"the {METRIC} filter cannot be evaluated: {ex}") from ex
        finally:
            con.close()
        return [r[0] for r in rows]

    def _grouped_by_metric(self, select: exp.Select, mine: Callable[[exp.Column], bool]) -> bool:
        group = select.args.get("group")
        if group is None:
            return False
        items = list(select.expressions)
        for g in group.expressions:
            if isinstance(g, exp.Literal) and g.is_int and 0 < int(g.this) <= len(items):
                g = items[int(g.this) - 1]
            g = g.this if isinstance(g, exp.Alias) else g
            if isinstance(g, exp.Column):
                if g.name == METRIC and mine(g):
                    return True
                if not g.table and any(isinstance(e, exp.Alias) and e.alias == g.name and isinstance(e.this, exp.Column)
                                       and e.this.name == METRIC for e in items):
                    return True
        return False

    # ------------------------------------------------------------------ #
    def _branch(self, select: exp.Select, table: exp.Table, alias: str, metric: str, agg: bool,
                mine: Callable[[exp.Column], bool]) -> exp.Select:
        """The query on one metric's table."""
        allmeta, meta = self.meta(), self.lookup(metric)
        if meta is None:
            raise PushdownError(f"metric {metric!r} has no series in the schema window")
        b = select.copy()
        new_table = exp.to_table(exp.to_identifier(metric, quoted=True).sql(dialect="duckdb"))
        new_table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
        b.args[FROM_KEY].set("this", new_table)
        # the conditions on metric_name are met by construction
        where = b.args.get("where")
        if where is not None:
            keep = [c for c in _conjuncts(where.this)
                    if not any(col.name == METRIC and mine(col) for col in c.find_all(exp.Column))]
            b.set("where", exp.Where(this=exp.and_(*keep)) if keep else None)
        # SELECT * -> every column of all_metrics
        items = []
        for e in b.expressions:
            if isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)):
                items += [exp.column(c.name, quoted=True) for c in allmeta.columns.values()]
            else:
                items.append(e)
        b.set("expressions", items)
        for col in _own_nodes(b, exp.Column):
            if not mine(col) or isinstance(col.this, exp.Star):
                continue
            name, spec = col.name, allmeta.columns.get(col.name)
            new: exp.Expression | None = None
            if name == METRIC:
                new = exp.Literal.string(metric)
            elif spec is not None and spec.role == "label":
                if spec.label in meta.labels:
                    col.set("this", exp.to_identifier(meta.label_column(spec.label), quoted=True))
                else:
                    new = _null("VARCHAR")
            elif name in VIRTUAL and not meta.is_counter:
                if agg:
                    raise PushdownError(f"{metric} is a {meta.kind}: {name} exists for counters only; use "
                                        "AVG(value), MAX(value) or MIN(value)")
                new = _null("DOUBLE")
            if new is not None:
                if col.parent is b and col.arg_key == "expressions":
                    new = exp.alias_(new, name, quoted=True)
                col.replace(new)
        # constant keys (the metric, missing labels) are left out of GROUP BY and ORDER BY
        items = list(b.expressions)
        for key in ("group", "order"):
            node = b.args.get(key)
            if node is None:
                continue
            kept = []
            for g in node.expressions:
                target = g.this if isinstance(g, exp.Ordered) else g
                if isinstance(target, exp.Literal) and target.is_int and 0 < int(target.this) <= len(items):
                    target = items[int(target.this) - 1]
                if not _is_const(target):
                    kept.append(g)
            if kept:
                node.set("expressions", kept)
            else:
                b.set(key, None)
        return b

    def _union(self, select: exp.Select, branches: list[exp.Select], alias: str, dedupe: bool) -> exp.Select:
        order, limit, offset = (select.args.get(k) for k in ("order", "limit", "offset"))
        outputs = {_output_name(e) for e in branches[0].expressions}
        movable = order is None or all(
            (isinstance(o.this, exp.Column) and o.this.name in outputs)
            or (isinstance(o.this, exp.Literal) and o.this.is_int) for o in order.expressions)
        for b in branches:
            if movable:
                for k in ("order", "limit", "offset"):
                    b.set(k, None)
            elif limit is not None and offset is not None:
                b.set("offset", None)             # each branch keeps its first rows, the union skips
                b.set("limit", exp.Limit(expression=exp.Literal.number(
                    int(limit.expression.this) + int(offset.expression.this))))
        union: exp.Expression = branches[0]
        for b in branches[1:]:
            union = exp.union(union, b, distinct=False)
        outer = exp.select("*").from_(exp.Subquery(this=union, alias=exp.TableAlias(this=exp.to_identifier(alias))))
        if dedupe:
            outer.set("distinct", exp.Distinct())
        if order is not None and movable:
            order = order.copy()
            for o in order.expressions:
                if isinstance(o.this, exp.Column):
                    o.this.set("table", None)
            outer.set("order", order)
        for k, v in (("limit", limit), ("offset", offset)):
            if v is not None:
                outer.set(k, v.copy())
        return outer

    # ------------------------------------------------------------------ #
    def _without_metric(self, select: exp.Select, alias: str, mine: Callable[[exp.Column], bool]) -> exp.Select:
        """No metric filter: the list of metrics, or the values of one label (filter boxes)."""
        used = {col.name for col in _own_nodes(select, exp.Column) if mine(col)}
        where = select.args.get("where")
        other = {col.name for c in _conjuncts(where.this if where else None) for col in c.find_all(exp.Column)
                 if mine(col)} - {"ts"}
        refused = PushdownError(f"{self.name} holds every metric: add a filter on {METRIC} (metric_name = '...', IN "
                                "(...) or LIKE '...'); in a dashboard, a native filter on metric_name")
        if _aggregating(select) and any(contains_agg(e) for e in select.expressions):
            raise refused
        allmeta = self.meta()
        if used - {"ts"} == {METRIC} and not other:
            column, values = METRIC, self.metric_names()
        elif len(used - {"ts"}) == 1 and not other:
            column = next(iter(used - {"ts"}))
            spec = allmeta.columns.get(column)
            if spec is None or spec.role != "label":
                raise refused
            values = self.label_values(spec.label)
        else:
            raise refused
        b = select.copy()
        if values:
            rows = [exp.Tuple(expressions=[exp.Literal.string(v)]) for v in values]
            src: exp.Expression = exp.Values(expressions=rows, alias=exp.TableAlias(
                this=exp.to_identifier(alias), columns=[exp.to_identifier(column, quoted=True)]))
        else:
            src = exp.Subquery(this=exp.select(exp.alias_(_null("VARCHAR"), column, quoted=True)).where(
                exp.false()), alias=exp.TableAlias(this=exp.to_identifier(alias)))
        b.args[FROM_KEY].set("this", src)
        if where is not None:                    # the time range: the lists cover the schema window
            keep = [c for c in _conjuncts(b.args["where"].this)
                    if not any(col.name == "ts" and mine(col) for col in c.find_all(exp.Column))]
            b.set("where", exp.Where(this=exp.and_(*keep)) if keep else None)
        return b

    def _empty(self, select: exp.Select, table: exp.Table, alias: str) -> exp.Select:
        """No metric matches: the same query over an empty table with all the columns."""
        cols = [exp.alias_(_null(c.sql_type), c.name, quoted=True) for c in self.meta().columns.values()]
        b = select.copy()
        b.args[FROM_KEY].set("this", exp.Subquery(this=exp.select(*cols).where(exp.false()),
                                                  alias=exp.TableAlias(this=exp.to_identifier(alias))))
        return b
