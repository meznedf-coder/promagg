"""PEP 249 (DB-API 2.0) interface of promagg."""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import logging
import re
import time
from typing import Any, Iterable, Sequence

import sqlglot
from sqlglot import exp

from promagg import duck
from promagg.client import PromClient
from promagg.errors import (  # noqa: F401  (re-exported)
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    PushdownError,
    Warning,
)
from promagg.executor import Executor
from promagg.planner import AggScan, LabelScan, Plan, Planner, PromqlScan, RowScan, Settings
from promagg.schema import MetricMeta, Schema
from promagg.timegrid import DAY, Zone, parse_duration

logger = logging.getLogger(__name__)

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

STRING = "VARCHAR"
NUMBER = "DOUBLE"
DATETIME = "TIMESTAMP"
BINARY = "BLOB"
ROWID = "VARCHAR"

_SCHEMAS: dict[tuple, Schema] = {}


def _bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("1", "true", "yes", "on")


def connect(host: str = "localhost", port: int | None = None, user: str | None = None,
            password: str | None = None, **kwargs: Any) -> "Connection":
    """Open a connection to a Prometheus-compatible query API.

    Keyword arguments (all optional):
      path (URL prefix, Mimir: "prometheus"), scheme ("http" | "https"), tenant (X-Scope-OrgID),
      token (Bearer), verify_certs, ca_certs, client_cert, client_key, timezone (default UTC),
      default_range ("24h": window of queries without a time filter), schema_window ("7d":
      where metric and label names are looked up), tables (metric name patterns, comma
      separated), request_timeout (s), concurrency (parallel queries), max_points,
      max_samples, scrape_interval ("15s", for $__rate_interval), allow_promql, counters (metric
      name patterns that are counters without a _total suffix or metadata, e.g. node_vmstat_*),
      now (tests).
    """
    return Connection(host=host, port=port, user=user, password=password, **kwargs)


class Connection:
    def __init__(self, host: str = "localhost", port: int | None = None, user: str | None = None,
                 password: str | None = None, **kw: Any) -> None:
        self.cfg = dict(kw)
        self.closed = False
        tz_name = kw.get("timezone") or kw.get("tz") or "UTC"
        try:
            self.zone = Zone(tz_name)
        except Exception as ex:  # pylint: disable=broad-except
            raise InterfaceError(f"unknown time zone {tz_name!r}") from ex
        self.tz_name = tz_name
        scheme = kw.get("scheme") or ("https" if _bool(kw.get("use_ssl")) else "http")
        path = str(kw.get("path") if kw.get("path") is not None else "prometheus").strip("/")
        base = f"{scheme}://{host}:{int(port or (443 if scheme == 'https' else 9009))}"
        self.url = base + (f"/{path}" if path else "")
        self.tenant = kw.get("tenant") or kw.get("org_id")
        token = kw.get("token")
        if user and user.lower() == "bearer" and password:
            token, user, password = password, None, None
        self.request_timeout = float(kw.get("request_timeout", 120))
        self.concurrency = int(kw.get("concurrency", 4))
        self.client = PromClient(self.url, tenant=self.tenant, user=user, password=password, token=token,
                                 verify=_bool(kw.get("verify_certs"), True), ca_certs=kw.get("ca_certs"),
                                 client_cert=kw.get("client_cert"), client_key=kw.get("client_key"),
                                 timeout=self.request_timeout, max_connections=max(8, self.concurrency * 2))
        self.default_range_ms = parse_duration(str(kw.get("default_range", "24h")))
        sw = str(kw.get("schema_window", "7d")).lower()
        self.schema_window_ms = None if sw in ("all", "0", "") else parse_duration(sw)
        self.patterns = [t.strip() for t in str(kw.get("tables", "")).split(",") if t.strip()]
        self.counters = [t.strip() for t in str(kw.get("counters", "")).split(",") if t.strip()]
        self.max_points = int(kw.get("max_points", 2_000_000))
        self.max_samples = int(kw.get("max_samples", 1_000_000))
        self.scrape_interval_ms = parse_duration(str(kw.get("scrape_interval", "15s")))
        self.allow_promql = _bool(kw.get("allow_promql"), True)
        self.fixed_now = kw.get("now")
        # metadata is shared by connections with the same credentials only (a wrong password must
        # not answer from what a right one read)
        secret = hashlib.sha256("\0".join(str(x or "") for x in (
            user, password, token, kw.get("client_cert"), kw.get("client_key"))).encode()).hexdigest()
        key = (self.url, self.tenant, secret, self.schema_window_ms, tuple(self.patterns), tuple(self.counters),
               self.fixed_now)
        schema = _SCHEMAS.get(key)
        if schema is None:
            schema = _SCHEMAS[key] = Schema(self.client, self.schema_window_ms, self.patterns, now_ms=self.now_ms,
                                            counters=self.counters)
        else:
            schema.client = self.client
        self.schema = schema

    def now_ms(self) -> int:
        if self.fixed_now:
            v = dt.datetime.fromisoformat(str(self.fixed_now))
            if v.tzinfo is None:
                return self.zone.utc_ms(v)
            return int(v.timestamp() * 1000)
        return int(time.time() * 1000)

    # PEP 249 --------------------------------------------------------------
    def close(self) -> None:
        if not self.closed:
            self.client.close()
            self.closed = True

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def cursor(self) -> "Cursor":
        if self.closed:
            raise InterfaceError("connection is closed")
        return Cursor(self)

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def ping(self) -> bool:
        """A real round trip (URL, credentials, tenant): Superset's "Test connection"."""
        self.client.label_values("__name__", None, self.now_ms() - DAY, self.now_ms(), limit=1)
        return True

    # metadata -------------------------------------------------------------
    def list_tables(self) -> list[str]:
        return self.schema.metric_names()

    def table_meta(self, name: str) -> MetricMeta | None:
        return self.schema.meta(name)

    def settings(self) -> Settings:
        return Settings(zone=self.zone, now_ms=self.now_ms(), default_range_ms=self.default_range_ms,
                        max_points=self.max_points, max_samples=self.max_samples,
                        scrape_interval_ms=self.scrape_interval_ms, allow_promql=self.allow_promql,
                        schema_window_ms=self.schema_window_ms)


class Cursor:
    arraysize = 1000

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.description: list[tuple] | None = None
        self.rowcount = -1
        self._rows: list[tuple] = []
        self._pos = 0
        self.closed = False
        self.last_plan: Plan | None = None
        self.last_queries: list[str] = []

    def close(self) -> None:
        self.closed = True
        self._rows = []

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass

    def executemany(self, operation: str, seq_of_parameters: Iterable[Any]) -> None:
        raise NotSupportedError("executemany is not supported (read-only engine)")

    def fetchone(self) -> tuple | None:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        size = size or self.arraysize
        out = self._rows[self._pos:self._pos + size]
        self._pos += len(out)
        return out

    def fetchall(self) -> list[tuple]:
        out = self._rows[self._pos:]
        self._pos = len(self._rows)
        return out

    def __iter__(self):
        return iter(self.fetchall())

    # execution ------------------------------------------------------------
    def execute(self, operation: str, parameters: Any = None) -> "Cursor":
        if self.closed:
            raise InterfaceError("cursor is closed")
        sql = _bind(operation, parameters)
        self._rows, self._pos, self.description, self.rowcount = [], 0, None, -1
        stripped = sql.strip().rstrip(";").strip()
        m = re.match(r"(?is)^explain(\s+analyze)?\s+(.*)$", stripped)
        if m:
            return self._explain(m.group(2), analyze=bool(m.group(1)))
        if re.match(r"(?is)^show\s+tables\b", stripped):
            self._set_result([("name", "VARCHAR")], [(t,) for t in self.connection.list_tables()])
            return self
        m = re.match(r'(?is)^(?:describe|desc)\s+(?:"?default"?\.)?"?([^"\s]+)"?$', stripped)
        if m and not re.match(r"(?is)^(describe|desc)\s+select", stripped):
            meta = self.connection.table_meta(m.group(1))
            if meta is None:
                raise ProgrammingError(f'metric "{m.group(1)}" does not exist')
            self._set_result([("column_name", "VARCHAR"), ("column_type", "VARCHAR"), ("comment", "VARCHAR")],
                             [(c.name, c.sql_type, c.comment) for c in meta.columns.values()])
            return self
        try:
            statements = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
        except sqlglot.errors.ParseError as ex:
            raise ProgrammingError(f"SQL syntax error: {ex}") from ex
        if not statements:
            raise ProgrammingError("empty statement")
        for stmt in statements:
            self._execute_one(stmt)
        return self

    def plan(self, stmt: exp.Expression, const_con) -> Plan:
        c = self.connection

        def const_eval(node: exp.Expression) -> Any:
            try:
                row = const_con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()
            except Exception as ex:  # pylint: disable=broad-except
                raise ProgrammingError(f"cannot evaluate {node.sql(dialect='duckdb')}: {ex}") from ex
            return row[0]

        planner = Planner(c.table_meta, c.settings(), const_eval)
        return planner.plan(stmt)

    def _execute_one(self, stmt: exp.Expression) -> None:
        t_start = time.perf_counter()
        c = self.connection
        const_con = duck.locked_session(c.tz_name)
        try:
            plan = self.plan(stmt, const_con)
        finally:
            const_con.close()
        self.last_plan = plan
        probe = _no_rows_by_construction(plan.statement)
        executor = Executor(c.client, c.settings(), concurrency=c.concurrency)
        con = duck.new_session(c.tz_name)
        queries: list[str] = []
        try:
            for scan in plan.scans:
                if probe:
                    if isinstance(scan, PromqlScan):
                        res = executor.run_promql(scan, probe=True)
                    else:
                        cols = [(n, t) for n, t, _l in scan.columns]
                        duck.register(con, scan.table, cols, {})
                        continue
                else:
                    res = executor.run(scan)
                queries.extend(res.queries)
                duck.register(con, scan.table, res.columns, res.data)
            duck.lock(con)
            sql = plan.statement.sql(dialect="duckdb")
            logger.debug("promagg residual SQL: %s", sql)
            try:
                cur = con.execute(sql)
            except Exception as ex:  # pylint: disable=broad-except
                raise ProgrammingError(f"{ex}") from ex
            desc = cur.description or []
            rows = cur.fetchall()
        finally:
            con.close()
        self.last_queries = queries
        cols = [(d[0], _type_name(d[1])) for d in desc]
        self._set_result(cols, [_fix_row(r) for r in rows])
        logger.info("promagg query: %d scan(s), %d PromQL quer%s, %d row(s), %.0f ms", len(plan.scans),
                    len(queries), "y" if len(queries) == 1 else "ies", len(rows), (time.perf_counter() - t_start) * 1000)

    def _set_result(self, cols: Sequence[tuple[str, str]], rows: list[tuple]) -> None:
        self.description = [(name, typ, None, None, None, None, True) for name, typ in cols]
        self._rows = rows
        self._pos = 0
        self.rowcount = len(rows)

    # EXPLAIN ----------------------------------------------------------------
    def _explain(self, sql: str, analyze: bool) -> "Cursor":
        try:
            stmt = sqlglot.parse_one(sql, read="duckdb")
        except sqlglot.errors.ParseError as ex:
            raise ProgrammingError(f"SQL syntax error: {ex}") from ex
        c = self.connection
        lines: list[str] = []
        if analyze:
            t0 = time.perf_counter()
            self._execute_one(stmt)
            plan = self.last_plan
            queries = list(self.last_queries)
            lines.append(f"-- executed in {(time.perf_counter() - t0) * 1000:.0f} ms, {self.rowcount} row(s)")
        else:
            const_con = duck.locked_session(c.tz_name)
            try:
                plan = self.plan(stmt, const_con)
            finally:
                const_con.close()
            queries = []
        executor = Executor(c.client, c.settings(), concurrency=1)
        from promagg.timegrid import buckets, from_ms

        for i, scan in enumerate(plan.scans, 1):
            if isinstance(scan, AggScan):
                bks = buckets(c.zone, scan.grain, scan.t0, scan.t1)
                runs = executor.runs(bks)
                lines.append(f"-- {i}. PromQL aggregation (mode {scan.mode}) on {scan.metric}: "
                             f"{len(bks)} bucket(s), {len(runs)} run(s), {len(scan.atoms)} expression(s)")
                lines.append(f"--    time range [{c.zone.local(scan.t0)}, {c.zone.local(scan.t1)}) {c.tz_name}")
                if runs:
                    width = runs[0][0]
                    for a, col in scan.atoms.items():
                        lines.append(f"--    {col}: {executor.atom_expr(scan, a, width)}")
            elif isinstance(scan, RowScan):
                lines.append(f"-- {i}. raw samples of {scan.metric}, order {scan.order}, limit {scan.limit}")
            elif isinstance(scan, LabelScan):
                lines.append(f"-- {i}. label values {', '.join(scan.labels)} of {scan.metric} (index lookup)")
            else:
                lines.append(f"-- {i}. promql(): {scan.expr}")
            for note in scan.notes:
                lines.append(f"--    note: {note}")
        for note in plan.notes:
            lines.append(f"-- note: {note}")
        if queries:
            lines.append("-- PromQL sent:")
            lines.extend(f"--    {q}" for q in queries)
        lines.append(f"-- {len(plan.scans) + 1}. DuckDB, on the rows returned above")
        lines.extend(plan.statement.sql(dialect="duckdb", pretty=True).splitlines())
        rows = []
        for line in lines:
            stripped = line.lstrip(" ")
            rows.append((" " * (len(line) - len(stripped)) + stripped,))
        self._set_result([("plan", "VARCHAR")], rows)
        return self


def _no_rows_by_construction(stmt: exp.Expression) -> bool:
    """LIMIT 0 (Superset's column probes): nothing needs to be read."""
    if not isinstance(stmt, exp.Select):
        return False
    limit = stmt.args.get("limit")
    value = limit.expression if isinstance(limit, exp.Limit) else None
    return isinstance(value, exp.Literal) and not value.is_string and str(value.this) == "0"


def _type_name(t: Any) -> str:
    name = str(t).upper()
    if name in ("HUGEINT", "UHUGEINT", "UBIGINT"):
        return "BIGINT"
    if name.startswith("TIMESTAMP WITH TIME ZONE"):
        return "TIMESTAMP WITH TIME ZONE"
    if name.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    return name


def _fix_row(row: tuple) -> tuple:
    return tuple(float(v) if isinstance(v, decimal.Decimal) else v for v in row)


def _quote(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float, decimal.Decimal)):
        return str(v)
    if isinstance(v, dt.datetime):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if isinstance(v, dt.date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, (list, tuple)):
        return "(" + ", ".join(_quote(x) for x in v) + ")"
    return "'" + str(v).replace("'", "''") + "'"


def _bind(sql: str, params: Any) -> str:
    if params is None:
        return sql
    if isinstance(params, dict):
        if not params:
            return sql
        out = re.sub(r"%\(([^)]+)\)s", lambda m: _quote(params[m.group(1)]), sql)
    else:
        seq = list(params)
        if not seq:
            return sql
        it = iter(seq)
        out = re.sub(r"%s", lambda m: _quote(next(it)), sql)
    return out.replace("%%", "%")
