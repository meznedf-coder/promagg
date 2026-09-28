"""SQLAlchemy dialect ``promagg://`` (SQLAlchemy 1.4 and 2.0).

SQL is compiled PostgreSQL/DuckDB style (double-quoted identifiers), which is what promagg
parses. Reflection lists metric names and their labels.

URL:  promagg://[user[:password]@]host[:port]/[path]?tenant=...&timezone=Europe/Paris&...
      promagg+https://...          (TLS)
      path: "prometheus" for Grafana Mimir (default), empty for Prometheus ("promagg://h:9090/")
      user "bearer" + password <token>: Authorization: Bearer <token>
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql.base import PGCompiler, PGDDLCompiler, PGIdentifierPreparer, PGTypeCompiler
from sqlalchemy.engine import default

SQL_TYPES = {"VARCHAR": sqltypes.String, "DOUBLE": sqltypes.Float, "TIMESTAMP": sqltypes.TIMESTAMP,
             "DATE": sqltypes.Date, "BIGINT": sqltypes.BigInteger}


class PromAggIdentifierPreparer(PGIdentifierPreparer):
    """Quote everything that is not a plain lower-case identifier (metric names have
    capitals and colons: node_memory_MemAvailable_bytes, job:rate5m)."""

    def _requires_quotes(self, value: str) -> bool:
        return True if value != value.lower() or ":" in value else super()._requires_quotes(value)


class PromAggDialect(default.DefaultDialect):
    name = "promagg"
    driver = "http"
    supports_statement_cache = True
    default_paramstyle = "pyformat"

    statement_compiler = PGCompiler
    ddl_compiler = PGDDLCompiler
    type_compiler = PGTypeCompiler
    preparer = PromAggIdentifierPreparer

    supports_alter = False
    supports_sequences = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_multivalues_insert = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False
    implicit_returning = False
    _backslash_escapes = False
    supports_smallserial = False
    supports_identity_columns = False
    supports_native_enum = False

    @classmethod
    def dbapi(cls):  # SQLAlchemy 1.4
        import promagg.dbapi as module

        return module

    @classmethod
    def import_dbapi(cls):  # SQLAlchemy 2.0
        import promagg.dbapi as module

        return module

    def create_connect_args(self, url) -> tuple[list[Any], dict[str, Any]]:
        kwargs: dict[str, Any] = dict(url.query)
        kwargs["host"] = url.host or "localhost"
        if url.port:
            kwargs["port"] = url.port
        if url.username:
            kwargs["user"] = url.username
        if url.password:
            kwargs["password"] = url.password
        if url.database is not None:
            kwargs.setdefault("path", url.database)
        if url.drivername.endswith("+https"):
            kwargs.setdefault("scheme", "https")
        return [], kwargs

    def initialize(self, connection) -> None:
        self.server_version_info = (1, 0)
        self.default_schema_name = "default"
        self.default_isolation_level = "AUTOCOMMIT"

    def _get_server_version_info(self, connection):
        return (1, 0)

    def _get_default_schema_name(self, connection):
        return "default"

    def get_isolation_level(self, dbapi_connection):
        return "AUTOCOMMIT"

    def set_isolation_level(self, dbapi_connection, level):
        pass

    def get_default_isolation_level(self, dbapi_conn):
        return "AUTOCOMMIT"

    def do_rollback(self, dbapi_connection) -> None:
        pass

    def do_commit(self, dbapi_connection) -> None:
        pass

    def do_ping(self, dbapi_connection) -> bool:
        return self._raw(dbapi_connection).ping()

    @staticmethod
    def _raw(connection):
        from promagg.dbapi import Connection

        obj = connection
        for _ in range(6):
            if isinstance(obj, Connection):
                return obj
            nxt = getattr(obj, "dbapi_connection", None)
            if nxt is None or nxt is obj:
                nxt = getattr(obj, "connection", None)
            if nxt is None or nxt is obj:
                break
            obj = nxt
        raise RuntimeError("not a promagg connection")

    def get_schema_names(self, connection, **kw) -> list[str]:
        return ["default"]

    def has_schema(self, connection, schema_name, **kw) -> bool:
        return schema_name in (None, "default")

    def get_table_names(self, connection, schema=None, **kw) -> list[str]:
        raw = self._raw(connection)
        return ([raw.all_metrics] if raw.all_metrics else []) + raw.list_tables()   # one dataset: every metric

    def has_table(self, connection, table_name, schema=None, **kw) -> bool:
        return self._raw(connection).table_meta(table_name) is not None

    def get_view_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_materialized_view_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_temp_table_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_temp_view_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_sequence_names(self, connection, schema=None, **kw) -> list[str]:
        return []

    def get_columns(self, connection, table_name, schema=None, **kw) -> list[dict[str, Any]]:
        meta = self._raw(connection).table_meta(table_name)
        if meta is None:
            from sqlalchemy.exc import NoSuchTableError

            raise NoSuchTableError(table_name)
        return [{"name": c.name, "type": SQL_TYPES.get(c.sql_type, sqltypes.String)(), "nullable": True,
                 "default": None, "autoincrement": False, "comment": c.comment} for c in meta.columns.values()]

    def get_pk_constraint(self, connection, table_name, schema=None, **kw) -> dict[str, Any]:
        return {"constrained_columns": [], "name": None}

    def get_primary_keys(self, connection, table_name, schema=None, **kw) -> list[str]:
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw) -> list[dict]:
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw) -> dict[str, Any]:
        meta = self._raw(connection).table_meta(table_name)
        text = None
        if meta is not None and meta.kind == "all":
            text = "Every metric: filter metric_name (one dataset for all the metrics)"
        elif meta is not None:
            text = f"Prometheus {meta.kind}" + (f": {meta.help}" if meta.help else "")
        return {"text": text}

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        return None


class PromAggHttpsDialect(PromAggDialect):
    supports_statement_cache = True     # SQLAlchemy asks every dialect class to say it
    driver = "https"
