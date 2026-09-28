"""Apache Superset DB engine spec for promagg.

Registered through the ``superset.db_engine_specs`` entry point, so installing the wheel
into Superset's virtualenv is enough (nothing in superset_config.py).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import types

from superset.constants import TimeGrain
from superset.db_engine_specs.base import BaseEngineSpec

try:  # SQL Lab parses promagg SQL with the DuckDB grammar
    from sqlglot.dialects.dialect import Dialects

    from superset.sql.parse import SQLGLOT_DIALECTS

    SQLGLOT_DIALECTS.setdefault("promagg", Dialects.DUCKDB)
except Exception:  # pragma: no cover - older Superset versions  # pylint: disable=broad-except
    pass


class PromAggEngineSpec(BaseEngineSpec):
    """Prometheus / Grafana Mimir through promagg: aggregates run as PromQL."""

    engine = "promagg"
    engine_name = "Prometheus / Mimir (PromQL pushdown)"
    engine_aliases = {"promagg"}
    default_driver = "http"
    drivers = {"http": "Prometheus HTTP API", "https": "Prometheus HTTP API over TLS"}
    sqlalchemy_uri_placeholder = "promagg://mimir-query-frontend:8080/prometheus?tenant=<org id>&timezone=Europe/Paris"

    # Superset emulates "series limit" with a pre-query + WHERE label IN (...) (pushed as a
    # regex matcher) instead of a JOIN when joins are disallowed.
    allows_joins = False
    allows_subqueries = True
    allows_alias_in_select = True
    allows_alias_in_orderby = True
    allows_sql_comments = True
    time_groupby_inline = False
    supports_file_upload = False
    supports_dynamic_schema = False
    disable_ssh_tunneling = False

    @classmethod
    def get_column_description_limit_size(cls) -> int:
        """Column types of virtual datasets are probed with LIMIT 0: promagg answers it from
        the metric's labels without reading samples."""
        return 0

    _time_grain_expressions = {
        getattr(TimeGrain, name): expr
        for name, expr in (
            ("SECOND", "DATE_TRUNC('second', {col})"),
            ("FIVE_SECONDS", "TIME_BUCKET(INTERVAL '5 seconds', {col})"),
            ("THIRTY_SECONDS", "TIME_BUCKET(INTERVAL '30 seconds', {col})"),
            ("MINUTE", "DATE_TRUNC('minute', {col})"),
            ("FIVE_MINUTES", "TIME_BUCKET(INTERVAL '5 minutes', {col})"),
            ("TEN_MINUTES", "TIME_BUCKET(INTERVAL '10 minutes', {col})"),
            ("FIFTEEN_MINUTES", "TIME_BUCKET(INTERVAL '15 minutes', {col})"),
            ("THIRTY_MINUTES", "TIME_BUCKET(INTERVAL '30 minutes', {col})"),
            ("HALF_HOUR", "TIME_BUCKET(INTERVAL '30 minutes', {col})"),
            ("HOUR", "DATE_TRUNC('hour', {col})"),
            ("SIX_HOURS", "TIME_BUCKET(INTERVAL '6 hours', {col})"),
            ("DAY", "DATE_TRUNC('day', {col})"),
            ("WEEK", "DATE_TRUNC('week', {col})"),
            ("WEEK_STARTING_MONDAY", "DATE_TRUNC('week', {col})"),
            ("WEEK_STARTING_SUNDAY", "DATE_TRUNC('week', {col} + INTERVAL '1 day') - INTERVAL '1 day'"),
            ("WEEK_ENDING_SATURDAY", "DATE_TRUNC('week', {col} + INTERVAL '1 day') + INTERVAL '5 day'"),
            ("WEEK_ENDING_SUNDAY", "DATE_TRUNC('week', {col}) + INTERVAL '6 day'"),
            ("MONTH", "DATE_TRUNC('month', {col})"),
            ("QUARTER", "DATE_TRUNC('quarter', {col})"),
            ("QUARTER_YEAR", "DATE_TRUNC('quarter', {col})"),
            ("YEAR", "DATE_TRUNC('year', {col})"),
        )
        if hasattr(TimeGrain, name)
    }
    _time_grain_expressions[None] = "{col}"

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "to_timestamp({col})"

    @classmethod
    def epoch_ms_to_dttm(cls) -> str:
        return "epoch_ms({col})"

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime,
                     db_extra: dict[str, Any] | None = None) -> str | None:
        sqla_type = cls.get_sqla_column_type(target_type)
        if isinstance(sqla_type, types.Date) and not isinstance(sqla_type, types.DateTime):
            return f"DATE '{dttm.date().isoformat()}'"
        if isinstance(sqla_type, (types.DateTime, types.TIMESTAMP)):
            return f"""TIMESTAMP '{dttm.isoformat(sep=" ", timespec="microseconds")}'"""
        return None

    @classmethod
    def _extract_error_message(cls, ex: Exception) -> str:
        """promagg's own message, not SQLAlchemy's wrapping of it (Superset's "Test connection"
        re-raises the error as the statement of an empty DBAPIError: "(builtins.NoneType) None
        [SQL: ...]")."""
        orig = getattr(ex, "orig", None)
        if orig is not None and str(orig):
            return str(orig)
        statement = getattr(ex, "statement", None)
        if orig is None and isinstance(statement, str) and statement:
            return statement
        return super()._extract_error_message(ex)

    @classmethod
    def get_datatype(cls, type_code: Any) -> str | None:
        if type_code is None:
            return None
        return str(type_code).upper()

    @classmethod
    def get_function_names(cls, database: Any) -> list[str]:
        """SQL Lab autocomplete: the PromQL functions promagg understands."""
        from promagg.planner import RANGE_FUNCS

        return sorted({n.upper() for n in RANGE_FUNCS} | {"HISTOGRAM_QUANTILE", "SUM", "AVG", "MIN", "MAX",
                                                           "COUNT", "STDDEV_POP", "VAR_POP", "QUANTILE_CONT",
                                                           "MEDIAN", "DATE_TRUNC", "TIME_BUCKET"})
