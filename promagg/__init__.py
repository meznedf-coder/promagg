"""promagg - Prometheus / Grafana Mimir as a SQL database for Apache Superset.

Superset (or any DB-API / SQLAlchemy client) sends plain SQL. promagg translates it into
PromQL, so the metrics backend aggregates (sum by, rate, *_over_time, histogram_quantile,
...) over billions of samples and returns small results; an embedded, locked-down DuckDB
evaluates what is left (ORDER BY, LIMIT, HAVING, arithmetic, joins of aggregated results).
"""

from promagg.dbapi import (  # noqa: F401
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    apilevel,
    connect,
    paramstyle,
    threadsafety,
)

__version__ = "0.2.1"
