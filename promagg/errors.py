"""PEP 249 exception hierarchy."""


class Warning(Exception):  # noqa: A001  (PEP 249 name)
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class PushdownError(NotSupportedError):
    """The query cannot be computed inside Prometheus / Mimir exactly (the message says
    why and how to write it instead)."""


class LimitError(OperationalError):
    """The metrics backend refused the query for one of its limits (samples, chunks, series,
    time): the executor splits the time range and retries when it can."""
