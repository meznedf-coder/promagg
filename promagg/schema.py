"""Tables and columns: one table per metric name.

Columns of a metric table:
  ts        TIMESTAMP  sample time (naive, connection time zone); in aggregates, the start
                       of the time bucket
  <labels>  VARCHAR    one column per label of the metric (NULL when a series lacks it)
  value     DOUBLE     sample value
  rate      DOUBLE     counters only: per-second rate over the time bucket (SUM(rate))
  increase  DOUBLE     counters only: increase over the time bucket (SUM(increase))
"""

from __future__ import annotations

import fnmatch
import re
import threading
import time
from dataclasses import dataclass, field

from promagg.client import PromClient

VIRTUAL = ("rate", "increase")
RESERVED = ("ts", "value") + VIRTUAL
COUNTER_SUFFIXES = ("_total", "_count", "_sum", "_bucket")


@dataclass
class Column:
    name: str          # SQL name
    sql_type: str      # VARCHAR | TIMESTAMP | DOUBLE
    role: str          # ts | label | value | rate | increase
    label: str | None = None
    comment: str = ""


@dataclass
class MetricMeta:
    name: str
    kind: str                                   # counter | gauge | histogram | summary | unknown
    labels: list[str]
    help: str = ""
    unit: str = ""
    columns: dict[str, Column] = field(default_factory=dict)

    @property
    def is_counter(self) -> bool:
        return self.kind in ("counter", "histogram") or self.name.endswith(COUNTER_SUFFIXES)

    def label_column(self, label: str) -> str:
        return f"label_{label}" if label in RESERVED else label

    def build(self) -> "MetricMeta":
        cols = [Column("ts", "TIMESTAMP", "ts", comment="Sample time (in aggregates: start of the time bucket)")]
        for lb in sorted(self.labels):
            name = self.label_column(lb)
            cols.append(Column(name, "VARCHAR", "label", lb, f"label {lb}"))
        what = f"{self.kind}" + (f", {self.unit}" if self.unit else "")
        cols.append(Column("value", "DOUBLE", "value",
                           comment=(self.help + " " if self.help else "") + f"({what}) raw sample value"))
        if self.is_counter:
            cols.append(Column("rate", "DOUBLE", "rate", comment="per-second rate over the time bucket, per series "
                               "(SUM(rate) = total per second; PromQL rate)"))
            cols.append(Column("increase", "DOUBLE", "increase", comment="increase over the time bucket, per series "
                               "(SUM(increase) = total count; PromQL increase)"))
        self.columns = {c.name: c for c in cols}
        return self


def infer_kind(name: str, meta_type: str | None) -> str:
    t = (meta_type or "").lower()
    if t in ("counter", "gauge", "histogram", "summary", "gaugehistogram"):
        if t == "histogram" and not name.endswith(("_bucket", "_sum", "_count")):
            return "histogram"          # native histogram
        return "counter" if t in ("histogram", "summary") and name.endswith(("_bucket", "_sum", "_count")) else t
    if name.endswith(COUNTER_SUFFIXES):
        return "counter"
    return "gauge" if t in ("", "unknown", "info", "stateset") else t


class Schema:
    """Metric names and their labels, cached per connection target."""

    def __init__(self, client: PromClient, window_ms: int | None, patterns: list[str] | None = None,
                 ttl: float = 300.0, now_ms=None, counters: list[str] | None = None) -> None:
        self.client = client
        self.counters = [c for c in (counters or []) if c]
        self.window_ms = window_ms
        self.patterns = [p for p in (patterns or []) if p]
        self.ttl = ttl
        self.now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._names: tuple[float, list[str]] | None = None
        self._metas: dict[str, tuple[float, MetricMeta | None]] = {}
        self._metadata: tuple[float, dict] | None = None
        self._lock = threading.Lock()

    def _range(self) -> tuple[int | None, int | None]:
        if not self.window_ms:
            return None, None
        end = self.now_ms()
        return end - self.window_ms, end

    def _visible(self, name: str) -> bool:
        if not self.patterns:
            return True
        return any(fnmatch.fnmatchcase(name, p) or (p.startswith("~") and re.fullmatch(p[1:], name))
                   for p in self.patterns)

    def metric_names(self) -> list[str]:
        with self._lock:
            if self._names and time.time() - self._names[0] < self.ttl:
                return self._names[1]
        start, end = self._range()
        names = sorted(n for n in self.client.label_values("__name__", None, start, end) if self._visible(n))
        with self._lock:
            self._names = (time.time(), names)
        return names

    def _all_metadata(self) -> dict:
        with self._lock:
            if self._metadata and time.time() - self._metadata[0] < self.ttl:
                return self._metadata[1]
        md = self.client.metadata()
        with self._lock:
            self._metadata = (time.time(), md)
        return md

    def meta(self, name: str) -> MetricMeta | None:
        with self._lock:
            hit = self._metas.get(name)
            if hit and time.time() - hit[0] < self.ttl:
                return hit[1]
        if not self._visible(name):
            return None
        start, end = self._range()
        sel = '{__name__="%s"}' % name.replace("\\", "\\\\").replace('"', '\\"')
        labels = [lb for lb in self.client.label_names([sel], start, end) if lb != "__name__"]
        meta: MetricMeta | None
        if not labels and name not in self.metric_names():
            meta = None
        else:
            md = (self._all_metadata().get(name) or [{}])[0]
            family = re.sub(r"_(bucket|sum|count|total)$", "", name)
            if not md and family != name:
                md = (self._all_metadata().get(family) or [{}])[0]
            kind = infer_kind(name, md.get("type"))
            if any(fnmatch.fnmatchcase(name, p) for p in self.counters):
                kind = "counter"                     # declared: untyped counters (node_vmstat_*)
            meta = MetricMeta(name, kind, labels, md.get("help", ""), md.get("unit", "")).build()
        with self._lock:
            self._metas[name] = (time.time(), meta)
        return meta

    def invalidate(self) -> None:
        with self._lock:
            self._names = None
            self._metas.clear()
            self._metadata = None
