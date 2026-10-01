"""Answers that would make a result silently incomplete are errors: a warning (part of the data left out by
a store or a remote read that failed, series a function dropped) and native histogram samples, which have
no single value (they were left out of the rows)."""

from __future__ import annotations

import json

import pytest

from promagg.client import PromClient
from promagg.errors import OperationalError, ProgrammingError


class Answer:
    def __init__(self, payload: dict, status: int = 200):
        self.status, self.data = status, json.dumps(payload).encode()


def client_answering(payload: dict) -> PromClient:
    c = PromClient("http://mimir.invalid:9009/prometheus")
    c.pool.request = lambda *a, **kw: Answer(payload)
    return c


VECTOR = {"resultType": "vector", "result": [{"metric": {"node": "a"}, "value": [1790000000, "3"]}]}


def test_a_warning_is_an_error_not_a_result():
    c = client_answering({"status": "success", "data": VECTOR,
                          "warnings": ["remote_read: error sending request: connection refused"]})
    with pytest.raises(OperationalError, match="may be incomplete.*remote_read"):
        c.query("sum(m)", 1_790_000_000_000)
    c = client_answering({"status": "success", "data": VECTOR, "warnings": [
        'PromQL warning: bucket label "le" is missing or has a malformed value of "x"']})
    with pytest.raises(OperationalError, match="bucket label"):
        c.query("histogram_quantile(0.9, m)", 1_790_000_000_000)


def test_infos_and_asked_truncations_are_not_errors():
    c = client_answering({"status": "success", "data": VECTOR,
                          "infos": ['PromQL info: metric might not be a counter, name does not end in _total']})
    assert c.query("rate(m[5m])", 1_790_000_000_000)[0].points == [(1_790_000_000_000, 3.0)]
    c = client_answering({"status": "success", "data": VECTOR,     # Prometheus 2 sends infos as warnings
                          "warnings": ['PromQL info: metric might not be a counter, name does not end in _total']})
    assert c.query("rate(m[5m])", 1_790_000_000_000)
    c = client_answering({"status": "success", "data": [{"__name__": "m"}],
                          "warnings": ["results truncated due to limit"]})
    assert c.series(["m"], None, None, limit=1) == [{"__name__": "m"}]


def test_native_histogram_samples_are_not_left_out():
    c = client_answering({"status": "success", "data": {"resultType": "vector", "result": [
        {"metric": {"node": "a"}, "value": [1790000000, "3"]},
        {"metric": {"node": "b"}, "histogram": [1790000000, {"count": "4", "sum": "9", "buckets": []}]}]}})
    with pytest.raises(ProgrammingError, match="1 sample.*native histograms.*promql"):
        c.query("sum_over_time(m[1h])", 1_790_000_000_000)
    c = client_answering({"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"node": "b"}, "histograms": [[1790000000, {"count": "4", "sum": "9"}]]}]}})
    with pytest.raises(ProgrammingError, match="native histograms"):
        c.query_range("m", 1_790_000_000_000, 1_790_000_060_000, 60_000)


def test_the_anchored_probe_is_kept_per_backend_and_tenant(monkeypatch):
    from promagg import client as C

    C._ANCHORED.clear()
    calls = []
    c = client_answering({"status": "success", "data": {"resultType": "vector", "result": []}})
    real = c.pool.request
    c.pool.request = lambda *a, **kw: (calls.append(1), real(*a, **kw))[1]
    assert c.anchored_ok() and c.anchored_ok() and len(calls) == 1
    C._ANCHORED.clear()
    refused = client_answering({"status": "error", "errorType": "bad_data", "error":
                                'invalid parameter "query": experimental extended range selector modifier '
                                '"anchored" is not enabled for tenant'})
    refused.pool.request = (lambda f: (lambda *a, **kw: Answer(json.loads(f().data), 400)))(
        lambda: Answer({"status": "error", "error": "anchored is not enabled for tenant"}))
    assert not refused.anchored_ok()
    C._ANCHORED.clear()


def test_a_failed_anchored_probe_never_fails_a_query(monkeypatch):
    """Busy, a limit, a warning, a gateway error: no answer about anchored ranges, extrapolated values, asked
    again a minute later; a query without rate / increase / delta never asks."""
    from promagg import client as C

    monkeypatch.setattr(C.time, "sleep", lambda s: None)
    C._ANCHORED.clear()
    for status, payload in ((503, {"status": "error", "error": "busy"}),
                            (422, {"status": "error", "error": "the query exceeded the maximum number of samples"}),
                            (200, {"status": "success", "data": {"resultType": "vector", "result": []},
                                   "warnings": ["remote_read: connection refused"]})):
        c = PromClient("http://mimir.invalid:9009/prometheus")
        c.pool.request = (lambda st, pl: lambda *a, **kw: Answer(pl, st))(status, payload)
        assert c.anchored_ok() is False
        assert C._ANCHORED[(c.url, c.tenant)][0] - C.time.monotonic() <= 61
        C._ANCHORED.clear()


def test_the_probe_is_asked_only_by_a_query_that_needs_it():
    import datetime as dt

    from promagg.planner import Settings
    from promagg.timegrid import Zone

    asked = []
    s = Settings(zone=Zone("UTC"), now_ms=0, probe_anchored=lambda: asked.append(1) or True)
    assert asked == []
    assert s.anchored_ranges() and s.anchored_ranges() and asked == [1]
    assert not Settings(zone=Zone("UTC"), now_ms=0, increase="prometheus",
                        probe_anchored=lambda: asked.append(2) or True).anchored_ranges() and asked == [1]
