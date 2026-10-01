"""HTTP client for the Prometheus query API.

Works with Grafana Mimir (URL prefix /prometheus), Prometheus, Thanos, Cortex and
VictoriaMetrics: only the standard endpoints are used (query, query_range, series, labels,
label values, metadata, buildinfo). Queries are POSTed as form data (long label filters do
not hit URL length limits). Busy answers (429, 502-504, connection resets) are retried.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import threading
import time
from typing import Any, NamedTuple

import urllib3

from promagg.errors import DatabaseError, LimitError, OperationalError, ProgrammingError

logger = logging.getLogger(__name__)

# a query frontend that is busy (queue full, rate limited, restarting) answers 429 / 5xx:
# "retry later". Several dashboard charts at once can cause it.
BUSY_RETRY_DELAYS = (0.5, 1.5, 4.0)
TENANT_LABEL = "__tenant_id__"        # added by Mimir to every series of a multi-tenant query
RETRY_STATUS = {429, 502, 503, 504}
ANCHORED_TTL = 600.0                  # seconds a backend's answer to the anchored-range probe is kept
_ANCHORED: dict[tuple, tuple[float, bool]] = {}


class Series(NamedTuple):
    labels: dict[str, str]
    points: list[tuple[int, float]]      # (epoch ms, value)


def fmt_time(ms: int) -> str:
    """Epoch milliseconds -> the API's seconds with exactly three decimals (no float noise)."""
    s, r = divmod(int(ms), 1000)
    return f"{s}.{r:03d}"


def _ms(ts: Any) -> int:
    return int(round(float(ts) * 1000))


def _series(result_type: str, result: Any) -> tuple[list[Series], int]:
    out: list[Series] = []
    skipped = 0
    if result_type == "vector":
        for r in result:
            if "value" in r:
                t, v = r["value"]
                out.append(Series(r.get("metric", {}), [(_ms(t), float(v))]))
            else:           # native histogram sample: no float value
                skipped += 1
    elif result_type == "matrix":
        for r in result:
            pts = [(_ms(t), float(v)) for t, v in r.get("values") or ()]
            skipped += len(r.get("histograms") or ())
            out.append(Series(r.get("metric", {}), pts))
    elif result_type == "scalar":
        t, v = result
        out.append(Series({}, [(_ms(t), float(v))]))
    return out, skipped


def _floats_only(expr: str, skipped: int) -> None:
    """Native histogram samples have no float value: leaving them out returned a part of the result."""
    if skipped:
        raise ProgrammingError(
            f"{skipped} sample(s) of the result are native histograms, which have no single value: "
            "the result would leave them out. Query them with promql() and a function that returns "
            "numbers (histogram_count, histogram_sum, histogram_quantile, histogram_fraction). "
            f"Query: {expr[:300]}")


def _incomplete(path: str, warnings: list[str]) -> str:
    return (f"the metrics backend answered {path} with a warning, so the result may be incomplete or "
            f"wrong: {'; '.join(warnings)[:600]}. No result is returned from it.")


def _benign(warning: str) -> bool:
    """Warnings that do not change a result: PromQL "info" annotations (Prometheus 2 sends them as
    warnings), and the truncation of label / series lists asked for with limit (existence probes)."""
    w = str(warning)
    return w.startswith("PromQL info") or "truncated due to limit" in w


class PromClient:
    """One client per connection; thread safe (the executor runs queries in parallel)."""

    def __init__(self, url: str, tenant: str | None = None, user: str | None = None,
                 password: str | None = None, token: str | None = None, verify: bool = True,
                 ca_certs: str | None = None, client_cert: str | None = None,
                 client_key: str | None = None, timeout: float = 120.0, max_connections: int = 8,
                 headers: dict[str, str] | None = None) -> None:
        self.url = url.rstrip("/")
        self.tenant = tenant
        for opt, path in (("ca_certs", ca_certs), ("client_cert", client_cert), ("client_key", client_key)):
            if path and not os.path.isfile(path):
                raise OperationalError(f"{opt}: file not found or not readable: {path}")
        self.timeout = timeout
        kw: dict[str, Any] = {"maxsize": max_connections, "block": False, "retries": False,
                              "timeout": urllib3.Timeout(connect=10.0, read=timeout)}
        if self.url.startswith("https"):
            kw["cert_reqs"] = "CERT_REQUIRED" if verify else "CERT_NONE"
            if ca_certs:
                kw["ca_certs"] = ca_certs
            if client_cert:
                kw["cert_file"] = client_cert
            if client_key:
                kw["key_file"] = client_key
            if not verify:
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.pool = urllib3.PoolManager(**kw)
        self.headers = {"Accept": "application/json", "Accept-Encoding": "gzip",
                        "User-Agent": "promagg"}
        if tenant:
            self.headers["X-Scope-OrgID"] = tenant
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        elif user is not None:
            raw = f"{user}:{password or ''}".encode()
            self.headers["Authorization"] = "Basic " + base64.b64encode(raw).decode()
        self.headers.update(headers or {})
        self._left_open: bool | None = None
        self._lock = threading.Lock()
        self.requests = 0                    # statistics (EXPLAIN ANALYZE, tests)

    # ------------------------------------------------------------------ #
    def _call(self, method: str, path: str, fields: list[tuple[str, str]] | None = None,
              timeout: float | None = None, tenant: str | None = None) -> Any:
        url = self.url + path
        headers = self.headers if tenant is None else {**self.headers, "X-Scope-OrgID": tenant}
        last: Exception | None = None
        for attempt in range(len(BUSY_RETRY_DELAYS) + 1):
            try:
                with self._lock:
                    self.requests += 1
                if method == "POST":
                    r = self.pool.request("POST", url, fields=fields or [], encode_multipart=False,
                                          headers=headers,
                                          timeout=urllib3.Timeout(connect=10.0, read=timeout or self.timeout))
                else:
                    r = self.pool.request("GET", url, fields=fields or [], headers=headers,
                                          timeout=urllib3.Timeout(connect=10.0, read=timeout or self.timeout))
            except (urllib3.exceptions.SSLError, FileNotFoundError) as ex:
                raise OperationalError(_tls_advice(self.url, ex)) from ex
            except urllib3.exceptions.HTTPError as ex:
                cause = getattr(ex, "reason", None) or (ex.args[1] if len(ex.args) > 1 else None)
                if isinstance(cause, (urllib3.exceptions.SSLError, FileNotFoundError, PermissionError)):
                    raise OperationalError(_tls_advice(self.url, cause)) from ex
                last = ex
                if attempt < len(BUSY_RETRY_DELAYS):
                    time.sleep(BUSY_RETRY_DELAYS[attempt])
                    continue
                raise OperationalError(f"cannot reach {self.url}: {ex}") from ex
            if r.status in RETRY_STATUS and attempt < len(BUSY_RETRY_DELAYS):
                logger.info("promagg: %s answered %s, retrying", path, r.status)
                time.sleep(BUSY_RETRY_DELAYS[attempt])
                continue
            return self._decode(r, path)
        raise OperationalError(f"{self.url}{path}: {last}")

    def _decode(self, r: Any, path: str) -> Any:
        text = r.data.decode("utf-8", "replace")
        try:
            payload = json.loads(text)
        except ValueError:
            payload = None
        if r.status == 200 and isinstance(payload, dict) and payload.get("status") == "success":
            # a warning says that part of the data was left out (a store or remote read that failed, series
            # a function dropped): an error here, never a result (infos are only logged)
            for w in payload.get("infos") or ():
                logger.debug("promagg: %s: %s", path, w)
            bad = []
            for w in payload.get("warnings") or ():
                if _benign(w):
                    logger.debug("promagg: %s: %s", path, w)
                else:
                    bad.append(str(w))
            if bad:
                raise OperationalError(_incomplete(path, bad))
            return payload.get("data")
        err = (payload or {}).get("error") if isinstance(payload, dict) else None
        err = err or _page_text(text)[:500] or f"HTTP {r.status}"
        low = err.lower()
        if r.status == 400 and ("ssl certificate" in low or "client certificate" in low):
            raise OperationalError(f"the gateway of {self.url} refused the connection: {err}. It requires a "
                                   "client certificate (mTLS): set client_cert and client_key (and ca_certs) "
                                   "to a certificate it trusts.")
        if "tenant id" in low and ("too many" in low or "multiple" in low):
            raise OperationalError(f"{err}. This backend does not accept queries over several tenants "
                                   f"(tenant={self.tenant}): enable tenant federation in Mimir "
                                   "(-tenant-federation.enabled=true on the query-frontends and queriers), "
                                   "or connect with a single tenant.")
        if r.status in (401, 403):
            raise OperationalError(f"access refused by {self.url} (HTTP {r.status}): {err}. Check "
                                   "the user/password or token and the tenant (X-Scope-OrgID).")
        if r.status == 404:
            raise OperationalError(f"{self.url}{path} not found: check the URL path (Mimir: "
                                   "/prometheus, Prometheus: nothing).")
        if r.status == 400:
            raise ProgrammingError(f"the metrics backend refused the query: {err}")
        if r.status == 422 or (r.status in (503, 504) and "deadline" in err.lower()):
            raise LimitError(_limit_advice(err))
        raise OperationalError(f"{self.url}{path}: HTTP {r.status}: {err}")

    # ------------------------------------------------------------------ #
    def query(self, expr: str, t_ms: int, timeout: float | None = None) -> list[Series]:
        data = self._call("POST", "/api/v1/query", [("query", expr), ("time", fmt_time(t_ms))], timeout)
        out, skipped = _series(data.get("resultType", ""), data.get("result"))
        _floats_only(expr, skipped)
        return out

    def query_range(self, expr: str, start_ms: int, end_ms: int, step_ms: int,
                    timeout: float | None = None) -> list[Series]:
        data = self._call("POST", "/api/v1/query_range", [
            ("query", expr), ("start", fmt_time(start_ms)), ("end", fmt_time(end_ms)),
            ("step", fmt_time(step_ms))], timeout)
        out, skipped = _series(data.get("resultType", ""), data.get("result"))
        _floats_only(expr, skipped)
        return out

    def label_names(self, match: list[str] | None, start_ms: int | None, end_ms: int | None) -> list[str]:
        fields = [("match[]", m) for m in match or ()]
        if start_ms is not None:
            fields += [("start", fmt_time(start_ms)), ("end", fmt_time(end_ms))]
        return list(self._call("POST", "/api/v1/labels", fields) or [])

    def label_values(self, name: str, match: list[str] | None, start_ms: int | None,
                     end_ms: int | None, limit: int | None = None) -> list[str]:
        fields = [("match[]", m) for m in match or ()]
        if start_ms is not None:
            fields += [("start", fmt_time(start_ms)), ("end", fmt_time(end_ms))]
        if limit:
            fields.append(("limit", str(limit)))
        from urllib.parse import quote

        return list(self._call("GET", f"/api/v1/label/{quote(name, safe='')}/values", fields) or [])

    def series(self, match: list[str], start_ms: int | None, end_ms: int | None,
               limit: int | None = None) -> list[dict[str, str]]:
        fields = [("match[]", m) for m in match]
        if start_ms is not None:
            fields += [("start", fmt_time(start_ms)), ("end", fmt_time(end_ms))]
        if limit:
            fields.append(("limit", str(limit)))
        return list(self._call("POST", "/api/v1/series", fields) or [])

    def metadata(self, metric: str | None = None) -> dict[str, list[dict]]:
        fields = [("metric", metric)] if metric else []
        try:
            return dict(self._call("GET", "/api/v1/metadata", fields) or {})
        except OperationalError:
            return {}

    def buildinfo(self) -> dict:
        try:
            return dict(self._call("GET", "/api/v1/status/buildinfo") or {})
        except (OperationalError, ProgrammingError):
            return {}

    def tenants(self) -> list[str]:
        """The tenants of the connection ("a|b": tenant federation in Mimir)."""
        return [t for t in (self.tenant or "").split("|") if t]

    def _ruler(self, path: str, key: str) -> list[tuple[str | None, dict]]:
        """(tenant, item) from the ruler API, which takes a single tenant: with several (a|b, or
        tenants set by a gateway), each tenant is asked on its own."""
        tenants = self.tenants()
        if len(tenants) < 2:
            try:
                return [(None, x) for x in (self._call("GET", path) or {}).get(key) or []]
            except (ProgrammingError, OperationalError) as ex:
                if "org id" not in str(ex).lower():
                    raise
                # the gateway sets several tenants for this account: find them, ask each one
                tenants = [t for t in self.label_values(TENANT_LABEL, None, None, None) if t]
                if len(tenants) < 2:
                    raise
        out: list[tuple[str | None, dict]] = []
        for t in tenants:
            try:
                items = (self._call("GET", path, tenant=t) or {}).get(key) or []
            except (ProgrammingError, OperationalError) as ex:
                if "org id" in str(ex).lower():
                    raise OperationalError(
                        f"alerts and rules are read one tenant at a time, but the gateway of {self.url} replaces "
                        "the tenant header (X-Scope-OrgID) by several tenants: let it pass the tenant the client "
                        "sends (checked against the account), or connect with a single tenant.") from ex
                raise
            out += [(t, x) for x in items]
        return out

    def alerts(self) -> list[dict]:
        """Alerts of the ruler; with several tenants, labelled with __tenant_id__ as in federated
        query results."""
        return [{**a, "labels": {**(a.get("labels") or {}), TENANT_LABEL: t}} if t else a
                for t, a in self._ruler("/api/v1/alerts", "alerts")]

    def rules(self) -> list[dict]:
        """Rule groups of the ruler (with several tenants: each group gets a "tenant" key)."""
        return [{**g, "tenant": t} if t else g for t, g in self._ruler("/api/v1/rules", "groups")]

    def anchored_ok(self) -> bool:
        """Can this backend, for this tenant, evaluate anchored ranges (the experimental extended range
        selectors of Prometheus 3 / Mimir 3): rate / increase / delta without extrapolation, the sample
        before the window included, so that the increase of a bucket is the exact sum of the counter's
        increments. Asked once per 10 minutes."""
        key = (self.url, self.tenant)
        hit = _ANCHORED.get(key)
        if hit is not None and hit[0] > time.monotonic():
            return hit[1]
        t = (int(time.time()) // 60 - 10) * 60 * 1000
        ttl = ANCHORED_TTL
        try:
            self.query("increase(promagg_anchored_probe[1m] anchored)", t)
            ok = True
        except ProgrammingError:                     # 400: "not enabled for tenant", or a parse error
            ok = False
        except DatabaseError as ex:                  # busy, a limit, a warning, unreachable: no answer
            logger.info("promagg: the anchored-range probe of %s failed (%s): extrapolated values", self.url, ex)
            ok, ttl = False, 60.0                    # asked again a minute later
        _ANCHORED[key] = (time.monotonic() + ttl, ok)
        return ok

    def range_left_open(self) -> bool:
        """Range selectors are left-open (t-range, t] in Prometheus 3 / Mimir 3 and closed
        [t-range, t] before. A subquery over vector(1) tells which, without any data."""
        if self._left_open is None:
            t = (int(time.time()) // 60 - 10) * 60 * 1000
            res = self.query("count_over_time(vector(1)[2s:1s])", t)
            n = int(res[0].points[0][1]) if res and res[0].points else 2
            self._left_open = n == 2
        return self._left_open

    def close(self) -> None:
        self.pool.clear()


def _page_text(text: str) -> str:
    """The message of an error page: JSON / plain text as is, HTML (gateways) by its title."""
    t = text.strip()
    if t[:1] != "<":
        return t
    import re

    m = re.search(r"<title>(.*?)</title>", t, re.S | re.I) or re.search(r"<h1>(.*?)</h1>", t, re.S | re.I)
    return re.sub(r"\s+", " ", m.group(1)).strip() if m else re.sub(r"<[^>]+>|\s+", " ", t).strip()


def _tls_advice(url: str, ex: BaseException) -> str:
    err = str(ex)
    low = err.lower()
    if "certificate_verify_failed" in low or "certificate verify failed" in low:
        if "hostname" in low or "ip address mismatch" in low:
            tip = "the certificate does not name this host: connect with a name it lists"
        else:
            tip = "set ca_certs to the CA file that signed the gateway's certificate (verify_certs=false only for tests)"
        return f"the TLS certificate of {url} is not trusted ({err[:200]}): {tip}."
    if "certificate required" in low or "handshake failure" in low or "unknown ca" in low or "bad certificate" in low:
        return (f"TLS handshake with {url} refused ({err[:200]}): the gateway probably requires a client "
                "certificate it trusts (client_cert, client_key).")
    if isinstance(ex, FileNotFoundError) or "no such file" in low:
        return f"TLS file not found ({err[:200]}): check ca_certs, client_cert and client_key."
    return f"TLS error with {url}: {err[:300]}"


def _limit_advice(err: str) -> str:
    low = err.lower()
    tip = ""
    if "samples" in low or "chunk" in low or "series" in low or "resolution" in low:
        tip = (" The query reads too much data for the backend's limits: narrow the time range, "
               "use a coarser time grain, or filter on labels (Mimir limits: -querier.max-samples, "
               "-querier.max-fetched-series-per-query, -querier.max-fetched-chunk-bytes-per-query).")
    elif "timeout" in low or "deadline" in low:
        tip = " The query took too long: narrow the time range or filter on labels."
    return f"the metrics backend could not run the query: {err}.{tip}"
