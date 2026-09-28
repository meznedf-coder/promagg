"""PromQL text: string and regex escaping, selectors, SQL predicates on labels -> matchers.

SQL semantics are kept exactly: a label a series does not have is NULL in SQL (Prometheus
treats it as the empty string), so `node <> 'x'` excludes series without `node`
(`node!="x", node!=""`), `node IS NULL` is `node=""`, etc.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
LABEL_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_RE2_META = set(r"\.+*?()|[]{}^$")


def quote(s: str) -> str:
    """PromQL double-quoted string (Go escapes)."""
    out = []
    for ch in s:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ord(ch) < 0x20:
            out.append(f"\\x{ord(ch):02x}")
        else:
            out.append(ch)
    return '"' + "".join(out) + '"'


def re_escape(s: str) -> str:
    return "".join("\\" + ch if ch in _RE2_META else ch for ch in s)


def like_regex(pattern: str, escape: str | None = None) -> str:
    """SQL LIKE pattern -> RE2 (PromQL regex matchers are anchored at both ends)."""
    out, i = [], 0
    while i < len(pattern):
        ch = pattern[i]
        if escape and ch == escape and i + 1 < len(pattern):
            out.append(re_escape(pattern[i + 1]))
            i += 2
            continue
        out.append(".*" if ch == "%" else "." if ch == "_" else re_escape(ch))
        i += 1
    return "(?s)" + "".join(out)


@dataclass(frozen=True)
class Matcher:
    label: str
    op: str              # = != =~ !~
    value: str

    def text(self) -> str:
        return f"{self.label}{self.op}{quote(self.value)}"


@dataclass
class Selector:
    """One series selector: metric name + matchers (all must hold)."""

    metric: str
    matchers: list[Matcher] = field(default_factory=list)
    never: bool = False               # a contradiction: matches nothing

    def text(self) -> str:
        ms = [m.text() for m in self.matchers]
        if METRIC_NAME.match(self.metric):
            return self.metric + ("{" + ", ".join(ms) + "}" if ms else "")
        return "{" + ", ".join([f"__name__={quote(self.metric)}"] + ms) + "}"

    def with_(self, more: list[Matcher]) -> "Selector":
        return Selector(self.metric, self.matchers + list(more), self.never)


# --------------------------------------------------------------------------- #
# label conditions: a small boolean algebra, normalized to OR of ANDs of matchers
# --------------------------------------------------------------------------- #
class Cond:
    """Condition on labels. dnf() -> list of conjunctions (lists of Matcher); [] = FALSE,
    [[]] = TRUE."""

    def dnf(self) -> list[list[Matcher]]:
        raise NotImplementedError

    def negate(self) -> "Cond":
        raise NotImplementedError


@dataclass
class Atom(Cond):
    matchers: list[Matcher]               # conjunction
    neg: list[list[Matcher]] | None = None  # DNF of the negation (precomputed)

    def dnf(self):
        return [list(self.matchers)]

    def negate(self):
        if self.neg is None:
            raise ValueError("condition cannot be negated")
        return Or([Atom(c, [self.matchers]) for c in self.neg]) if len(self.neg) != 1 else \
            Atom(self.neg[0], [self.matchers])


@dataclass
class Const(Cond):
    value: bool

    def dnf(self):
        return [[]] if self.value else []

    def negate(self):
        return Const(not self.value)


@dataclass
class Never(Cond):
    """Never true, and its negation is given explicitly (SQL NULL logic: NOT NULL is NULL)."""

    neg: Cond | None = None

    def dnf(self):
        return []

    def negate(self):
        return self.neg if self.neg is not None else Never()


@dataclass
class And(Cond):
    parts: list[Cond]

    def dnf(self):
        out: list[list[Matcher]] = [[]]
        for p in self.parts:
            d = p.dnf()
            out = [a + b for a in out for b in d]
            if len(out) > 64:
                raise ValueError("label condition too complex")
        return out

    def negate(self):
        return Or([p.negate() for p in self.parts])


@dataclass
class Or(Cond):
    parts: list[Cond]

    def dnf(self):
        out: list[list[Matcher]] = []
        for p in self.parts:
            out.extend(p.dnf())
        return out

    def negate(self):
        return And([p.negate() for p in self.parts])


def eq(label: str, value: str | None) -> Cond:
    if value is None:
        return Never()                            # label = NULL
    if value == "":                               # a missing label is NULL, never ''
        return Never(Atom([Matcher(label, "!=", "")], [[Matcher(label, "=", "")]]))
    return Atom([Matcher(label, "=", value)], [[Matcher(label, "!=", value), Matcher(label, "!=", "")]])


def ne(label: str, value: str | None) -> Cond:
    # SQL: NULL <> 'x' is not true -> the label must exist
    if value is None:
        return Never()
    if value == "":
        return Atom([Matcher(label, "!=", "")], [[Matcher(label, "=", "")]])
    return Atom([Matcher(label, "!=", value), Matcher(label, "!=", "")], [[Matcher(label, "=", value)], [Matcher(label, "=", "")]])


def in_(label: str, values: list[str]) -> Cond:
    vals = sorted({v for v in values if v not in (None, "")})
    if not vals:
        return Const(False)
    if len(vals) == 1:
        return eq(label, vals[0])
    rx = "|".join(re_escape(v) for v in vals)
    return Atom([Matcher(label, "=~", rx)], [[Matcher(label, "!~", rx), Matcher(label, "!=", "")]])


def not_in(label: str, values: list[str]) -> Cond:
    vals = sorted({v for v in values if v not in (None, "")})
    if any(v is None for v in values):   # NOT IN (.., NULL) is never true
        return Never(in_(label, [v for v in values if v is not None]))
    if not vals:
        return is_null(label, negate=True)
    rx = "|".join(re_escape(v) for v in vals)
    return Atom([Matcher(label, "!~", rx), Matcher(label, "!=", "")], [[Matcher(label, "=~", rx)], [Matcher(label, "=", "")]])


def regex(label: str, rx: str, negate: bool = False) -> Atom:
    """Anchored regex over an existing label (SQL: NULL never matches)."""
    pos = Atom([Matcher(label, "=~", rx), Matcher(label, "!=", "")], [[Matcher(label, "!~", rx)], [Matcher(label, "=", "")]])
    return pos if not negate else Atom([Matcher(label, "!~", rx), Matcher(label, "!=", "")], [[Matcher(label, "=~", rx)], [Matcher(label, "=", "")]])


def is_null(label: str, negate: bool = False) -> Atom:
    if negate:
        return Atom([Matcher(label, "!=", "")], [[Matcher(label, "=", "")]])
    return Atom([Matcher(label, "=", "")], [[Matcher(label, "!=", "")]])


def _merge_equalities(conj: list[list[Matcher]]) -> list[list[Matcher]]:
    """(rest AND l="a") OR (rest AND l="b") -> rest AND l=~"a|b" (Superset writes series
    limits as OR chains)."""
    groups: dict[tuple, list[str]] = {}
    order: list[tuple] = []
    out: list[list[Matcher]] = []
    for c in conj:
        eqs = [m for m in c if m.op == "=" and m.value != ""]
        if len(eqs) != 1:
            out.append(c)
            continue
        rest = tuple(sorted({m for m in c if m != eqs[0]}, key=lambda m: (m.label, m.op, m.value)))
        key = (eqs[0].label, rest)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(eqs[0].value)
    for key in order:
        label, rest = key
        vals = sorted(set(groups[key]))
        m = Matcher(label, "=", vals[0]) if len(vals) == 1 else Matcher(label, "=~", "|".join(re_escape(v) for v in vals))
        out.append(list(rest) + [m])
    return out


def selectors(metric: str, cond: Cond | None, extra: list[Matcher] | None = None) -> list[Selector]:
    """Selectors whose union is exactly the series satisfying cond (the caller unions the
    per-selector results with PromQL `or`, which drops the duplicates of a series)."""
    conj = _merge_equalities(cond.dnf()) if cond is not None else [[]]
    out = []
    for ms in conj:
        s = Selector(metric, list(extra or []) + _simplify(ms))
        if _contradiction(s.matchers):
            continue
        out.append(s)
    # a selector with no matcher at all matches everything: keep just that one
    for s in out:
        if not [m for m in s.matchers if m not in (extra or [])]:
            return [s]
    return out


def _simplify(ms: list[Matcher]) -> list[Matcher]:
    seen, out = set(), []
    for m in ms:
        if m not in seen:
            seen.add(m)
            out.append(m)
    # label="x" makes label!="" redundant
    eqs = {m.label for m in out if m.op == "=" and m.value != ""}
    return [m for m in out if not (m.op == "!=" and m.value == "" and m.label in eqs)]


def _contradiction(ms: list[Matcher]) -> bool:
    by: dict[str, list[Matcher]] = {}
    for m in ms:
        by.setdefault(m.label, []).append(m)
    for label, group in by.items():
        eqv = {m.value for m in group if m.op == "="}
        if len(eqv) > 1:
            return True
        if eqv:
            v = next(iter(eqv))
            for m in group:
                if m.op == "!=" and m.value == v:
                    return True
                if m.op in ("=~", "!~"):
                    try:
                        hit = re.fullmatch(_py_regex(m.value), v) is not None
                    except re.error:
                        continue
                    if (m.op == "=~") != hit:
                        return True
    return False


def _py_regex(rx: str) -> str:
    return rx.replace("(?s)", "", 1) if rx.startswith("(?s)") else rx


def union(exprs: list[str]) -> str:
    """PromQL union of instant vectors of the same metric (duplicates are dropped by `or`)."""
    if len(exprs) == 1:
        return exprs[0]
    return " or ".join(f"({e})" for e in exprs)
