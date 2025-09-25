
from __future__ import annotations

import math
import re
from datetime import datetime, date
from typing import Optional

_RANGE_RE = re.compile(
    r"""
    (?P<a>\d+(?:\.\d+)?)       # first number
    \s*(?:-|–|—|to)\s*         # dash or 'to'
    (?P<b>\d+(?:\.\d+)?)       # second number
    \s*(?P<u>w(?:eeks?)?|wk?s?|days?|business\s+days?)?  # units
    """,
    re.IGNORECASE | re.VERBOSE,
)

_SINGLE_RE = re.compile(
    r"""
    (?<!\d)                    # not preceded by a digit
    (?P<n>\d+(?:\.\d+)?)       # number
    \s*(?P<u>w(?:eeks?)?|wk?s?|days?|business\s+days?)   # units
    (?!\d)                     # not followed by a digit
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _to_weeks(n: float, unit: Optional[str]) -> float:
    if unit is None:
        return n
    u = unit.lower().strip()
    if u.startswith("w"):
        return n
    if u.startswith("wk"):
        return n
    # 'days' and 'business days': treat as business days -> 5 per week, ceil conservatively
    return math.ceil(n / 5.0)


def parse_upper_bound_weeks(text: str) -> float:
    """
    Parse all ranges and single durations from a freeform lead-time string.
    Return the maximum upper bound in weeks.
    Examples:
      '2–3 weeks' -> 3
      '14 days' -> ceil(14/5)=3
      '2–3 weeks (PC 4–5 weeks)' -> 5
    If nothing found, return 0.
    """
    if not text:
        return 0.0
    best = 0.0
    for m in _RANGE_RE.finditer(text):
        a = float(m.group("a"))
        b = float(m.group("b"))
        u = m.group("u")
        hi = max(a, b)
        best = max(best, _to_weeks(hi, u))
    for m in _SINGLE_RE.finditer(text):
        n = float(m.group("n"))
        u = m.group("u")
        best = max(best, _to_weeks(n, u))
    return float(best)


def parse_au_date(s: str) -> date:
    """
    Accept 'YYYY-MM-DD', 'dd/mm/yy', 'dd/mm/yyyy'.
    Normalize to date. Raise ValueError if invalid.
    """
    ss = s.strip()
    if not ss:
        raise ValueError("empty date")
    if "-" in ss and len(ss) >= 8:
        return datetime.strptime(ss, "%Y-%m-%d").date()
    # handle dd/mm/yy or dd/mm/yyyy
    parts = ss.split("/")
    if len(parts) == 3:
        d, m, y = parts
        if len(y) == 2:
            y = ("20" + y) if int(y) <= 69 else ("19" + y)
        return datetime.strptime(f"{d}-{m}-{y}", "%d-%m-%Y").date()
    raise ValueError(f"Unrecognized date format: {s}")


def display_date_ddmmyy(d: date) -> str:
    return d.strftime("%d/%m/%y")


def col_letter_to_index(letter: str) -> int:
    s = letter.strip().upper()
    total = 0
    for ch in s:
        total = total * 26 + (ord(ch) - ord("A") + 1)
    return total
