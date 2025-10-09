# services/lead_times/textops.py
from __future__ import annotations

import re
from typing import Optional, Tuple


# Match the header token exactly as typed (case-insensitive), but keep the
# original casing/spacing by slicing around the match.
_LEAD_HEADER_RE = re.compile(r'(?i)lead\s*time\s*:\s*')

# Find the "ready in" phrase (case-insensitive), while preserving original text.
_READY_IN_RE = re.compile(r'(?i)\bready\s*in\b')


def _find_eol_index(s: str, start: int) -> int:
    """
    Return the index of the earliest end-of-line marker at or after `start`,
    considering either a literal '\\n' token or real newlines. If none, return -1.
    """
    i_lit = s.find("\\n", start)
    i_n = s.find("\n", start)
    i_r = s.find("\r", start)
    candidates = [i for i in (i_lit, i_n, i_r) if i != -1]
    return min(candidates) if candidates else -1


def rewrite_detailed_preserving_context(cell_text: str, new_lead: str) -> str:
    """
    Detailed rule:
      - The 'lead time' value is everything AFTER 'Lead Time:' up to the next newline (or end).
      - Keep everything before and after EXACTLY as-is.
      - Replace ONLY that value with `new_lead` (you provide the unit/format).
    """
    if not cell_text:
        return cell_text

    m = _LEAD_HEADER_RE.search(cell_text)
    if not m:
        # No 'Lead Time:' header; return untouched
        return cell_text

    lead_start = m.end()  # right after 'Lead Time:' (incl. spaces)
    lead_end = _find_eol_index(cell_text, lead_start)
    if lead_end == -1:
        # Entire remainder of string is the lead block
        return cell_text[:lead_start] + new_lead

    # Replace only the lead substring; preserve prefix/suffix verbatim
    return cell_text[:lead_start] + new_lead + cell_text[lead_end:]


def _scan_paren_span(s: str, open_idx: int) -> int:
    """
    Given s[open_idx] == '(', return the index JUST AFTER the matching ')'.
    Handles nesting. If unmatched, returns len(s) (i.e., treat as until end).
    """
    depth = 0
    i = open_idx
    n = len(s)
    while i < n:
        ch = s[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i + 1  # just after the closing ')'
        i += 1
    return n  # no closing ')', consume to end


def rewrite_summary_preserving_context(cell_text: str, new_lead: str) -> str:
    """
    Summary rule:
      - Find the 'ready in' phrase (case-insensitive). Everything BEFORE stays as-is.
      - The lead-time VALUE starts at the first non-space after 'ready in'
        and ends right BEFORE:
          * the first '(' at depth 0  (the bracket tail belongs to 'after'), OR
          * the first comma at depth 0, OR
          * a newline/end of string.
      - Keep one full bracket group (if present) EXACTLY as-is, then keep the
        comma (if any) and EVERYTHING after it EXACTLY as-is.
      - Replace ONLY the lead-time VALUE with `new_lead`.
    """
    if not cell_text:
        return cell_text

    m = _READY_IN_RE.search(cell_text)
    if not m:
        return cell_text

    i = m.end()  # index right after the 'ready in' phrase (as typed)
    # Lead value starts at first non-space after the phrase (spaces belong to prefix).
    j = i
    while j < len(cell_text) and cell_text[j].isspace():
        j += 1
    lead_start = j

    # Scan for the boundary: first '(' at depth 0, or first ',' at depth 0,
    # or a newline/end-of-string. We also treat a literal '\n' as EOL.
    n = len(cell_text)
    k = lead_start
    bracket_start: Optional[int] = None

    while k < n:
        # Literal '\n'
        if k + 1 < n and cell_text[k] == "\\" and cell_text[k + 1] == "n":
            break
        # Real newline
        if cell_text[k] in ("\n", "\r"):
            break
        ch = cell_text[k]
        if ch == "(":
            bracket_start = k
            break
        if ch == ",":
            break
        k += 1

    lead_end = k  # exclusive

    # Determine the suffix:
    # - If we hit a bracket at depth 0, include the entire balanced bracket group as 'bracket_tail'.
    # - After that, EVERYTHING remaining (including a trailing comma and the rest) is preserved.
    if bracket_start is not None and bracket_start == lead_end:
        bracket_end = _scan_paren_span(cell_text, bracket_start)
        bracket_tail = cell_text[lead_end:bracket_end]
        after = cell_text[bracket_end:]
    else:
        bracket_tail = ""
        after = cell_text[lead_end:]

    prefix = cell_text[:lead_start]
    # Replace only the lead span; keep prefix, bracket tail, and after verbatim.
    return prefix + new_lead + bracket_tail + after
