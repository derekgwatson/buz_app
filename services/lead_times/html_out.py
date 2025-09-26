# services/lead_times/html_out.py
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Set
import html as _html


_TRAIL_PUNCT_RE = re.compile(r"[\s:–—-]+$")  # spaces, colon, hyphen, en/em dash at the end


# Keywords
_CHRISTMAS_RE = re.compile(r'(?i)\b(xmas|christmas|pre-?christmas)\b')
_CUTOFF_RE = re.compile(
    r'(?i)\b('
    r'cut[\s-]?off|cut[\s-]?offs|deadline|closing|close(?:s|d)?|'
    r'final day|last day|order by'
    r')\b'
)
_CUTOFF_SUFFIX_RE = re.compile(
    r'(\s*(?:\*{3}\s*)?(?:xmas|christmas|pre-?christmas)\s+cut[\s-]?off\b.*)$',
    re.IGNORECASE
)


# Also treat these phrases as cut-off lines even if "cutoff" isn't present
_EXTRA_PHRASES_RE = re.compile(r'(?i)\b(pre-?christmas delivery|before christmas)\b')


def to_pasteable_html_bold_cutoff_suffix(text: str, *, collapse_blank_lines: bool = True) -> str:
    """
    Produce paste-ready HTML with <br /> per line, and ONLY bold the trailing
    '... Christmas cutoff ...' suffix (e.g., '***CHRISTMAS CUTOFF 26/9/25***').
    """
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in t.split("\n")]

    if collapse_blank_lines:
        collapsed, last_blank = [], False
        for ln in lines:
            if ln.strip():
                collapsed.append(ln); last_blank = False
            else:
                if not last_blank: collapsed.append(""); last_blank = True
        lines = collapsed

    out: list[str] = []
    for ln in lines:
        if not ln.strip():
            out.append("")  # preserve blank line
            continue

        m = _CUTOFF_SUFFIX_RE.search(ln)
        if m:
            head = ln[:m.start(1)]
            tail = ln[m.start(1):]  # the cutoff suffix to bold
            safe_head = _html.escape(head)
            safe_tail = _html.escape(tail)
            out.append(f"{safe_head}<strong>{safe_tail}</strong>")
        else:
            out.append(_html.escape(ln))

    return "<br />\n".join(out)


def _looks_like_christmas_cutoff(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    return (
        bool(_CHRISTMAS_RE.search(s) and _CUTOFF_RE.search(s)) or
        bool(re.search(r'(?i)\bchristmas\s+cut[\s-]?off\b', s)) or
        bool(_EXTRA_PHRASES_RE.search(s))
    )


def to_pasteable_html(text: str, *, collapse_blank_lines: bool = True) -> str:
    """
    Convert arbitrary text to paste-ready HTML:
      - normalize CRLF/CR to LF
      - (optionally) collapse runs of blank lines
      - bold 'Christmas cutoff' lines
      - join with <br /> after every line
    """
    # Normalize newlines
    t = text.replace("\r\n", "\n").replace("\r", "\n")

    # Split + trim right-side spaces
    lines = [ln.rstrip() for ln in t.split("\n")]

    # Collapse multiple blank lines to a single blank line (optional)
    if collapse_blank_lines:
        collapsed: list[str] = []
        last_blank = False
        for ln in lines:
            if ln.strip():
                collapsed.append(ln)
                last_blank = False
            else:
                if not last_blank:
                    collapsed.append("")  # keep one empty
                last_blank = True
        lines = collapsed

    # Escape + bold the cutoff lines
    out: list[str] = []
    for ln in lines:
        safe = _html.escape(ln)
        if ln.strip() and _looks_like_christmas_cutoff(ln):
            safe = f"<strong>{safe}</strong>"
        out.append(safe)

    # One <br /> per line
    return "<br />\n".join(out)


def _tidy_product(name: str) -> str:
    """Collapse whitespace and strip any trailing ':', dashes, etc."""
    if not name:
        return ""
    s = re.sub(r"\s+", " ", str(name)).strip()
    return _TRAIL_PUNCT_RE.sub("", s)


def build_html_lines(
    by_product: Iterable[tuple],
    *,
    product_to_codes: Dict[str, Set[str]],
    cutoffs_by_code: Dict[str, Dict],
    placeholder: str = "TBC",
) -> List[str]:
    """
    Build display lines from (product, lead_time_text) pairs and append a cutoff marker
    when ANY code mapped to that product has a cutoff date.

    - Matching is done by *Buz code* (via product_to_codes), not by product text.
    - If lead_time_text is blank, uses `placeholder` (default 'TBC').
    - Output is sorted by product name (case-insensitive).
    """

    # product -> cutoff date (via any of its codes)
    cutoff_for_product: Dict[str, str] = {}
    for product, codes in product_to_codes.items():
        for code in codes:
            rec = cutoffs_by_code.get(code)
            if not rec:
                continue
            cutoff = str(rec.get("cutoff") or "").strip()
            if cutoff:
                # keep the first non-empty cutoff we see for this product
                cutoff_for_product.setdefault(product, cutoff)
                break

    lines: List[str] = []
    seen_products: set[str] = set()

    for row in by_product:
        if not row:
            continue

        product = str(row[0]).strip()
        if not product or product in seen_products:
            continue
        seen_products.add(product)

        lead_text = str(row[1]).strip() if len(row) > 1 else ""
        if not lead_text:
            lead_text = placeholder

        # Tidy to avoid '::' and trailing colons/dashes from source text
        display_product = _tidy_product(product).replace("::", ":")

        line = f"{display_product}: {lead_text}"
        cutoff = cutoff_for_product.get(product)
        if cutoff:
            line += f" ***CHRISTMAS CUTOFF {cutoff}***"

        lines.append(line)

    lines.sort(key=lambda s: s.split(":", 1)[0].casefold())
    return lines


def build_pasteable_html(
    by_product: Iterable[tuple],
    *,
    product_to_codes: Dict[str, Set[str]],
    cutoffs_by_code: Dict[str, Dict],
    placeholder: str = "TBC",
    collapse_blank_lines: bool = True,
) -> str:
    lines = build_html_lines(
        by_product,
        product_to_codes=product_to_codes,
        cutoffs_by_code=cutoffs_by_code,
        placeholder=placeholder,
    )
    text = "\n".join(lines)
    return to_pasteable_html_bold_cutoff_suffix(text, collapse_blank_lines=collapse_blank_lines)
