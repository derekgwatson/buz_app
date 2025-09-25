# services/lead_times/html_out.py
from __future__ import annotations
import re
from typing import Dict, Iterable, List


_TRAIL_PUNCT_RE = re.compile(r"[\s:–—-]+$")  # spaces, colon, hyphen, en/em dash at the end


def _tidy_product(name: str) -> str:
    """Collapse whitespace and remove any trailing ':'/dashes so we don't render '::'."""
    if not name:
        return ""
    s = re.sub(r"\s+", " ", str(name)).strip()
    return _TRAIL_PUNCT_RE.sub("", s)


def build_html_lines(
    by_product: Iterable[tuple],
    *,
    cutoffs_by_code: Dict[str, Dict],
    placeholder: str = "TBC",
) -> List[str]:
    """
    Build display lines from (product, lead_time_text) pairs.
    Accepts either 2-tuples or longer; only the first two items are used.
    Appends '***CHRISTMAS CUTOFF ...***' when any code maps to that product with a cutoff.
    Output is sorted alphabetically by product name.

    If lead_time_text is blank, uses `placeholder` (default 'TBC').
    """
    # product -> cutoff date (string as-is)
    cutoff_by_product: Dict[str, str] = {}
    for rec in cutoffs_by_code.values():
        product = str(rec.get("product") or "").strip()
        cutoff = str(rec.get("cutoff") or "").strip()
        if product and cutoff:
            cutoff_by_product[product] = cutoff

    lines: List[str] = []
    seen_products: set[str] = set()

    for row in by_product:
        if not row:
            continue

        product = str(row[0]).strip()
        lead_text = str(row[1]).strip() if len(row) > 1 else ""

        if not product or product in seen_products:
            continue
        seen_products.add(product)

        # Use TBC when lead time missing
        lead_text = lead_text if lead_text else placeholder

        # Normalise common double-colon artefacts
        display_product = product.replace("::", ":")

        line = f"{display_product}: {lead_text}"
        cutoff = cutoff_by_product.get(product)
        if cutoff:
            line += f" ***CHRISTMAS CUTOFF {cutoff}***"

        lines.append(line)

    lines.sort(key=lambda s: s.split(":", 1)[0].casefold())
    return lines

