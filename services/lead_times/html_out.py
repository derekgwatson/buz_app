# services/lead_times/html_out.py
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Set


_TRAIL_PUNCT_RE = re.compile(r"[\s:–—-]+$")  # spaces, colon, hyphen, en/em dash at the end


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
