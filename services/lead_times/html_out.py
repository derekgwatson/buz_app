
from __future__ import annotations

from typing import List, Tuple
from .parse import display_date_ddmmyy


def build_html_lines(items: List[Tuple[str, str, tuple]], *, cutoffs_by_code) -> List[str]:
    lines: List[str] = []
    for product, lead_text, codes in items:
        line = f"{product}: {lead_text}"
        dates = []
        for c in codes:
            cr = cutoffs_by_code.get(c)
            if cr:
                dates.append(cr.cutoff_date)
        if dates:
            dd = min(dates)
            line += f" ***CHRISTMAS CUTOFF {display_date_ddmmyy(dd)}***"
        lines.append(line)
    return lines
