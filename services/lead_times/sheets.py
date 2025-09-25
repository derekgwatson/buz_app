from __future__ import annotations
from markupsafe import Markup
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple
import re


# ———————————————————————————————————————————————————————————
# Normalization + parsing
# ———————————————————————————————————————————————————————————

_CODE_RE = re.compile(r"[A-Z0-9]+")


def _norm_code(token: str) -> str:
    """Normalize a code. Keep only A–Z and 0–9; uppercase."""
    if not token:
        return ""
    return "".join(_CODE_RE.findall(str(token).upper()))


def parse_mapping_codes(cell: str) -> List[str]:
    """
    Split a mapping cell like 'ROLL, ROLLCB,  ZIPSV2' into
    a cleaned list of codes. Empty items are dropped.
    """
    if not cell:
        return []
    parts = [p.strip() for p in str(cell).split(",")]
    return [c for c in (_norm_code(p) for p in parts) if c]


def codes_from_rows(rows: List[List[str]], mapping_col_idx0: int) -> Set[str]:
    """
    Extract the control set of codes appearing in a column of comma-separated
    lists. NO filtering by allowed/ignored lists.
    """
    out: Set[str] = set()
    for r in rows:
        if mapping_col_idx0 < len(r):
            for c in parse_mapping_codes(r[mapping_col_idx0]):
                out.add(c)
    return out


@dataclass
class ImportResultStore:
    # lead_rows: per-code record used by excel_out.inject_and_prune()
    lead_rows: Dict[str, Dict]
    # cutoff_rows: per-code record used for appending the date
    cutoff_rows: Dict[str, Dict]
    # control_codes: the set of codes we operate on (from mapping column)
    control_codes: Set[str]
    # convenience for HTML building
    by_product_html: List[Tuple[str, str]]  # (product_name, lead_time_text)


def _preview(items: set[str], limit: int = 12) -> str:
    if not items:
        return "—"
    s = sorted(items)
    return (", ".join(s[:limit]) + f", +{len(s) - limit} more") if len(s) > limit else ", ".join(s)


def _validate_three_way_sets_or_die(
    *,
    can_codes: set[str],
    reg_codes: set[str],
    cut_codes: set[str],
    lead_mapping_col_letter: str,     # e.g. 'B'
    cutoff_mapping_col_letter: str,   # e.g. 'C'
) -> None:
    """
    Enforce equality across the three sets and report what's missing
    per-source relative to the other two.
    """
    # What’s missing in each source (present in the other two)
    missing_in_can = (reg_codes & cut_codes) - can_codes
    missing_in_reg = (can_codes & cut_codes) - reg_codes
    missing_in_cut = (can_codes & reg_codes) - cut_codes

    if not (missing_in_can or missing_in_reg or missing_in_cut):
        return  # all three sets match

    # Counts for quick sanity
    n_can, n_reg, n_cut = len(can_codes), len(reg_codes), len(cut_codes)

    msg = (
        "<div class='flash-compact'>"
        "<strong>Lead Times validation failed — code sets differ</strong> "
        f"(Lead mapping col <code>{lead_mapping_col_letter}</code>, "
        f"Cutoffs mapping col <code>{cutoff_mapping_col_letter}</code>). "
        f"[Canberra: {n_can} · Regional: {n_reg} · Cutoffs: {n_cut}] "
        "Missing in "
        f"Canberra: <code>{_preview(missing_in_can)}</code> · "
        f"Regional: <code>{_preview(missing_in_reg)}</code> · "
        f"Cutoffs: <code>{_preview(missing_in_cut)}</code>"
        "<details class='mt-1'><summary>Show full lists</summary>"
        f"<div><small><strong>Missing in Canberra ({len(missing_in_can)}):</strong> "
        f"<code>{', '.join(sorted(missing_in_can)) or '—'}</code></small></div>"
        f"<div><small><strong>Missing in Regional ({len(missing_in_reg)}):</strong> "
        f"<code>{', '.join(sorted(missing_in_reg)) or '—'}</code></small></div>"
        f"<div><small><strong>Missing in Cutoffs ({len(missing_in_cut)}):</strong> "
        f"<code>{', '.join(sorted(missing_in_cut)) or '—'}</code></small></div>"
        "</details></div>"
    )
    raise ValueError(Markup(msg))


def import_and_merge(
    *,
    canberra_rows: List[List[str]],
    regional_rows: List[List[str]],
    cutoff_rows: List[List[str]],
    lead_cols: Dict[str, str],   # {"product":"A","mapping":"B","lead_time":"C"}
    cutoff_cols: Dict[str, str], # {"product":"B","mapping":"C","cutoff_date":"G"}
) -> Dict[str, ImportResultStore]:
    """
    Build the structures needed by excel/html steps.
    IMPORTANT: control_codes are derived solely from the mapping columns (no filtering).
    """
    # Column letters → 0-based indexes
    def idx(letter: str) -> int:
        letter = letter.strip().upper()
        n = 0
        for ch in letter:
            n = n * 26 + (ord(ch) - 64)
        return n - 1

    can_map_i = idx(lead_cols["mapping"])
    can_prod_i = idx(lead_cols["product"])
    can_lt_i   = idx(lead_cols["lead_time"])

    reg_map_i = can_map_i

    cut_map_i = idx(cutoff_cols["mapping"])
    cut_prod_i = idx(cutoff_cols["product"])
    cut_dt_i   = idx(cutoff_cols["cutoff_date"])

    # Build control sets straight from mapping columns (NO allowed/ignored filters)
    can_codes = codes_from_rows(canberra_rows, can_map_i)
    reg_codes = codes_from_rows(regional_rows, reg_map_i)
    cut_codes = codes_from_rows(cutoff_rows, cut_map_i)

    _validate_three_way_sets_or_die(
        can_codes=can_codes,
        reg_codes=reg_codes,
        cut_codes=cut_codes,
        lead_mapping_col_letter=lead_cols["mapping"],  # 'B'
        cutoff_mapping_col_letter=cutoff_cols["mapping"],  # 'C'
    )

    # Build per-code lead rows (keep the winning lead-time text, using your "largest upper bound" rule)
    def make_lead_rows(rows: List[List[str]]) -> Tuple[Dict[str, Dict], List[Tuple[str, str]]]:
        per_code: Dict[str, Dict] = {}
        by_product: List[Tuple[str, str]] = []

        def upper_bound_weeks(text: str) -> float:
            # Extract an upper-bound number for comparison; very forgiving.
            # Examples: "2-3 weeks" -> 3, "1.5 weeks" -> 1.5, "9 - 11 weeks" -> 11, "4-5 wks" -> 5
            if not text:
                return -1.0
            s = str(text)
            nums = re.findall(r"(\d+(?:\.\d+)?)", s)
            if not nums:
                return -1.0
            try:
                return float(nums[-1])
            except ValueError:
                return -1.0

        for r in rows:
            product = r[can_prod_i].strip() if can_prod_i < len(r) else ""
            lt_text = r[can_lt_i].strip() if can_lt_i < len(r) else ""
            if not product:
                continue

            codes = parse_mapping_codes(r[can_map_i] if can_map_i < len(r) else "")
            if not codes:
                continue

            by_product.append((product, lt_text))

            score = upper_bound_weeks(lt_text)
            for code in codes:
                rec = per_code.get(code)
                if rec is None or score > rec.get("_score", -1.0):
                    per_code[code] = {
                        "product": product,
                        "lead_time_text": lt_text,
                        "_score": score,
                    }

        # strip internal score before returning
        for v in per_code.values():
            v.pop("_score", None)
        return per_code, by_product

    can_leads, can_html = make_lead_rows(canberra_rows)
    reg_leads, reg_html = make_lead_rows(regional_rows)

    # Cutoff per-code rows
    def make_cutoff_rows(rows: List[List[str]]) -> Dict[str, Dict]:
        out: Dict[str, Dict] = {}
        for r in rows:
            product = r[cut_prod_i].strip() if cut_prod_i < len(r) else ""
            cutoff = r[cut_dt_i].strip() if cut_dt_i < len(r) else ""
            if not product:
                continue
            for code in parse_mapping_codes(r[cut_map_i] if cut_map_i < len(r) else ""):
                out[code] = {"product": product, "cutoff": cutoff}
        return out

    cut_by_code = make_cutoff_rows(cutoff_rows)

    return {
        "CANBERRA": ImportResultStore(
            lead_rows=can_leads,
            cutoff_rows=cut_by_code,
            control_codes=can_codes,
            by_product_html=sorted(can_html, key=lambda t: t[0].casefold()),
        ),
        "REGIONAL": ImportResultStore(
            lead_rows=reg_leads,
            cutoff_rows=cut_by_code,
            control_codes=reg_codes,
            by_product_html=sorted(reg_html, key=lambda t: t[0].casefold()),
        ),
    }
