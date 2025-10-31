from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Mapping
import re


def resolve_cutoff_sheet_name(scope: str, cfg: Mapping[str, object]) -> str:
    """
    Return the Google Sheets tab name to read cutoffs from, based on scope.
    Falls back to legacy 'cutoff_sheet' if the new keys are not set.
    """
    cut = cfg["lead_times"]["cutoffs"]

    name_can = cut.get("sheet_name_canberra")
    name_reg = cut.get("sheet_name_regional")

    if scope.lower() == "canberra" and name_can:
        return name_can
    if scope.lower() == "regional" and name_reg:
        return name_reg

    raise KeyError(
        "No cutoff sheet configured. "
        "Set google_sheets.cutoffs.sheet_name_canberra / sheet_name_regional."
    )


# ———————————————————————————————————————————————————————————
# Simple parsing
# ———————————————————————————————————————————————————————————

def parse_mapping_codes(cell: str) -> List[str]:
    """
    Split a mapping cell like 'ROLL, ROLLCB,  ZIPSV2' into a cleaned list
    of tokens. No normalisation: we keep whatever is in the sheet.
    """
    if not cell:
        return []
    return [t.strip() for t in str(cell).split(",") if t.strip()]


def codes_from_rows(rows: List[List[str]], mapping_col_idx0: int) -> Set[str]:
    """
    Extract the *set* of codes found in a given column across the rows.
    """
    out: Set[str] = set()
    for r in rows:
        if mapping_col_idx0 < len(r):
            for c in parse_mapping_codes(r[mapping_col_idx0]):
                out.add(c)
    return out


def codes_from_triples(triples: List[tuple[str, str, str]], mapping_pos: int = 1) -> Set[str]:
    """
    Same idea as codes_from_rows(), but for (A, B, C) row triples.
    """
    out: Set[str] = set()
    for t in triples:
        cell = t[mapping_pos] if len(t) > mapping_pos else ""
        for c in parse_mapping_codes(cell):
            out.add(c)
    return out


def _triples(
    rows: list[list[str]],
    a_i: int,
    b_i: int,
    c_i: int,
    *,
    label: str,
    warnings: list[str],
) -> list[tuple[str, str, str]]:
    """
    Return a list of (colA, colB, colC) with rows padded so missing cells become ''.
    Skips fully-blank rows.
    """
    need = max(a_i, b_i, c_i)
    out: list[tuple[str, str, str]] = []
    for rnum, row in enumerate(rows, start=1):
        row = list(row)
        if len(row) <= need:
            row += [""] * (need + 1 - len(row))
        a, b, c = row[a_i], row[b_i], row[c_i]
        if not (str(a).strip() or str(b).strip() or str(c).strip()):
            continue
        out.append((a, b, c))
    return out


# ———————————————————————————————————————————————————————————
# Data model
# ———————————————————————————————————————————————————————————

@dataclass
class ImportResultStore:
    # Per-code record used by excel_out.inject_and_prune()
    lead_rows: Dict[str, Dict]
    # Per-code record used for appending the date
    cutoff_rows: Dict[str, Dict]
    # The set of codes we operate on for this store (from mapping column)
    control_codes: Set[str]
    # Convenience for HTML building
    by_product_html: List[Tuple[str, str]]  # (product_name, lead_time_text)
    # NEW: product -> set(codes) for code-based joins to Cutoffs
    product_to_codes: Dict[str, Set[str]]
    # Direct product -> cutoff mapping from cutoff sheet (for HTML, no code merging)
    product_to_cutoff: Dict[str, str]  # product_name -> cutoff_date


# ———————————————————————————————————————————————————————————
# Validation helpers
# ———————————————————————————————————————————————————————————

def _preview(items: set[str], limit: int = 12) -> str:
    if not items:
        return "—"
    s = sorted(items)
    return (", ".join(s[:limit]) + f", +{len(s) - limit} more") if len(s) > limit else ", ".join(s)


def _validate_scope_sets_or_die(
    *,
    scope: str,                         # "CANBERRA" or "REGIONAL"
    lead_codes: set[str],
    cut_codes: set[str],
    lead_mapping_col_letter: str,       # e.g. 'B'
    cutoff_mapping_col_letter: str,     # e.g. 'C'
) -> None:
    """
    Single-scope validation for the new either/or flow.

    - Only compare the selected scope's Lead set against Cutoffs.
    - Extra codes in the shared Cutoffs sheet (belonging to the other scope) do NOT block the run.
    - We fail only when this scope has leads that are missing in Cutoffs.
    """
    def preview(items: set[str], limit: int = 12) -> str:
        if not items:
            return "—"
        s = sorted(items)
        return ", ".join(s[:limit]) + (f", +{len(items)-limit} more" if len(items) > limit else "")

    missing_in_cutoffs = lead_codes - cut_codes          # present in this scope's leads, missing in Cutoffs
    # Note: cutoffs_not_in_scope = cut_codes - lead  # allowed (shared sheet); do not block

    if missing_in_cutoffs:
        msg = (
            "<div class='flash-compact'>"
            "<strong>Lead Times validation failed — code sets differ</strong> "
            f"(Lead mapping col <code>{lead_mapping_col_letter}</code>, "
            f"Cutoffs mapping col <code>{cutoff_mapping_col_letter}</code>)."
            "<ul style='margin-top:6px'>"
            f"<li><small><strong>Missing in Cutoffs (present in {scope.title()} leads):</strong> "
            f"<code>{preview(missing_in_cutoffs)}</code></small></li>"
            "</ul>"
            "</div>"
        )
        from markupsafe import Markup
        raise ValueError(Markup(msg))


# ———————————————————————————————————————————————————————————
# Import and merge
# ———————————————————————————————————————————————————————————

def import_and_merge(
    *,
    lead_rows: List[List[str]],
    cutoff_rows: List[List[str]],
    lead_cols: Dict[str, str],   # {"product":"A","mapping":"B","lead_time":"C"}
    cutoff_cols: Dict[str, str], # {"product":"B","mapping":"C","cutoff_date":"G"}
    scope: str,
) -> ImportResultStore:
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

    cut_map_i = idx(cutoff_cols["mapping"])
    cut_prod_i = idx(cutoff_cols["product"])
    cut_dt_i   = idx(cutoff_cols["cutoff_date"])

    # Build padded triples so missing cells don't explode downstream
    _noop_warnings: list[str] = []
    lead_triples = _triples(lead_rows, can_prod_i, can_map_i, can_lt_i, label="LEADTIMES", warnings=_noop_warnings)
    cut_triples = _triples(cutoff_rows,   cut_prod_i, cut_map_i, cut_dt_i,  label="CUTOFFS",  warnings=_noop_warnings)

    # Control sets from mapping column (triple[1])
    lead_codes = codes_from_triples(lead_triples, 1)
    cut_codes = codes_from_triples(cut_triples, 1)

    _validate_scope_sets_or_die(
        scope=scope,
        lead_codes=lead_codes,
        cut_codes=cut_codes,
        lead_mapping_col_letter=lead_cols["mapping"],
        cutoff_mapping_col_letter=cutoff_cols["mapping"],
    )

    # Build per-code lead rows and product→codes map
    def make_lead_rows(
        triples: List[Tuple[str, str, str]]
    ) -> Tuple[Dict[str, Dict], List[Tuple[str, str]], Dict[str, Set[str]]]:
        per_code: Dict[str, Dict] = {}
        by_product: List[Tuple[str, str]] = []
        product_to_codes: Dict[str, Set[str]] = {}

        def upper_bound_weeks(text: str) -> float:
            # Extract an upper-bound number for comparison; very forgiving.
            if not text:
                return -1.0
            nums = re.findall(r"(\d+(?:\.\d+)?)", str(text))
            if not nums:
                return -1.0
            try:
                return float(nums[-1])
            except ValueError:
                return -1.0

        for product, mapping_cell, lt_text in triples:
            product = (product or "").strip()
            lt_text = (lt_text or "").strip()
            if not product:
                continue

            codes = parse_mapping_codes(mapping_cell or "")
            if not codes:
                continue

            by_product.append((product, lt_text))

            # maintain product -> set(codes)
            s = product_to_codes.setdefault(product, set())
            for c in codes:
                s.add(c)

            # keep "best" lead text per code by largest upper bound
            score = upper_bound_weeks(lt_text)
            for code in codes:
                rec = per_code.get(code)
                if rec is None or score > rec.get("_score", -1.0):
                    per_code[code] = {
                        "product": product,
                        "lead_time_text": lt_text,
                        "_score": score,
                    }

        for v in per_code.values():
            v.pop("_score", None)

        return per_code, by_product, product_to_codes

    lead_rows, cut_html, lead_prod2codes = make_lead_rows(lead_triples)

    # Cutoff per-code rows and direct product->cutoff mapping
    def make_cutoff_rows(triples: List[Tuple[str, str, str]]) -> Tuple[Dict[str, Dict], Dict[str, str]]:
        out: Dict[str, Dict] = {}
        product_to_cutoff: Dict[str, str] = {}

        for product, mapping_cell, cutoff in triples:
            product = (product or "").strip()
            cutoff = (cutoff or "").strip()
            if not product:
                continue

            # Direct product->cutoff mapping (first occurrence wins for HTML)
            if product not in product_to_cutoff:
                product_to_cutoff[product] = cutoff

            # Per-code dict for Excel generation
            for code in parse_mapping_codes(mapping_cell or ""):
                out[code] = {"product": product, "cutoff": cutoff}
        return out, product_to_cutoff

    cut_by_code, prod_to_cutoff = make_cutoff_rows(cut_triples)

    return ImportResultStore(
            lead_rows=lead_rows,
            cutoff_rows=cut_by_code,
            control_codes=lead_codes,
            by_product_html=sorted(cut_html, key=lambda t: t[0].casefold()),
            product_to_codes=lead_prod2codes,
            product_to_cutoff=prod_to_cutoff,
        )
