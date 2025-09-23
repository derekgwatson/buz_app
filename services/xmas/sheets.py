
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set
from datetime import date

from .model import LeadRow, CutoffRow
from .parse import parse_upper_bound_weeks, parse_au_date, col_letter_to_index


@dataclass
class ImportResult:
    lead_rows: Dict[str, LeadRow]          # per code
    cutoff_rows: Dict[str, CutoffRow]      # per code
    by_product_html: List[Tuple[str, str, Tuple[str, ...]]]  # (product, lead_text, codes)
    control_codes: List[str]


def _norm_codes(cell: str) -> List[str]:
    if not cell:
        return []
    parts = [p.strip().upper() for p in cell.split(",")]
    return [p for p in parts if p]


def _best_lead_row_for_code(existing: Optional[LeadRow], candidate: LeadRow) -> LeadRow:
    if existing is None:
        return candidate
    return candidate if candidate.upper_bound_weeks >= existing.upper_bound_weeks else existing


def import_and_merge(
    *,
    canberra_rows: List[List[str]],
    regional_rows: List[List[str]],
    cutoff_rows: List[List[str]],
    lead_cols: Dict[str, str],
    cutoff_cols: Dict[str, str],
) -> Dict[str, ImportResult]:
    def idx(letter: str) -> int:
        return col_letter_to_index(letter) - 1

    lead_idx = {k: idx(v) for k, v in lead_cols.items()}
    cut_idx = {k: idx(v) for k, v in cutoff_cols.items()}

    def build_leads(rows: List[List[str]]) -> Dict[str, LeadRow]:
        per_code: Dict[str, LeadRow] = {}
        for r in rows:
            try:
                product = (r[lead_idx["product"]] or "").strip()
                mapping_cell = (r[lead_idx["mapping"]] if lead_idx["mapping"] < len(r) else "")
                codes = _norm_codes(mapping_cell)
                lead_text = (r[lead_idx["lead_time"]] or "").strip()
            except Exception:
                continue
            low_product = product.lower()
            low_map = str(mapping_cell).strip().lower()
            low_lead = lead_text.lower()
            if not product and not codes and not lead_text:
                continue
            if low_product == "product" or "inventory code" in low_map or low_lead.startswith("lead"):
                continue
            if not product or not codes or not lead_text:
                continue
            ub = parse_upper_bound_weeks(lead_text)
            lr = LeadRow(product=product, codes=tuple(codes), lead_text=lead_text, upper_bound_weeks=ub)
            for code in codes:
                per_code[code] = _best_lead_row_for_code(per_code.get(code), lr)
        return per_code

    can_leads = build_leads(canberra_rows)
    reg_leads = build_leads(regional_rows)

    per_code_cut: Dict[str, CutoffRow] = {}
    for r in cutoff_rows:
        try:
            product = (r[cut_idx["product"]] or "").strip()
            codes = _norm_codes(r[cut_idx["mapping"]] if cut_idx["mapping"] < len(r) else "")
            cutoff_raw = (r[cut_idx["cutoff_date"]] or "").strip()
        except Exception:
            continue
        if not product or not codes or not cutoff_raw:
            continue
        try:
            cutoff_d = parse_au_date(cutoff_raw)
        except Exception:
            continue
        cr = CutoffRow(product=product, codes=tuple(codes), cutoff_date=cutoff_d)
        for code in codes:
            prev = per_code_cut.get(code)
            if prev is None or cutoff_d < prev.cutoff_date:
                per_code_cut[code] = cr

    def validate(store_name: str, leads: Dict[str, LeadRow]) -> None:
        lead_codes = set(leads.keys())
        cut_codes = set(per_code_cut.keys())
        if lead_codes != cut_codes:
            missing_in_cut = sorted(lead_codes - cut_codes)
            extra_in_cut = sorted(cut_codes - lead_codes)
            lines = [f"Validation failed for {store_name}: code sets differ."]
            if missing_in_cut:
                lines.append("  Missing in CUT: " + ", ".join(missing_in_cut))
            if extra_in_cut:
                lines.append("  Extra in CUT: " + ", ".join(extra_in_cut))
            raise ValueError("\n".join(lines))

    validate("CANBERRA", can_leads)
    validate("REGIONAL", reg_leads)

    def build_product_html(leads: Dict[str, LeadRow]) -> List[tuple]:
        seen_products: Set[str] = set()
        items: List[tuple] = []
        tri: List[tuple] = []
        for code, lr in leads.items():
            tri.append((lr.product.upper(), lr.product, lr.lead_text, lr.codes))
        tri.sort(key=lambda t: t[0])
        for _, product, lead_text, codes in tri:
            if product in seen_products:
                continue
            items.append((product, lead_text, codes))
            seen_products.add(product)
        return items

    can_html = build_product_html(can_leads)
    reg_html = build_product_html(reg_leads)

    can_codes = sorted(set(can_leads.keys()))
    reg_codes = sorted(set(reg_leads.keys()))

    return {
        "CANBERRA": ImportResult(can_leads, per_code_cut, can_html, can_codes),
        "REGIONAL": ImportResult(reg_leads, per_code_cut, reg_html, reg_codes),
    }
