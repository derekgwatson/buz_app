# services/lead_times/api.py
from __future__ import annotations

import html
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple
from openpyxl import load_workbook

from .excel_out import (
    inject_and_prune,
    save_review_only_workbook,
    InjectResult,
)
from .html_out import build_html_lines
from .sheets import import_and_merge, codes_from_rows


def _need(d: dict, *path):
    cur = d
    trail = []
    for k in path:
        trail.append(k)
        if not isinstance(cur, dict) or k not in cur:
            raise ValueError(f"Config missing `{'/'.join(trail)}`")
        cur = cur[k]
    return cur


def _opt(d: dict, default, *path):
    try:
        return _need(d, *path)
    except ValueError:
        return default


def _valid_codes_from_templates(*paths: str) -> set[str]:
    """Union of sheet names from the uploaded templates (exact, case-sensitive)."""
    codes: set[str] = set()
    for p in paths:
        wb = load_workbook(filename=p, keep_vba=True, data_only=True)
        codes.update(wb.sheetnames)
    return codes


def _col_idx(a1: str) -> int:
    """Convert a column letter (e.g. 'C') to a 0-based index."""
    a1 = a1.strip().upper()
    n = 0
    for ch in a1:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _cutoff_attach_note(
    warnings: list[str],
    store_name: str,
    by_product_html: list[tuple[str, str]],
    product_to_codes: dict[str, set[str]],
    cutoff_rows: dict[str, dict],
) -> None:
    """
    Report how many products actually received a CHRISTMAS CUTOFF banner.
    A product is 'attached' if ANY of its mapped buz codes has a cutoff date.
    """
    cutoff_codes_with_date = {
        code
        for code, rec in cutoff_rows.items()
        if str(rec.get("cutoff", "")).strip()
    }
    products = [str(p).strip() for p, _ in by_product_html if str(p).strip()]
    attached = [
        p for p in products
        if product_to_codes.get(p, set()) & cutoff_codes_with_date
    ]
    if attached:
        sample = ", ".join(sorted(attached)[:5]) + ("…" if len(attached) > 5 else "")
        warnings.append(
            f"[{store_name}] CHRISTMAS CUTOFF appended to {len(attached)} product(s) (e.g., {sample})."
        )
    else:
        if cutoff_codes_with_date:
            cutoff_products = {
                str(rec.get("product", "")).strip()
                for code, rec in cutoff_rows.items()
                if code in cutoff_codes_with_date and str(rec.get("product", "")).strip()
            }
            missing_in_leads = sorted(set(cutoff_products) - set(products))
            if missing_in_leads:
                sample = ", ".join(missing_in_leads[:3]) + (
                    "…" if len(missing_in_leads) > 3 else ""
                )
                warnings.append(
                    f"[{store_name}] No banners appended. "
                    f"{len(missing_in_leads)} cutoff product(s) aren’t present in this store’s "
                    f"Lead Times mapping (e.g., {sample})."
                )


def run_publish(
    *,
    gsheets_service,
    lead_times_cfg: dict,
    detailed_template_path: str,
    summary_template_path: str,
    save_dir: str,
    scope: Tuple[str, ...] = ("CANBERRA", "REGIONAL"),
) -> dict:
    """Read Sheets, validate/merge, generate HTML + Excel files (or review-only)."""

    if not isinstance(lead_times_cfg, dict) or "lead_times_ss" not in lead_times_cfg:
        raise ValueError("Missing or invalid config: get_cfg('lead_times') did not return expected structure")

    # 1) Read Sheets
    lt_block = lead_times_cfg["lead_times_ss"]
    lt_id = lt_block["sheet_id"]
    t_can = lt_block["tabs"]["canberra"]
    t_reg = lt_block["tabs"]["regional"]
    lt_hdr = int(lt_block.get("header_row", 1))  # 1-based

    co_block = lead_times_cfg["cutoffs"]
    co_id = co_block["sheet_id"]
    t_cut = co_block["tab"]
    co_hdr = int(co_block.get("header_row", 1))  # 1-based

    def a1(tab: str, cols: str = "A:Z") -> str:
        return f"{tab}!{cols}"

    can_rows = gsheets_service.fetch_sheet_data(lt_id, a1(t_can))
    reg_rows = gsheets_service.fetch_sheet_data(lt_id, a1(t_reg))
    cut_rows = gsheets_service.fetch_sheet_data(co_id, a1(t_cut))

    sa_email = getattr(gsheets_service, "service_account_email", lambda: "<unknown>")()
    if not can_rows or not reg_rows:
        raise ValueError(
            f"Could not read Lead Times sheet(s). Share {lt_id} with {sa_email} and re-run."
        )

    # Drop configured header rows (1-based)
    def strip_headers(rows: list[list[str]], header_row_1based: int) -> list[list[str]]:
        if not rows:
            return rows
        if header_row_1based is None or header_row_1based < 1:
            return rows
        return rows[header_row_1based:]

    can_rows = strip_headers(can_rows, lt_hdr)
    reg_rows = strip_headers(reg_rows, lt_hdr)
    cut_rows = strip_headers(cut_rows, co_hdr)

    # Columns
    lead_cols = lead_times_cfg["columns"]["lead_times_ss"]
    cut_cols = lead_times_cfg["columns"]["cutoff"]
    map_i = _col_idx(lead_cols["mapping"])
    cut_map_i = _col_idx(cut_cols["mapping"])

    # Strict: workbook tabs define the valid code list
    valid_tabs = _valid_codes_from_templates(
        detailed_template_path, summary_template_path
    )
    can_codes = codes_from_rows(can_rows, map_i)
    reg_codes = codes_from_rows(reg_rows, map_i)
    cut_codes = codes_from_rows(cut_rows, cut_map_i)

    unknown_can = can_codes - valid_tabs
    unknown_reg = reg_codes - valid_tabs
    unknown_cut = cut_codes - valid_tabs
    if unknown_can or unknown_reg or unknown_cut:
        def pv(s: set[str]) -> str:
            return "—" if not s else (
                ", ".join(sorted(s)[:12]) + (f", +{len(s) - 12} more" if len(s) > 12 else "")
            )
        from markupsafe import Markup
        raise ValueError(Markup(
            "<strong>Unknown codes</strong> — present in Google Sheets but not found as tabs "
            "in the uploaded templates (tabs define the valid list)."
            f"<div class='mt-1'><small><strong>Canberra:</strong> <code>{pv(unknown_can)}</code></small></div>"
            f"<div class='mt-1'><small><strong>Regional:</strong> <code>{pv(unknown_reg)}</code></small></div>"
            f"<div class='mt-1'><small><strong>Cutoffs:</strong> <code>{pv(unknown_cut)}</code></small></div>"
        ))

    # 2) Merge + HTML
    merged = import_and_merge(
        canberra_rows=can_rows,
        regional_rows=reg_rows,
        cutoff_rows=cut_rows,
        lead_cols=lead_cols,
        cutoff_cols=cut_cols,
    )

    warnings: list[str] = []
    html_out: Dict[str, str] = {}
    for store in scope:
        ir = merged[store]

        from services.lead_times.html_out import build_pasteable_html

        html_out[store.lower()] = build_pasteable_html(
            ir.by_product_html,
            product_to_codes=ir.product_to_codes,
            cutoffs_by_code=ir.cutoff_rows,
        )

    if "CANBERRA" in scope:
        _cutoff_attach_note(
            warnings,
            "CANBERRA",
            merged["CANBERRA"].by_product_html,
            merged["CANBERRA"].product_to_codes,
            merged["CANBERRA"].cutoff_rows,
        )
    if "REGIONAL" in scope:
        _cutoff_attach_note(
            warnings,
            "REGIONAL",
            merged["REGIONAL"].by_product_html,
            merged["REGIONAL"].product_to_codes,
            merged["REGIONAL"].cutoff_rows,
        )

    # 3) Excel generation
    outdir = Path(save_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    warnings.append(f"[DEBUG] Output dir resolved to: {outdir}")

    patterns = lead_times_cfg.get("filename_patterns", {})
    ins_cols = lead_times_cfg["insertion_columns"]
    anchor_col_letter = lead_times_cfg["columns"]["do_not_show_header"]  # e.g. "F"
    anchor_header_row = lead_times_cfg["columns"].get("do_not_show_header_row", 2)

    # --- timestamps for filename tokens ---
    _now = datetime.now()
    _tag_yyyyymmdd = _now.strftime("%Y%m%d")
    _tag_yymmdd = _now.strftime("%y%m%d")
    _tag_hhmmss = _now.strftime("%H%M%S")

    def outname(key: str, default: str) -> Path:
        pat = patterns.get(key, default)
        return outdir / (
            pat
            .replace("{YYYYMMDD}", _tag_yyyyymmdd)
            .replace("{YYMMDD}", _tag_yymmdd)
            .replace("{HHMMSS}", _tag_hhmmss)
        )

    # Text templates/regex from config (with safe defaults)
    tmpl = lead_times_cfg.get("templates", {})
    detailed_review_template = tmpl.get("detailed_review")
    summary_review_template = tmpl.get("summary_review")
    summary_lead_regex = tmpl.get("summary_lead_regex")  # optional override
    detailed_prefix_template = tmpl.get(
        "detailed_prefix_template",
        "\n       -       Lead Time: {LEAD} \n       -       ",
    )

    files: Dict[str, str] = {}
    review_detailed: set[str] = set()
    review_summary: set[str] = set()

    # Detailed
    if "CANBERRA" in scope:
        res: InjectResult = inject_and_prune(
            template_path=Path(detailed_template_path),
            out_path=outname("canberra_detailed", "Quote_Detailed_Canberra_{YYYYMMDD}.xlsm"),
            store_name="CANBERRA",
            leads_by_code=merged["CANBERRA"].lead_rows,
            cutoffs_by_code=merged["CANBERRA"].cutoff_rows,
            insertion_col_letter=ins_cols["detailed"],   # "B"
            anchor_col_letter=anchor_col_letter,         # "F"
            anchor_header_row=anchor_header_row,         # 2
            control_codes=merged["CANBERRA"].control_codes,
            warnings=warnings,
            workbook_kind="Detailed",
            summary_lead_regex=None,
            detailed_prefix_template=detailed_prefix_template,
        )
        review_detailed |= set(res.review_codes)
        files["canberra_detailed"] = os.path.basename(str(res.saved_path))

    if "REGIONAL" in scope:
        res: InjectResult = inject_and_prune(
            template_path=Path(detailed_template_path),
            out_path=outname("regional_detailed", "Quote_Detailed_Regional_{YYYYMMDD}.xlsm"),
            store_name="REGIONAL",
            leads_by_code=merged["REGIONAL"].lead_rows,
            cutoffs_by_code=merged["REGIONAL"].cutoff_rows,
            insertion_col_letter=ins_cols["detailed"],
            anchor_col_letter=anchor_col_letter,
            anchor_header_row=anchor_header_row,
            control_codes=merged["REGIONAL"].control_codes,
            warnings=warnings,
            workbook_kind="Detailed",
            detailed_prefix_template=detailed_prefix_template,
        )
        review_detailed |= set(res.review_codes)
        files["regional_detailed"] = os.path.basename(str(res.saved_path))

    # Summary
    if "CANBERRA" in scope:
        res: InjectResult = inject_and_prune(
            template_path=Path(summary_template_path),
            out_path=outname("canberra_summary", "Quote_Summary_Canberra_{YYYYMMDD}.xlsm"),
            store_name="CANBERRA",
            leads_by_code=merged["CANBERRA"].lead_rows,
            cutoffs_by_code=merged["CANBERRA"].cutoff_rows,
            insertion_col_letter=ins_cols["summary"],    # "C"
            anchor_col_letter=anchor_col_letter,         # "F"
            anchor_header_row=anchor_header_row,         # 2
            control_codes=merged["CANBERRA"].control_codes,
            warnings=warnings,
            workbook_kind="Summary",
            summary_lead_regex=summary_lead_regex,
        )
        review_summary |= set(res.review_codes)
        files["canberra_summary"] = os.path.basename(str(res.saved_path))

    if "REGIONAL" in scope:
        res: InjectResult = inject_and_prune(
            template_path=Path(summary_template_path),
            out_path=outname("regional_summary", "Quote_Summary_Regional_{YYYYMMDD}.xlsm"),
            store_name="REGIONAL",
            leads_by_code=merged["REGIONAL"].lead_rows,
            cutoffs_by_code=merged["REGIONAL"].cutoff_rows,
            insertion_col_letter=ins_cols["summary"],
            anchor_col_letter=anchor_col_letter,
            anchor_header_row=anchor_header_row,
            control_codes=merged["REGIONAL"].control_codes,
            warnings=warnings,
            workbook_kind="Summary",
            summary_lead_regex=summary_lead_regex,
        )
        review_summary |= set(res.review_codes)
        files["regional_summary"] = os.path.basename(str(res.saved_path))

    # If anything needs review, emit consolidated review-only workbooks and suppress the 4 normal links.
    review_files: Dict[str, str] = {}
    if review_detailed:
        p = save_review_only_workbook(
            template_path=Path(detailed_template_path),
            out_path=outname("detailed_review", "Quote_Detailed_REVIEW_{YYYYMMDD}.xlsm"),
            review_codes=review_detailed,
            warnings=warnings,
        )
        review_files["detailed_review"] = os.path.basename(str(p))
        warnings.append(
            f"[REVIEW] Detailed: {len(review_detailed)} tab(s) require attention — providing review-only workbook."
        )
    if review_summary:
        p = save_review_only_workbook(
            template_path=Path(summary_template_path),
            out_path=outname("summary_review", "Quote_Summary_REVIEW_{YYYYMMDD}.xlsm"),
            review_codes=review_summary,
            warnings=warnings,
        )
        review_files["summary_review"] = os.path.basename(str(p))
        warnings.append(
            f"[REVIEW] Summary: {len(review_summary)} tab(s) require attention — providing review-only workbook."
        )

    # Prefer review-only links if any exist
    final_files = review_files if review_files else files

    return {
        "warnings": warnings,
        "html": html_out,   # {"canberra": "<p>..</p>", "regional": "<p>..</p>"}
        "files": final_files,  # basenames; your download route serves from save_dir
    }
