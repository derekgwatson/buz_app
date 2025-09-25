# services/lead_times/api.py
from __future__ import annotations

import html
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

from openpyxl import load_workbook

from .excel_out import inject_and_prune
from .html_out import build_html_lines
from .sheets import import_and_merge, codes_from_rows


def _cutoff_attach_note(
    warnings: list[str],
    store_name: str,
    by_product_html: list[tuple[str, str]],
    cutoff_rows: dict[str, dict],
) -> None:
    """Report how many products actually received a CHRISTMAS CUTOFF banner."""
    # products present in the HTML list for this store
    html_products = {str(p).strip() for p, _ in by_product_html if str(p).strip()}
    # products that have a cutoff date in the cutoffs sheet
    cutoff_products = {
        str(rec.get("product", "")).strip()
        for rec in cutoff_rows.values()
        if str(rec.get("product", "")).strip() and str(rec.get("cutoff", "")).strip()
    }
    attached = sorted(html_products & cutoff_products)
    if attached:
        sample = ", ".join(attached[:5]) + ("…" if len(attached) > 5 else "")
        warnings.append(f"[{store_name}] CHRISTMAS CUTOFF appended to {len(attached)} product(s) (e.g., {sample}).")
    else:
        # If none attached but there *are* dated cutoffs, hint if they’re absent from leads
        if cutoff_products:
            missing_in_leads = sorted(cutoff_products - html_products)
            if missing_in_leads:
                sample = ", ".join(missing_in_leads[:3]) + ("…" if len(missing_in_leads) > 3 else "")
                warnings.append(
                    f"[{store_name}] No banners appended. "
                    f"{len(missing_in_leads)} cutoff product(s) aren’t present in this store’s Lead Times mapping "
                    f"(e.g., {sample})."
                )


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


def run_publish(
    *,
    gsheets_service,
    lead_times_cfg: dict,
    detailed_template_path: str,
    summary_template_path: str,
    save_dir: str,
    scope: Tuple[str, ...] = ("CANBERRA", "REGIONAL"),
) -> dict:
    """Read Sheets, validate/merge, generate HTML + 4 Excel files, return results."""

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

    # Fail fast if Lead Times sheets weren't readable
    sa_email = getattr(gsheets_service, "service_account_email", lambda: "<unknown>")()
    if not can_rows or not reg_rows:
        raise ValueError(
            f"Could not read Lead Times sheet(s). Share {lt_id} with {sa_email} and re-run."
        )

    # Explicit header slicing by config (drop the header row; 1-based)
    def strip_headers(rows: list[list[str]], header_row_1based: int) -> list[list[str]]:
        if not rows:
            return rows
        if header_row_1based is None or header_row_1based < 1:
            return rows
        # keep rows after the header row
        return rows[header_row_1based:]

    can_rows = strip_headers(can_rows, lt_hdr)
    reg_rows = strip_headers(reg_rows, lt_hdr)
    cut_rows = strip_headers(cut_rows, co_hdr)

    # Column letters → indexes
    lead_cols = lead_times_cfg["columns"]["lead_times_ss"]
    cut_cols = lead_times_cfg["columns"]["cutoff"]
    prod_i = _col_idx(lead_cols["product"])
    map_i = _col_idx(lead_cols["mapping"])
    cut_prod_i = _col_idx(cut_cols["product"])
    cut_map_i = _col_idx(cut_cols["mapping"])

    # Strict: workbook tabs define the valid list
    valid_tabs = _valid_codes_from_templates(detailed_template_path, summary_template_path)
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

    # 2) Merge + build HTML
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
        lines = build_html_lines(ir.by_product_html, cutoffs_by_code=ir.cutoff_rows)
        html_out[store.lower()] = "\n".join(
            f"<p>{html.escape(ln)}<br /></p>" for ln in lines
        )

    warnings: list[str] = warnings  # use your existing list

    if "CANBERRA" in scope:
        _cutoff_attach_note(
            warnings,
            "CANBERRA",
            merged["CANBERRA"].by_product_html,
            merged["CANBERRA"].cutoff_rows,
        )
    if "REGIONAL" in scope:
        _cutoff_attach_note(
            warnings,
            "REGIONAL",
            merged["REGIONAL"].by_product_html,
            merged["REGIONAL"].cutoff_rows,
        )

    # 3) Excel generation
    outdir = Path(save_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    patterns = lead_times_cfg["filename_patterns"]
    ins_cols = lead_times_cfg["insertion_columns"]
    anchor_col_letter = lead_times_cfg["columns"]["do_not_show_header"]  # e.g. "F"
    anchor_header_row = lead_times_cfg["columns"].get("do_not_show_header_row", 2)
    tag = datetime.now().strftime("%Y%m%d")

    def outname(key: str) -> Path:
        return outdir / patterns[key].replace("{YYYYMMDD}", tag)

    files: Dict[str, str] = {}

    if "CANBERRA" in scope:
        p = outname("canberra_detailed")
        inject_and_prune(
            template_path=Path(detailed_template_path),
            out_path=p,
            store_name="CANBERRA",
            leads_by_code=merged["CANBERRA"].lead_rows,
            cutoffs_by_code=merged["CANBERRA"].cutoff_rows,
            insertion_col_letter=ins_cols["detailed"],
            anchor_col_letter=anchor_col_letter,
            anchor_header_row=anchor_header_row,
            control_codes=merged["CANBERRA"].control_codes,
            warnings=warnings,
            workbook_kind="Detailed",
        )
        files["canberra_detailed"] = os.path.basename(str(p))

        p = outname("canberra_summary")
        inject_and_prune(
            template_path=Path(summary_template_path),
            out_path=p,
            store_name="CANBERRA",
            leads_by_code=merged["CANBERRA"].lead_rows,
            cutoffs_by_code=merged["CANBERRA"].cutoff_rows,
            insertion_col_letter=ins_cols["summary"],
            anchor_col_letter=anchor_col_letter,
            anchor_header_row=anchor_header_row,
            control_codes=merged["CANBERRA"].control_codes,
            warnings=warnings,
            workbook_kind="Summary",
        )
        files["canberra_summary"] = os.path.basename(str(p))

    if "REGIONAL" in scope:
        p = outname("regional_detailed")
        inject_and_prune(
            template_path=Path(detailed_template_path),
            out_path=p,
            store_name="REGIONAL",
            leads_by_code=merged["REGIONAL"].lead_rows,
            cutoffs_by_code=merged["REGIONAL"].cutoff_rows,
            insertion_col_letter=ins_cols["detailed"],
            anchor_col_letter=anchor_col_letter,
            anchor_header_row=anchor_header_row,
            control_codes=merged["REGIONAL"].control_codes,
            warnings=warnings,
            workbook_kind="Detailed",
        )
        files["regional_detailed"] = os.path.basename(str(p))

        p = outname("regional_summary")
        inject_and_prune(
            template_path=Path(summary_template_path),
            out_path=p,
            store_name="REGIONAL",
            leads_by_code=merged["REGIONAL"].lead_rows,
            cutoffs_by_code=merged["REGIONAL"].cutoff_rows,
            insertion_col_letter=ins_cols["summary"],
            anchor_col_letter=anchor_col_letter,
            anchor_header_row=anchor_header_row,
            control_codes=merged["REGIONAL"].control_codes,
            warnings=warnings,
            workbook_kind="Summary",
        )
        files["regional_summary"] = os.path.basename(str(p))

    return {
        "warnings": warnings,
        "html": html_out,   # {"canberra": "<p>..</p>", "regional": "<p>..</p>"}
        "files": files,     # basenames; your download route serves from save_dir
    }
