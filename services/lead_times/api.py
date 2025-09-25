# services/lead_times/api.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple
import html
import os

from .excel_out import inject_and_prune
from .html_out import build_html_lines
from .sheets import import_and_merge


# helpers inside run_publish(), after can_rows/reg_rows/cut_rows are fetched

def _col_idx(a1: str) -> int:
    a1 = a1.strip().upper()
    n = 0
    for ch in a1:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _norm(s: str) -> str:
    # case/space insensitive; also removes non-breaking spaces
    return "".join(str(s or "").lower().replace("\xa0", "").split())


def _strip_header(rows: list[list[str]], prod_i: int, map_i: int) -> list[list[str]]:
    """
    Remove the header row if present. Looks for something like:
      product  |  buz inventory code(s)
    within the first few rows.
    """
    if not rows:
        return rows

    scan = min(8, len(rows))
    for i in range(scan):
        prod = _norm(rows[i][prod_i] if prod_i < len(rows[i]) else "")
        mapp = _norm(rows[i][map_i] if map_i < len(rows[i]) else "")
        if prod.startswith("product") and mapp.startswith("buzinventorycode"):
            return rows[i + 1 :]

    # fallback: if the very first row smells like headers (non-datay),
    # drop it; otherwise keep as-is
    return rows[1:] if len(rows) > 1 and any(c.isalpha() for c in " ".join(rows[0])) else rows


def run_publish(
    *,
    gsheets_service,                 # services.google_sheets_service.GoogleSheetsService()
    lead_times_cfg: dict,            # current_app.config["lead_times"]
    detailed_template_path: str,     # uploaded Detailed .xlsm path
    summary_template_path: str,      # uploaded Summary .xlsm path
    save_dir: str,                   # ABSOLUTE dir to save generated files (your download base)
    scope: Tuple[str, ...] = ("CANBERRA", "REGIONAL"),
) -> dict:
    """Read Sheets, validate/merge, generate HTML + 4 Excel files, return results."""

    # 1) Read Sheets (config uses 'lead_times_ss')
    lt_block = lead_times_cfg["lead_times_ss"]
    lt_id = lt_block["sheet_id"]
    t_can = lt_block["tabs"]["canberra"]
    t_reg = lt_block["tabs"]["regional"]

    co_id = lead_times_cfg["cutoffs"]["sheet_id"]
    t_cut = lead_times_cfg["cutoffs"]["tab"]

    def a1(tab: str, cols: str = "A:Z") -> str:
        return f"{tab}!{cols}"

    can_rows = gsheets_service.fetch_sheet_data(lt_id, a1(t_can))
    reg_rows = gsheets_service.fetch_sheet_data(lt_id, a1(t_reg))
    cut_rows = gsheets_service.fetch_sheet_data(co_id, a1(t_cut))

    # apply to all three sheets using your config letters
    lead_cols = lead_times_cfg["columns"]["lead_times_ss"]
    cut_cols = lead_times_cfg["columns"]["cutoff"]

    prod_i = _col_idx(lead_cols["product"])
    map_i = _col_idx(lead_cols["mapping"])
    cut_prod_i = _col_idx(cut_cols["product"])
    cut_map_i = _col_idx(cut_cols["mapping"])

    can_rows = _strip_header(can_rows, prod_i, map_i)
    reg_rows = _strip_header(reg_rows, prod_i, map_i)
    cut_rows = _strip_header(cut_rows, cut_prod_i, cut_map_i)

    warnings: list[str] = []
    merged = import_and_merge(
        canberra_rows=can_rows,
        regional_rows=reg_rows,
        cutoff_rows=cut_rows,
        lead_cols=lead_times_cfg["columns"]["lead_times_ss"],
        cutoff_cols=lead_times_cfg["columns"]["cutoff"],
    )
    warnings.append(
        "[CONTROL] CANBERRA codes=%d (e.g., %s) | REGIONAL codes=%d (e.g., %s)" % (
            len(merged["CANBERRA"].control_codes),
            ", ".join(sorted(merged["CANBERRA"].control_codes)[:10]) or "—",
            len(merged["REGIONAL"].control_codes),
            ", ".join(sorted(merged["REGIONAL"].control_codes)[:10]) or "—",
        )
    )

    # 2) Build HTML (kept in memory)
    html_out: Dict[str, str] = {}
    for store in scope:
        ir = merged[store]
        lines = build_html_lines(ir.by_product_html, cutoffs_by_code=ir.cutoff_rows)
        html_out[store.lower()] = "\n".join(
            f"<p>{html.escape(ln)}<br /></p>" for ln in lines
        )

    # 3) Excel generation → write to provided save_dir
    outdir = Path(save_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    patterns = lead_times_cfg["filename_patterns"]
    ins_cols = lead_times_cfg["insertion_columns"]
    tag = datetime.now().strftime("%Y%m%d")  # use %Y%m%d_%H%M%S if you want uniqueness

    def outname(key: str) -> Path:
        return outdir / patterns[key].replace("{YYYYMMDD}", tag)

    files: Dict[str, str] = {}

    anchor_col = lead_times_cfg["columns"]["do_not_show_header"]  # "F"
    anchor_header_row = lead_times_cfg["columns"].get("do_not_show_header_row", 2)
    if "CANBERRA" in scope:

        p = outname("canberra_detailed")
        inject_and_prune(
            template_path=Path(detailed_template_path),
            out_path=p,
            store_name="CANBERRA",
            leads_by_code=merged["CANBERRA"].lead_rows,
            cutoffs_by_code=merged["CANBERRA"].cutoff_rows,
            insertion_col_letter=ins_cols["detailed"],  # "B"
            anchor_col_letter=anchor_col,  # "F"
            anchor_header_row=anchor_header_row,  # 2
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
            insertion_col_letter=ins_cols["summary"],  # "C"
            anchor_col_letter=anchor_col,  # "F"
            anchor_header_row=anchor_header_row,  # 2
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
            anchor_col_letter=anchor_col,  # "F"
            anchor_header_row=anchor_header_row,  # 2
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
            anchor_col_letter=anchor_col,  # "F"
            anchor_header_row=anchor_header_row,  # 2
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
