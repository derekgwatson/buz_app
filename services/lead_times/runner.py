
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

from .sheets import import_and_merge
from .excel_out import inject_and_prune
from .html_out import build_html_lines

import os
from flask import current_app


def _base_output_dir():
    base = current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config.get("upload_folder")
    # make absolute (important if config is a relative path)
    if not os.path.isabs(base):
        base = os.path.join(current_app.root_path, base)
    return base


def _save_dir():
    root = _base_output_dir()
    path = os.path.join(root, "lead_times")  # keep outputs tidy; or drop this line to use root directly
    os.makedirs(path, exist_ok=True)
    return path


def _load_sheets_service():
    try:
        from services.google_sheets_service import GoogleSheetsService  # type: ignore
    except Exception as e:
        raise RuntimeError("Could not import GoogleSheetsService from services.google_sheets_service") from e
    return GoogleSheetsService()


def a1(tab: str, cols: str = "A:Z") -> str:
    return f"{tab}!{cols}"


def read_sheet_values(svc, sheet_id: str, tab_name: str, cols: str) -> List[List[str]]:
    rng = a1(tab_name, cols)
    values = svc.fetch_sheet_data(sheet_id, rng)
    return values or []


def main() -> None:
    ap = argparse.ArgumentParser(description="Pre-Christmas Cutoff Publisher")
    ap.add_argument("--config", default=str(Path(__file__).with_name("config.json")), help="Path to config.json")
    ap.add_argument("--outdir", default=None, help="Override output dir")
    ap.add_argument("--canberra", action="store_true", help="Generate Canberra outputs only")
    ap.add_argument("--regional", action="store_true", help="Generate Regional outputs only")
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    svc = _load_sheets_service()

    lt_id = cfg["lead_times_sheet_id"]
    co_id = cfg["cutoff_sheet_id"]
    t_can = cfg["tabs"]["lead_times_canberra_tab"]
    t_reg = cfg["tabs"]["lead_times_regional_tab"]
    t_cut = cfg["tabs"]["cutoff_tab"]

    col_lt = cfg["columns"]["lead_times_ss"]
    col_co = cfg["columns"]["cutoff"]

    can_rows = read_sheet_values(svc, lt_id, t_can, "A:Z")
    reg_rows = read_sheet_values(svc, lt_id, t_reg, "A:Z")
    cut_rows = read_sheet_values(svc, co_id, t_cut, "A:Z")

    merged = import_and_merge(
        canberra_rows=can_rows,
        regional_rows=reg_rows,
        cutoff_rows=cut_rows,
        lead_cols=col_lt,
        cutoff_cols=col_co,
    )

    outdir = Path(args.outdir or cfg["output"]["dir"])
    outdir.mkdir(parents=True, exist_ok=True)

    # HTML
    for store in ("CANBERRA", "REGIONAL"):
        if args.canberra and store != "CANBERRA":
            continue
        if args.regional and store != "REGIONAL":
            continue
        ir = merged[store]
        lines = build_html_lines(ir.by_product_html, cutoffs_by_code=ir.cutoff_rows)
        html_text = "\n".join(f"<p>{ln.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')}<br /></p>" for ln in lines)
        (outdir / f"lead_times_{store.lower()}.html").write_text(html_text, encoding="utf-8")

    # Excel
    ins_cols = cfg["insertion_columns"]
    header_text = cfg["columns"]["do_not_show_header"]
    templates = cfg["templates"]
    fnpat = cfg["output"]["filename_patterns"]

    from datetime import datetime as _dt
    tag = _dt.now().strftime("%Y%m%d")

    warnings: List[str] = []

    def outname(key: str) -> Path:
        pat = fnpat[key].replace("{YYYYMMDD}", tag)
        return outdir / pat

    if not args.regional:
        inject_and_prune(
            template_path=Path(templates["detailed_path"]),
            out_path=outname("canberra_detailed"),
            store_name="CANBERRA",
            leads_by_code=merged["CANBERRA"].lead_rows,
            cutoffs_by_code=merged["CANBERRA"].cutoff_rows,
            insertion_col_letter=ins_cols["detailed"],
            header_text=header_text,
            control_codes=merged["CANBERRA"].control_codes,
            warnings=warnings,
        )
        inject_and_prune(
            template_path=Path(templates["summary_path"]),
            out_path=outname("canberra_summary"),
            store_name="CANBERRA",
            leads_by_code=merged["CANBERRA"].lead_rows,
            cutoffs_by_code=merged["CANBERRA"].cutoff_rows,
            insertion_col_letter=ins_cols["summary"],
            header_text=header_text,
            control_codes=merged["CANBERRA"].control_codes,
            warnings=warnings,
        )

    if not args.canberra:
        inject_and_prune(
            template_path=Path(templates["detailed_path"]),
            out_path=outname("regional_detailed"),
            store_name="REGIONAL",
            leads_by_code=merged["REGIONAL"].lead_rows,
            cutoffs_by_code=merged["REGIONAL"].cutoff_rows,
            insertion_col_letter=ins_cols["detailed"],
            header_text=header_text,
            control_codes=merged["REGIONAL"].control_codes,
            warnings=warnings,
        )
        inject_and_prune(
            template_path=Path(templates["summary_path"]),
            out_path=outname("regional_summary"),
            store_name="REGIONAL",
            leads_by_code=merged["REGIONAL"].lead_rows,
            cutoffs_by_code=merged["REGIONAL"].cutoff_rows,
            insertion_col_letter=ins_cols["summary"],
            header_text=header_text,
            control_codes=merged["REGIONAL"].control_codes,
            warnings=warnings,
        )

    if warnings:
        print("\n=== WARNINGS ===")
        for w in warnings:
            print(f"- {w}")
    else:
        print("No warnings.")
    print(f"\nDone. Outputs in {outdir}")


if __name__ == "__main__":
    main()
