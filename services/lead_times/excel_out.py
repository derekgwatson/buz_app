from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string
from openpyxl.worksheet.worksheet import Worksheet


_LEAD_LABEL_RE = re.compile(r"(?i)\bLead\s*Time\s*:\s*")


def _is_nonblank(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _is_false_checkbox(value) -> bool:
    if value is False:
        return True
    if isinstance(value, str) and value.strip().upper() == "FALSE":
        return True
    if value == 0:
        return True
    return False


def _first_insertion_row_summary(
    ws: Worksheet,
    *,
    insertion_col_letter: str,      # e.g. "C"
    anchor_col_letter: str,         # e.g. "F"
    anchor_header_row: int | None,  # e.g. 2
) -> tuple[int | None, str]:
    """
    SUMMARY logic:
      Pass 1: first row after header where insertion column is non-blank.
      Pass 2: fallback to first FALSE in anchor column (Do Not Show?).
      Returns (row_index, reason) where reason in {"C_NONBLANK", "F_FALSE", "NONE"}.
    """
    start_row = (int(anchor_header_row) if anchor_header_row and anchor_header_row >= 1 else 1) + 1
    max_row = ws.max_row or start_row

    ins_col = column_index_from_string(insertion_col_letter)
    anc_col = column_index_from_string(anchor_col_letter)

    for r in range(start_row, max_row + 1):
        if _is_nonblank(ws.cell(row=r, column=ins_col).value):
            return r, "C_NONBLANK"

    for r in range(start_row, max_row + 1):
        if _is_false_checkbox(ws.cell(row=r, column=anc_col).value):
            return r, "F_FALSE"

    return None, "NONE"


def _is_effectively_empty_sheet(ws: Worksheet, header_row: int | None) -> bool:
    """
    Treat a sheet as 'heading-only' if there is no meaningful data below the header row.
    We scan A:Z starting at (header_row + 1) and ignore a lone value in G{header_row+1}.
    """
    start_row = (int(header_row) if header_row and header_row >= 1 else 1) + 1
    max_row = ws.max_row or start_row

    for r in range(start_row, max_row + 1):
        for c in range(1, 27):  # A..Z
            if r == start_row and c == 7:  # ignore G on the first data row
                continue
            v = ws.cell(row=r, column=c).value
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            return False
    return True


def _norm_name(s: str) -> str:
    return (s or "").strip().upper()


def _prune_tabs_safe(wb, keep_names: set[str], warnings: list[str]) -> None:
    keep_norm = {_norm_name(s) for s in keep_names if s}
    tab_raw = list(wb.sheetnames)
    tab_norm_map = {_norm_name(n): n for n in tab_raw}

    if not keep_norm:
        warnings.append("[PRUNE] Control list is empty — skipping prune.")
        return

    tabs_norm = set(tab_norm_map.keys())
    missing_tabs_for_codes = sorted(keep_norm - tabs_norm)
    extra_tabs_not_codes = sorted(tabs_norm - keep_norm)

    def _pv(items, k: int = 12) -> str:
        return "—" if not items else (
            ", ".join(items[:k]) + (f", +{len(items) - k} more" if len(items) > k else "")
        )

    warnings.append(
        "[PRUNE] control_codes=%d, workbook_tabs=%d; codes-without-tabs: %s; tabs-not-in-codes: %s"
        % (len(keep_norm), len(tab_raw), _pv(missing_tabs_for_codes), _pv(extra_tabs_not_codes))
    )

    to_keep_raw = [tab_norm_map[k] for k in (keep_norm & tabs_norm)]
    if not to_keep_raw:
        warnings.append("[PRUNE] No tabs match control list — skipping prune (will NOT delete anything).")
        return

    for name in tab_raw:
        if name not in to_keep_raw:
            wb.remove(wb[name])

    first_keep = to_keep_raw[0]
    ws0 = wb[first_keep]
    if getattr(ws0, "sheet_state", "visible") != "visible":
        ws0.sheet_state = "visible"
    wb.active = wb.sheetnames.index(first_keep)


def _col_to_idx(letter: str) -> int:
    letter = (letter or "").strip().upper()
    n = 0
    for ch in letter:
        n = n * 26 + (ord(ch) - 64)
    return max(1, n)


def _find_first_false(ws: Worksheet, col_idx: int, header_row: int) -> int | None:
    max_row = ws.max_row or header_row
    for r in range(header_row + 1, max_row + 1):
        v = ws.cell(row=r, column=col_idx).value
        if v is False:
            return r
        if isinstance(v, str) and v.strip().upper() == "FALSE":
            return r
    return None


def _first_nonblank(ws: Worksheet, col_idx: int, header_row: int) -> int | None:
    max_row = ws.max_row or header_row
    for r in range(header_row + 1, max_row + 1):
        v = ws.cell(row=r, column=col_idx).value
        if v not in (None, ""):
            return r
    return None


def _append_text(cell, text: str) -> None:
    existing = cell.value
    if isinstance(existing, str) and existing.strip():
        sep = "" if existing.endswith((" ", ",")) else " "
        cell.value = f"{existing}{sep}{text}"
    else:
        cell.value = text


def _apply_template(template: str, lead: str) -> str:
    # Accept {LEAD}, {lead}, {lead_time}
    return (template or "{LEAD}").replace("{LEAD}", lead).replace("{lead}", lead).replace("{lead_time}", lead)


def _replace_lead_in_text(
    text: str,
    lead: str,
    lead_regex: Optional[str],
) -> tuple[str, bool]:
    """
    (Summary) Replace the first lead-time phrase (e.g., '3-4 weeks') in `text` with `lead`.
    Returns (new_text, replaced?).
    """
    if not isinstance(text, str) or not text.strip():
        return lead, False
    pattern = lead_regex or r"(?i)\b\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?\s*(?:weeks?|wks?|wk)\b"
    rx = re.compile(pattern)
    if rx.search(text):
        return rx.sub(lead, text, count=1), True
    return text, False


def _replace_detailed_lead_line(text: str, lead: str, suffix: str) -> tuple[str, bool]:
    """
    (Detailed) Replace the value on the 'Lead Time:' line within `text`, preserving everything else.
    We match the label case-insensitively and replace the remainder of that line up to newline.
    Returns (new_text, replaced?).
    """
    if not isinstance(text, str) or not text.strip():
        return text, False

    # Match "Lead Time:" and capture the rest of the line (until \r or \n)
    rx = re.compile(r"(?im)(\bLead\s*Time\s*:\s*)([^\r\n]*)")
    m = rx.search(text)
    if not m:
        return text, False

    new_line_value = lead + (f" {suffix}" if suffix and not suffix.startswith(" ") else suffix)
    new_text = rx.sub(lambda mo: mo.group(1) + new_line_value, text, count=1)
    return new_text, True


def inject_and_prune(
    *,
    template_path: Path,
    out_path: Path,
    store_name: str,
    leads_by_code: Dict[str, Dict],
    cutoffs_by_code: Dict[str, Dict],
    insertion_col_letter: str,
    anchor_col_letter: str,
    anchor_header_row: int,
    control_codes: Set[str],
    warnings: list[str],
    workbook_kind: str | None = None,
    # Summary-only options
    summary_template: Optional[str] = None,
    summary_lead_regex: Optional[str] = None,
) -> Path:
    """
    Open the .xlsm template, prune tabs, then for each remaining tab:
      Detailed:
        - Anchor row = first FALSE under anchor column.
        - Require a 'Lead Time:' line in the target cell; replace only its value.
          If missing, do not edit this tab (collect and later output only these tabs).
      Summary:
        - Prefer first non-blank in insertion column; else first FALSE in anchor column.
        - Replace lead phrase in-place; if nothing to replace, write with template.
    Always returns the final saved path (forces .xlsm extension if workbook has macros).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb = load_workbook(filename=str(template_path), keep_vba=True, data_only=True)

    _prune_tabs_safe(wb, keep_names=set(control_codes), warnings=warnings)

    ins_idx = _col_to_idx(insertion_col_letter)
    anc_idx = _col_to_idx(anchor_col_letter)
    is_summary = (workbook_kind or "").lower() == "summary"

    # Track Detailed tabs that are missing the seed 'Lead Time:' line
    detailed_missing_seed: list[str] = []

    for code in list(wb.sheetnames):
        ws = wb[code]

        # Drop heading-only tabs silently
        if _is_effectively_empty_sheet(ws, anchor_header_row):
            wb.remove(ws)
            continue

        lead_rec = leads_by_code.get(code)
        if not lead_rec:
            warnings.append(f"[{store_name}/{code}] No lead-time text found — skipped.")
            continue
        lt_text = (lead_rec.get("lead_time_text") or "").strip()
        if not lt_text:
            warnings.append(f"[{store_name}/{code}] Empty lead-time text — skipped.")
            continue

        cut = (cutoffs_by_code.get(code, {}).get("cutoff") or "").strip()
        suffix = f"***CHRISTMAS CUTOFF {cut}***" if cut else ""

        if is_summary:
            row, reason = _first_insertion_row_summary(
                ws,
                insertion_col_letter=insertion_col_letter,
                anchor_col_letter=anchor_col_letter,
                anchor_header_row=anchor_header_row,
            )
            if row is None:
                warnings.append(
                    f"[{store_name}/{code}] Summary: could not find insertion row — skipped."
                )
                continue

            cell = ws.cell(row=row, column=ins_idx)

            if reason == "C_NONBLANK" and isinstance(cell.value, str) and cell.value.strip():
                new_text, replaced = _replace_lead_in_text(cell.value, lt_text, summary_lead_regex)
                if not replaced:
                    # If no recognizable lead phrase, use template if provided, else append the lead
                    if summary_template:
                        new_text = _apply_template(summary_template, lt_text)
                    else:
                        new_text = f"{cell.value} {lt_text}"
                    warnings.append(f"[{store_name}/{code}] Summary: pattern not found — used fallback.")
                if suffix:
                    # Add suffix once; avoid duplicate spaces
                    new_text = new_text.rstrip() + " " + suffix
                cell.value = new_text
            else:
                template_text = summary_template or "{LEAD}"
                value = _apply_template(template_text, lt_text)
                if suffix:
                    value = value.rstrip() + " " + suffix
                cell.value = value

        else:
            # Detailed: anchor is first FALSE below header
            anchor_row = _find_first_false(ws, anc_idx, header_row=anchor_header_row)
            if anchor_row is None:
                kind = f"{workbook_kind}:" if workbook_kind else ""
                warnings.append(
                    f"[{store_name}/{code}] {kind} No FALSE found in column "
                    f"{anchor_col_letter} below row {anchor_header_row} — skipped."
                )
                continue

            cell = ws.cell(row=anchor_row, column=ins_idx)
            cell_text = "" if cell.value is None else str(cell.value)

            # Require a 'Lead Time:' line; replace its value
            if not _LEAD_LABEL_RE.search(cell_text):
                detailed_missing_seed.append(code)
                # Do NOT modify this tab
                continue

            new_text, replaced = _replace_detailed_lead_line(cell_text, lt_text, suffix)
            if not replaced:
                # Shouldn’t happen if _LEAD_LABEL_RE matched, but be safe
                detailed_missing_seed.append(code)
                continue

            cell.value = new_text

    # If any Detailed tabs are missing the seed "Lead Time:" line, save a workbook
    # that contains only those tabs (unchanged), so the user can seed them.
    if not is_summary and detailed_missing_seed:
        keep = set(detailed_missing_seed)
        names = ", ".join(sorted(detailed_missing_seed))
        warnings.append(
            f"[{store_name}] Detailed: {len(detailed_missing_seed)} tab(s) have no 'Lead Time:' line "
            f"in column {insertion_col_letter}. Output file contains only these tabs so you can seed them: "
            f"{names or '—'}."
        )
        for name in list(wb.sheetnames):
            if name not in keep:
                wb.remove(wb[name])

        if not any(s.sheet_state == "visible" for s in wb.worksheets):
            stub = wb.create_sheet("EMPTY")
            stub["A1"] = f"{store_name} — tabs need 'Lead Time:' line"
            stub.sheet_state = "visible"

        # Ensure extension matches macro presence
        final_path = out_path
        try:
            has_macros = getattr(wb, "vba_archive", None) is not None
        except Exception:
            has_macros = False
        if has_macros and out_path.suffix.lower() != ".xlsm":
            final_path = out_path.with_suffix(".xlsm")

        wb.save(str(final_path))
        return final_path

    # Ensure at least one visible sheet remains
    if not any(s.sheet_state == "visible" for s in wb.worksheets):
        stub = wb.create_sheet("EMPTY")
        stub["A1"] = f"{store_name} — no tabs with data"
        stub.sheet_state = "visible"

    # Save with correct extension when macros exist
    final_path = out_path
    try:
        has_macros = getattr(wb, "vba_archive", None) is not None
    except Exception:
        has_macros = False
    if has_macros and out_path.suffix.lower() != ".xlsm":
        final_path = out_path.with_suffix(".xlsm")

    wb.save(str(final_path))
    return final_path
