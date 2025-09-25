# services/lead_times/excel_out.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Set
from openpyxl import load_workbook


def _norm_name(s: str) -> str:
    return (s or "").strip().upper()


def _prune_tabs_safe(wb, keep_names: set[str], warnings: list[str]) -> None:
    """
    Keep only tabs whose names match keep_names (case-insensitive, trimmed).
    Never remove all sheets. Emit a clear diff.
    """
    keep_norm = {_norm_name(s) for s in keep_names if s}
    tab_raw = list(wb.sheetnames)
    tab_norm_map = {_norm_name(n): n for n in tab_raw}

    if not keep_norm:
        warnings.append("[PRUNE] Control list is empty — skipping prune.")
        return

    # Diff before pruning
    tabs_norm = set(tab_norm_map.keys())
    missing_tabs_for_codes = sorted(keep_norm - tabs_norm)   # codes we want but no tab
    extra_tabs_not_codes   = sorted(tabs_norm - keep_norm)   # tabs present but not in codes

    # Helpful, short previews
    def _pv(items, k=12):
        return "—" if not items else (", ".join(items[:k]) + (f", +{len(items)-k} more" if len(items)>k else ""))

    warnings.append(
        "[PRUNE] control_codes=%d, workbook_tabs=%d; "
        "codes-without-tabs: %s; tabs-not-in-codes: %s" % (
            len(keep_norm), len(tab_raw),
            _pv(missing_tabs_for_codes), _pv(extra_tabs_not_codes)
        )
    )

    # Compute the actual keep list (use original raw names for openpyxl)
    to_keep_raw = [tab_norm_map[k] for k in (keep_norm & tabs_norm)]

    if not to_keep_raw:
        warnings.append("[PRUNE] No tabs match control list — skipping prune (will NOT delete anything).")
        return

    # Delete others
    for name in tab_raw:
        if name not in to_keep_raw:
            wb.remove(wb[name])

    # Ensure at least one visible & active
    first_keep = to_keep_raw[0]
    ws0 = wb[first_keep]
    if getattr(ws0, "sheet_state", "visible") != "visible":
        ws0.sheet_state = "visible"
    wb.active = wb.sheetnames.index(first_keep)


def _col_to_idx(letter: str) -> int:
    """Excel column letter -> 1-based index (A=1)."""
    letter = (letter or "").strip().upper()
    n = 0
    for ch in letter:
        n = n * 26 + (ord(ch) - 64)
    return max(1, n)


def _find_first_false(ws, col_idx: int, header_row: int) -> int | None:
    """
    Strict anchor: first FALSE below the given header_row in the given column.
    Accepts Excel booleans or string 'FALSE' (any case/spacing).
    """
    max_row = ws.max_row or header_row
    for r in range(header_row + 1, max_row + 1):
        v = ws.cell(row=r, column=col_idx).value
        if v is False:
            return r
        if isinstance(v, str) and v.strip().upper() == "FALSE":
            return r
    return None


def _first_nonblank(ws, col_idx: int, header_row: int) -> int | None:
    """First non-blank cell below header_row in a column."""
    max_row = ws.max_row or header_row
    for r in range(header_row + 1, max_row + 1):
        v = ws.cell(row=r, column=col_idx).value
        if v not in (None, ""):
            return r
    return None


def _append_text(cell, text: str) -> None:
    """Append a space + text to existing string, else set."""
    existing = cell.value
    if isinstance(existing, str) and existing.strip():
        sep = "" if existing.endswith((" ", ",")) else " "
        cell.value = f"{existing}{sep}{text}"
    else:
        cell.value = text


def _prune_tabs(wb, keep_names: Set[str]) -> None:
    """Delete all worksheets not in keep_names."""
    for name in list(wb.sheetnames):
        if name not in keep_names:
            ws = wb[name]
            wb.remove(ws)


def inject_and_prune(
    *,
    template_path: Path,
    out_path: Path,
    store_name: str,                    # "CANBERRA" | "REGIONAL"
    leads_by_code: Dict[str, Dict],     # {code: {"product":..., "lead_time_text":...}}
    cutoffs_by_code: Dict[str, Dict],   # {code: {"product":..., "cutoff":...}}
    insertion_col_letter: str,          # "B" (Detailed) | "C" (Summary)
    anchor_col_letter: str,             # "F"  (Do Not Show? column)
    anchor_header_row: int,             # 2    (header lives on row 2)
    control_codes: Set[str],            # tabs to keep
    warnings: list[str],
    workbook_kind: str | None = None,   # "Detailed" | "Summary" (for clearer warnings)
) -> None:
    """
    Open the .xlsm template, prune tabs not in control list, then for each remaining tab:
      - find first FALSE under column F (below row 2),
      - append " <lead time>[ ***CHRISTMAS CUTOFF dd/mm/yy***]" into insertion column.
    Strict: if no FALSE found, skip the tab and warn.
    For Summary, also warn if first-nonblank in insertion column != anchor row.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb = load_workbook(filename=str(template_path), keep_vba=True, data_only=True)

    # 1) Prune first to avoid noisy warnings on helper/heading tabs
    _prune_tabs_safe(wb, keep_names=set(control_codes), warnings=warnings)

    ins_idx = _col_to_idx(insertion_col_letter)
    anc_idx = _col_to_idx(anchor_col_letter)

    for code in list(wb.sheetnames):
        ws = wb[code]

        # lead text (required)
        lead_rec = leads_by_code.get(code)
        if not lead_rec:
            warnings.append(f"[{store_name}/{code}] No lead-time text found — skipped.")
            continue
        lt_text = (lead_rec.get("lead_time_text") or "").strip()
        if not lt_text:
            warnings.append(f"[{store_name}/{code}] Empty lead-time text — skipped.")
            continue

        # cutoff (optional)
        cut = (cutoffs_by_code.get(code, {}).get("cutoff") or "").strip()
        suffix = f" ***CHRISTMAS CUTOFF {cut}***" if cut else ""

        # 2) Strict anchor: first FALSE in col F below row 2
        anchor_row = _find_first_false(ws, anc_idx, header_row=anchor_header_row)
        if anchor_row is None:
            kind = f"{workbook_kind}:" if workbook_kind else ""
            warnings.append(
                f"[{store_name}/{code}] {kind} No FALSE found in column "
                f"{anchor_col_letter} below row {anchor_header_row} — skipped."
            )
            continue

        # 3) Summary layout sanity: first non-blank in insertion column should be same row
        if (workbook_kind or "").lower() == "summary":
            first_nn = _first_nonblank(ws, ins_idx, header_row=anchor_header_row)
            if first_nn is not None and first_nn != anchor_row:
                warnings.append(
                    f"[{store_name}/{code}] Summary: anchor row {anchor_row} "
                    f"!= first non-blank in {insertion_col_letter} ({first_nn}). Wrote at anchor."
                )

        # 4) Append the text
        cell = ws.cell(row=anchor_row, column=ins_idx)
        _append_text(cell, f"{lt_text}{suffix}")

    wb.save(str(out_path))
