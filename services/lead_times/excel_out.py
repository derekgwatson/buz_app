from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Set, Optional
import os, time
import re

from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string
from openpyxl.worksheet.worksheet import Worksheet


_DETAILED_LEAD_RE = re.compile(r"(?is)(lead\s*time\s*:\s*)(.*?)(?=(?:\\n|\n)\s*-\s|$)")
_CUTOFF_RE = re.compile(r"(?is)\b(?:christmas|xmas)\s*cut[\s-]*off\s*:\s*(.*?)(?=(?:\r?\n)\s*-\s|$)")


def _review(review_set: set[str], warnings: list[str], store: str, code: str, reason: str) -> None:
    review_set.add(code)
    warnings.append(f"[{store}/{code}] {reason} — needs review.")


def _tab_rgb(ws: Worksheet) -> tuple[int, int, int] | None:
    """Return (r,g,b) from the sheet tab colour if present, else None."""
    col = getattr(ws.sheet_properties, "tabColor", None)
    if not col or not getattr(col, "rgb", None):
        return None
    h = col.rgb.upper()
    # ARGB or RGB → keep last 6 as RRGGBB
    if len(h) >= 6:
        h = h[-6:]
    try:
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    except Exception:
        return None


def _tab_is_reddish(ws: Worksheet) -> bool:
    """True if tab colour is red-dominant (robust to shades)."""
    rgb = _tab_rgb(ws)
    if not rgb:
        return False
    r, g, b = rgb
    return r >= 180 and (r - max(g, b)) >= 40  # clearly red-dominant


def _find_summary_ready_row(
    ws: Worksheet,
    ins_idx: int,
    header_row: int,
    ready_phrase: str = "Ready in",
) -> Optional[int]:
    """
    Scan column C for the first cell containing 'Ready in' (case-insensitive),
    starting strictly below the header row.
    """
    start = max(1, int(header_row)) + 1
    max_row = ws.max_row or start
    want = (ready_phrase or "Ready in").lower()
    for r in range(start, max_row + 1):
        v = ws.cell(row=r, column=ins_idx).value
        if isinstance(v, str) and want in v.lower():
            return r
    return None


def _replace_ready_in_line(
    text: str,
    new_lead: str,
    lead_regex: Optional[str],
    ready_phrase: str = "Ready in",
) -> tuple[str, bool]:
    """
    In a summary cell value, find 'Ready in ...' and replace the *lead-time phrase*
    that follows using `lead_regex`. If no phrase is found after 'Ready in',
    insert the new lead right after the phrase and keep the rest unchanged.
    Returns (new_text, replaced?).
    """
    if not isinstance(text, str) or not text.strip():
        return text, False

    s = text
    rp = (ready_phrase or "Ready in")
    low = s.lower()
    i = low.find(rp.lower())
    if i == -1:
        return text, False

    # Build a case-insensitive regex for the lead phrase
    pat = lead_regex or r"\b\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?\s*(?:weeks?|wks?|wk)\b"
    if pat.startswith("(?i)"):
        pat = pat[4:]
    rx = re.compile(pat, re.IGNORECASE)

    # Try to replace an existing lead phrase found *after* 'Ready in'
    m = rx.search(s, i)
    if m:
        return s[:m.start()] + new_lead + s[m.end():], True

    # No existing phrase: insert after 'Ready in'
    insert_at = i + len(rp)
    needs_space = not (insert_at < len(s) and s[insert_at].isspace())
    new_val = s[:insert_at] + (" " if needs_space else "") + new_lead + s[insert_at:]
    return new_val, True


def _replace_detailed_lead_line(text: str, new_lead: str) -> tuple[str, bool]:
    """
    Replace the value after 'Lead Time:' up to the next bullet ('-') or newline.
    Works with literal '\n' or real newlines. Returns (new_text, replaced?).
    """
    if not isinstance(text, str) or not text:
        return new_lead, False
    m = _DETAILED_LEAD_RE.search(text)
    if not m:
        return text, False
    a, b = m.span(2)  # the existing lead-time value
    return text[:a] + new_lead + text[b:], True


def _find_detailed_lead_row(ws: Worksheet, ins_idx: int, header_row: int) -> int | None:
    """
    Find the row (in column B) that contains 'Lead Time:'.
    Start at the first data row (header_row + 1) so rows like 3 are included.
    """
    start = max(1, int(header_row)) + 1
    max_row = ws.max_row or start
    for r in range(start, max_row + 1):
        v = ws.cell(row=r, column=ins_idx).value
        if isinstance(v, str) and "lead time" in v.lower():
            return r
    return None


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
    Treat as a heading-only tab when there's no meaningful data below the header.
    We scan A:Z; ignore a lone value in G{header_row+1}.
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
    """
    Keep only tabs whose names match keep_names (case-insensitive, trimmed).
    Never remove all sheets. Emit a concise diff warning.
    """
    keep_norm = {_norm_name(s) for s in keep_names if s}
    tab_raw = list(wb.sheetnames)
    tab_norm_map = {_norm_name(n): n for n in tab_raw}

    if not keep_norm:
        warnings.append("[PRUNE] Control list is empty — skipping prune.")
        return

    tabs_norm = set(tab_norm_map.keys())
    missing_tabs_for_codes = sorted(keep_norm - tabs_norm)
    extra_tabs_not_codes   = sorted(tabs_norm - keep_norm)

    def _pv(items, k=12):
        return "—" if not items else (", ".join(items[:k]) + (f", +{len(items)-k} more" if len(items)>k else ""))

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


def _find_first_false(ws, col_idx: int, header_row: int) -> int | None:
    max_row = ws.max_row or header_row
    for r in range(header_row + 1, max_row + 1):
        v = ws.cell(row=r, column=col_idx).value
        if v is False:
            return r
        if isinstance(v, str) and v.strip().upper() == "FALSE":
            return r
    return None


def _first_nonblank(ws, col_idx: int, header_row: int) -> int | None:
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


@dataclass
class InjectResult:
    saved_path: Path
    review_codes: Set[str]  # tabs that need manual review for this workbook


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
    # SUMMARY options
    summary_lead_regex: Optional[str] = None,
    # DETAILED: template to prefix when NO 'Lead Time:' exists in col B
    detailed_prefix_template: Optional[str] = None,
) -> InjectResult:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb = load_workbook(filename=str(template_path), keep_vba=True, data_only=True)

    _prune_tabs_safe(wb, keep_names=set(control_codes), warnings=warnings)

    ins_idx = _col_to_idx(insertion_col_letter)
    anc_idx = _col_to_idx(anchor_col_letter)
    is_summary = (workbook_kind or "").lower() == "summary"

    review_codes: Set[str] = set()

    for code in list(wb.sheetnames):
        ws = wb[code]

        # Silently drop heading-only tabs
        if _is_effectively_empty_sheet(ws, anchor_header_row):
            wb.remove(ws)
            continue

        # Review by tab colour
        if _tab_is_reddish(ws):
            _review(review_codes, warnings, store_name, code, f"Tab coloured red (RGB={_tab_rgb(ws)})")
            continue  # do not write into coloured tabs

        # Lead record
        lead_rec = leads_by_code.get(code)
        if not lead_rec:
            warnings.append(f"[{store_name}/{code}] No lead-time text found — skipped.")
            _review(review_codes, warnings, store_name, code, "No lead-time text")
            continue
        lt_text = (lead_rec.get("lead_time_text") or "").strip()
        if not lt_text:
            warnings.append(f"[{store_name}/{code}] Empty lead-time text — skipped.")
            continue

        # Optional cutoff suffix (proposed)
        cut = (cutoffs_by_code.get(code, {}).get("cutoff") or "").strip()
        suffix = f" ***CHRISTMAS CUTOFF {cut}***" if cut else ""

        # === APPLY CHANGES (only for tabs that differ) ===
        if is_summary:
            # --- SUMMARY ---
            ready_row = _find_summary_ready_row(ws, ins_idx, anchor_header_row, ready_phrase="Ready in")
            if ready_row is None:
                warnings.append(
                    f"[{store_name}/{code}] Summary: no 'Ready in' in {insertion_col_letter} — needs review.")
                _review(review_codes, warnings, store_name, code, "Summary: missing 'Ready in' in column C")
                continue

            cell = ws.cell(row=ready_row, column=ins_idx)
            original = cell.value if isinstance(cell.value, str) else ""

            # Build the text we WOULD write, without touching the sheet yet
            new_core, replaced = _replace_ready_in_line(
                original,
                lt_text,
                summary_lead_regex,
                ready_phrase="Ready in",
            )
            if not replaced:
                warnings.append(f"[{store_name}/{code}] Summary: couldn't update lead after 'Ready in' — needs review.")
                _review(review_codes, warnings, store_name, code, "Summary: failed to replace lead after 'Ready in'")
                continue

            proposed = f"{new_core}{suffix}"

            # === Drop unchanged tabs by exact string compare ===
            if proposed == original:
                wb.remove(ws)
                warnings.append(f"[{store_name}/{code}] Unchanged (exact match). Dropped tab.")
                continue

            # Commit write only if changed
            cell.value = proposed

        else:
            # === DETAILED ===
            lead_row = _find_detailed_lead_row(ws, ins_idx, anchor_header_row)

            if lead_row is None:
                # No 'Lead Time:' anywhere in col B → prefix at first FALSE in anchor column
                anchor_row = _find_first_false(ws, anc_idx, header_row=anchor_header_row)
                if anchor_row is None:
                    warnings.append(
                        f"[{store_name}/{code}] Detailed: no 'Lead Time:' and no FALSE in {anchor_col_letter} — skipped."
                    )
                    review_codes.add(code)
                    continue

                cell = ws.cell(row=anchor_row, column=ins_idx)
                existing = cell.value if isinstance(cell.value, str) else (
                    "" if cell.value is None else str(cell.value))
                template = detailed_prefix_template or "\n       -       Lead Time: {LEAD} \n       -       "
                proposed = f"{_apply_template(template, lt_text)}{existing}{suffix}"

                if proposed == (
                cell.value if isinstance(cell.value, str) else ("" if cell.value is None else str(cell.value))):
                    wb.remove(ws)
                    warnings.append(f"[{store_name}/{code}] Unchanged (exact match). Dropped tab.")
                    continue

                cell.value = proposed
                _review(review_codes, warnings, store_name, code, "Detailed: no 'Lead Time:'; prefixed at first FALSE")

            else:
                # Replace in-place on the found 'Lead Time:' line
                cell = ws.cell(row=lead_row, column=ins_idx)
                original = cell.value if isinstance(cell.value, str) else ""
                new_core, replaced = _replace_detailed_lead_line(original, lt_text)
                if not replaced:
                    warnings.append(
                        f"[{store_name}/{code}] Detailed: couldn't replace value after 'Lead Time:' — needs review."
                    )
                    _review(review_codes, warnings, store_name, code,
                            "Detailed: failed to replace value after 'Lead Time:'")
                    continue

                proposed = f"{new_core}{suffix}"

                # Drop unchanged tabs by exact string compare
                if proposed == original:
                    wb.remove(ws)
                    warnings.append(f"[{store_name}/{code}] Unchanged (exact match). Dropped tab.")
                    continue

                cell.value = proposed

    # Keep at least one visible sheet
    if not any(s.sheet_state == "visible" for s in wb.worksheets):
        stub = wb.create_sheet("NO CHANGES")
        stub["A1"] = f"{store_name} — All tabs unchanged (lead time & Christmas cut-off match current)."
        stub.sheet_state = "visible"

    # Save with .xlsm if workbook has macros
    final_path = out_path
    try:
        has_macros = getattr(wb, "vba_archive", None) is not None
    except Exception:
        has_macros = False
    if has_macros and out_path.suffix.lower() != ".xlsm":
        final_path = out_path.with_suffix(".xlsm")
    wb.save(str(final_path))
    try:
        ts = time.strftime("%H:%M:%S", time.localtime(os.path.getmtime(final_path)))
        warnings.append(f"[DEBUG] Saved {Path(final_path).name} at {ts} -> {final_path}")
    except Exception:
        pass

    if review_codes:
        warnings.append(f"[DEBUG/{store_name}] Review tabs: " + ", ".join(sorted(review_codes))[:400])

    return InjectResult(saved_path=final_path, review_codes=review_codes)


def save_review_only_workbook(
    *,
    template_path: Path,
    out_path: Path,
    review_codes: Set[str],
    warnings: list[str],
) -> Path:
    """
    Create a review-only workbook: no edits, only the tabs in `review_codes`.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb = load_workbook(filename=str(template_path), keep_vba=True, data_only=True)
    _prune_tabs_safe(wb, keep_names=set(review_codes), warnings=warnings)

    final = out_path
    try:
        has_macros = getattr(wb, "vba_archive", None) is not None
    except Exception:
        has_macros = False
    if has_macros and out_path.suffix.lower() != ".xlsm":
        final = out_path.with_suffix(".xlsm")

    wb.save(str(final))
    return final
