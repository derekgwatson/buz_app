from __future__ import annotations


def sheet_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"


def tab_url(gsheets_service, sheet_id: str, tab_name: str = '') -> str:
    """
    Resolve a tab by name → return a deep link with the correct gid.
    Falls back to the sheet root if the tab can't be found.
    """
    try:
        ws = gsheets_service._worksheet(sheet_id, tab_name)  # gspread.Worksheet
        gid = getattr(ws, "id", None) or getattr(ws, "properties", {}).get("sheetId")
        if gid is not None:
            return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"
    except Exception:
        pass
    return sheet_url(sheet_id)
