# services/buz_export_inventory.py
from __future__ import annotations

import os
import argparse
import json
from pathlib import Path
from typing import Iterable, Dict, Tuple, List
import requests


def cookies_from_storage_state(path: str | os.PathLike,
                               domain: str = "go.buzmanager.com") -> Dict[str, str]:
    """
    Read Playwright storage_state.json and return cookies for the given domain.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    jar: Dict[str, str] = {}
    for c in data.get("cookies", []):
        d = (c.get("domain") or "").lstrip(".")
        if d == domain or d.endswith(domain) or domain.endswith(d):
            jar[c["name"]] = c["value"]
    return jar


def export_inventory_xlsm(
    cookies: Dict[str, str],
    group_codes: Iterable[str],
    include_not_current: bool = False,
    out_dir: str = "./exports",
    url: str = "https://go.buzmanager.com/Settings/Inventory/Export",
    referer: str = "https://go.buzmanager.com/Settings/Inventory",
    suggested_filename: str | None = None,
) -> str:
    """
    Replicates Buzz 'Inventory Export' button via form POST.
    """
    os.makedirs(out_dir, exist_ok=True)

    form: List[Tuple[str, str]] = []
    for code in group_codes:
        form.append(("inventoryGroupCodes", code))

    form.append(("includeNotCurrent", "true" if include_not_current else "false"))
    form.append(("btnExport", "print"))

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://go.buzmanager.com",
        "Referer": referer,
        "Accept": "application/vnd.ms-excel,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0",
    }

    with requests.post(url, data=form, headers=headers, cookies=cookies, stream=True) as resp:
        resp.raise_for_status()

        cd = resp.headers.get("Content-Disposition", "") or ""
        filename = suggested_filename or "InventoryExport.xlsm"
        if "filename=" in cd:
            filename = cd.split("filename=", 1)[-1].strip().strip('"').strip("'")

        path = os.path.join(out_dir, filename)
        with open(path, "wb") as f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)
    return path


def _cookie_str_to_dict(raw_cookie: str) -> Dict[str, str]:
    """
    Converts 'k1=v1; k2=v2; ...' into {'k1': 'v1', 'k2': 'v2', ...}
    Strips non-latin1 characters to avoid Unicode header errors.
    """
    if raw_cookie is None:
        return {}
    safe = raw_cookie.encode("latin-1", "ignore").decode("latin-1")
    cookies: Dict[str, str] = {}
    for pair in safe.split("; "):
        if "=" in pair:
            k, v = pair.split("=", 1)
            cookies[k] = v
    return cookies


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download Buzz Inventory export (XLSM) by mimicking the UI form POST."
    )
    parser.add_argument(
        "--storage-state",
        default=".secrets/buz_storage_state.json",
        help="Playwright storage_state.json to load cookies from (preferred).",
    )
    parser.add_argument(
        "--cookie",
        default=None,
        help="Full Cookie header value (fallback if no storage-state).",
    )
    parser.add_argument(
        "--group",
        dest="groups",
        action="append",
        required=True,
        help="Inventory group code to include. Repeat for multiple.",
    )
    parser.add_argument(
        "--include-not-current",
        action="store_true",
        help="Include 'not current' items.",
    )
    parser.add_argument(
        "--out",
        default="./exports",
        help="Output directory (default: ./exports).",
    )
    parser.add_argument(
        "--url",
        default="https://go.buzmanager.com/Settings/Inventory/Export",
        help="Override export URL.",
    )
    parser.add_argument(
        "--referer",
        default="https://go.buzmanager.com/Settings/Inventory",
        help="Referer header to send.",
    )
    parser.add_argument(
        "--filename",
        default=None,
        help="Force output filename (otherwise use server's Content-Disposition).",
    )

    args = parser.parse_args()

    cookies: Dict[str, str] = {}
    if args.storage_state and Path(args.storage_state).exists():
        cookies = cookies_from_storage_state(args.storage_state, "go.buzmanager.com")
    if not cookies and args.cookie:
        cookies = _cookie_str_to_dict(args.cookie)

    if not cookies:
        raise SystemExit(
            "No cookies available. Provide --storage-state (preferred) or --cookie."
        )

    path = export_inventory_xlsm(
        cookies=cookies,
        group_codes=args.groups,
        include_not_current=bool(args.include_not_current),
        out_dir=args.out,
        url=args.url,
        referer=args.referer,
        suggested_filename=args.filename,
    )
    print(f"Saved: {path}")
