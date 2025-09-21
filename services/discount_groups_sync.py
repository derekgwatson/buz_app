# services/discount_groups_sync.py
# PEP 8 compliant

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import re
import time

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# Uses your existing helpers per your project memories:
# - GoogleSheetsService (auth + read_range)
# - OpenPyXLFileHandler (if you prefer; here we use openpyxl directly to preserve VBA)
from services.google_sheets_service import GoogleSheetsService  # adjust import path if needed
from services.config_service import ConfigManager


@dataclass
class GridConfig:
    sheet_name: str = "Discount Groups"
    header_row: int = 1                # row with column headers
    code_col: int = 1                  # column index (1-based) for product code
    desc_col: int = 2                  # column index for product description
    first_group_col: int = 3           # the first discount group (customer/group) column


@dataclass
class MappingConfig:
    sheet_name: str = "Buz name mapping"
    # Expected headers in row 1 of the mapping sheet:
    # Example: Column A = "Tab Product", Column B = "Grid Product Code"
    tab_product_header: str = "Tab Product"
    grid_code_header: str = "Grid Product Code"


@dataclass
class CustomerTabConfig:
    header_row: int = 4                # row with headers in each customer tab
    # Column letters or names in row header; we normalise by text:
    product_col_header: str = "Product"         # Column A (data starts row 5)
    discount_col_header: str = "Discount"       # Column C (as number: 15 or 15% or 0.15)


def _norm(s: str) -> str:
    return re.sub(r"[\s_]+", "", str(s or "").strip().lower())


def _find_header_row_in_col(col_values: list[str], wanted_header: str, default_header_row: int) -> int:
    """Return 1-based header row; fall back to default if not found."""
    wanted = _norm(wanted_header)
    for i, v in enumerate(col_values, start=1):  # 1-based
        if _norm(v) == wanted:
            return i
    return default_header_row


def _as_percent(val: str | float | int) -> Optional[float]:
    """
    Accept:
      15 -> 15.0
      '15%' -> 15.0
      0.15 -> 15.0
      '0.15' -> 15.0
      '' or None -> None
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("%"):
        s = s[:-1].strip()
    try:
        n = float(s)
    except ValueError:
        return None
    if 0 < n <= 1:
        return round(n * 100.0, 4)
    return round(n, 4)


class DiscountGroupsSync:
    """
    Orchestrates:
      1) Manual step hints (create group, attach to customer).
      2) Read Google Sheet tabs and mapping.
      3) Write a new column (per customer) into Discount Grid .xlsm, preserving macros.
    """

    def __init__(self, config: ConfigManager) -> None:
        self.config = config
        self.grid_cfg = GridConfig(**config.get("discount_grid.grid", default={}))
        self.map_cfg = MappingConfig(**config.get("discount_grid.mapping_tab", default={}))
        self.tab_cfg = CustomerTabConfig(**config.get("discount_grid.customer_tabs", default={}))

        self.sheet_id = (config.get("discount_grid.google_sheet_id", default="") or "").strip()
        if not self.sheet_id:
            raise ValueError("Missing discount_grid.google_sheet_id in config.json")

        # Buz URLs for manual steps
        self.url_create_group = "https://go.buzmanager.com/Settings/CustomerDiscountGroups"
        self.url_find_customer = "https://go.buzmanager.com/Contacts/Customers"

        # Name pattern for the newly added discount group columns in the grid
        # By default use the tab name (customer name). Override via config if needed.
        self.new_group_name_template = config.get(
            "discount_grid.new_group_name_template",
            default="{customer_tab}"
        )
        self.ignore_tabs_raw = config.get("discount_grid", "ignore_tabs", default=[]) \
                               or config.get("discount_grid.ignore_tabs", default=[]) \
                               or []
        self.ignore_tabs = {_norm(x) for x in self.ignore_tabs_raw}

        self.throttle_seconds = float(
            config.get("discount_grid", "throttle_seconds", default=0.4)
            or config.get("discount_grid.throttle_seconds", default=0.4)
        )

    def _should_ignore_tab(self, tab: str) -> bool:
        """True if tab is mapping/utility/ignored by config."""
        if tab == self.map_cfg.sheet_name:
            return True
        if tab.startswith("_"):
            return True
        return _norm(tab) in self.ignore_tabs

    def list_customer_tabs(self, gs: GoogleSheetsService) -> list[str]:
        """All usable customer tabs after filtering."""
        tabs = gs.get_sheet_names(self.sheet_id)
        return [t for t in tabs if not self._should_ignore_tab(t)]

    @staticmethod
    def _extract_sheet_id(sheet_url: str) -> str:
        # Supports full URLs like:
        # https://docs.google.com/spreadsheets/d/<ID>/edit
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
        return m.group(1) if m else sheet_url

    # -----------------------------
    # Public API
    # -----------------------------

    def get_manual_steps(self, customer_tabs: List[str]) -> List[Dict[str, str]]:
        """
        Produce human steps with links for each tab/customer.
        """
        steps: List[Dict[str, str]] = []
        for tab in customer_tabs:
            group_name = self.new_group_name_template.format(customer_tab=tab).strip()
            steps.append({
                "customer_tab": tab,
                "discount_group_to_create": group_name,
                "create_group_url": self.url_create_group,
                "find_customer_url": self.url_find_customer,
                "note": "Create the discount group in Buz, then edit the customer card and attach the new group."
            })
        return steps

    def _resolve_header(self,actual_headers: List[str], wanted: str) -> Optional[str]:
        """Return the actual header text in the sheet that matches 'wanted' (case/space insensitive)."""
        want = _norm(wanted)
        for h in actual_headers:
            if _norm(h) == want:
                return h
        return None

    # top-level helpers in this file
    def build(self, input_xlsm_path: str, output_xlsm_path: str) -> Dict[str, any]:
        """
        Read Google Sheet, add/update columns in XLSM grid, and save as a new file.
        Returns a summary (for UI).
        """
        gs = GoogleSheetsService()

        # --- 1) Mapping sheet: tab product -> grid product code (headers are on row 1)
        mapping_rows = gs.read_tab_as_records(
            spreadsheet_id=self.sheet_id,
            sheet_name=self.map_cfg.sheet_name,
            header_row=1,  # mapping tab headers are in the first row
        )
        if not mapping_rows:
            raise ValueError(f"Mapping sheet '{self.map_cfg.sheet_name}' is empty or missing.")

        headers = list(mapping_rows[0].keys())
        tab_hdr = self._resolve_header(headers, self.map_cfg.tab_product_header)
        grid_hdr = self._resolve_header(headers, self.map_cfg.grid_code_header)
        if not tab_hdr or not grid_hdr:
            raise ValueError(
                f"Mapping sheet must have headers '{self.map_cfg.tab_product_header}' and "
                f"'{self.map_cfg.grid_code_header}'. Found: {headers}"
            )

        tab_to_grid_code: Dict[str, str] = {}
        for row in mapping_rows:
            tab_name = str(row.get(tab_hdr, "")).strip()
            grid_code = str(row.get(grid_hdr, "")).strip()
            if tab_name and grid_code:
                tab_to_grid_code[tab_name] = grid_code

        # --- 2) Discover customer tabs (all tabs except mapping + those not prefixed with "_")
        customer_tabs = self.list_customer_tabs(gs)  # gs.get_sheet_names(self.sheet_id)

        # --- 3) Open .xlsm grid (preserve VBA)
        wb = load_workbook(input_xlsm_path, keep_vba=True)
        ws: Worksheet = wb[self.grid_cfg.sheet_name]

        # --- 4) Build row index in grid: grid product code -> row index
        code_to_row: Dict[str, int] = {}
        for r in range(self.grid_cfg.header_row + 1, ws.max_row + 1):
            code = str(ws.cell(row=r, column=self.grid_cfg.code_col).value or "").strip()
            if code:
                code_to_row[code] = r

        # --- 5) Existing group columns (header row)
        existing_headers: Dict[str, int] = {}
        for c in range(1, ws.max_column + 1):
            name = str(ws.cell(row=self.grid_cfg.header_row, column=c).value or "").strip()
            if name:
                existing_headers[name] = c

        # --- 6) For each customer tab, add/update a column
        customers_summary: List[Dict[str, any]] = []
        total_changes = 0
        changes_sample: List[Dict[str, any]] = []

        for tab in customer_tabs:
            rows = self._read_products_discounts_fast(gs, tab, max_rows=2000)

            if self.throttle_seconds > 0:
                time.sleep(self.throttle_seconds)

            if not rows:
                customers_summary.append({"tab": tab, "added": False, "reason": "empty tab"})
                continue

            # Resolve product/discount keys on this tab (case/space insensitive)
            hdr_keys = list(rows[0].keys())
            prod_key = self._resolve_header(hdr_keys, self.tab_cfg.product_col_header)
            disc_key = self._resolve_header(hdr_keys, self.tab_cfg.discount_col_header)
            if not prod_key or not disc_key:
                customers_summary.append({
                    "tab": tab,
                    "group_column": "",
                    "cells_changed": 0,
                    "reason": f"missing headers '{self.tab_cfg.product_col_header}' and/or '{self.tab_cfg.discount_col_header}'"
                })
                continue

            # Determine/ensure the target group column in the grid
            group_col_name = self.new_group_name_template.format(customer_tab=tab).strip()
            target_col = existing_headers.get(group_col_name)
            if not target_col:
                target_col = ws.max_column + 1
                ws.cell(row=self.grid_cfg.header_row, column=target_col).value = group_col_name
                existing_headers[group_col_name] = target_col

            # Push discounts
            changed = 0
            for row in rows:
                tab_product = str(row.get(prod_key, "")).strip()
                pct = _as_percent(row.get(disc_key))
                if not tab_product or pct is None:
                    continue

                grid_code = tab_to_grid_code.get(tab_product)
                if not grid_code:
                    continue  # no mapping from tab product -> grid product code

                grid_row = code_to_row.get(grid_code)
                if not grid_row:
                    continue  # product code not present in the Excel grid

                # Only count/write when value actually changes
                cell = ws.cell(row=grid_row, column=target_col)
                before = cell.value
                after = float(pct)
                if before is None or float(before) != after:
                    cell.value = after
                    changed += 1
                    total_changes += 1
                    if len(changes_sample) < 50:
                        changes_sample.append({
                            "product_code": grid_code,
                            "group_column": group_col_name,
                            "before": before,
                            "after": after,
                        })

            customers_summary.append({
                "tab": tab,
                "group_column": group_col_name,
                "cells_changed": changed,
            })

        # --- 7) Save new copy (preserving macros)
        wb.save(output_xlsm_path)

        return {
            "output_file": output_xlsm_path,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "totals": {"cells_changed": total_changes},
            "customers": customers_summary,
            "changes_sample": changes_sample,
        }

    def _read_products_discounts_fast(
        self,
        gs: GoogleSheetsService,
        sheet_name: str,
        *,
        max_rows: int = 2000,
    ) -> list[dict[str, str]]:
        """
        Discount-grid specific: scan column A for the 'Product' header,
        then read columns A (Product) and C (Discount) only.
        """
        col_a = gs.col_values(self.sheet_id, sheet_name, 1, max_rows=max_rows)  # A
        hdr_row = _find_header_row_in_col(col_a, self.tab_cfg.product_col_header, self.tab_cfg.header_row)
        start_row = hdr_row + 1

        col_c = gs.col_values(self.sheet_id, sheet_name, 3, max_rows=max_rows)  # C

        out: list[dict[str, str]] = []
        end = max(len(col_a), len(col_c))
        for r in range(start_row, end + 1):
            i = r - 1
            prod = col_a[i] if i < len(col_a) else ""
            disc = col_c[i] if i < len(col_c) else ""
            if not (prod or disc):
                continue
            out.append({
                self.tab_cfg.product_col_header: prod,
                self.tab_cfg.discount_col_header: disc,
            })
        return out


