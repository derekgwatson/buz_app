import json
import os
from datetime import datetime, timedelta
from openpyxl import Workbook
import logging

logger = logging.getLogger(__name__)

UNLEASHED_SUPPLIER_CODE = "UNLEASHED"
UNLEASHED_SUPPLIER_NAME = "Unleashed ERP"


# ---------- small utils ----------

def _norm_code(s: str | None) -> str:
    """Strip any leading asterisks and normalise case/whitespace."""
    if not s:
        return ""
    return s.lstrip("*").strip().upper()


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _resolve_output_dir(output_dir: str) -> str:
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(_project_root(), output_dir)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def _safe_wb():
    wb = Workbook()
    ws = wb.active
    ws.title = "README"
    ws.append(["This file contains Buz upload data. This sheet is removed if any data sheets are created."])
    return wb


def _norm_group_key(s: str | None) -> str:
    return (s or "").strip()


def build_sequential_code_provider(db, config: dict, default_start: int = 10000):
    """
    Returns a callable(db, supplier_code_norm) -> new Buz 'Code'.

    Prefix choice:
      1) Try to infer from Unleashed ProductGroup via config['unleashed_group_to_inventory_groups'] (first entry)
      2) Fallback to 'FAB' if no mapping is found

    Uniqueness:
      - Looks up the current MAX numeric suffix for that prefix in inventory_items.Code
      - Caches next number per prefix to avoid multiple queries when adding many items
    """
    # Normalize mapping keys once
    group_map_raw = config.get("unleashed_group_to_inventory_groups", {})
    group_map = { _norm_group_key(k): v for k, v in group_map_raw.items() }

    next_by_prefix: dict[str, int] = {}

    def _infer_prefix(db_inner, supplier_code_norm: str) -> str:
        # Find the Unleashed ProductGroup for this product code
        row = db_inner.execute_query(
            """
            SELECT ProductGroup
            FROM unleashed_products
            WHERE UPPER(LTRIM(TRIM(ProductCode), '*')) = ?
            LIMIT 1
            """,
            (supplier_code_norm,)
        ).fetchone()

        if row:
            pg = _norm_group_key(row["ProductGroup"])
            inv_groups = group_map.get(pg) or []
            if inv_groups:
                return inv_groups[0]  # take the first configured inventory group code

        return "FAB"  # safe fallback

    def _init_prefix_counter(db_inner, prefix: str) -> int:
        # SUBSTR is 1-based in SQLite; strip the prefix so we can cast the remainder to INTEGER
        res = db_inner.execute_query(
            """
            SELECT MAX(CAST(SUBSTR(Code, ?) AS INTEGER)) AS max_num
            FROM inventory_items
            WHERE Code LIKE ?
            """,
            (len(prefix) + 1, f"{prefix}%")
        ).fetchone()

        max_num = res["max_num"] if res and res["max_num"] is not None else (default_start - 1)
        return int(max_num) + 1

    def provider(db_inner, supplier_code_norm: str) -> str:
        prefix = _infer_prefix(db_inner, supplier_code_norm)

        if prefix not in next_by_prefix:
            next_by_prefix[prefix] = _init_prefix_counter(db_inner, prefix)

        num = next_by_prefix[prefix]
        code = f"{prefix}{num:05d}"
        next_by_prefix[prefix] = num + 1
        return code

    return provider

# ---------- supplier setup ----------

def get_or_create_unleashed_supplier(db) -> int:
    row = db.execute_query(
        "SELECT id FROM suppliers WHERE supplier_code = ?",
        (UNLEASHED_SUPPLIER_CODE,)
    ).fetchone()
    if row:
        return row["id"]

    now = datetime.utcnow()
    db.insert_item("suppliers", {
        "supplier_code": UNLEASHED_SUPPLIER_CODE,
        "supplier": UNLEASHED_SUPPLIER_NAME,   # NOTE: your column name is 'supplier'
        "is_active": 1,
        "created_at": now,
        "updated_at": now,
    })
    row = db.execute_query(
        "SELECT id FROM suppliers WHERE supplier_code = ?",
        (UNLEASHED_SUPPLIER_CODE,)
    ).fetchone()
    logger.info(f"Created supplier {UNLEASHED_SUPPLIER_NAME} with id={row['id']}")
    return row["id"]


def tag_local_fabrics_as_unleashed(db, unleashed_supplier_id: int):
    """Mark any fabric whose code appears in Unleashed (after stripping leading *) as Unleashed-sourced."""
    u_rows = db.execute_query(
        "SELECT DISTINCT ProductCode FROM unleashed_products WHERE ProductCode IS NOT NULL AND TRIM(ProductCode) != ''"
    ).fetchall()
    u_codes = {_norm_code(r["ProductCode"]) for r in u_rows}

    f_rows = db.execute_query(
        "SELECT id, supplier_product_code, supplier_id FROM fabrics"
    ).fetchall()

    updated = 0
    now = datetime.utcnow()
    for f in f_rows:
        if _norm_code(f["supplier_product_code"]) in u_codes and f["supplier_id"] != unleashed_supplier_id:
            db.execute_query(
                "UPDATE fabrics SET supplier_id = ?, updated_at = ? WHERE id = ?",
                (unleashed_supplier_id, now, f["id"])
            )
            updated += 1
    if updated:
        logger.info(f"Tagged {updated} local fabrics as Unleashed.")


# ---------- Buz code helpers ----------

def _get_existing_buz_code(db, supplier_product_code_norm: str) -> str | None:
    """
    Find the current Buz 'Code' for a fabric by supplier product code.
    Assumes inventory_items table stores SupplierProductCode and Code.
    """
    row = db.execute_query(
        """
        SELECT Code
        FROM inventory_items
        WHERE UPPER(LTRIM(TRIM(SupplierProductCode), '*')) = ?
        ORDER BY LastEditDate DESC NULLS LAST
        LIMIT 1
        """,
        (supplier_product_code_norm,)
    ).fetchone()
    return row["Code"] if row else None


def _append_item_row(ws, items_headers: list[str], buz_code: str, op: str):
    """
    Append one inventory row. Items file uses 'Code' and 'Operation'.
    We leave everything else blank. Row 1 blank, Row 2 headers.
    """
    row = [""] * len(items_headers)

    def idx(name):
        lname = name.lower()
        for i, col in enumerate(items_headers):
            if col.lower() == lname:
                return i
        return None

    i_code = idx("Code")
    i_op   = idx("Operation")

    if i_code is not None:
        row[i_code] = buz_code
    if i_op is not None:
        row[i_op] = op

    ws.append([""] + row)


def _append_pricing_row(ws, pricing_headers: list[str], buz_code: str, date_str: str, sell: float | None, cost: float | None):
    row = [""] * len(pricing_headers)

    def idx(name):
        lname = name.lower()
        for i, col in enumerate(pricing_headers):
            if col.lower() == lname:
                return i
        return None

    i_code = idx("Inventory Code")
    i_date = idx("Date From")
    i_op   = idx("Operation")

    if i_code is not None:
        row[i_code] = buz_code
    if i_date is not None:
        row[i_date] = date_str
    if i_op is not None:
        row[i_op] = "A"

    i_sell = idx("SellSQM")
    i_cost = idx("CostSQM")
    if i_sell is not None and sell is not None:
        row[i_sell] = sell
    if i_cost is not None and cost is not None:
        row[i_cost] = cost

    ws.append(row)


# ---------- main orchestration ----------

def sync_unleashed_fabrics(
    db,
    config_path: str = "config.json",
    output_dir: str = "uploads",
    code_provider=None,            # optional: callable(db, normalized_supplier_code) -> new Buz Code string
    price_provider=None,           # optional: callable(db, normalized_supplier_code) -> {"sell": x, "cost": y}
    pricing_tolerance: float = 0.005
):
    """
    Sync Unleashed ↔ Buz (scoped to Unleashed-sourced fabrics only).

    - Ensures UNLEASHED supplier exists and is active.
    - Tags local fabrics as Unleashed where code appears in Unleashed export (handles any leading '*').
    - Computes adds (inventory A) and deletes (inventory D).
    - Optionally emits pricing adds (A) for codes where price delta exceeds tolerance.
    - Uses Buz 'Code' for upload rows (NOT the Unleashed ProductCode).

    Returns:
        dict {
          "items_file": <path|None>,
          "pricing_file": <path|None>,
          "adds": [supplier_product_code_norm...],
          "deletes": [supplier_product_code_norm...],
          "pricing_count": int
        }
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    items_headers   = [h["spreadsheet_column"] for h in config["headers"]["buz_inventory_item_file"]]
    pricing_headers = [h["spreadsheet_column"] for h in config["headers"]["buz_pricing_file"]]

    unleashed_supplier_id = get_or_create_unleashed_supplier(db)
    tag_local_fabrics_as_unleashed(db, unleashed_supplier_id)

    # Build Unleashed set (U) and Local-Unleashed set (L)
    u_rows = db.execute_query(
        "SELECT DISTINCT ProductCode FROM unleashed_products WHERE ProductCode IS NOT NULL AND TRIM(ProductCode) != ''"
    ).fetchall()
    U = {_norm_code(r["ProductCode"]) for r in u_rows}

    l_rows = db.execute_query(
        "SELECT supplier_product_code FROM fabrics WHERE supplier_id = ?",
        (unleashed_supplier_id,)
    ).fetchall()
    L = {_norm_code(r["supplier_product_code"]) for r in l_rows}

    adds = sorted(U - L)
    deletes = sorted(L - U)

    logger.info(f"Unleashed sync — adds: {len(adds)}, deletes: {len(deletes)}")
    logger.info(f"ADD sample (first 20): {adds[:20]}")
    logger.info(f"DEL sample (first 20): {deletes[:20]}")

    tomorrow_str = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    outdir = _resolve_output_dir(output_dir)

    # ----- INVENTORY UPLOAD (adds + deletes) -----
    items_file_path = None
    wrote_items = False
    items_wb = _safe_wb()
    ws_items = items_wb.create_sheet(title="Items")
    ws_items.append([""] + items_headers)

    # Adds → need a Buz 'Code' for each; use code_provider if given
    for supplier_code_norm in adds:
        if callable(code_provider):
            buz_code = code_provider(db, supplier_code_norm)
            if not buz_code:
                logger.warning(f"No Buz code generated for add {supplier_code_norm}; skipping.")
                continue
        else:
            # fallback: use supplier code as Buz code (only if that matches your convention)
            buz_code = supplier_code_norm
        _append_item_row(ws_items, items_headers, buz_code, op="A")
        wrote_items = True

    # Deletes → look up existing Buz 'Code' in inventory_items
    for supplier_code_norm in deletes:
        buz_code = _get_existing_buz_code(db, supplier_code_norm)
        if not buz_code:
            logger.warning(f"No existing Buz Code found for delete {supplier_code_norm}; skipping.")
            continue
        _append_item_row(ws_items, items_headers, buz_code, op="D")
        wrote_items = True

    if wrote_items:
        items_wb.active = items_wb.sheetnames.index("Items")
        if "README" in items_wb.sheetnames and len(items_wb.sheetnames) > 1:
            try:
                items_wb.remove(items_wb["README"])
            except Exception as e:
                logger.warning(f"Could not remove README from items workbook: {e}")
        items_file_path = os.path.join(outdir, f"buz_items_unleashed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        items_wb.save(items_file_path)

    # ----- PRICING UPLOAD (optional) -----
    pricing_file_path = None
    pricing_count = 0
    if callable(price_provider):
        pricing_wb = _safe_wb()
        ws_price = pricing_wb.create_sheet(title="Pricing")
        ws_price.append(pricing_headers)
        wrote_pricing = False

        # Only price codes that exist both sides (U ∩ L)
        for supplier_code_norm in sorted(U & L):
            prices = price_provider(db, supplier_code_norm) or {}
            sell = prices.get("sell") or prices.get("SellSQM")
            cost = prices.get("cost") or prices.get("CostSQM")

            # We need the Buz Code to put in the pricing file
            buz_code = _get_existing_buz_code(db, supplier_code_norm)
            if not buz_code:
                logger.warning(f"No existing Buz Code found for pricing {supplier_code_norm}; skipping.")
                continue

            _append_pricing_row(ws_price, pricing_headers, buz_code, tomorrow_str, sell, cost)
            wrote_pricing = True
            pricing_count += 1

        if wrote_pricing:
            pricing_wb.active = pricing_wb.sheetnames.index("Pricing")
            if "README" in pricing_wb.sheetnames and len(pricing_wb.sheetnames) > 1:
                try:
                    pricing_wb.remove(pricing_wb["README"])
                except Exception as e:
                    logger.warning(f"Could not remove README from pricing workbook: {e}")
            pricing_file_path = os.path.join(outdir, f"buz_pricing_unleashed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            pricing_wb.save(pricing_file_path)

    summary = {
        "items_file": items_file_path,
        "pricing_file": pricing_file_path,
        "adds": adds,
        "deletes": deletes,
        "pricing_count": pricing_count,
    }
    logger.info(f"Unleashed sync summary: {summary}")
    return summary
