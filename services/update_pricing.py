from collections import defaultdict
from datetime import date, timedelta
import logging
from services.excel import OpenPyXLFileHandler

logger = logging.getLogger(__name__)

# --- Markup Spreadsheet Info ---
MARKUP_SPREADSHEET_ID = "1tDG-SacdTHkNPH_f_-upgfEStCnhHk4Lwjh0IJumH6I"
MARKUP_RANGE = "Data!A:P"

# --- Column Names: Markup Sheet ---
COL_MARKUP_GROUP = "Buz inventory group code"
COL_MARKUP_PERCENT = "WS Markup 2025"
COL_WASTAGE_PERCENT = "Wastage (Fabric)"

# --- Pricing Spreadsheet Info ---
PRICING_SPREADSHEET_ID = "1c8jTXzHBH7yJRT1AW7r7DcZpzPyanYhojQ2GQanJmJE"
PRICING_MARKUP_RANGE = "Categories!A:E"

# --- Column Names: Pricing Sheet ---
COL_PRICE_SUBGROUP = "Sub Category"
COL_PRICE_VALUE = "Price"


def tomorrow():
    return (date.today() + timedelta(days=1)).isoformat()


def generate_pricing_upload_from_unleashed(db_manager, sheets_service, pricing_config):
    # --- 1. Load markup map from Google Sheet ---
    raw_rows = sheets_service.fetch_sheet_data(MARKUP_SPREADSHEET_ID, MARKUP_RANGE)
    headers = raw_rows[0]
    required_markup_columns = [COL_MARKUP_GROUP, COL_MARKUP_PERCENT, COL_WASTAGE_PERCENT]
    for col in required_markup_columns:
        if col not in headers:
            raise ValueError(f"Missing expected column in markup sheet: {col}")
    group_col = headers.index(COL_MARKUP_GROUP)
    markup_col = headers.index(COL_MARKUP_PERCENT)
    wastage_col = headers.index(COL_WASTAGE_PERCENT)

    markup_map = {}
    wastage_map = {}
    for row in raw_rows[1:]:
        if len(row) <= max(group_col, markup_col):
            continue
        group_cell = row[group_col].strip()
        try:
            markup = float(row[markup_col].strip().rstrip('%'))
            wastage = float(row[wastage_col].strip().rstrip('%')) if len(row) > wastage_col and row[wastage_col].strip() else 0
            markup_factor = 1 + (markup / 100)
            wastage_factor = 1 + (wastage / 100)
            for group in [g.strip() for g in group_cell.split(",") if g.strip()]:
                markup_map[group] = markup_factor
                wastage_map[group] = wastage_factor
        except (ValueError, IndexError):
            continue

    # --- 2. Load base costs from Google Sheet ---
    subgroup_cost_map = {}
    raw_rows_pricing = sheets_service.fetch_sheet_data(PRICING_SPREADSHEET_ID, PRICING_MARKUP_RANGE)
    if not raw_rows_pricing:
        return {
            "file": None,
            "log": ["❌ Failed to load pricing sheet or sheet is empty."]
        }

    headers_pricing = raw_rows_pricing[0]
    required_pricing_columns = [COL_PRICE_SUBGROUP, COL_PRICE_VALUE]
    for col in required_pricing_columns:
        if col not in headers_pricing:
            raise ValueError(f"Missing expected column in pricing sheet: {col}")
    cost_group_col = headers_pricing.index(COL_PRICE_SUBGROUP)
    base_cost_col = headers_pricing.index(COL_PRICE_VALUE)

    for row in raw_rows_pricing[1:]:
        if len(row) <= max(cost_group_col, base_cost_col):
            continue
        subgroup = row[cost_group_col].strip()
        try:
            base_cost = float(row[base_cost_col].strip())
            if subgroup and base_cost:
                subgroup_cost_map[subgroup] = round(base_cost, 2)
        except (ValueError, IndexError):
            continue

    # --- 3. Load Buz pricing and inventory items ---
    pricing_rows = db_manager.execute_query("SELECT * FROM pricing_data").fetchall()
    inventory_rows = db_manager.execute_query("SELECT Code, SupplierProductCode, inventory_group_code FROM inventory_items").fetchall()
    inventory_map = {r["Code"]: r for r in inventory_rows}
    unleashed = db_manager.execute_query("SELECT * FROM unleashed_products").fetchall()
    unleashed_map = {r["ProductCode"]: r for r in unleashed}

    # --- 4. Process updates ---
    updates_by_group = defaultdict(list)
    updated_headers = [entry["spreadsheet_column"] for entry in pricing_config]
    db_fields = [entry["database_field"] for entry in pricing_config]

    from collections import Counter

    log_messages = []
    error_counts = Counter()
    for row in pricing_rows:
        inv_code = row["InventoryCode"]
        item = inventory_map.get(inv_code)
        if not item:
            continue

        supplier_code = item["SupplierProductCode"]
        group_code = item["inventory_group_code"]
        unleashed_row = unleashed_map.get(supplier_code)
        if not unleashed_row:
            if supplier_code:
                error_counts["Missing Unleashed row"] += 1
                log_messages.append(f"⚠️ No Unleashed row found for supplier code {supplier_code}")
            else:
                error_counts["Missing supplier code"] += 1
                log_messages.append(f"⚠️ Inventory item {inv_code} has no supplier code")
            continue

        subgroup = unleashed_row["ProductSubGroup"]
        cost = subgroup_cost_map.get(subgroup)
        markup = markup_map.get(group_code)
        if cost is None:
            error_counts["Missing base cost"] += 1
            log_messages.append(f"⚠️ No cost found for subgroup '{subgroup}' ({supplier_code})")
            continue

        if markup is None:
            error_counts["Missing markup"] += 1
            log_messages.append(f"⚠️ No markup found for inventory group '{group_code}'")
            continue

        wastage_factor = wastage_map.get(group_code)
        if wastage_factor is None:
            error_counts["Missing wastage"] += 1
            log_messages.append(f"⚠️ No wastage defined for inventory group '{group_code}'")
            continue

        adjusted_cost = cost * wastage_factor
        new_cost = round(adjusted_cost, 2)
        new_sell = round(new_cost * markup, 2)

        current_cost = round(row["CostSQM"], 2) if row["CostSQM"] is not None else None
        current_sell = round(row["SellSQM"], 2) if row["SellSQM"] is not None else None

        def changed(a, b):
            if a is None or b is None:
                return True
            return abs(a - b) / b > 0.005

        if changed(current_cost, new_cost) or changed(current_sell, new_sell):
            updated_row = {field: row[field] for field in db_fields}
            updated_row.update({
                "PkId": "",
                "Operation": "A",
                "DateFrom": tomorrow(),
                "CostSQM": new_cost,
                "SellSQM": new_sell
            })
            ordered_values = [updated_row.get(field) for field in db_fields]
            updates_by_group[group_code].append(ordered_values)
#            log_messages.append(f"✅ Price updated for {inv_code} (new cost: {new_cost}, new sell: {new_sell})")

    # --- 5. Return OpenPyXLFileHandler with updated workbook ---
    if not updates_by_group:
        logger.info("No pricing updates detected — nothing to generate.")
        return {
            "file": None,
            "log": log_messages or ["✅ No changes detected."]
        }

    if error_counts:
        summary_lines = ["🔍 Summary of issues:"]
        for key, count in error_counts.items():
            summary_lines.append(f"• {key}: {count}")
        log_messages = summary_lines + log_messages

    from collections import Counter

    # Collapse identical log lines with counts
    line_counts = Counter(log_messages)
    collapsed_log = []

    for line, count in line_counts.items():
        if count > 1:
            collapsed_log.append(f"{line} (x{count})")
        else:
            collapsed_log.append(line)

    log_messages = collapsed_log

    return {
        "file": OpenPyXLFileHandler.from_sheets_data(
            updates_by_group,
            {
                "headers": updated_headers,
                "header_row": 1
            }
        ),
        "log": log_messages
    }
