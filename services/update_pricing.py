from collections import defaultdict, Counter
from datetime import date, timedelta
import logging
from services.excel import OpenPyXLFileHandler

logger = logging.getLogger(__name__)

MARKUP_SPREADSHEET_ID = "1tDG-SacdTHkNPH_f_-upgfEStCnhHk4Lwjh0IJumH6I"
MARKUP_RANGE = "Data!A:P"


def tomorrow():
    return (date.today() + timedelta(days=1)).isoformat()


def generate_pricing_upload_from_unleashed(db_manager, sheets_service, pricing_config):
    # --- 1. Load markup map from Google Sheet ---
    raw_rows = sheets_service.fetch_sheet_data(MARKUP_SPREADSHEET_ID, MARKUP_RANGE)
    headers = raw_rows[0]
    group_col = headers.index("Buz inventory group code")
    markup_col = headers.index("WS Markup 2025")

    markup_map = {}
    for row in raw_rows[1:]:
        if len(row) <= max(group_col, markup_col):
            continue
        group_cell = row[group_col].strip()
        try:
            markup = float(row[markup_col].strip().rstrip('%'))
            markup_factor = 1 + (markup / 100)
            for group in [g.strip() for g in group_cell.split(",") if g.strip()]:
                markup_map[group] = markup_factor
        except (ValueError, IndexError):
            continue

    # --- 2. Build product_sub_group → cost map ---
    unleashed = db_manager.execute_query("SELECT * FROM unleashed_products").fetchall()
    subgroup_price_map = defaultdict(list)
    subgroup_cost_map = {}
    pricing_conflicts = {}

    for row in unleashed:
        subgroup = row["ProductSubGroup"]
        price = row["SellPriceTier9"]
        if subgroup and price and subgroup.strip().lower() != "ignore":
            rounded_price = round(price, 2)
            subgroup_price_map[subgroup].append((rounded_price, row["ProductCode"]))

    for subgroup, price_entries in subgroup_price_map.items():
        price_counts = Counter(p for p, _ in price_entries)
        if len(price_counts) > 1:
            most_common_price, _ = price_counts.most_common(1)[0]
            conflicts = [code for price, code in price_entries if price != most_common_price]
            matches = [code for price, code in price_entries if price == most_common_price][:1]  # Just show one example
            pricing_conflicts[subgroup] = {
                "expected_price": most_common_price,
                "conflicting_items": conflicts,
                "unexpected_prices": sorted(set(p for p, _ in price_entries if p != most_common_price)),
                "example_matching_item": matches[0] if matches else None
            }
        else:
            subgroup_cost_map[subgroup] = price_entries[0][0]

    if pricing_conflicts:
        return {
            "error": True,
            "conflicts": pricing_conflicts
        }

    # --- 3. Load Buz pricing and inventory items ---
    pricing_rows = db_manager.execute_query("SELECT * FROM pricing_data").fetchall()
    inventory_rows = db_manager.execute_query("SELECT Code, SupplierProductCode, inventory_group_code FROM inventory_items").fetchall()
    inventory_map = {r["Code"]: r for r in inventory_rows}
    unleashed_map = {r["ProductCode"]: r for r in unleashed}

    # --- 4. Process updates ---
    updates_by_group = defaultdict(list)
    updated_headers = [entry["spreadsheet_column"] for entry in pricing_config]
    db_fields = [entry["database_field"] for entry in pricing_config]

    for row in pricing_rows:
        inv_code = row["InventoryCode"]
        item = inventory_map.get(inv_code)
        if not item:
            continue

        supplier_code = item["SupplierProductCode"]
        group_code = item["inventory_group_code"]
        unleashed_row = unleashed_map.get(supplier_code)
        if not unleashed_row:
            continue

        subgroup = unleashed_row["ProductSubGroup"]
        cost = subgroup_cost_map.get(subgroup)
        markup = markup_map.get(group_code)
        if cost is None or markup is None:
            continue

        new_cost = round(cost, 2)
        new_sell = round(new_cost * markup, 2)

        current_cost = round(row["CostSQM"], 2) if row["CostSQM"] is not None else None
        current_sell = round(row["SellSQM"], 2) if row["SellSQM"] is not None else None

        def changed(a, b):
            if a is None or b is None:
                return True
            return abs(a - b) / b > 0.005

        if changed(current_cost, new_cost) or changed(current_sell, new_sell):
            # Start with the base row
            updated_row = {field: row[field] for field in db_fields}
            # Apply updates
            updated_row.update({
                "PkId": "",
                "Operation": "A",
                "DateFrom": tomorrow(),
                "CostSQM": new_cost,
                "SellSQM": new_sell
            })
            # Append as ordered list
            ordered_values = [updated_row.get(field) for field in db_fields]
            updates_by_group[group_code].append(ordered_values)

    # --- 5. Return OpenPyXLFileHandler with updated workbook ---
    if not updates_by_group:
        logger.info("No pricing updates detected — nothing to generate.")
        return None

    return OpenPyXLFileHandler.from_sheets_data(
        updates_by_group,
        {
            "headers": updated_headers,
            "header_row": 1
        }
    )
