from openpyxl import Workbook
from datetime import datetime
from flask import current_app
from services.database import DatabaseManager
from services.helper import parse_headers


def generate_deactivation_upload(db_manager: DatabaseManager):
    """
    Generate an upload file for deactivating obsolete/unsellable items in Buz.

    Args:
        db_manager (DatabaseManager): Instance of your DatabaseManager class.
    """
    # Load headers from app config
    headers_config = current_app.config["headers"]
    expected_headers, db_fields = parse_headers(headers_config, "buz_inventory_item_file")

    # Query database for obsolete/unsellable items
    query = f"""
    SELECT {', '.join(db_fields)} FROM inventory_items
    WHERE Supplier = 'Unleashed'
    AND SupplierProductCode IN (
        SELECT ProductCode FROM unleashed_products
        WHERE IsObsoleted = 'Yes' OR IsSellable = 'No'
    )
    """
    items = db_manager.execute_query(query)

    # Organize items by InventoryGroupCode
    grouped_items = {}
    for item in items:
        group = item['InventoryGroupCode']
        if group not in grouped_items:
            grouped_items[group] = []
        grouped_items[group].append(item)

    # Create Excel workbook
    workbook = Workbook()
    for group, items in grouped_items.items():
        sheet = workbook.create_sheet(title=group)
        # Add headers
        sheet.append(expected_headers)
        for item in items:
            # Populate row with database fields
            row = [item.get(field) for field in db_fields]
            row.append('D')  # Add 'D' for deactivation
            sheet.append(row)

    # Remove default empty sheet
    if 'Sheet' in workbook.sheetnames:
        del workbook['Sheet']

    # Save workbook
    filename = f"deactivate_items_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    workbook.save(filename)
    print(f"Deactivation upload file saved as {filename}")
