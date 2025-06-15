import logging
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


# Explanation:
# We group rows under keys in the 'changes' dictionary, one per inventory group code (e.g., CRTWT, CRTNT).
# This ensures the output Excel file has separate tabs per group.


def build_sheet_dict(sheet_data, num_header_rows=1, column_titles=None):
    sheet_dict = {}
    headers = {}

    for idx, row in enumerate(sheet_data):
        if idx == num_header_rows - 1 and column_titles:
            # Build map of sheet column name → index
            headers = {col.strip(): i for i, col in enumerate(row)}
            continue
        if idx < num_header_rows or not headers:
            continue

        row_data = {}
        try:
            for key, sheet_col_name in column_titles.items():
                col_index = headers.get(sheet_col_name)
                if col_index is not None and col_index < len(row):
                    row_data[key] = row[col_index].strip()
                else:
                    row_data[key] = ''
        except Exception:
            continue  # If anything goes wrong, skip this row

        code = row_data.get("code")
        if not code:
            continue

        sheet_dict[code] = {
            **row_data,
            'raw_row': row
        }

    return sheet_dict


def build_buz_dict(db_rows):
    buz_dict = {}
    for row in db_rows:
        group_code = row['inventory_group_code'].strip()
        if group_code not in ('CRTWT', 'CRTNT'):
            continue
        code = row['SupplierProductCode'].strip()
        buz_dict[code] = dict(row)
    return buz_dict


def compare_fabrics_by_code(sheet_dict, buz_dict):
    new, updated, removed = [], [], []

    for code, sheet_item in sheet_dict.items():
        if code not in buz_dict:
            new.append(sheet_item)
        else:
            buz_item = buz_dict[code]
            if (
                sheet_item['brand'] != buz_item['DescnPart1'] or
                sheet_item['fabric_name'] != buz_item['DescnPart2'] or
                sheet_item['colour'] != buz_item['DescnPart3']
            ):
                updated.append(sheet_item)

    for code, buz_item in buz_dict.items():
        if code not in sheet_dict:
            removed.append(buz_item)

    return new, updated, removed


def prepare_item_changes_dict(new_items, updated_items, removed_items):
    changes = {}

    for item in new_items:
        group = 'CRTWT' if item.get('inventory_group_code') == 'CRTWT' else 'CRTNT'
        row_data = {
            'PkId': '',
            'Operation': 'A',
            'SupplierProductCode': item['code'],
            'Supplier': item['brand'],
            'DescnPart1': item['brand'],
            'DescnPart2': item['fabric_name'],
            'DescnPart3': item['colour'],
            'CustomVar1': float(item['raw_row'][7]) * 10 if item['raw_row'][7] else '',
            'CustomVar2': float(item['raw_row'][5]) * 10 if item['raw_row'][5] else '',
            'CustomVar3': item['raw_row'][6]
        }
        if group not in changes:
            changes[group] = []
        changes[group].append(row_data)

    for item in updated_items:
        group = str(item.get('inventory_group_code', 'UNKNOWN'))
        row_data = dict(item)
        row_data['Operation'] = 'E'
        if group not in changes:
            changes[group] = []
        changes[group].append(row_data)

    for item in removed_items:
        group = str(item.get('inventory_group_code', 'UNKNOWN'))
        row_data = dict(item)
        row_data['Operation'] = 'D'
        if group not in changes:
            changes[group] = []
        changes[group].append(row_data)

    return changes


def prepare_pricing_changes(sheet_dict, buz_pricing):
    changes = {}

    for code, sheet_item in sheet_dict.items():
        raw_row = sheet_item['raw_row']
        cost_value = raw_row[10]  # Column K: Cost to DD per metre CUT (ex GST)
        sell_value = raw_row[12]  # Column M: Buzz Sell Price (ex GST)

        if not cost_value.replace('.', '', 1).isdigit() or not sell_value.replace('.', '', 1).isdigit():
            logger.warning(f"Skipping non-numeric cost/sell for code {code}: cost='{cost_value}', sell='{sell_value}'")
            continue

        sheet_cost = float(cost_value)
        sheet_sell = float(sell_value)
        buz_item = buz_pricing.get(code)

        needs_update = False
        if not buz_item:
            needs_update = True
        else:
            buz_cost = float(buz_item.get('CostSQM', 0))
            buz_sell = float(buz_item.get('SellSQM', 0))
            if buz_cost == 0 or abs(sheet_cost - buz_cost) / buz_cost > 0.005 or buz_sell == 0 or abs(sheet_sell - buz_sell) / buz_sell > 0.005:
                needs_update = True

        if needs_update:
            group = buz_item['inventory_group_code'] if buz_item else 'CRTWT'
            row_data = {
                'Operation': 'A',
                'InventoryCode': code,
                'CostSQM': sheet_cost,
                'SellSQM': sheet_sell,
                'DateFrom': (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            }
            if group not in changes:
                changes[group] = []
            changes[group].append(row_data)

    return changes


def run_curtain_fabric_sync(app, db, column_titles):
    from services.buz_inventory_items import create_inventory_workbook_creator, get_current_buz_fabrics
    from services.buz_inventory_pricing import create_pricing_workbook_creator, get_current_buz_pricing, \
        prepare_pricing_changes
    from services.google_sheets_service import GoogleSheetsService

    sheets_service = GoogleSheetsService()
    sheet_data = sheets_service.fetch_sheet_data(
        app.config["spreadsheets"]["master_curtain_fabric_list"]["id"],
        app.config["spreadsheets"]["master_curtain_fabric_list"]["range"]
    )

    sheet_dict = build_sheet_dict(sheet_data, column_titles=column_titles)
    buz_items = get_current_buz_fabrics(db)
    buz_dict = build_buz_dict(buz_items)
    buz_pricing = get_current_buz_pricing(db)

    # Item updates
    new_items, updated_items, removed_items = compare_fabrics_by_code(sheet_dict, buz_dict)
    item_changes = prepare_item_changes_dict(new_items, updated_items, removed_items)
    item_creator = create_inventory_workbook_creator(app)
    item_creator.populate_workbook(item_changes)
    item_creator.auto_fit_columns()
    item_output_file = 'items_upload.xlsx'
    item_creator.save_workbook(item_output_file)

    # Pricing updates
    pricing_changes = prepare_pricing_changes(sheet_dict, buz_pricing)
    pricing_creator = create_pricing_workbook_creator(app)
    pricing_creator.populate_workbook(pricing_changes)
    pricing_creator.auto_fit_columns()
    pricing_output_file = 'pricing_upload.xlsx'
    pricing_creator.save_workbook(pricing_output_file)

    logger.info('Generated item and pricing upload files.')

    return {
            'items_file': item_output_file,
            'pricing_file': pricing_output_file,
            'summary': {
                'new_items': len(new_items),
                'updated_items': len(updated_items),
                'removed_items': len(removed_items),
                'pricing_changes': {k: len(v) for k, v in pricing_changes.items()}
            }
    }
