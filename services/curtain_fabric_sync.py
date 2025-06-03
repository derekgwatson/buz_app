import logging
from flask import current_app, send_file


logger = logging.getLogger(__name__)


# Explanation:
# We group rows under keys in the 'changes' dictionary, one per inventory group code (e.g., CRTWT, CRTNT).
# This ensures the output Excel file has separate tabs per group.


def build_sheet_dict(sheet_data, num_header_rows=1):
    sheet_dict = {}
    for idx, row in enumerate(sheet_data):
        if idx < num_header_rows:
            continue  # Skip header rows
        if len(row) < 8:
            continue
        code = row[0].strip()
        brand = row[2].strip()
        fabric_name = row[3].strip()
        colour = row[4].strip()
        sheet_dict[code] = {
            'code': code,
            'brand': brand,
            'fabric_name': fabric_name,
            'colour': colour,
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
        sheet_price = float(sheet_item['raw_row'][8]) if sheet_item['raw_row'][8] else 0
        buz_item = buz_pricing.get(code)
        if not buz_item or abs(sheet_price - buz_item['SellSQM']) > 0.01:
            group = buz_item.get('inventory_group_code', 'CRTWT') if buz_item else 'CRTWT'
            row_data = {
                'Operation': 'A',
                'InventoryCode': code,
                'SellSQM': sheet_price,
                'SupplierCode': sheet_item['code'],
                'SupplierDescn': sheet_item['fabric_name']
            }
            if group not in changes:
                changes[group] = []
            changes[group].append(row_data)

    return changes
