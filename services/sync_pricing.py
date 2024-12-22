import pandas as pd
import logging
import os
from services.data_processing import get_pricing_data
from services.database import DatabaseManager
from datetime import datetime, timedelta


# Configure logging
logger = logging.getLogger(__name__)


def get_item_additions(
        db_manager: DatabaseManager,
        upload_folder: str,
        inventory_item_fields
):
    pass


def get_item_deletions(
        db_manager: DatabaseManager,
        upload_folder: str,
        inventory_item_fields
):
    pass


def get_item_changes(
        db_manager: DatabaseManager,
        upload_folder: str,
        inventory_item_fields
):
    pass


def get_unleashed_price(unit_of_measure, width, sell_price_tier9, default_purchase_price):
    price = sell_price_tier9 if sell_price_tier9 else default_purchase_price
    if unit_of_measure != "SQM" and width != 0:
        price /= width
    return price


def get_pricing_changes(
        db_manager: DatabaseManager,
        upload_folder: str,
        pricing_fields,
        wastage_percentages
):
    data, columns = get_pricing_data(db_manager=db_manager)
    price_changes = []

    # Filter data to only include rows with inventory_group_code in wastage_percentages
    logger.debug(f"Initially there were  {len(data)} rows")
    filtered_data = [row for row in data if dict(zip(columns, row))['inventory_group_code'] in wastage_percentages]
    logger.debug(f"Now processing {len(filtered_data)} rows")
    for row in filtered_data:
        row_dict = dict(zip(columns, row))

        # for now lets just focus on Zips
        unleashed_price = get_unleashed_price(
            row_dict['up_unitofmeasure'], row_dict['up_width'],
            row_dict['up_sellpricetier9'], row_dict['up_defaultpurchaseprice']
        )

        # Apply wastage percentage to adjust price
        wastage_percentage = wastage_percentages.get(row_dict['inventory_group_code'], 0)
        adjusted_price = unleashed_price * (1 + wastage_percentage)
        variance = abs(adjusted_price - row_dict['CostSQM']) / row_dict['CostSQM']

        # Check if the adjusted price deviates from the original CostSQM
        if variance > 0.005:
            logger.debug(
               f"For{row_dict['InventoryCode']}: " +
               "UL Code: {row_dict['ProductCode']}, " +
               "Old Cost: {row_dict['CostSQM']}," +
               "Wastage: {wastage_percentage}, " +
               "UL Price: {unleashed_price}, " +
               "Adjusted price (with wastage): {adjusted_price}, " +
               "Variance: {variance}")

            # Update fields for pricing export
            row_dict['CostSQM'] = adjusted_price
            row_dict['PkId'] = ""  # Set PkId to blank
            row_dict['Operation'] = "A"  # Set Operation to 'A'
            row_dict['DateFrom'] = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")  # Set DateFrom to tomorrow
            price_changes.append(row_dict)

    # Create Excel files
    if price_changes:
        logger.debug(f"compare_and_export: Rows in mismatches {len(price_changes)}")
        price_changes_df = pd.DataFrame(price_changes)
        return save_to_excel(price_changes_df, upload_folder,  pricing_fields)
    return None


def save_to_excel(df, upload_folder, pricing_fields):
    logger.debug(f"DataFrame passed to save_to_excel has {len(df)} rows")
    logger.debug(f"DataFrame columns: {df.columns.tolist()}")
    logger.debug(f"Sample DataFrame data: {df.head()}")

    if 'inventory_group_code' not in df.columns:
        logger.error("Missing 'inventory_group_code' in DataFrame")
        return None

    grouped = df.groupby('inventory_group_code')
    logger.debug(f"Group keys: {list(grouped.groups.keys())}")

#    items_export_filename = 'items_export.xlsx'
    pricing_export_filename = 'pricing_export.xlsx'

#    items_export_path = os.path.join(upload_folder, items_export_filename)
    pricing_export_path = os.path.join(upload_folder, pricing_export_filename)

    # Create Items File
    # with pd.ExcelWriter(items_export_path) as writer:
    #     for name, group in grouped:
    #         items_columns = [field['database_field'] for field in inventory_item_fields if field['database_field'] in group.columns]
    #         output = group[items_columns]
    #
    #         # Write headers
    #         headers = [field['spreadsheet_column'] for field in inventory_item_fields if
    #                    field['database_field'] in group.columns]
    #         header_row = pd.DataFrame([headers], columns=output.columns)
    #
    #         # Add empty row, headers, and data
    #         empty_row = pd.DataFrame([[''] * len(output.columns)], columns=output.columns)
    #         output = pd.concat([empty_row, header_row, output], ignore_index=True)
    #
    #         # Ensure only one set of headers is written
    #         output.iloc[1] = headers
    #         output.iloc[0] = [''] * len(output.columns)
    #         output.to_excel(writer, sheet_name=str(name), index=False, header=False)

    # Create Pricing File
    with pd.ExcelWriter(pricing_export_path) as writer:
        for name, group in grouped:
            pricing_columns = [field['database_field'] for field in pricing_fields
                               if field['database_field'] in group.columns]
            output = group[pricing_columns]
            output.to_excel(writer, sheet_name=str(name), index=False)

#    return items_export_filename, pricing_export_filename
    return pricing_export_filename


