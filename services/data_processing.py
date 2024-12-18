import re
from services.database import DatabaseManager
from services.process_buz_workbooks import validate_headers
import logging
from datetime import datetime


# Configure logging
logger = logging.getLogger(__name__)


def safe_float(value):
    """Safely convert a value to float, defaulting to 0.0 for empty strings."""
    try:
        return float(value) if value else 0.0  # Default to 0.0 for empty strings
    except ValueError:
        return 0.0  # Return 0.0 if conversion fails


def clean_value(value):
    """
    Clean up the value by handling double quotes, leading equal signs, control characters, and formatting appropriately.

    :param value:
    :type value:
    :return:
    """
    if isinstance(value, str):
        # Remove control characters (including BOM)
        value = re.sub(r'[\x00-\x1F\x7F-\x9F\uFEFF]', '', value)
        # Remove leading equal sign and any double quotes
        value = value.lstrip('=')  # Remove leading equal sign
        value = value.replace('"', '').strip()  # Remove double quotes
        return value
    return value


def clear_unleashed_table(db_manager: DatabaseManager):
    """Clear all data from the unleashed_products table."""
    db_manager.execute_query('DELETE FROM unleashed_products', auto_commit=True)  # Clear all rows

    
def insert_unleashed_data(db_manager: DatabaseManager, file_path: str, expected_headers: list[str]):
    import csv

    clear_unleashed_table(db_manager)  # Clear the table before inserting new data

    with open(file_path, 'r', encoding='UTF-8', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        is_valid, cleaned_headers = validate_headers(reader.fieldnames, expected_headers)
        if not is_valid:
            return None

        # Iterate through each row in the CSV
        for row in reader:
            # Clean the row keys and values
            cleaned_row = {clean_value(key): clean_value(value) for key, value in row.items()}
            cleaned_row = {k.replace('*', '').strip(): v for k, v in cleaned_row.items()}  # Clean keys again after values

            # Prepare values for insertion, using safe_float for numeric fields
            values = (
                cleaned_row.get('Product Code'), cleaned_row.get('Product Description'), cleaned_row.get('Notes'),
                cleaned_row.get('Barcode'), cleaned_row.get('Unit of Measure'),
                safe_float(cleaned_row.get('Min Stock Alert Level')), 
                safe_float(cleaned_row.get('Max Stock Alert Level')), 
                cleaned_row.get('Label Template'), cleaned_row.get('SO Label Template'),
                cleaned_row.get('PO Label Template'), cleaned_row.get('SO Label Quantity'), cleaned_row.get('PO Label Quantity'),
                cleaned_row.get('Supplier Code'), cleaned_row.get('Supplier Name'), cleaned_row.get('Supplier Product Code'),
                safe_float(cleaned_row.get('Default Purchase Price')), 
                safe_float(cleaned_row.get('Minimum Order Quantity')), 
                safe_float(cleaned_row.get('Minimum Sale Quantity')), 
                safe_float(cleaned_row.get('Default Sell Price')), 
                safe_float(cleaned_row.get('Minimum Sell Price')), 
                safe_float(cleaned_row.get('Sell Price Tier 1')), 
                safe_float(cleaned_row.get('Sell Price Tier 2')), 
                safe_float(cleaned_row.get('Sell Price Tier 3')), 
                safe_float(cleaned_row.get('Sell Price Tier 4')), 
                safe_float(cleaned_row.get('Sell Price Tier 5')), 
                safe_float(cleaned_row.get('Sell Price Tier 6')), 
                safe_float(cleaned_row.get('Sell Price Tier 7')), 
                safe_float(cleaned_row.get('Sell Price Tier 8')), 
                safe_float(cleaned_row.get('Sell Price Tier 9')), 
                safe_float(cleaned_row.get('Sell Price Tier 10')), 
                safe_float(cleaned_row.get('Pack Size')), 
                safe_float(cleaned_row.get('Weight')), 
                safe_float(cleaned_row.get('Width')), 
                safe_float(cleaned_row.get('Height')), 
                safe_float(cleaned_row.get('Depth')), 
                safe_float(cleaned_row.get('Reminder')), 
                safe_float(cleaned_row.get('Last Cost')), 
                safe_float(cleaned_row.get('Nominal Cost')), 
                cleaned_row.get('Never Diminishing'), cleaned_row.get('Product Group'),
                cleaned_row.get('Sales Account'), cleaned_row.get('COGS Account'), 
                safe_float(cleaned_row.get('Purchase Account')), 
                cleaned_row.get('Purchase Tax Type'), 
                safe_float(cleaned_row.get('Purchase Tax Rate')),
                cleaned_row.get('Sales Tax Type'), 
                safe_float(cleaned_row.get('Sales Tax Rate')),
                cleaned_row.get('IsAssembledProduct'), cleaned_row.get('IsComponent'), 
                cleaned_row.get('IsObsoleted'), 
                cleaned_row.get('Is Sellable'), cleaned_row.get('Is Purchasable'), 
                cleaned_row.get('Default Purchasing Unit of Measure'),
                cleaned_row.get('Is Serialized'), cleaned_row.get('Is Batch Tracked'),
            )
            
            # Check if the number of values matches the number of columns in the table
            if len(values) != len(expected_headers):
                logger.warning(f"Warning: Expected {len(expected_headers)} values but got {len(values)}. Row: {cleaned_row}")
                continue
            
            db_manager.execute_query('''
                INSERT INTO unleashed_products (
                    ProductCode, ProductDescription, Notes, Barcode, UnitOfMeasure,
                    MinStockAlertLevel, MaxStockAlertLevel, LabelTemplate, SOLabelTemplate,
                    POLabelTemplate, SOLabelQuantity, POLabelQuantity, SupplierCode,
                    SupplierName, SupplierProductCode,
                    DefaultPurchasePrice,
                    MinimumOrderQuantity, MinimumSaleQuantity, DefaultSellPrice,
                    MinimumSellPrice, SellPriceTier1, SellPriceTier2, SellPriceTier3,
                    SellPriceTier4, SellPriceTier5, SellPriceTier6, SellPriceTier7,
                    SellPriceTier8, SellPriceTier9, SellPriceTier10, PackSize,
                    Weight, Width, Height, Depth, Reminder, LastCost, NominalCost,
                    NeverDiminishing, ProductGroup, SalesAccount, COGSAccount,
                    PurchaseAccount, PurchaseTaxType, PurchaseTaxRate,
                    SalesTaxType, SaleTaxRate, IsAssembledProduct, IsComponent,
                    IsObsoleted, IsSellable, IsPurchasable, DefaultPurchasingUnitOfMeasure,
                    IsSerialized, IsBatchTracked
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, 
                    ?, ?, ?, ?, 
                    ?, ?, ?, ?, 
                    ?, ?, ?, ?, 
                    ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, 
                    ?, ?, ?, 
                    ?, ?, ?, ?, 
                    ?, ?, ?, ?, 
                    ?, ?
                )
            ''', values)

    # Now, delete records where IsObsoleted='yes' or IsSellable='no'
    db_manager.execute_query("DELETE FROM unleashed_products WHERE IsObsoleted = 'Yes' OR IsSellable = 'No'")
    db_manager.commit()

    
def generate_supplier_code_report_list(db_manager: DatabaseManager):
    # Retrieve all supplier codes from the unleashed products table
    db_manager.db.execute('SELECT DISTINCT ProductCode FROM unleashed_products')
    product_codes = {row['ProductCode'].lower() for row in db_manager.db.cursor.fetchall()}

    # Retrieve all inventory items with supplier codes not in the unleashed products
    missing_codes = db_manager.execute_query('''
        SELECT SupplierProductCode, inventory_group_code, Code 
        FROM inventory_items 
        WHERE SupplierProductCode IS NOT NULL 
        AND SupplierProductCode != ''
        AND LOWER(SupplierProductCode) NOT IN (SELECT DISTINCT LOWER(ProductCode) FROM unleashed_products)
    ''').fetchall()

    return missing_codes


def db_get_all_unleashed_product_codes(db_manager: DatabaseManager):
    """
    Retrieve all product codes from the unleashed products table (in lowercase)

    :param db_manager:
    :return list of unleashed_product rows
    :rtype: list[tuple]
    """
    cursor = db_manager.execute_query('SELECT DISTINCT ProductCode FROM unleashed_products')
    product_codes = {row['ProductCode'].lower() for row in cursor.fetchall()}
    
    return product_codes


def search_items_by_supplier_code(db_manager: DatabaseManager, code: str):
    """
    Search for inventory items by supplier product code

    :param db_manager: Database Manager
    :param code: Supplier inventory code
    :type code: str
    :return: list[tuple]
    """
    # Query to find items matching the given supplier product code
    query = '''
        SELECT inventory_group_code, Code, Description, SupplierProductCode, 
               Supplier, PriceGridCode 
        FROM inventory_items 
        WHERE SupplierProductCode = ?
    '''
    results = db_manager.execute_query(query, (code,)).fetchall()
    return results


def get_inventory_group_codes(db_manager: DatabaseManager) -> list[str]:
    """
    Retrieve all inventory group codes from the database.

    :param db_manager: Database Manager
    :return: A list of inventory group codes as strings.
    :rtype: list[str]
    """
    cursor = db_manager.execute_query('SELECT group_code FROM inventory_group_codes')
    codes = [row['group_code'] for row in cursor.fetchall()]
    codes.sort(key=lambda x: x.lower())  # Sort manually if necessary
    return codes


def db_delete_inventory_group(db_manager: DatabaseManager, group_code: str):
    """
    Delete an inventory group by its group code using DatabaseManager.

    :param db_manager: An instance of DatabaseManager.
    :type db_manager: DatabaseManager
    :param group_code: The group code to delete.
    :type group_code: str
    """
    print(f"Deleting code: {group_code}")
    db_manager.delete_item("inventory_group_codes", {"group_code": group_code})


def get_table_row_count(db_manager: DatabaseManager, table_name: str):
    """Get the count of rows in the specified table."""
    cursor = db_manager.execute_query(f'SELECT COUNT(*) FROM {table_name}')
    count = cursor.fetchone()[0]
    return count
    
    
def get_unique_inventory_group_count(db_manager: DatabaseManager):
    sql = 'SELECT COUNT(DISTINCT inventory_group_code) FROM inventory_items'
    cursor = db_manager.execute_query(sql)
    count = cursor.fetchone()[0]  # Get the first item from the result
    return count


def get_wholesale_markups(db_manager: DatabaseManager, wholesale_markups):
    # first clear all existing data
    db_manager.execute_query('DELETE FROM wholesale_markups')

    # Insert each value into the table
    db_manager.executemany('INSERT INTO wholesale_markups (product, markup) VALUES (?, ?)', wholesale_markups)

    db_manager.commit()


def db_delete_records_by_inventory_group(db_manager: DatabaseManager, inventory_group_code: str):
    """
    Delete all items from given group

    :param db_manager:
    :param inventory_group_code:
    """

    # Delete from inventory_items
    db_manager.execute_query('DELETE FROM inventory_items WHERE inventory_group_code = ?', (inventory_group_code,))
    
    # Delete from pricing_data
    db_manager.execute_query('DELETE FROM pricing_data WHERE inventory_group_code = ?', (inventory_group_code,))

    db_manager.commit()


def db_delete_items_not_in_unleashed(db_manager: DatabaseManager):
    """

    :param db_manager:
    """

    # Retrieve all supplier codes from the unleashed products table
    cursor = db_manager.execute_query('SELECT DISTINCT SupplierProductCode FROM unleashed_products')
    existing_supplier_codes = {row[0].lower() for row in cursor.fetchall()}

    # Delete inventory items with supplier codes not found in the unleashed products,
    # and ignore items with a blank SupplierProductCode
    db_manager.execute_query(
        'DELETE FROM inventory_items WHERE SupplierProductCode NOT IN ({}) AND SupplierProductCode <> ""'.format(
        ', '.join(['?'] * len(existing_supplier_codes))
    ), list(existing_supplier_codes), auto_commit=True)
    
    
def validate_data(data, required_fields):
    """Ensure all required fields are present and non-empty in the data."""
    missing_fields = [field for field in required_fields if field not in data or not data[field]]
    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
    return True


def transform_data(data):
    """Apply necessary transformations, e.g., convert all text to uppercase."""
    return {key: value.upper() if isinstance(value, str) else value for key, value in data.items()}


def max_last_edit_date(db_manager: DatabaseManager) -> datetime or None:
    cursor = db_manager.execute_query('SELECT MAX(LastEditDate) FROM inventory_items')
    result = cursor.fetchone()
    if result and result[0]:
        # Ensure it's a datetime object
        return datetime.strptime(result[0], '%Y-%m-%d')
    return None


def update_table_history(db_manager: DatabaseManager, table_name: str):
    query = '''
            INSERT INTO upload_history (table_name, last_upload)
            VALUES (?, CURRENT_TIMESTAMP)
            ON CONFLICT(table_name)
            DO UPDATE SET last_upload = CURRENT_TIMESTAMP;
        '''
    db_manager.execute_query(query, (table_name,), True)


def get_last_upload_time(db_manager: DatabaseManager, table_name: str):
    query = '''
            SELECT last_upload 
            FROM upload_history 
            WHERE table_name = ?;
        '''
    cursor = db_manager.execute_query(query, (table_name,))
    result = cursor.fetchone()
    return result[0] if result else None
