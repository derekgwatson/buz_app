import csv
from database import DatabaseManager


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


def clear_unleashed_table():
    """Clear all data from the unleashed_products table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM unleashed_products')  # Clear all rows
    conn.commit()
    conn.close()
    
    
def insert_unleashed_data(file_path):
    clear_unleashed_table()  # Clear the table before inserting new data

    conn = get_db_connection()
    cursor = conn.cursor()

    with open(file_path, 'r', encoding='UTF-8', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Clean the headers by removing '*' and any leading/trailing whitespace
        cleaned_headers = [header.replace('*', '').strip() for header in reader.fieldnames]
        cleaned_headers = [re.sub(r'[\x00-\x1F\x7F-\x9F\uFEFF]', '', header).strip() for header in cleaned_headers]  # Remove BOM and control chars

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
            if len(values) != 55:
                print(f"Warning: Expected 55 values but got {len(values)}. Row: {cleaned_row}")
                continue
            
            cursor.execute('''
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

    conn.commit()
    
    # Now, delete records where IsObsoleted='yes' or IsSellable='no'
    cursor.execute("DELETE FROM unleashed_products WHERE IsObsoleted = 'Yes' OR IsSellable = 'No'")
    
    conn.commit()    
    conn.close()
    
    
def generate_supplier_code_report_list():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Retrieve all supplier codes from the unleashed products table
    cursor.execute('SELECT DISTINCT ProductCode FROM unleashed_products')
    product_codes = {row['ProductCode'].lower() for row in cursor.fetchall()}

    # Retrieve all inventory items with supplier codes not in the unleashed products
    cursor.execute('''
        SELECT SupplierProductCode, inventory_group_code, Code 
        FROM inventory_items 
        WHERE SupplierProductCode IS NOT NULL 
        AND SupplierProductCode != ''
        AND LOWER(SupplierProductCode) NOT IN (SELECT DISTINCT LOWER(ProductCode) FROM unleashed_products)
    ''')
    
    # Fetching all inventory items with supplier codes not in the unleashed products
    missing_codes = cursor.fetchall()

    conn.close()
    
    return missing_codes





def db_get_all_unleashed_product_codes(conn):
    """
    Retrieve all product codes from the unleashed products table (in lowercase)

    :param conn: An active SQLite database connection with a row factory set to sqlite3.Row.
    :type conn: sqlite3.Connection
    :return list of unleashed_product rows
    :rtype: list[tuple]
    """
    cursor = conn.cursor()

    cursor.execute('SELECT DISTINCT ProductCode FROM unleashed_products')
    product_codes = {row['ProductCode'].lower() for row in cursor.fetchall()}
    
    conn.close()
    return product_codes


def search_items_by_supplier_code(conn, code):
    """
    Search for inventory items by supplier product code

    :param conn: An active SQLite database connection with a row factory set to sqlite3.Row.
    :type conn: sqlite3.Connection
    :param code: Supplier inventory code
    :type code: str
    :return: list[tuple]
    """
    cursor = conn.cursor()

    # Query to find items matching the given supplier product code
    query = '''
        SELECT inventory_group_code, Code, Description, SupplierProductCode, 
               Supplier, PriceGridCode 
        FROM inventory_items 
        WHERE SupplierProductCode = ?
    '''
    cursor.execute(query, (code,))
    results = cursor.fetchall()
    return results


def get_inventory_group_codes(conn):
    """
    Retrieve all inventory group codes from the database.

    :param conn: An active SQLite database connection with a row factory set to sqlite3.Row.
    :type conn: sqlite3.Connection
    :return: A list of inventory group codes as strings.
    :rtype: list[str]
    """
    cursor = conn.cursor()
    cursor.execute('SELECT group_code FROM inventory_group_codes')
    codes = [row['group_code'] for row in cursor.fetchall()]
    codes.sort(key=lambda x: x.lower())  # Sort manually if necessary
    return codes


def db_delete_inventory_group(db_manager, group_code):
    """
    Delete an inventory group by its group code using DatabaseManager.

    :param db_manager: An instance of DatabaseManager.
    :type db_manager: DatabaseManager
    :param group_code: The group code to delete.
    :type group_code: str
    """
    print(f"Deleting code: {group_code}")
    db_manager.delete_item("inventory_group_codes", {"group_code": group_code})



def get_table_row_count(table_name):
    """Get the count of rows in the specified table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    count = cursor.fetchone()[0]
    conn.close()
    return count
    
    
def get_unique_inventory_group_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT inventory_group_code) FROM inventory_items')
    count = cursor.fetchone()[0]  # Get the first item from the result
    conn.close()
    return count


def get_wholesale_markups(db_manager, wholesale_markups):
    # first clear all existing data
    db_manager.db.execute('DELETE FROM wholesale_markups')

    # Insert each value into the table
    db_manager.cursor.executemany('INSERT INTO wholesale_markups (product, markup) VALUES (?, ?)', wholesale_markups)

    
def db_delete_records_by_inventory_group(db_manager, inventory_group_code):
    """
    Delete all items from given group

    :param db_manager:
    :param inventory_group_code:
    """

    # Delete from inventory_items
    db_manager.db.execute('DELETE FROM inventory_items WHERE inventory_group_code = ?', (inventory_group_code,))
    
    # Delete from pricing_data
    db_manager.cursor.execute('DELETE FROM pricing_data WHERE inventory_group_code = ?', (inventory_group_code,))


def db_delete_items_not_in_unleashed(db_manager):
    """

    :param db_manager:
    """

    # Retrieve all supplier codes from the unleashed products table
    db_manager.db.execute('SELECT DISTINCT SupplierProductCode FROM unleashed_products')
    existing_supplier_codes = {row[0].lower() for row in db_manager.cursor.fetchall()}

    # Delete inventory items with supplier codes not found in the unleashed products,
    # and ignore items with a blank SupplierProductCode
    db_manager.db.execute(
        'DELETE FROM inventory_items WHERE SupplierProductCode NOT IN ({}) AND SupplierProductCode <> ""'.format(
        ', '.join(['?'] * len(existing_supplier_codes))
    ), list(existing_supplier_codes))

    
    
def validate_data(data, required_fields):
    """Ensure all required fields are present and non-empty in the data."""
    missing_fields = [field for field in required_fields if field not in data or not data[field]]
    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
    return True


def transform_data(data):
    """Apply necessary transformations, e.g., convert all text to uppercase."""
    return {key: value.upper() if isinstance(value, str) else value for key, value in data.items()}
