import sqlite3
from flask import g

DATABASE = 'buz_data.db'


def get_db_connection():
    """
    Get a database connection and store it in the Flask `g` object.

    :return:
    """
    if not hasattr(g, 'db'):
        g.db = sqlite3.connect(database=DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db_connection(exception=None):
    """
    Close the database connection if it exists.

    :param exception:
    """
    if hasattr(g, 'db'):
        g.db.close()


def init_db(db_manager):
    """
    Initialize the database by creating required tables if they do not already exist.

    This function uses the provided DatabaseManager instance to execute SQL commands
    for creating tables defined in the `tables` dictionary. Each table is created
    only if it does not already exist.

    :param db_manager: An instance of DatabaseManager to manage database operations.
    :type db_manager: DatabaseManager
    :raises sqlite3.Error: If an error occurs during table creation.
    """
    tables = {
        "inventory_items": '''
            CREATE TABLE IF NOT EXISTS inventory_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inventory_group_code TEXT NOT NULL,
                PkId TEXT,
                Code TEXT,
                Description TEXT,
                DescnPart1 TEXT,
                DescnPart2 TEXT,
                DescnPart3 TEXT,
                PriceGridCode TEXT,
                CostGridCode TEXT,
                DiscountGroupCode TEXT,
                LastPurchasePrice REAL,
                StandardCost REAL,
                TaxRate REAL,
                UnitsPurchase TEXT,
                MinQty INTEGER,
                MaxQty INTEGER,
                ReorderMultiplier REAL,
                ForeXCode TEXT,
                LastPurchaseForeX REAL,
                PurchasingLeadDays INTEGER,
                StockingMultiplier REAL,
                UnitsStock TEXT,
                SellingMultiplier REAL,
                UnitsSell TEXT,
                CostMethod TEXT,
                ProductSize TEXT,
                ProductType TEXT,
                Supplier TEXT,
                SupplierProductCode TEXT,
                SupplierProductDescription TEXT,
                Length REAL,
                MaximumWidth REAL,
                ExtraTimeToProduce INTEGER,
                ExtraTimeToFit INTEGER,
                CustomVar1 TEXT,
                CustomVar2 TEXT,
                CustomVar3 TEXT,
                Warning TEXT,
                RptCat TEXT,
                Active TEXT,
                LastEditDate TEXT,
                Operation TEXT
            );
        ''',
        
        "pricing_data": '''
            CREATE TABLE IF NOT EXISTS pricing_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inventory_group_code TEXT,
                PkId TEXT,
                InventoryCode TEXT,
                Description TEXT,
                CustomerPriceGroupCode TEXT,
                DateFrom TEXT,
                SellEach REAL,
                SellLMWide REAL,
                SellLMHeight REAL,
                SellLMDepth REAL,
                SellSQM REAL,
                SellPercentageOnMain REAL,
                SellMinimum REAL,
                CostEach REAL,
                CostLMWide REAL,
                CostLMHeight REAL,
                CostLMDepth REAL,
                CostSQM REAL,
                CostPercentageOnMain REAL,
                CostMinimum REAL,
                InstallCostEach REAL,
                InstallCostLMWidth REAL,
                InstallCostHeight REAL,
                InstallCostDepth REAL,
                InstallCostSQM REAL,
                InstallCostPercentageOfMain REAL,
                InstallCostMinimum REAL,
                InstallSellEach REAL,
                InstallSellMinimum REAL,
                InstallSellLMWide REAL,
                InstallSellSQM REAL,
                InstallSellHeight REAL,
                InstallSellDepth REAL,
                InstallSellPercentageOfMain REAL,
                SupplierCode TEXT,
                SupplierDescn TEXT,
                IsNotCurrent TEXT,
                Operation TEXT
            );
        ''',
        
        "unleashed_products": '''
            CREATE TABLE IF NOT EXISTS unleashed_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ProductCode TEXT,
                ProductDescription TEXT,
                Notes TEXT,
                Barcode TEXT,
                UnitOfMeasure TEXT,
                MinStockAlertLevel REAL,
                MaxStockAlertLevel REAL,
                LabelTemplate REAL,
                SOLabelTemplate REAL,
                POLabelTemplate REAL,
                SOLabelQuantity TEXT,
                POLabelQuantity TEXT,
                SupplierCode TEXT,
                SupplierName TEXT,
                SupplierProductCode TEXT,
                DefaultPurchasePrice REAL,
                MinimumOrderQuantity REAL,
                MinimumSaleQuantity REAL,
                DefaultSellPrice REAL,
                MinimumSellPrice REAL,
                SellPriceTier1 REAL,
                SellPriceTier2 REAL,
                SellPriceTier3 REAL,
                SellPriceTier4 REAL,
                SellPriceTier5 REAL,
                SellPriceTier6 REAL,
                SellPriceTier7 REAL,
                SellPriceTier8 REAL,
                SellPriceTier9 REAL,
                SellPriceTier10 REAL,
                PackSize REAL,
                Weight REAL,
                Width REAL,
                Height REAL,
                Depth REAL,
                Reminder REAL,
                LastCost REAL,
                NominalCost REAL,
                NeverDiminishing TEXT,
                ProductGroup TEXT,
                SalesAccount TEXT,
                COGSAccount TEXT,
                PurchaseAccount REAL,
                PurchaseTaxType TEXT,
                PurchaseTaxRate REAL,
                SalesTaxType TEXT,
                SaleTaxRate REAL,
                IsAssembledProduct TEXT,
                IsComponent TEXT,
                IsObsoleted TEXT,
                IsSellable TEXT,
                IsPurchasable TEXT,
                DefaultPurchasingUnitOfMeasure TEXT,
                IsSerialized TEXT,
                IsBatchTracked TEXT
            );
        ''',
        
        "inventory_group_codes": '''
            CREATE TABLE IF NOT EXISTS inventory_group_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_code TEXT UNIQUE NOT NULL
            );
        ''',
        
        "wholesale_markups": '''
            CREATE TABLE IF NOT EXISTS wholesale_markups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product TEXT,
                markup REAL
            );
        ''',
    }
    
    try:
        for name, schema in tables.items():
            print(f"Creating table: {name} (if required)")
            db_manager.execute_query(schema)

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        db_manager.commit()
        print("Database initialized")


def clear_database(db_manager):
    """
    Clear all records from the database tables.

    :param db_manager: An instance of DatabaseManager.
    :type db_manager: DatabaseManager
    """
    tables_to_clear = [
        "inventory_items",
        "pricing_data",
        "unleashed_products",
        "inventory_group_codes",
        "wholesale_markups",
    ]

    try:
        for table in tables_to_clear:
            print(f"Clearing data in table: {table} (if required)")
            db_manager.execute_query(f"DELETE FROM {table}")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        db_manager.commit()
        print("Database initialized")


class DatabaseManager:
    def __init__(self, connection):
        self.connection = connection

    def execute_query(self, query, params=None):
        if params is None:
            params = []
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
        return cursor

    def commit(self):
        self.connection.commit()

    def insert_item(self, table, data):
        """
        Insert a new item into the specified table.

        :param table:
        :param data:
        :return:
        """
        query = f"INSERT OR IGNORE INTO {table} ({', '.join(data.keys())}) VALUES ({', '.join(['?'] * len(data))})"
        self.execute_query(query, tuple(data.values()))
        self.connection.commit()

    def get_item(self, table, criteria):
        """
        Retrieve item(s) based on criteria.

        :param table:
        :param criteria:
        :return:
        """
        query = f"SELECT * FROM {table} WHERE " + " AND ".join(f"{k}=?" for k in criteria.keys())
        cursor = self.execute_query(query, tuple(criteria.values()))
        return cursor.fetchall()

    def delete_item(self, table, criteria):
        """
        Delete item(s) based on criteria.

        :param table:
        :param criteria:
        :return:
        """
        query = f"DELETE FROM {table} WHERE " + " AND ".join(f"{k}=?" for k in criteria.keys())
        self.execute_query(query, tuple(criteria.values()))
        self.connection.commit()

    def executemany(self, query, param_list):
        """
        Execute a query multiple times with different parameter sets.

        :param query: The SQL query to execute.
        :type query: str
        :param param_list: A list of parameter tuples for the query.
        :type param_list: list[tuple]
        :return: The number of rows inserted/updated/deleted.
        :rtype: int
        """
        cursor = self.connection.cursor()
        cursor.executemany(query, param_list)
        self.connection.commit()
        return cursor.rowcount
