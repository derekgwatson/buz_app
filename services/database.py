import sqlite3
from flask import g, current_app
from flask.cli import with_appcontext
import click
import logging


# Configure logging
logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception raised for database-related errors."""
    pass


class DatabaseManager:
    def __init__(self, connection):
        """
        Initialize the DatabaseManager with a database connection.

        :param connection: A database connection object.
        """
        self.connection = connection

    def close(self):
        """
        Close the database connection.

        :raises DatabaseError: If closing the connection fails.
        """
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                raise DatabaseError(f"Failed to close the database connection: {e}")

    def __del__(self):
        """
        Ensure the database connection is closed when the object is deleted.
        """
        try:
            self.close()
        except DatabaseError as e:
            logger.error(f"Warning: {e}")

    def execute_query(self, query, params=None, auto_commit=False):
        """
        Execute a single SQL query with optional parameters.

        :param auto_commit:
        :param query: The SQL query to execute.
        :type query: str
        :param params: A list or tuple of query parameters, defaults to None.
        :type params: list | tuple, optional
        :return: The cursor after executing the query.
        :rtype: sqlite3.Cursor
        :raises DatabaseError: If an error occurs during query execution.
        """
        if params is None:
            params = []
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)

            if auto_commit:
                self.commit()

            return cursor
        except Exception as e:
            raise DatabaseError(f"Database query failed: {e}")

    def commit(self):
        """
        Commit the current database transaction.

        :raises DatabaseError: If an error occurs during the commit operation.
        """
        try:
            self.connection.commit()
        except Exception as e:
            raise DatabaseError(f"Commit failed: {e}")

    def rollback(self):
        """
        Rollback the current transaction in case of errors.

        :raises DatabaseError: If the rollback operation fails.
        """
        try:
            self.connection.rollback()
            logger.info("Transaction rolled back successfully.")
        except Exception as e:
            raise DatabaseError(f"Rollback failed: {e}")

    def insert_item(self, table, data):
        """
        Insert a new record into a specified table.

        :param table: The name of the table to insert into.
        :type table: str
        :param data: A dictionary of column-value pairs to insert.
        :type data: dict
        :raises DatabaseError: If the insertion fails.
        """
        query = f"INSERT OR IGNORE INTO {table} ({', '.join(data.keys())}) VALUES ({', '.join(['?'] * len(data))})"
        try:
            self.execute_query(query, tuple(data.values()))
            self.commit()
        except DatabaseError as e:
            raise DatabaseError(f"Insertion failed: {e}")

    def get_item(self, table, criteria):
        """
        Retrieve records from a table based on search criteria.

        :param table: The name of the table to query.
        :type table: str
        :param criteria: A dictionary of column-value pairs for filtering results.
        :type criteria: dict
        :return: A list of matching records.
        :rtype: list
        :raises DatabaseError: If the retrieval fails.
        """
        query = f"SELECT * FROM {table} WHERE " + " AND ".join(f"{k}=?" for k in criteria.keys())
        try:
            cursor = self.execute_query(query, tuple(criteria.values()))
            return cursor.fetchall()
        except DatabaseError as e:
            raise DatabaseError(f"Retrieval failed: {e}")

    def delete_item(self, table, criteria):
        """
        Delete records from a table based on search criteria.

        :param table: The name of the table to delete from.
        :type table: str
        :param criteria: A dictionary of column-value pairs for filtering records to delete.
        :type criteria: dict
        :raises DatabaseError: If the deletion fails.
        """
        query = f"DELETE FROM {table} WHERE " + " AND ".join(f"{k}=?" for k in criteria.keys())
        try:
            self.execute_query(query, tuple(criteria.values()))
            self.commit()
        except DatabaseError as e:
            raise DatabaseError(f"Deletion failed: {e}")

    def executemany(self, query, param_list):
        """
        Execute a SQL query multiple times with different parameter sets.

        :param query: The SQL query to execute.
        :type query: str
        :param param_list: A list of parameter tuples to execute the query with.
        :type param_list: list[tuple]
        :return: The number of rows affected.
        :rtype: int
        :raises DatabaseError: If the bulk execution fails.
        """
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, param_list)
            self.commit()
            return cursor.rowcount
        except Exception as e:
            raise DatabaseError(f"Bulk execution failed: {e}")


def clear_database(db_manager: DatabaseManager):
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
            logger.info(f"Clearing data in table: {table} (if required)")
            db_manager.execute_query(f"DELETE FROM {table}")
        db_manager.commit()

    except DatabaseError as e:
        logger.error(f"An error occurred: {e}")
    finally:
        db_manager.commit()
        logger.info("Database cleared")


def create_db_manager(db_file: str):
    """
    Creates a DatabaseManager instance with a static SQLite connection.
    """
    db_connector = sqlite3.connect
    db_params = {
        "database": db_file,
        "detect_types": sqlite3.PARSE_DECLTYPES
    }

    connection = db_connector(**db_params)
    connection.row_factory = sqlite3.Row  # Enable dict-like row access
    return DatabaseManager(connection)


@click.command('init-db')
@with_appcontext
def init_db_command():
    """
    Initialize the database using the CLI command.
    """
    db_manager = current_app.extensions['db_manager']
    init_db(db_manager)


def init_db(db_manager: DatabaseManager):
    """
    Initialize the database using the CLI command.
    """
    try:
        logger.info("Initializing the database...")
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
                    LastEditDate DATE,
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
                    FriendlyDescription1 TEXT,
                    FriendlyDescription2 TEXT,
                    FriendlyDescription3 TEXT,
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
                    ProductSubGroup TEXT,
                    SalesAccount TEXT,
                    ProductBrand TEXT,
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

            "inventory_groups": '''
                CREATE TABLE IF NOT EXISTS inventory_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_code TEXT UNIQUE NOT NULL,
                    group_description TEXT NOT NULL
                );
            ''',

            "wholesale_markups": '''
                CREATE TABLE IF NOT EXISTS wholesale_markups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product TEXT,
                    markup REAL
                );
            ''',

            "upload_history": '''
                CREATE TABLE IF NOT EXISTS upload_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT UNIQUE NOT NULL,
                    last_upload TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            ''',

            "suppliers": '''
                CREATE TABLE IF NOT EXISTS suppliers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    supplier_code VARCHAR(255) NOT NULL,
                    supplier VARCHAR(255) NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            ''',

            "fabrics": '''
                CREATE TABLE IF NOT EXISTS fabrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    supplier_id INTEGER NULL,
                    supplier_product_code VARCHAR(50) NOT NULL,
                    description_1 VARCHAR(255),
                    description_2 VARCHAR(255),
                    description_3 VARCHAR(255),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (supplier_id) REFERENCES suppliers (id) ON DELETE SET NULL
                );
            ''',

            "fabric_group_mappings": '''
                CREATE TABLE IF NOT EXISTS fabric_group_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fabric_id INTEGER NOT NULL,
                    inventory_group_code VARCHAR(50) NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (fabric_id) REFERENCES fabrics (id) ON DELETE CASCADE
                );
            '''
        }

        for name, schema in tables.items():
            logger.info(f"Creating table: {name} (if required)")
            db_manager.execute_query(schema)

        db_manager.commit()
        logger.info("Database initialized successfully!")
    except DatabaseError as e:
        logger.error(f"An error occurred: {e}")


