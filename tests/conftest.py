import pytest
from base64 import b64encode
from services.config_service import ConfigManager
from services.database import create_db_manager, init_db
from app import create_app
from services.helper import generate_unique_id


@pytest.fixture
def app_context(app_config):
    """Fixture for Flask app context."""
    app = create_app('Testing')
    app.config.update(app_config)
    with app.app_context():
        yield app


@pytest.fixture(scope="session")
def config_manager():
    """Fixture for initializing ConfigManager."""
    return ConfigManager()


@pytest.fixture(scope="session")
def app_config(config_manager):
    """Fixture for application configuration."""
    return config_manager.config


@pytest.fixture
def get_db_manager(app_config):
    """Fixture for database manager with per-test isolation."""
    db_manager = create_db_manager(":memory:")  # Use an in-memory database for isolation
    init_db(db_manager=db_manager)
    yield db_manager
    db_manager.close()  # Ensure database connection is closed after the test


@pytest.fixture
def auth_headers():
    """Fixture for authorization headers."""
    credentials = b64encode(b"testuser:testpassword").decode("utf-8")
    return {
        'Authorization': f'Basic {credentials}'
    }


def add_row_to_sheet(
        sheet_data,
        sheet_name,
        code="",
        description="",
        supplier_product_code="",
        warning=""):
    """
    Helper function to add a single row to the sheet data.

    Args:
        sheet_data (dict): The sheet data dictionary.
        sheet_name (str): The name of the sheet to which the row will be added.
        code (str): The code for the item.
        description (str): The description of the item.
        supplier_product_code (str): The supplier product code.
        warning (str): The warning message.
    """
    # Ensure the sheet exists
    if sheet_name not in sheet_data:
        sheet_data[sheet_name] = []

    if code == "":
        code = generate_unique_id()

    # Add the new row
    sheet_data[sheet_name].append([
        generate_unique_id(),  # PkId
        code,
        description,
        supplier_product_code,
        warning,
        "",  # Operation
    ])


@pytest.fixture
def mock_buz_inventory_items_warning():
    """Fixture for mock Buz inventory items Excel file."""
    sheet_data = {}
    add_row_to_sheet(
        sheet_data=sheet_data,
        sheet_name="Sheet3",
        description="Item E",
        supplier_product_code="SC-E",
        warning="Fabric on backorder until 3 Jan 2010",
    )

    add_row_to_sheet(
        sheet_data=sheet_data,
        sheet_name="Sheet3",
        description="Item E",
        supplier_product_code="SC-E",
        warning="Fabric on backorder until 3 Jan 2020",
    )

    sheets_header_data = {
        "headers": ["PkId", "Code", "Description", "Supplier Product Code", "Warning", "Operation"],
        "header_row": 2
    }
    return sheet_data, sheets_header_data


@pytest.fixture
def mock_buz_inventory_items():
    """Fixture for mock Buz inventory items Excel file."""
    sheet_data = {}
    add_row_to_sheet(
        sheet_data=sheet_data,
        sheet_name="Sheet1",
        description="Item A",
        supplier_product_code="SC-A",
    )

    add_row_to_sheet(
        sheet_data=sheet_data,
        sheet_name="Sheet1",
        description="Item B",
        supplier_product_code="SC-B",
    )

    add_row_to_sheet(
        sheet_data=sheet_data,
        sheet_name="Sheet2",
        description="Item C",
        supplier_product_code="SC-C",
    )

    add_row_to_sheet(
        sheet_data=sheet_data,
        sheet_name="Sheet2",
        description="Item D",
        supplier_product_code="SC-D",
    )

    sheet_data["EmptySheet"] = []  # No data rows

    sheets_header_data = {
        "headers": ["PkId", "Code", "Description", "Supplier Product Code", "Operation"],
        "header_row": 2
    }
    return sheet_data, sheets_header_data


@pytest.fixture
def unleashed_expected_headers(app_config):
    """Fixture for expected headers in the Unleashed CSV file."""
    return app_config["headers"]["unleashed_csv_file"]


@pytest.fixture
def mock_supplier_codes():
    """Fixture for supplier codes."""
    return ["SC-A", "SC-C"]


@pytest.fixture
def mock_inventory_group_data(get_db_manager):
    """Fixture to populate the inventory_groups table."""
    db_manager = get_db_manager
    db_manager.insert_item("inventory_groups",
                           {
                               "group_code": "GRP1",
                               "group_description": "Inventory Group 1"
                           })
    db_manager.insert_item("inventory_groups",
                           {
                               "group_code": "GRP2",
                               "group_description": "Inventory Group 2"
                           })
    yield db_manager
