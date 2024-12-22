from base64 import b64encode
import pandas as pd
import pytest
from io import BytesIO
from services.config_service import ConfigManager
from services.database import create_db_manager


@pytest.fixture(scope="session")
def config_manager():
    """Fixture for initializing ConfigManager."""
    return ConfigManager()


@pytest.fixture(scope="session")
def app_config(config_manager):
    """Fixture for application configuration."""
    return config_manager.config


@pytest.fixture(scope="session")
def get_db_manager(app_config):
    """Fixture for database manager."""
    return create_db_manager(app_config.get('database'))


@pytest.fixture
def auth_headers():
    """Fixture for authorization headers."""
    credentials = b64encode(b"testuser:testpassword").decode("utf-8")
    return {
        'Authorization': f'Basic {credentials}'
    }


def create_mock_excel(expected_headers, sheet_data):
    """Create a mock Excel file."""
    header_names = [header['spreadsheet_column'] for header in expected_headers]
    mock_excel = BytesIO()

    with pd.ExcelWriter(mock_excel, engine="openpyxl") as writer:
        for sheet_name, mock_data in sheet_data.items():
            rows = [[""] * len(header_names)]  # Row 1 (empty)
            rows.append(header_names)         # Row 2 (headers)

            for row in mock_data:
                full_row = [row.get(header, None) for header in header_names]
                rows.append(full_row)

            df = pd.DataFrame(rows)
            df.to_excel(writer, index=False, header=False, sheet_name=sheet_name)

    mock_excel.seek(0)
    return mock_excel


@pytest.fixture
def mock_buz_inventory_items(app_config):
    """Fixture for mock Buz inventory items Excel file."""
    sheet_data = {
        "Sheet1": [
            {"PkId": 1, "Code": "ABC123", "Description": "Item A", "Supplier Product Code": "PG1", "Operation": ""},
            {"PkId": 2, "Code": "DEF456", "Description": "Item B", "Supplier Product Code": "ABC", "Operation": ""},
        ],
        "Sheet2": [
            {"PkId": 3, "Code": "GHI789", "Description": "Item C"},
            {"PkId": 4, "Code": "JKL012", "Description": "Item D", "Supplier Product Code": "001"},
        ],
        "Sheet3": [
            {"PkId": 1, "Warning": "Fabric on backorder until 3 Jan 2050", "Supplier Product Code": "PG4"},
            {"PkId": 2, "Warning": "Fabric on backorder until 3 Jan 2020", "Supplier Product Code": "PG5"},
        ],
        "EmptySheet": []  # No data rows
    }

    expected_headers = app_config["headers"]["buz_inventory_item_file"]
    return create_mock_excel(expected_headers, sheet_data)


@pytest.fixture
def unleashed_expected_headers(app_config):
    """Fixture for expected headers in the Unleashed CSV file."""
    return app_config["headers"]["unleashed_csv_file"]


@pytest.fixture
def mock_supplier_codes():
    """Fixture for supplier codes."""
    return ["PG1", "001"]


@pytest.fixture
def mock_unleashed_data(unleashed_expected_headers):
    """Fixture for mock Unleashed data."""
    sheet_data = {
        "DataSheet": [
            {"Supplier Code": "PG1", "Item Name": "Item A", "Price": 10.0},
            {"Supplier Code": "001", "Item Name": "Item D", "Price": 20.0},
        ]
    }
    return create_mock_excel(unleashed_expected_headers, sheet_data)
