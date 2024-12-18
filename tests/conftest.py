import pandas as pd
import pytest
from io import BytesIO
from services.config_service import ConfigManager
from services.database import create_db_manager


# Initialize config manager once
config_manager = ConfigManager()


def create_mock_excel(expected_headers, sheet_data):
    """
    Create a mock Excel file with multiple sheets, each with headers in row 2 and mock data from row 3 onwards.

    :param expected_headers: List of expected headers.
    :param sheet_data: Dictionary where keys are sheet names and values are lists of dictionaries representing mock data.
    :return: BytesIO object containing the mock Excel file.
    """
    # Save the data into an Excel file in memory
    mock_excel = BytesIO()
    with pd.ExcelWriter(mock_excel, engine="openpyxl") as writer:
        for sheet_name, mock_data in sheet_data.items():
            # Create row 1 with blanks and row 2 with headers
            first_row = [None] * len(expected_headers)
            header_row = expected_headers

            # Create mock data rows
            data_rows = []
            for row in mock_data:
                # Fill unspecified columns with None
                full_row = [row.get(header, None) for header in expected_headers]
                data_rows.append(full_row)

            # Combine all rows into a DataFrame
            df = pd.DataFrame([first_row, header_row] + data_rows)

            # Write the DataFrame to the specified sheet
            df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)  # No index/headers in Excel

    mock_excel.seek(0)
    return mock_excel


@pytest.fixture
def mock_buz_inventory_items():
    sheet_data = {
        "Sheet1": [
            {"PkId": 1, "Code": "ABC123", "Description": "Item A", "Supplier Product Code": "PG1"},
            {"PkId": 2, "Code": "DEF456", "Description": "Item B", "Supplier Product Code": "ABC"},
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

    expected_headers = config_manager.get("headers", "buz_inventory_item_file")
    return create_mock_excel(expected_headers, sheet_data)


@pytest.fixture
def unleashed_expected_headers():
    """Fixture for expected headers in the Unleashed CSV file."""
    return config_manager.get("headers", "unleashed_csv_file")


@pytest.fixture
def supplier_codes():
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


@pytest.fixture(scope='session')
def app_config():
    config_manager = ConfigManager()
    return config_manager.config


@pytest.fixture(scope='session')
def get_db_manager():
    return create_db_manager(config_manager.config.get('database'))

