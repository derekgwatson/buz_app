import pytest
from unittest.mock import MagicMock
from services.database import DatabaseManager
from services.google_sheets_service import GoogleSheetsService
from services.backorders import process_inventory_backorder_with_services


@pytest.fixture
def mock_db_manager(mocker):
    """
    Mock the database manager to return inventory data.

    Scenarios:
    Product found in backorders sheet, but date has passed
        Product was on backorder => remove the warning (ignore what warning was)
        Product wasn't on backorder => ignore

    Product found in backorders sheet, date is in the future
        update warning (doesn't matter what existing warning was)

    Product not found in backorders sheet
        Product was on backorder -> remove the warning (ignore what warning was)
        Product wasn't on backorder => ignore
    """
    mock_db_manager = MagicMock(spec=DatabaseManager)

    # Mock inventory data grouped by `inventory_group_code`
    mock_inventory_data = {
        "GRP1": [
            {
                "PkId": "1a",
                "SupplierProductCode": "SUP001",
                "Warning": "",
                "DescnPart1": "Product A",
                "DescnPart2": "",
                "DescnPart3": "",
            },
            {
                "PkId": "1b",
                "SupplierProductCode": "SUP002",
                "Warning": "On backorder until 01 Jan 2022.",
                "DescnPart1": "Product B",
                "DescnPart2": "Variant 1",
                "DescnPart3": "",
            },
        ],
        "GRP2": [
            {
                "PkId": "2a",
                "SupplierProductCode": "SUP003",
                "Warning": "",
                "DescnPart1": "Product C",
                "DescnPart2": "Size Large",
                "DescnPart3": "",
            },
            {
                "PkId": "2b",
                "SupplierProductCode": "SUP003",
                "Warning": "On backorder until 04 Apr 2100",
                "DescnPart1": "Product D",
                "DescnPart2": "Size Tiny",
                "DescnPart3": "",
            },
            {
                "PkId": "2c",
                "SupplierProductCode": "SUP003",
                "Warning": "On backorder until 04 Apr 2000",
                "DescnPart1": "Product E",
                "DescnPart2": "Size Normal",
                "DescnPart3": "",
            },
        ],
    }

    # Ensure the mock path matches the actual import location
    mocker.patch(
        "services.backorders.get_all_inventory_items_by_group",  # Match the actual import path
        return_value=mock_inventory_data,
    )

    return mock_db_manager


@pytest.fixture
def mock_sheets_service():
    """Mock the Google Sheets service."""
    mock_service = MagicMock(spec=GoogleSheetsService)

    # Mock Google Sheets data
    mock_sheet_data = [
        ["Unleashed Code", "On backorder until"],  # Header row
        ["SUP001", "15/02/2050"],
        ["SUP003", "20/03/2000"],
    ]
    mock_service.fetch_sheet_data.return_value = mock_sheet_data

    return mock_service


def test_process_inventory_backorder(mock_db_manager, mock_sheets_service):
    """Test processing inventory backorders with mocked database and Google Sheets service."""
    spreadsheet_id = "mock_spreadsheet_id"
    range_name = "mock_range"

    # Run the function
    upload_workbook, original_workbook = process_inventory_backorder_with_services(
        _db_manager=mock_db_manager,
        _sheets_service=mock_sheets_service,
        spreadsheet_id=spreadsheet_id,
        range_name=range_name,
    )

    # Assertions for upload workbook
    assert len(upload_workbook.sheetnames) == 2, "Upload workbook should have 2 sheets."
    sheet = upload_workbook["GRP1"]
    assert sheet.cell(1, 1).value is None, "Row 1 should be empty, checked A1"
    assert sheet.cell(2, 2).value == "SupplierProductCode", "Row 2 should have headers, checked B2"
    assert sheet.cell(3, 2).value == "SUP001", "Row 3 should contain data - checked B3"
    assert sheet.cell(3, 3).value == "Product A on backorder until 15 Feb 2050.", "Row should have backorder message."
    assert sheet.cell(4, 2).value == "SUP002", "Row 3 should contain data - checked B4"
    assert sheet.cell(4, 3).value == "", "Cleared backorder message given date has passed."

    sheet = upload_workbook["GRP2"]
    assert sheet.cell(1, 1).value is None, "Row 1 should be empty, checked A1"
    assert sheet.cell(2, 1).value == "PkId", "Row 2 should have headers, checked A2"
    assert sheet.cell(3, 1).value == "2b", "Row 3 should contain data - checked A3"
    assert sheet.cell(3, 3).value == "", "Cleared backorder message given date has passed."
    assert sheet.cell(4, 1).value == "2c", "Row 3 should contain data - checked A4"
    assert sheet.cell(4, 3).value == "", "Cleared backorder message given date has passed."

    # Assertions for original workbook
    assert len(original_workbook.sheetnames) == 2, "Original workbook should have 2 sheets."
    sheet = original_workbook["GRP1"]
    assert sheet.cell(1, 1).value is None, "Row 1 should be empty, checked A1"
    assert sheet.cell(2, 2).value == "SupplierProductCode", "Row 2 should have headers, checked B2"
    assert sheet.cell(3, 2).value == "SUP001", "Row 3 should contain data - B3 should contain 'SUP001'."
    assert sheet.cell(3, 3).value == "", "Blank backorder message in original row."
