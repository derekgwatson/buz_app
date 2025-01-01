import pytest
import logging
from services.excel import OpenPyXLFileHandler
import pandas as pd
from services.buz_items_by_supplier_code import process_buz_items_by_supplier_codes


class TestProcessBuzItems:
    """Class-based tests for process_buz_items_by_supplier_codes."""

    def test_process_buz_items_valid(self, mock_buz_inventory_items, mock_supplier_codes):
        """Test processing a valid Excel file with matching supplier codes."""
        sheet_data, sheets_header_data = mock_buz_inventory_items
        excel = OpenPyXLFileHandler.from_sheets_data(sheet_data, sheets_header_data)
        result = process_buz_items_by_supplier_codes(excel, mock_supplier_codes)
        processed_data = pd.read_excel(result, sheet_name=None, engine="openpyxl", header=0)

        assert "Sheet1" in processed_data, "Sheet1 should be present in the processed output."
        sheet1 = processed_data["Sheet1"]

        # Locate the 'Operation' column by header name
        operation_col_index = sheet1.columns.get_loc("Operation")
        assert operation_col_index is not None, "'Operation' column should be present in Sheet1."

        # Check that the 'Operation' column contains 'E'
        assert (sheet1.iloc[1:, operation_col_index] == "E").all(), "All rows in the 'Operation' column should contain 'E'."

        # Ensure rows with matching supplier codes are preserved
        assert len(sheet1) == 3, "Sheet1 should have 3 rows: 1 header and 2 matching rows."

        # Validate Sheet2
        assert "Sheet2" in processed_data, "Sheet2 should be present in the processed output."
        sheet2 = processed_data["Sheet2"]
        assert len(sheet2) == 3, "Sheet2 should have 3 rows: 1 header and 2 matching rows."

        # Validate absence of unrelated sheets
        assert "Sheet3" not in processed_data, "Sheet3 should not be in the processed output."
        assert "EmptySheet" not in processed_data, "EmptySheet should not be in the processed output."

    def test_process_buz_items_no_matching_codes(self, mock_buz_inventory_items):
        """Test processing a valid Excel file with no matching supplier codes."""
        non_matching_codes = ["SUP999"]
        sheet_data, sheets_header_data = mock_buz_inventory_items
        excel = OpenPyXLFileHandler.from_sheets_data(sheet_data, sheets_header_data)
        result = process_buz_items_by_supplier_codes(
            uploaded_file=excel,
            supplier_codes=non_matching_codes
        )
        assert result is None

    def test_process_buz_items_invalid_headers(self, caplog, app_config, inventory_items_sheets_data_invalid, mock_supplier_codes):
        """Test processing a sheet with insufficient non-blank columns."""
        logger = logging.getLogger("test_logger")
        logger.setLevel(logging.INFO)

        sheet_data, sheets_header_data = inventory_items_sheets_data_invalid
        mock_file = OpenPyXLFileHandler.from_sheets_data(sheet_data, sheets_header_data)

        with caplog.at_level(logging.INFO, logger="test_logger"):
            process_buz_items_by_supplier_codes(mock_file, mock_supplier_codes)

        assert "Required headers missing in sheet 'Sheet1'. Skipping." in caplog.text

    def test_process_buz_items_empty_file(self, mock_supplier_codes):
        """Test processing an empty Excel file."""
        with pytest.raises(ValueError, match="No file uploaded."):
            process_buz_items_by_supplier_codes(
                OpenPyXLFileHandler(),
                mock_supplier_codes
            )
