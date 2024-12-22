import pandas as pd
from io import BytesIO
from services.buz_items_by_supplier_code import process_buz_items_by_supplier_codes


class TestProcessBuzItems:
    """Class-based tests for process_buz_items_by_supplier_codes."""

    def test_process_buz_items_valid(self, mock_buz_inventory_items, mock_supplier_codes):
        """Test processing a valid Excel file with matching supplier codes."""
        result = process_buz_items_by_supplier_codes(mock_buz_inventory_items, mock_supplier_codes)
        processed_data = pd.read_excel(result, sheet_name=None, engine="openpyxl", header=None)

        assert "Sheet1" in processed_data, "Sheet1 should be present in the processed output."
        sheet1 = processed_data["Sheet1"]

        # Check that the 41st column contains 'E'
        assert (sheet1.iloc[3:, 40] == "E").all(), "All rows in column 41 should contain 'E'."

        # Ensure rows with matching supplier codes are preserved
        assert len(sheet1) == 3, "Sheet1 should have 3 rows: 2 headers and 1 matching row."

        # Validate Sheet2
        assert "Sheet2" in processed_data, "Sheet2 should be present in the processed output."
        sheet2 = processed_data["Sheet2"]
        assert len(sheet2) == 3, "Sheet2 should have 3 rows: 2 headers and 1 matching row."

        # Validate absence of unrelated sheets
        assert "Sheet3" not in processed_data, "Sheet3 should not be in the processed output."
        assert "EmptySheet" not in processed_data, "EmptySheet should not be in the processed output."

    def test_process_buz_items_no_matching_codes(self, mock_buz_inventory_items):
        """Test processing a valid Excel file with no matching supplier codes."""
        non_matching_codes = ["SUP999"]
        result = process_buz_items_by_supplier_codes(mock_buz_inventory_items, non_matching_codes)
        assert result is None

    def test_process_buz_items_insufficient_columns(self, capsys, supplier_codes):
        """Test processing a sheet with insufficient non-blank columns."""
        # Modify the mock file to have insufficient columns
        mock_file = BytesIO()
        data = {
            "InsufficientColumns": pd.DataFrame({
                0: ["Header", "ShortRow", None],
                1: [None, "ShortRow", None],
            }),
        }
        with pd.ExcelWriter(mock_file, engine="openpyxl") as writer:
            for sheet_name, df in data.items():
                df.to_excel(writer, index=False, header=False, sheet_name=sheet_name)
        mock_file.seek(0)

        process_buz_items_by_supplier_codes(mock_file, [])

        captured = capsys.readouterr()
        assert "Skipping sheet 'InsufficientColumns' due to invalid headers." in captured.out

    def test_process_buz_items_empty_file(self, capsys, supplier_codes):
        """Test processing an empty Excel file."""
        # Create an in-memory empty Excel file with one sheet
        empty_file = BytesIO()
        with pd.ExcelWriter(empty_file, engine="openpyxl") as writer:
            pd.DataFrame().to_excel(writer, index=False, header=False, sheet_name="EmptySheet")
        empty_file.seek(0)  # Reset the pointer to the start of the file

        process_buz_items_by_supplier_codes(empty_file, [])

        captured = capsys.readouterr()
        assert "No sheets met the criteria for processing or contained matching rows." in captured.out
