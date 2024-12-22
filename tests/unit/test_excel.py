import unittest
from services.excel import OpenPyXLFileHandler


class TestCreateExcelFile(unittest.TestCase):
    def setUp(self):
        # Set up mock data for the test
        self.sheets_data = {
            "Sheet1": (
                [["Row1Col1", "Row1Col2"], ["Row2Col1", "Row2Col2"]],
                ["Header1", "Header2"],
                2,
            ),
            "Sheet2": (
                [["Row1", "Row2"]],
                ["HeaderA", "HeaderB"],
            ),
        }

    def test_create_excel_file(self):
        # Create an instance of the file handler
        file_handler = OpenPyXLFileHandler.from_sheets_data (sheets_data=self.sheets_data)

        # Assert that the sheets exist
        self.assertIn("Sheet1", file_handler.workbook.sheetnames)
        self.assertIn("Sheet2", file_handler.workbook.sheetnames)

        # Check the content of Sheet1
        sheet1 = file_handler.workbook["Sheet1"]
        self.assertEqual(sheet1.cell(row=2, column=1).value, "Header1")
        self.assertEqual(sheet1.cell(row=2, column=2).value, "Header2")
        self.assertEqual(sheet1.cell(row=3, column=1).value, "Row1Col1")
        self.assertEqual(sheet1.cell(row=3, column=2).value, "Row1Col2")
        self.assertEqual(sheet1.cell(row=4, column=1).value, "Row2Col1")
        self.assertEqual(sheet1.cell(row=4, column=2).value, "Row2Col2")

        # Check the content of Sheet2
        sheet2 = file_handler.workbook["Sheet2"]
        self.assertEqual(sheet2.cell(row=1, column=1).value, "HeaderA")
        self.assertEqual(sheet2.cell(row=1, column=2).value, "HeaderB")
        self.assertEqual(sheet2.cell(row=2, column=1).value, "Row1")
        self.assertEqual(sheet2.cell(row=2, column=2).value, "Row2")


if __name__ == "__main__":
    unittest.main()
