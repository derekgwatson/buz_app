import unittest
from unittest.mock import MagicMock, patch
from werkzeug.datastructures import FileStorage
from services.upload import upload


class TestUploadFunction(unittest.TestCase):
    @patch('upload_module.process_workbook')
    @patch('upload_module.insert_unleashed_data')
    @patch('upload_module.OpenPyXLFileHandler')
    def test_upload_all_files(self, mock_file_handler, mock_insert, mock_process, app_config):
        # Mock files
        inventory_file = FileStorage(filename='inventory.xlsx')
        pricing_file = FileStorage(filename='pricing.xlsx')
        unleashed_file = FileStorage(filename='unleashed.csv')

        # Mock save method
        inventory_file.save = MagicMock()
        pricing_file.save = MagicMock()
        unleashed_file.save = MagicMock()

        # Call the function
        upload_folder = app_config['UPLOAD_FOLDER']
        result = upload(inventory_file, pricing_file, unleashed_file, upload_folder)

        # Assertions
        inventory_file.save.assert_called_once_with(upload_folder+'/inventory.xlsx')
        pricing_file.save.assert_called_once_with(upload_folder+'/pricing.xlsx')
        unleashed_file.save.assert_called_once_with(upload_folder+'/unleashed.csv')

        mock_file_handler.assert_any_call(file_path=upload_folder+'/inventory.xlsx')
        mock_process.assert_any_call(
            file_handler=mock_file_handler.return_value,
            table_name='inventory_items',
            expected_headers=app_config['headers', 'buz_inventory_item_file'],
            header_row=2
        )

        mock_process.assert_any_call(
            file_handler=mock_file_handler.return_value,
            table_name='pricing_data',
            expected_headers=app_config['headers', 'buz_pricing_file'],
            header_row=1
        )

        mock_insert.assert_called_once_with(upload_folder+'/unleashed.csv')
        self.assertEqual(result, ['inventory_file', 'pricing_file', 'unleashed_file'])

    def test_upload_no_files(self):
        upload_folder = '/uploads'
        result = upload(None, None, None, upload_folder)
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
