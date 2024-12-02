# test_google_sheets.py

import unittest
from unittest.mock import patch

from queries import query_get_items_not_in_unleashed
from process_buz_workbooks import db_get_all_inventory_items
from process_buz_workbooks import db_get_all_inventory_item_columns
from data_processing import db_get_all_unleashed_product_codes

class TestQueryGetItemsNotInUnleashed(unittest.TestCase):

    def test_query_items_not_in_unleashed(self):
        # Manually create the input data
        param1 = {'key1': 'value1', 'key2': 'value2'}
        param2 = {'keyA': 'valueA', 'keyB': 'valueB'}
        param3 = {'keyX': 'valueX', 'keyY': 'valueY'}
        expected_output = {'product_code': ['item1', 'item2']}

        # Call the function with the test inputs
        result = query_get_items_not_in_unleashed(param1, param2, param3)

        # Check if the result matches the expected output
        self.assertEqual(result, expected_output)

if __name__ == '__main__':
    unittest.main(
