# test_data_processing.py

import unittest
from services.data_processing import validate_data, transform_data

class TestDataProcessing(unittest.TestCase):
    def test_validate_data_missing_fields(self):
        data = {'name': 'John'}
        with self.assertRaises(ValueError):
            validate_data(data, ['name', 'age'])

    def test_validate_data_all_fields_present(self):
        data = {'name': 'John', 'age': 30}
        result = validate_data(data, ['name', 'age'])
        self.assertTrue(result)

    def test_transform_data(self):
        data = {'name': 'john', 'age': 30}
        result = transform_data(data)
        self.assertEqual(result['name'], 'JOHN')
        self.assertEqual(result['age'], 30)

if __name__ == '__main__':
    unittest.main()
