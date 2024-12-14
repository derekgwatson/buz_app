import unittest
from unittest.mock import MagicMock, patch
import sqlite3
import io
from services.database import DatabaseManager
from services.config_service import ConfigManager


def generate_create_table_sql(table_name, fields):
    """
    Generate a SQL CREATE TABLE statement.

    :param table_name: Name of the table to create.
    :param fields: List of fields from config.json.
    :return: A formatted SQL statement.
    """
    field_definitions = [f'"{field.replace(" ", "")}" TEXT' for field in fields]
    create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(field_definitions)});'
    return create_table_sql


class TestDataProcessing(unittest.TestCase):
    def setUp(self):
        """Set up an in-memory SQLite database using DatabaseManager."""
        # Create an in-memory SQLite database
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row

        # Initialize DatabaseManager with the in-memory connection
        self.db_manager = DatabaseManager(self.conn)

        config = ConfigManager()
        unleashed_fields = config.get("headers","unleashed_fields")

        # Initialize the database manager (assuming it's already set up)
        create_table_sql = generate_create_table_sql("unleashed_products", unleashed_fields)
        self.db_manager.execute_query(create_table_sql)
        self.db_manager.commit()
        print(f"Table unleashed_products created with fields: {', '.join(unleashed_fields)}")

    def tearDown(self):
        """Close the database connection after each test."""
        self.conn.close()

    def test_safe_float(self):
        from services.data_processing import safe_float

        self.assertEqual(safe_float(""), 0.0)
        self.assertEqual(safe_float("123.45"), 123.45)
        self.assertEqual(safe_float("abc"), 0.0)

    def test_clean_value(self):
        from services.data_processing import clean_value

        self.assertEqual(clean_value("=test"), "test")
        self.assertEqual(clean_value("\uFEFFvalue"), "value")
        self.assertEqual(clean_value("\x1Fdata"), "data")

    @patch('services.data_processing.DatabaseManager')
    def test_clear_unleashed_table(self, mock_database_manager):
        from services.data_processing import clear_unleashed_table

        # Mock the DatabaseManager
        mock_db_manager = mock_database_manager.return_value
        mock_db_manager.execute_query.return_value = None

        # Run the function under test
        clear_unleashed_table(mock_db_manager)

        # Assert the correct query was executed
        mock_db_manager.execute_query.assert_called_once_with("DELETE FROM unleashed_products", auto_commit=True)

    @patch('services.database.get_db_connection')
    def test_insert_unleashed_data(self, mock_get_db_connection):
        from services.data_processing import insert_unleashed_data

        mock_get_db_connection.return_value = self.conn

        test_csv_data = io.StringIO(
            """Product Code,Product Description,IsObsoleted,IsSellable\nP001,Test Product,No,Yes\nP002,Obsolete Product,Yes,No""")

        with patch('builtins.open', return_value=test_csv_data):
            insert_unleashed_data(self.db_manager, 'mock_file.csv')

            # Verify database state
            rows = self.conn.execute('SELECT * FROM unleashed_products').fetchall()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['ProductCode'], 'P001')

    def test_validate_data(self):
        from services.data_processing import validate_data

        required_fields = ["ProductCode", "ProductDescription"]
        valid_data = {"ProductCode": "P001", "ProductDescription": "Test Product"}
        invalid_data = {"ProductCode": "P001"}
        self.assertTrue(validate_data(valid_data, required_fields))
        with self.assertRaises(ValueError):
            validate_data(invalid_data, required_fields)

    def test_transform_data(self):
        from services.data_processing import transform_data

        data = {"key1": "value", "key2": 123}
        transformed = transform_data(data)
        self.assertEqual(transformed, {"key1": "VALUE", "key2": 123})


if __name__ == '__main__':
    unittest.main()
