# test_database.py

import unittest
from unittest.mock import MagicMock
from services.database import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        # Mock the database connection
        self.mock_db = MagicMock()
        self.db_manager = DatabaseManager(self.mock_db)

    def test_insert_item(self):
        # Arrange
        table = 'users'
        data = {'name': 'John', 'age': 30}

        # Act
        self.db_manager.insert_item(table, data)

        # Assert
        query = "INSERT OR IGNORE INTO users (name, age) VALUES (?, ?)"
        params = ('John', 30)
        self.mock_db.execute.assert_called_once_with(query, params)
        self.mock_db.commit.assert_called_once()

    def test_get_item(self):
        # Arrange
        table = 'users'
        criteria = {'name': 'John'}
        self.mock_db.execute.return_value.fetchall.return_value = [{'name': 'John', 'age': 30}]

        # Act
        result = self.db_manager.get_item(table, criteria)

        # Assert
        query = "SELECT * FROM users WHERE name=?"
        params = ('John',)
        self.mock_db.execute.assert_called_once_with(query, params)
        self.assertEqual(result, [{'name': 'John', 'age': 30}])

    def test_delete_item(self):
        # Arrange
        table = 'users'
        criteria = {'name': 'John'}

        # Act
        self.db_manager.delete_item(table, criteria)

        # Assert
        query = "DELETE FROM users WHERE name=?"
        params = ('John',)
        self.mock_db.execute.assert_called_once_with(query, params)
        self.mock_db.commit.assert_called_once()

    def test_tables_created(self):
        # Verify that all expected tables are created
        cursor = self.test_db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row['name'] for row in cursor.fetchall()}

        expected_tables = {'inventory_items', 'pricing_data', 'unleashed_products',
                           'inventory_group_codes', 'wholesale_markups'}
        self.assertTrue(expected_tables.issubset(tables))
        

if __name__ == '__main__':
    unittest.main()
