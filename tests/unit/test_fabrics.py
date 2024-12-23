import unittest
from app import create_app
from services.database import create_db_manager, init_db
from flask import g

class TestFabrics(unittest.TestCase):
    def setUp(self):
        """Set up a test-specific app instance."""
        self.app = create_app('Testing')
        self.app.config["database"] = ":memory:"  # Use in-memory SQLite database
        self.client = self.app.test_client()  # Create a test client

        with self.app.app_context():
            g.db = create_db_manager(self.app.config['database'])
            self.app.extensions['db_manager'] = g.db  # Store in Flask extensions for reuse

            # Initialize the database
            init_db(g.db)

    def tearDown(self):
        """Clean up after tests."""
        with self.app.app_context():
            if hasattr(g, 'db') and g.db:
                g.db.close()

    def test_form_renders(self):
        """Test that the fabric creation form renders correctly."""
        response = self.client.get("/fabrics/create")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Code", response.data)
        self.assertIn(b"Product Type", response.data)
        self.assertIn(b"Description 1", response.data)
        self.assertIn(b"Description 2", response.data)
        self.assertIn(b"Description 3", response.data)
