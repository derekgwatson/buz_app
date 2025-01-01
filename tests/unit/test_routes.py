import pytest
from unittest.mock import patch, MagicMock
from flask import g
from app import create_app
from app.routes import before_request


class TestRoutes:
    """Class-based tests for Flask routes."""

    @pytest.fixture(autouse=True)
    def setup(self, client):
        """Set up the test client and app context."""
        self.client = client

    def test_home_route(self, auth_headers):
        """Test the home route."""
        response = self.client.get(
            '/',
            headers=auth_headers
        )
        assert response.status_code == 200
        assert b'Welcome to the Buz Data Analysis Tool!' in response.data

    def test_upload_inventory_groups(self, auth_headers):
        """Test the upload_inventory_groups route."""
        data = {'file': (b'mock file content', 'test.csv')}
        response = self.client.post(
            '/upload_inventory_groups',
            data=data,
            content_type='multipart/form-data',
            headers=auth_headers
        )
        assert response.status_code == 302  # Assuming a redirect after success
        assert response.headers['Location'] == '/manage_inventory_groups'

    @patch('services.database.create_db_manager')  # Mock the create_db_manager function
    @patch('time.time', return_value=123456.78)  # Mock time.time to return a fixed value
    def test_before_request(self, mock_time, mock_create_db_manager):
        """Test the before_request function."""
        # Mock database manager
        mock_db_manager = MagicMock()
        mock_create_db_manager.return_value = mock_db_manager

        with self.app.app_context():
            # Call the actual before_request function
            before_request()

            # Assert that g.db is set correctly
            assert g.db == mock_db_manager

            # Assert that the start time is set correctly
            assert g.start_time == 123456.78

            # Assert that create_db_manager was called with the correct argument
            mock_create_db_manager.assert_called_once_with(self.app.config['database'])
