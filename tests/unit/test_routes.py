import pytest
from flask import g


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
