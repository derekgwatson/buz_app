import os
import json
import hmac
import hashlib
import base64
import requests
from urllib.parse import urlencode


class UnleashedAPIClient:
    def __init__(self, credentials_filename: str = 'unleashed.json'):
        credentials_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..', 'credentials', credentials_filename
        )
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)
        self.api_id = credentials['api_id']
        self.api_key = credentials['api_key']
        self.base_url = "https://api.unleashedsoftware.com"

    def _generate_signature(self, args: str) -> str:
        import hmac
        import hashlib
        import base64

        encoding = "utf-8"
        key_bytes = self.api_key.encode(encoding)
        msg_bytes = args.encode(encoding)

        hmac_sha256 = hmac.new(key_bytes, msg_bytes, hashlib.sha256)
        signature = base64.b64encode(hmac_sha256.digest()).decode(encoding)

        return signature

    def _get_headers(self, query_string: str):
        return {
            "api-auth-id": self.api_id,
            "api-auth-signature": self._generate_signature(query_string),
            "Accept": "application/json",
            "Content-Type": "application/json",
            "client-type": "WatsonBlinds/buz_app"
        }

    def get_paginated_data(self, endpoint: str, page_size: int = 1000, params: dict = None):
        page = 1
        all_items = []

        while True:
            query_params = params.copy() if params else {}
            query_params["pageSize"] = page_size
            uri = f"/{endpoint}/{page}"
            query_string = urlencode(query_params)

            headers = self._get_headers(query_string)
            url = f"{self.base_url}{uri}?{query_string}"

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")

            data = response.json()
            items = data.get("Items", [])
            all_items.extend(items)

            pagination = data.get("Pagination", {})
            if page >= pagination.get("NumberOfPages", 1):
                break
            page += 1

        return all_items
