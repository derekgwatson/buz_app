from services.odata_client import ODataClient
import pandas as pd


def get_buz_data(instance):
    # Base URL for SalesReport
    odata_client = ODataClient(instance)

    # Define instance-specific filters
    filter_conditions = [
            "pkid eq '0f0a12fe-20b6-4043-b574-04b6d492f27b'",
        ]

    # Fetch filtered SalesReport data
    report_data = odata_client.get("inventory", filter_conditions)

    print(f"report_data: {report_data}")
    return report_data
