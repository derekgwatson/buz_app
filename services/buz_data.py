from services.odata_client import ODataClient
import pandas as pd


def get_buz_data(instance):
    # Base URL for SalesReport
    odata_client = ODataClient(instance)

    # Define instance-specific filters
    filter_conditions = [
            "OrderStatus eq 'Work in Progress'",
            "ProductionStatus ne 'null'",
        ]

    # Fetch filtered SalesReport data
    report_data = odata_client.get("JobsScheduleDetailed", filter_conditions)

    statuses = {item["ProductionStatus"] for item in report_data if "ProductionStatus" in item}
    print(f"Statuses are: {statuses}")
    return statuses
