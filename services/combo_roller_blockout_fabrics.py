from services.database import DatabaseManager


def get_inventory_items(db_manager: DatabaseManager, group_code):
    """Fetch inventory items based on the given SQL query."""
    query = """
    SELECT
        DescnPart1,
        DescnPart3,
        Code
    FROM
        inventory_items
    WHERE
        inventory_group_code = ? AND
        DescnPart2 = "Blockout";
    """

    # Execute the query with the group_code parameter
    results = db_manager.execute_query(query, (group_code,)).fetchall()

    # Convert the results into a list of dictionaries for easy use in templates
    items = [
        {
            "desc_part_1": row["DescnPart1"],
            "desc_part_3": row["DescnPart3"],
            "code": row["Code"]
        }
        for row in results
    ]

    # Sort items by desc_part_1, then desc_part_3
    items = sorted(items, key=lambda x: (x["desc_part_1"], x["desc_part_3"]))

    # Extract unique values for desc_part_1
    unique_desc_part_1 = sorted({item["desc_part_1"] for item in items})

    return items, unique_desc_part_1
