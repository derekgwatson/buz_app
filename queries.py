def query_get_items_not_in_unleashed(inventory_items, columns_to_export, product_codes):
    """
    Identifies items in Buz that no longer have a corresponding product code in Unleashed.
    It returns a dictionary of items to be deleted from Buz.

    Args:
        inventory_items (dict): inventory items from Buz - db_get_all_inventory_items()
        columns_to_export (dict): the columns from the inventory items - db_get_all_inventory_item_columns()
        product_codes (dict): list of unleashed product codes - db_get_all_unleashed_product_codes()

    Returns:
        dictionary: holds DataFrames for each inventory group with all inventory items to be removed from Buz
    """

    # Prepare a dictionary to hold DataFrames for each inventory group
    group_dfs = {}

    # Find supplier codes not in product codes and gather their inventory group codes
    for item in inventory_items:
        if item['SupplierProductCode'].lower() not in product_codes:
            inventory_group_code = item['inventory_group_code']  # Get the group code

            # Create a new entry for the missing code with all columns in the 'columns_to_export' list
            entry = {col: item[col] if col in item.keys() else '' for col in columns_to_export}
            entry['Operation'] = 'D'  # Add Operation column with 'D'

            # Add entry to the corresponding group DataFrame
            if inventory_group_code not in group_dfs:
                group_dfs[inventory_group_code] = []
            group_dfs[inventory_group_code].append(entry)

    return group_dfs

