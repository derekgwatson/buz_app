from services.data_processing import (get_all_fabrics, get_all_fabric_group_mappings, get_inventory_groups)
from services.database import DatabaseManager


def get_fabric_grid_data(db_manager: DatabaseManager):
    # Get all fabrics with concatenated descriptions
    fabrics = get_all_fabrics(db_manager=db_manager)

    # Get all inventory groups
    inventory_groups = get_inventory_groups(db_manager=db_manager)
    print("Inventory Groups:", inventory_groups)
    for group in inventory_groups:
        print("Available keys in group:", group.keys())  # This shows the actual column names

    # Get all mappings
    mappings = get_all_fabric_group_mappings(db_manager=db_manager)

    # Convert data to dictionaries for easy processing
    fabric_list = {fabric["id"]: fabric for fabric in fabrics}
    group_list = {group["group_code"]: group["group_description"] for group in inventory_groups}
    mapping_set = {(mapping["fabric_id"], mapping["inventory_group_code"]) for mapping in mappings}

    return fabric_list, group_list, mapping_set
