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


def prepare_fabric_grid_data(fabric_list, group_list, mapping_set):
    grid = []
    for fabric_id, fabric in fabric_list.items():
        # Concatenate descriptions
        concatenated_description = " ".join(
            filter(None, [fabric["description_1"], fabric["description_2"], fabric["description_3"]])
        )
        grid.append({
            "fabric_id": fabric_id,
            "fabric_description": concatenated_description,
            "description_1": fabric["description_1"],
            "description_2": fabric["description_2"],
            "description_3": fabric["description_3"],
            "fabric_code": fabric["supplier_code"],
            "groups": {
                group_code: (fabric_id, group_code) in mapping_set
                for group_code in group_list.keys()
            }
        })

    return {"grid": grid, "groups": group_list}


def process_fabric_mappings(mappings, db_manager):
    """
    Update the database based on the submitted mappings.
    """
    # Example: Convert mappings into usable format and update database
    for fabric_id, group_mappings in mappings.items():
        for group_code, is_checked in group_mappings.items():
            if is_checked == "on":
                # Add mapping to the database
                add_fabric_to_group(fabric_id, group_code, db_manager)
            else:
                # Remove mapping from the database
                remove_fabric_from_group(fabric_id, group_code, db_manager)


def add_fabric_to_group(db_manager: DatabaseManager, fabric_id, group_code):
    """
    Add a fabric-to-group mapping to the database with error handling.
    """
    try:
        db_manager.execute_query(
            "INSERT INTO fabric_group_mappings (fabric_id, inventory_group_code) VALUES (?, ?)",
            (fabric_id, group_code), True
        )
        print(f"Mapping added successfully: fabric_id={fabric_id}, group_code={group_code}")
    except Exception as e:
        print(f"Error adding mapping: fabric_id={fabric_id}, group_code={group_code}, error={e}")
        raise


def remove_fabric_from_group(db_manager, fabric_id, group_code):
    """
    Remove a fabric-to-group mapping from the database with error handling.
    """
    try:
        db_manager.execute_query(
            "DELETE FROM fabric_group_mappings WHERE fabric_id = ? AND inventory_group_code = ?",
            (fabric_id, group_code), True
        )
        print(f"Mapping removed successfully: fabric_id={fabric_id}, group_code={group_code}")
    except Exception as e:
        print(f"Error removing mapping: fabric_id={fabric_id}, group_code={group_code}, error={e}")
        raise


def get_fabric_by_id(fabric_id, db):
    query = "SELECT * FROM fabrics WHERE id = ?"
    return db.execute_query(query, (fabric_id,)).fetchone()


def add_new_fabric(fabric_data, db):
    query = """
        INSERT INTO fabrics (description_1, description_2, description_3, supplier_code)
        VALUES (?, ?, ?, ?)
    """
    cursor = db.execute_query(query, (
        fabric_data["description_1"],
        fabric_data["description_2"],
        fabric_data["description_3"],
        fabric_data["supplier_code"],
    ))
    return cursor.lastrowid


def get_fabric_mappings(fabric_id, db):
    query = "SELECT * FROM fabric_group_mappings WHERE fabric_id = ?"
    return db.execute_query(query, (fabric_id,)).fetchall()


def add_mapping(fabric_id, group_code, db):
    query = "INSERT INTO fabric_group_mappings (fabric_id, inventory_group_code) VALUES (?, ?)"
    db.execute_query(query, (fabric_id, group_code))


def update_fabric_in_db(fabric_id, fabric_data, db):
    query = """
        UPDATE fabrics
        SET supplier_code = ?, description_1 = ?, description_2 = ?, description_3 = ?
        WHERE id = ?
    """
    db.execute_query(query, (
        fabric_data["supplier_code"],
        fabric_data["description_1"],
        fabric_data["description_2"],
        fabric_data["description_3"],
        fabric_id,
    ), True)
