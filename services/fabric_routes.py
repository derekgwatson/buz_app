from flask import Blueprint, request, render_template, redirect, url_for, g, jsonify, current_app
import os
from services.fabrics import (
    get_fabric_grid_data,
    process_fabric_mappings,
    prepare_fabric_grid_data,
    add_fabric_to_group,
    remove_fabric_from_group,
    get_fabric_by_id,
    add_mapping,
    add_new_fabric,
    get_fabric_mappings,
    update_fabric_in_db,
    get_fabrics_and_mappings,
    get_inventory_items,
    get_inventory_groups,
    process_data,
    create_workbook,
)


fabrics_blueprint = Blueprint("fabrics", __name__)


@fabrics_blueprint.route("/fabrics/grid", methods=["GET", "POST"])
def fabric_grid():
    if request.method == "POST":
        # Process form submission
        mappings = request.form.getlist("mappings")
        # Logic to update the database with the submitted mappings
        process_fabric_mappings(mappings, g.db)
        return redirect(url_for("fabrics.fabric_grid"))

    # Fetch grid data
    fabric_list, group_list, mapping_set = get_fabric_grid_data(g.db)

    # Prepare data for the template
    prepared_data = prepare_fabric_grid_data(fabric_list, group_list, mapping_set)
    grid = prepared_data["grid"]
    groups = prepared_data["groups"]

    # Render template
    return render_template(
        "fabric_grid.html",
        grid=grid,
        groups=groups
    )


@fabrics_blueprint.route('/fabrics/update-mapping', methods=['POST'])
def update_mapping():
    try:
        data = request.json  # Get the change details
        fabric_id = data['fabric_id']
        group_code = data['group_code']
        is_checked = data['is_checked']

        # Update the database
        if is_checked:
            add_fabric_to_group(g.db, fabric_id, group_code)
        else:
            remove_fabric_from_group(g.db, fabric_id, group_code)

        return jsonify({"status": "success"})
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@fabrics_blueprint.route('/fabrics/batch-update', methods=['POST'])
def batch_update_mappings():
    try:
        updates = request.json  # Receive the list of changes

        for update in updates:
            fabric_id = update['fabric_id']
            group_code = update['group_code']
            is_checked = update['is_checked']

            # Update the database
            if is_checked:
                add_fabric_to_group(g.db, fabric_id, group_code)
            else:
                remove_fabric_from_group(g.db, fabric_id, group_code)

        return jsonify({"status": "success"})
    except Exception as e:
        print(f"Error during batch update: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@fabrics_blueprint.route("/fabrics/<int:fabric_id>", methods=["DELETE"])
def delete_fabric(fabric_id):
    try:
        g.db.delete_item('fabrics', {'id': fabric_id})
        return jsonify({'message': 'Fabric deleted successfully.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@fabrics_blueprint.route('/fabrics/<int:fabric_id>', methods=['PUT'])
def update_fabric(fabric_id):
    db = g.db  # Assuming you are using Flask's `g` object to manage the database connection

    # Check if the fabric exists
    existing_fabric = get_fabric_by_id(fabric_id, db)
    if not existing_fabric:
        return jsonify({"error": "Fabric not found."}), 404

    # Parse the JSON body
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON body."}), 400

    # Validate required fields
    required_fields = ["supplier_product_code", "description_1", "description_2", "description_3"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields."}), 400

    # Update the fabric
    update_fabric_in_db(fabric_id, data, db)

    # Respond with success
    return jsonify({
        "message": "Fabric updated successfully.",
        "fabric": {
            "id": fabric_id,
            "supplier_product_code": data["supplier_product_code"],
            "description_1": data["description_1"],
            "description_2": data["description_2"],
            "description_3": data["description_3"],
        },
    }), 200


@fabrics_blueprint.route("/fabrics/clone", methods=["POST"])
def clone_fabric():
    db = g.db
    try:
        # Parse the data from the request
        data = request.get_json()
        fabric_id = data.get("fabric_id")

        if not fabric_id:
            return jsonify({"error": "Fabric ID is required"}), 400

        # Fetch the original fabric details
        original_fabric = get_fabric_by_id(fabric_id, db)

        if not original_fabric:
            return jsonify({"error": "Fabric not found"}), 404

        # Clone the fabric and insert a new record
        new_fabric_id = add_new_fabric({
            "description_1": original_fabric["description_1"],
            "description_2": original_fabric["description_2"],
            "description_3": original_fabric["description_3"],
            "supplier_product_code": original_fabric["supplier_product_code"],
        }, db)

        # Clone the mappings
        original_mappings = get_fabric_mappings(fabric_id, db)
        for mapping in original_mappings:
            add_mapping(new_fabric_id, mapping["inventory_group_code"], db)

        db.commit()

        return jsonify({"new_fabric_id": new_fabric_id}), 201

    except Exception as e:
        db.rollback()
        print(e)
        return jsonify({"error": str(e)}), 500


@fabrics_blueprint.route("/fabrics/<int:fabric_id>", methods=["GET"])
def get_fabric_details(fabric_id):
    db = g.db
    try:
        # Fetch fabric details
        fabric = get_fabric_by_id(fabric_id, db)
        if not fabric:
            return jsonify({"error": "Fabric not found"}), 404

        # Convert the Row object to a dictionary
        fabric_dict = dict(fabric)

        return jsonify(fabric_dict), 200
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500


@fabrics_blueprint.route('/fabrics/generate-upload', methods=['GET'])
def generate_workbook():
    db_manager = g.db

    fabrics = get_fabrics_and_mappings(db_manager)
    inventory_items = get_inventory_items(db_manager)
    inventory_groups_list = get_inventory_groups(db_manager)
    inventory_groups_dict = {group["group_code"]: group["group_description"] for group in inventory_groups_list}

    additions, deletions = process_data(fabrics, inventory_items, inventory_groups_dict)

    output_path = os.path.join("uploads", "fabric_sync.xlsx")
    create_workbook(current_app.config["headers"], additions, deletions, output_path)

    return jsonify({"message": "Workbook created", "path": output_path})
