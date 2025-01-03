from flask import Blueprint, request, render_template, redirect, url_for, g, jsonify
from services.fabrics import (
    get_fabric_grid_data,
    process_fabric_mappings,
    prepare_fabric_grid_data,
    add_fabric_to_group,
    remove_fabric_from_group
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
