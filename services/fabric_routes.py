from flask import Blueprint, request, render_template, redirect, url_for, g
from services.fabrics import get_fabric_grid_data, process_fabric_mappings, prepare_fabric_grid_data


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

