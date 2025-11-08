from flask import render_template, request, send_from_directory
from flask import abort, flash, redirect, url_for, send_file
from flask import Blueprint, jsonify, current_app, g
import tempfile
from services.database import create_db_manager
import services.curtain_fabric_sync
from services.group_options_check import extract_codes_from_excel_flat_dedup
from services.excel import OpenPyXLFileHandler
from services.config_service import ConfigManager
import logging
from services.auth import auth
from services.fabric_mapping_sync import sync_fabric_mappings
from services.unleashed_sync import sync_unleashed_fabrics, build_sequential_code_provider
import threading
import uuid
import os
from services.curtain_sync_db import run_curtain_fabric_sync_db
from datetime import timezone, datetime, timedelta
from services.data_processing import get_last_upload_time
from services.curtain_fabric_sync import generate_uploads_from_db
import pytz
import sys
import re
from services.job_service import create_job, update_job, get_job, make_progress
from services.google_sheets_service import GoogleSheetsService
from services.excel_safety import save_workbook_gracefully


# Create Blueprint
main_routes_bp = Blueprint('main_routes', __name__)

# Get logger
logger = logging.getLogger(__name__)


@auth.verify_password
def verify_password(username, password):
    users = current_app.config['USERS']
    if username in users and users[username] == password:
        return username


@main_routes_bp.route('/debug')
@auth.login_required
def debug():
    """Debug route to check g variables."""
    return f"g.start_time: {getattr(g, 'start_time', 'None')}, g.request_duration: {getattr(g, 'request_duration', 'None')}"


@main_routes_bp.route('/')
@auth.login_required
def homepage():
    return render_template('home.html')


@main_routes_bp.route('/upload', methods=['GET', 'POST'])
@auth.login_required
def upload_route():
    from services.upload import upload, init_last_upload_times
    from services.helper import parse_headers
    from markupsafe import Markup

    # Initialize last upload times
    last_upload_times = init_last_upload_times(g.db)

    if request.method == 'POST':
        # Retrieve files from the request
        inventory_file = request.files.get('inventory_file')
        pricing_file = request.files.get('pricing_file')
        unleashed_file = request.files.get('unleashed_file')

        # Parse headers
        inventory_file_expected_headers, inventory_file_db_fields = parse_headers(
            current_app.config["headers"], "buz_inventory_item_file"
        )
        pricing_file_expected_headers, pricing_file_db_fields = parse_headers(
            current_app.config["headers"], "buz_pricing_file"
        )

        try:
            # Process uploaded files
            uploaded_files = upload(
                db_manager=g.db,
                inventory_file=inventory_file,
                inventory_file_expected_headers=inventory_file_expected_headers,
                inventory_file_db_fields=inventory_file_db_fields,
                pricing_file=pricing_file,
                pricing_file_expected_headers=pricing_file_expected_headers,
                pricing_file_db_fields=pricing_file_db_fields,
                unleashed_file=unleashed_file,
                unleashed_field_config=current_app.config["headers"]["unleashed_fields"],
                upload_folder=current_app.config['upload_folder'],
                invalid_pkid=current_app.config['invalid_pkid'],
                override_friendly_descriptions_id=current_app.config["spreadsheets"]["friendly_descriptions"]["id"],
                override_friendly_descriptions_range=current_app.config["spreadsheets"]["friendly_descriptions"]["range"],
                ignored_groups=current_app.config["ignored_inventory_groups"]
            )

            if uploaded_files is None:
                flash('No files to upload')
            elif isinstance(uploaded_files, dict) and 'error' in uploaded_files:
                flash(Markup(uploaded_files['error']), 'danger')
            else:
                last_upload_times.update(uploaded_files)
                flash('Files successfully uploaded and data stored in the database')
        except Exception as exc:
            logging.exception("Upload failed: %s", exc)
            return jsonify({"ok": False, "error": str(exc)}), 500

    # Render the upload page
    from services.data_processing import (
        get_unique_inventory_group_count,
        get_table_row_count
    )

    return render_template(
        'upload.html',
        inventory_count=get_table_row_count(g.db, 'inventory_items'),
        pricing_count=get_table_row_count(g.db, 'pricing_data'),
        unleashed_count=get_table_row_count(g.db, 'unleashed_products'),
        inventory_group_count=get_unique_inventory_group_count(g.db),
        last_upload_times=last_upload_times
    )


@main_routes_bp.route('/upload_inventory_groups', methods=['POST'])
@auth.login_required
def upload_inventory_groups():
    if 'groups_file' not in request.files:
        flash('No file part')
        return redirect(url_for('main_routes.manage_inventory_groups'))

    groups_file = request.files['groups_file']

    if groups_file.filename == '':
        flash('No selected file')
        return redirect(url_for('main_routes.manage_inventory_groups'))

    # Read the uploaded text file and add each line as an inventory group code
    codes_added = []
    if groups_file:
        lines = groups_file.read().decode('utf-8').splitlines()  # Read lines from the file
        for line in lines:
            group_code = line.strip()  # Remove any surrounding whitespace
            if group_code:  # Only add non-empty lines
                g.db.insert_item('inventory_group_codes', {'group_code': group_code})
                codes_added.append(group_code)

    flash(f'Added inventory groups: {", ".join(codes_added)}')
    return redirect(url_for('main_routes.manage_inventory_groups'))


# Route to search for items by supplier product code
@main_routes_bp.route('/search', methods=['GET', 'POST'])
@auth.login_required
def search():
    from services.data_processing import search_items_by_supplier_product_code

    results = []
    if request.method == 'POST':
        code = request.form['code']
        results = search_items_by_supplier_product_code(db_manager=g.db, code=code)
    return render_template('search.html', results=results)


@main_routes_bp.route('/manage_inventory_groups', methods=['GET', 'POST'])
@auth.login_required
def manage_inventory_groups():
    from services.data_processing import get_inventory_groups

    if request.method == 'POST':
        # Handle adding a new inventory group code
        new_group_code = request.form['new_group_code']
        new_group_description = request.form['new_group_description']
        g.db.insert_item("inventory_groups", new_group_code, new_group_description)
        flash(f'Added inventory group: {new_group_description} ({new_group_code})')
        return redirect(url_for('main_routes.manage_inventory_groups'))

    inventory_groups = get_inventory_groups(g.db)  # Fetch only the codes
    logging.info(f"allowed groups: {inventory_groups}")
    return render_template('manage_inventory_groups.html', inventory_groups=inventory_groups)


@main_routes_bp.route('/delete_inventory_group/<string:inventory_group_code>', methods=['POST'])
@auth.login_required
def delete_inventory_group(inventory_group_code):
    from services.data_processing import db_delete_inventory_group
    from services.data_processing import db_delete_records_by_inventory_group

    # Delete inventory group code
    g.db.delete_item("inventory_groups", {"group_code": inventory_group_code})

    # Delete records from items and pricing tables
    db_delete_records_by_inventory_group(inventory_group_code)

    # Optionally, delete the inventory group code from the allowed list as well
    db_delete_inventory_group(inventory_group_code)

    flash(f'Deleted all records for inventory group code: {inventory_group_code}')
    return redirect(url_for('main_routes.manage_inventory_groups'))


@main_routes_bp.route('/download/<path:filename>')
@auth.login_required
def download_file(filename):
    base_dir = current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config.get("upload_folder")
    if not base_dir or not os.path.isdir(base_dir):
        abort(500, description="Upload directory misconfigured")
    try:
        return send_from_directory(base_dir, filename, as_attachment=True, max_age=0)
    except FileNotFoundError:
        flash('File not found.', 'warning')
        return redirect(url_for('main_routes.homepage'))


@main_routes_bp.route('/get_items_not_in_unleashed', methods=['GET', 'POST'])
@auth.login_required
def get_items_not_in_unleashed():
    from services.remove_old_items import remove_old_items

    if request.method == 'POST':
        output_file = 'output_file.xlsx'
        if remove_old_items(db_manager=g.db, app_config=current_app.config, output_file=output_file):
            return render_template('delete_items_not_in_unleashed.html', output_file=output_file)
        flash("No items found to delete!")

    return render_template('delete_items_not_in_unleashed.html')


@main_routes_bp.route('/get_group_option_codes', methods=['GET', 'POST'])
@auth.login_required
def get_group_option_codes():
    if request.method == 'POST':
        from services.group_options_check import map_inventory_items_to_tabs
        from services.group_options_check import filter_inventory_items

        # Initialize the file handler for the input workbook
        g_file_handler = OpenPyXLFileHandler.from_file_like(file=request.files.get('group_options_file'))
        items = extract_codes_from_excel_flat_dedup(g_file_handler)
        g_file_handler = OpenPyXLFileHandler.from_file_like(file=request.files.get('inventory_items_file'))
        items = map_inventory_items_to_tabs(g_file_handler, items)
        items = filter_inventory_items(items)

        return render_template('get_group_option_codes.html', codes=items)

    return render_template('get_group_option_codes.html')


@main_routes_bp.route('/get_duplicate_codes', methods=["GET", "POST"])
@auth.login_required
def get_duplicate_codes():
    if request.method == 'POST':
        from services.group_options_check import extract_duplicate_codes_with_locations
        from services.group_options_check import build_views  # the helper we added

        fh = OpenPyXLFileHandler.from_file_like(file=request.files.get('group_options_file'))

        # 1) Scan all (tab, code) pairs from the workbook
        flat_pairs = extract_codes_from_excel_flat_dedup(fh)  # [(tab, code)]
        total_scanned_codes = len(flat_pairs)

        # 2) Keep only codes that appear in more than one tab
        dupes = extract_duplicate_codes_with_locations(flat_pairs)  # [(code, [tabs])]
        duplicate_only_count = len(dupes)

        # 3) Shape for UI
        code_to_groups, all_groups, group_counts, degree_counts, group_to_codes = build_views(dupes)

        return render_template(
            'get_duplicate_codes.html',
            code_to_groups=code_to_groups,
            all_groups=all_groups,
            group_counts=group_counts,
            degree_counts=degree_counts,
            group_to_codes=group_to_codes,
            total_scanned_codes=total_scanned_codes,
            duplicate_only_count=duplicate_only_count,
        )

    return render_template('get_duplicate_codes.html')


@main_routes_bp.route('/generate_codes', methods=["GET", "POST"])
@auth.login_required
def generate_codes():
    if request.method == "POST":
        from services.helper import generate_multiple_unique_ids

        try:
            count = int(request.form.get("count", 1))
            if count <= 0:
                raise ValueError("Count must be greater than 0.")
            ids = generate_multiple_unique_ids(count)
        except ValueError as e:
            flash(f'Error: {e}', 'danger')
            return render_template('show_generated_ids.html')

        return render_template('show_generated_ids.html', ids=ids)

    else:
        return render_template('show_generated_ids.html')


@main_routes_bp.route('/generate_backorder_file', methods=["GET", "POST"])
@auth.login_required
def generate_backorder_file():
    from services.config_service import SpreadsheetConfigUpdater

    if request.method == "POST":
        # Update config with user-provided values
        spreadsheet_config_manager = SpreadsheetConfigUpdater(ConfigManager())

        spreadsheet_id = request.form.get('spreadsheet_id')
        spreadsheet_range = request.form.get('spreadsheet_range')
        if spreadsheet_config_manager.update_spreadsheet_config("backorders", spreadsheet_id, spreadsheet_range):
            flash('Config updated', 'success')

        # Resolve the path to the JSON credentials file using __file__
        credentials_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..', 'credentials', 'service_account.json'
        )
        g_sheets_service = GoogleSheetsService(json_file=credentials_path)

        from services.backorders import process_inventory_backorder_with_services

        # Save updated config back to the file
        upload_wb, original_wb = process_inventory_backorder_with_services(
            _db_manager=g.db,
            _sheets_service=g_sheets_service,
            spreadsheet_id=spreadsheet_id,
            range_name=spreadsheet_range
        )
        original_filename = 'original_file.xlsx'
        upload_filename = 'upload_file.xlsx'

        out_dir = current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config["upload_folder"]

        if not save_workbook_gracefully(upload_wb, os.path.join(out_dir, upload_filename)):
            flash("Upload workbook: No data matched your filters — exported a placeholder workbook.")

        if not save_workbook_gracefully(original_wb, os.path.join(out_dir, original_filename)):
            flash("Original workbook: No data matched your filters — exported a placeholder workbook.")

        return render_template(
            'generate_backorder_file.html',
            original_filename=original_filename,
            upload_filename=upload_filename,
            original_download=url_for('main_routes.download_file', filename=original_filename),
            upload_download=url_for('main_routes.download_file', filename=upload_filename),
        )

    return render_template(
        'generate_backorder_file.html',
        spreadsheet_id=current_app.config["spreadsheets"]["backorders"]["id"],
        spreadsheet_range=current_app.config["spreadsheets"]["backorders"]["range"]
    )


@main_routes_bp.route('/robots.txt')
def robots_txt():
    return send_from_directory(current_app.static_folder, 'robots.txt')


@main_routes_bp.route('/get_buz_items_by_supplier_product_codes', methods=['GET', 'POST'])
@auth.login_required
def get_buz_items_by_supplier_product_codes():
    from services.buz_items_by_supplier_product_code import process_buz_items_by_supplier_product_codes

    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        supplier_product_codes_input = request.form.get('supplier_product_codes')

        if not uploaded_file or not supplier_product_codes_input:
            return "Error: File or supplier codes missing.", 400

        # Process multi-line supplier codes input
        supplier_product_codes = [code.strip() for code in supplier_product_codes_input.splitlines() if code.strip()]

        if not uploaded_file.filename.endswith(('.xlsx', '.xlsm')):
            logging.warning("Only .xlsx or .xlsm files are supported.")
            flash("Only .xlsx or .xlsm files are supported.")
        else:
            try:
                excel = OpenPyXLFileHandler().from_file_like(uploaded_file)
                filtered_excel = process_buz_items_by_supplier_product_codes(
                    excel,
                    supplier_product_codes,
                    current_app.config["headers"]["buz_inventory_item_file"]
                )
                if filtered_excel:
                    return send_file(
                        filtered_excel,
                        as_attachment=True,
                        download_name='filtered_items.xlsx',
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                else:
                    logging.warning("No items found that match the list of supplier codes")
                    flash("Nothing found")
            except Exception as e:
                logging.error(f"Error: {e}")
                if current_app.debug:
                    raise e
                else:
                    flash(f"Error: {e}")

    return render_template('get_buz_items_by_supplier_product_codes.html')


@main_routes_bp.route("/get_matching_buz_items", methods=["GET", "POST"])
@auth.login_required
def get_matching_buz_items():
    from services.get_matching_buz_items import process_matching_buz_items

    if request.method == "POST":
        # Get the uploaded files
        first_file = request.files["first_file"]
        second_file = request.files["second_file"]

        # Save the uploaded files
        first_path = os.path.join(current_app.config['upload_folder'], first_file.filename)
        second_path = os.path.join(current_app.config['upload_folder'], second_file.filename)
        first_file.save(first_path)
        second_file.save(second_path)

        # Process the files
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx",
                                          dir=current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config[
                                              "upload_folder"])
        tmp.close()
        output_path = tmp.name
        matches_found = process_matching_buz_items(first_path, second_path, output_path)

        if not matches_found:
            flash("No matches found in any sheets.")
            return render_template("get_matching_buz_items.html")

        # Provide the output file for download
        return send_file(
            output_path,
            as_attachment=True,
            download_name="filtered_output.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return render_template("get_matching_buz_items.html")


@main_routes_bp.route("/sync_pricing", methods=["GET", "POST"])
@auth.login_required
def sync_pricing():
    from services.sync_pricing import get_pricing_changes

    if request.method == "POST":
        filenames = get_pricing_changes(
            db_manager=g.db,
            upload_folder=current_app.config['upload_folder'],
            pricing_fields=current_app.config["headers"]["buz_pricing_file"],
            wastage_percentages=current_app.config["wastage_percentages"]
        )
        return render_template("sync_pricing.html", filenames=filenames)
    return render_template("sync_pricing.html")


@main_routes_bp.route('/fabrics/create', methods=['GET'])
@auth.login_required
def create_fabric():
    """
    Render the form to create a new fabric.
    """
    return render_template('fabric_create.html')


@main_routes_bp.route('/generate-deactivation-file', methods=['GET', 'POST'])
@auth.login_required
def generate_deactivation_file():
    """
    Route to generate a deactivation file for obsolete/unsellable items.
    """
    from services.deactivated_items import generate_deactivation_upload

    if request.method == 'POST':
        # Call the function to generate the file
        filename = generate_deactivation_upload(g.db)  # Your implementation from earlier

        if filename:
            flash('Deactivation file generated successfully!', 'success')
            upload_filename = os.path.basename(filename)
            return render_template(
                'generate_deactivation_file.html',
                upload_filename=upload_filename,
                download_url=url_for('main_routes.download_file', filename=upload_filename),
            )
        else:
            flash('Failed to generate deactivation file. Check logs for details.', 'danger')

    return render_template('generate_deactivation_file.html', upload_filename=None)


@main_routes_bp.route('/fabric-duplicates-report', methods=['GET', 'POST'])
@auth.login_required
def generate_duplicates_report():
    from services.fabrics import get_duplicate_fabric_details
    from services.buz_inventory_items import create_inventory_workbook_creator

    if request.method == 'POST':
        action = request.form.get('action', 'find')

        # Step 1: Fetch duplicate inventory details
        duplicate_details = get_duplicate_fabric_details(g.db)

        if not duplicate_details:
            flash('No duplicates found.', 'information')
            return render_template('fabric_duplicates.html')

        # Step 2: Group duplicate details by inventory group code
        grouped_data = {}
        for item in duplicate_details:
            item_dict = dict(item)
            item_dict['Operation'] = 'D'

            # Group by inventory group code
            group_name = item_dict["inventory_group_code"] if item_dict["inventory_group_code"] else "Uncategorized"
            if group_name not in grouped_data:
                grouped_data[group_name] = []
            grouped_data[group_name].append(item_dict)

        # If action is download, generate and serve the file
        if action == 'download':
            # Step 3: Create the workbook creator instance
            inventory_creator = create_inventory_workbook_creator(current_app)

            # Step 4: Populate the workbook
            inventory_creator.populate_workbook(grouped_data)
            inventory_creator.auto_fit_columns()

            # Step 5: Save to buffer and serve directly
            buffer = inventory_creator.save_to_buffer()

            if not buffer:
                flash('No duplicates to report.', 'warning')
                return render_template('fabric_duplicates.html')

            return send_file(
                buffer,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name="Duplicates_Report.xlsx",
                max_age=0,
            )

        # Otherwise, show the results on screen
        # Calculate summary statistics
        total_duplicates = len(duplicate_details)
        summary = {}
        for group_name, items in grouped_data.items():
            summary[group_name] = {
                'count': len(items),
                'duplicates': items[:10]  # Show first 10 items as preview
            }

        return render_template(
            'fabric_duplicates.html',
            summary=summary,
            total_duplicates=total_duplicates,
            has_results=True
        )

    return render_template('fabric_duplicates.html')


@main_routes_bp.route('/buz', methods=['GET', 'POST'])
@auth.login_required
def get_buz_data():
    from services.buz_data import get_buz_data

    return render_template('show_buz_data.html', buzdata=get_buz_data("CBR"))


@main_routes_bp.route('/check_inventory_groups', methods=['GET'])
@auth.login_required
def check_inventory_groups():
    from services.check_fabric_group_mappings import check_inventory_groups_against_unleashed

    # Capture logs as a string instead of sending to stdout
    import io
    import logging

    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.INFO)

    logger = logging.getLogger('fabric_check')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    violations = check_inventory_groups_against_unleashed(g.db)
    if violations:
        flash(f"{len(violations)} issue(s) found. Check logs or see below.", "warning")
        return render_template("check_inventory_groups.html", violations=violations)
    else:
        flash("✅ All fabrics are valid!", "success")
        return render_template("check_inventory_groups.html", violations=[])


@main_routes_bp.route('/pricing_update', methods=['GET', 'POST'])
@auth.login_required
def pricing_update():
    if request.method == 'POST':
        from services.update_pricing import generate_pricing_upload_from_unleashed

        credentials_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..', 'credentials', 'service_account.json'
        )

        result = generate_pricing_upload_from_unleashed(
            g.db,
            GoogleSheetsService(json_file=credentials_path),
            current_app.config["headers"]["buz_pricing_file"],
            current_app.config["unleashed_group_to_inventory_groups"].get("Fabric - Verticals", [])
        )

        log = result.get("log", [])
        if result.get("file"):
            filename = "buz_pricing_upload.xlsx"
            upload_dir = current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config["upload_folder"]
            output_path = os.path.join(upload_dir, filename)
            try:
                result["file"].save_workbook(output_path)
                return render_template(
                    "pricing_result.html",
                    updated=True,
                    download_url=url_for('main_routes.download_file', filename=filename),
                    log=log
                )
            except PermissionError:
                log.insert(0, "❌ Failed to save Excel file — is it open in another program?")
                return render_template("pricing_result.html", updated=False, spreadsheet_failed=True, log=log, ran_update=True)
        else:
            spreadsheet_failed = any("Failed to load" in msg for msg in log)
            return render_template("pricing_result.html", updated=False, spreadsheet_failed=spreadsheet_failed, log=log, ran_update=True)

    # If GET request
    return render_template('pricing_result.html', ran_update=False)


@main_routes_bp.route('/unleashed', methods=['GET'])
@auth.login_required
def unleashed_demo():
    from services.unleashed_api import UnleashedAPIClient  # Adjust based on your file name

    unleashed = UnleashedAPIClient()
    products = unleashed.get_paginated_data(
        "Products"
    )
    filtered_products = []
    for p in products:
        group = str(p.get("ProductGroup", "")).strip()
        subgroup = str(p.get("ProductSubGroup", "")).strip()
        if group and subgroup and subgroup.lower() != "ignore":
            filtered_products.append(p)

    for product in filtered_products:
        print(product["ProductCode"], product["ProductDescription"])


@main_routes_bp.route("/allowed_codes", methods=["GET", "POST"])
@auth.login_required
def allowed_codes():
    config_manager = ConfigManager()
    db = g.db
    all_codes = sorted({row["inventory_group_code"] for row in db.execute_query(
        "SELECT DISTINCT inventory_group_code FROM inventory_items"
    ).fetchall()})

    if request.method == "POST":
        new_list = request.form.getlist("allowed_codes[]")
        config_manager.update_config(["allowed_inventory_group_codes"], new_list)
        return redirect(url_for("main_routes.allowed_codes"))

    allowed = set(config_manager.get("allowed_inventory_group_codes", default=[]))
    available = sorted(set(all_codes) - allowed)

    return render_template("allowed_codes.html", available_codes=available, allowed_codes=sorted(allowed))


@main_routes_bp.route("/clean_excel_upload", methods=["GET", "POST"])
@auth.login_required
def clean_excel_upload():
    config_manager = ConfigManager()

    if request.method == "POST":
        file = request.files.get("file")
        if not file or not (file.filename.endswith(".xlsx") or file.filename.endswith(".xlsm")):
            flash("Please upload a valid .xlsx or .xlsm file.", "error")
            return redirect(request.url)

        # Load the file without keeping macros
        _allowed_codes = config_manager.get("allowed_inventory_group_codes", default=[])
        handler = OpenPyXLFileHandler.from_file_like(file)  # just regular load

        handler.clean_for_upload(
            db_manager=g.db,
            allowed_sheets=_allowed_codes,
            show_only_valid_unleashed="show_invalid_unleashed" in request.form)

        # Always save as .xlsx
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        handler.save_workbook(temp_file.name)
        temp_file.close()

        return send_file(
            temp_file.name,
            as_attachment=True,
            download_name="cleaned_upload.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    return render_template("clean_excel_upload.html")


@main_routes_bp.route("/motorisation-data", methods=["GET", "POST"])
@auth.login_required
def motorisation_data():
    data = []
    pricing_fields = []

    if request.method == "POST":
        file = request.files.get("file")
        if file:
            handler = OpenPyXLFileHandler.from_file_like(file)
            data, pricing_fields = handler.extract_motorisation_data(g.db)

    return render_template("motorisation_data.html", data=data, pricing_fields=pricing_fields)


@main_routes_bp.route('/curtain-fabric-sync', methods=['GET', 'POST'])
@auth.login_required
def curtain_fabric_sync():
    config = current_app.config
    column_titles = config["curtain_fabric_columns"].copy()

    if request.method == 'POST':
        for key in column_titles.keys():
            new_value = request.form.get(f"columns[{key}]")
            if new_value:
                column_titles[key] = new_value.strip()

        result = services.curtain_fabric_sync.run_curtain_fabric_sync(current_app, g.db, column_titles)
        result["column_titles"] = column_titles
        return render_template("curtain_fabric_sync.html", **result)

    return render_template("curtain_fabric_sync.html", column_titles=column_titles)


@main_routes_bp.route("/sync-fabric-mappings", methods=["GET"])
@auth.login_required
def run_fabric_mapping_sync():
    """
    Run fabric mapping sync and return generated Buz upload file.
    """
    try:
        output_file = sync_fabric_mappings(
            db_manager=g.db,
            config_path=current_app.config.get("CONFIG_JSON_PATH", "config.json"),
            output_dir=current_app.config.get("UPLOAD_OUTPUT_DIR", "uploads")
        )

        if not output_file:
            return "No fabric mapping changes found.", 204  # or render a small HTML message

        return send_file(
            output_file,
            as_attachment=True,
            download_name=os.path.basename(output_file),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    finally:
        pass


def _read_config_paths():
    # Fallbacks if you haven't set these in config.py
    cfg_path = current_app.config.get("CONFIG_JSON_PATH", "config.json")
    out_dir = current_app.config.get("UPLOAD_OUTPUT_DIR", "uploads")
    return cfg_path, out_dir


# /app/routes.py
@main_routes_bp.route("/sync-unleashed-old", methods=["GET", "POST"])
@auth.login_required
def sync_unleashed():
    """
    GET: show a landing page with a 'Run Sync' button.
    POST: run the sync and render results + download links.
    """
    cfg_path = current_app.config.get("CONFIG_JSON_PATH", "config.json")
    out_dir  = current_app.config.get("UPLOAD_OUTPUT_DIR", "uploads")

    if request.method == "GET":
        # Just render the landing page; nothing runs yet
        return render_template("unleashed_sync.html", ran=False, files=[], adds=[], deletes=[], pricing_count=0)

    # POST → run the sync
    minimal_cfg = {
        "unleashed_group_to_inventory_groups": current_app.config.get(
            "unleashed_group_to_inventory_groups", {}
        )
    }

    try:
        code_provider = build_sequential_code_provider(g.db, minimal_cfg, default_start=10000)

        result = sync_unleashed_fabrics(
            db=g.db,
            config_path=cfg_path,
            output_dir=out_dir,
            code_provider=code_provider,
            price_provider=None,           # plug in later
            pricing_tolerance=0.005
        )

        items_fp   = result.get("items_file")
        pricing_fp = result.get("pricing_file")

        files = []
        if items_fp and os.path.exists(items_fp):
            bn = os.path.basename(items_fp)
            files.append(
                {"label": "Items upload", "filename": bn, "url": url_for('main_routes.download_file', filename=bn)})
        if pricing_fp and os.path.exists(pricing_fp):
            bn = os.path.basename(pricing_fp)
            files.append(
                {"label": "Pricing upload", "filename": bn, "url": url_for('main_routes.download_file', filename=bn)})

        return render_template(
            "unleashed_sync.html",
            ran=True,
            error=None,
            adds=result.get("adds", []),
            deletes=result.get("deletes", []),
            pricing_count=result.get("pricing_count", 0),
            files=files,
        )
    except Exception as e:
        logging.exception("Unleashed sync failed")
        return render_template(
            "unleashed_sync.html",
            ran=True,
            error=str(e),
            adds=[],
            deletes=[],
            pricing_count=0,
            files=[],
        ), 500


@main_routes_bp.route("/sync-unleashed", methods=["GET"])
@auth.login_required
def unleashed_sync_landing():
    # just render the landing with a GO button
    return render_template("unleashed_sync_run.html")


@main_routes_bp.route("/sync-unleashed/start", methods=["POST"])
@auth.login_required
def unleashed_sync_start():
    job_id = str(uuid.uuid4())
    create_job(job_id)

    db_path  = current_app.config["database"]
    cfg_path = current_app.config.get("CONFIG_JSON_PATH", "config.json")
    out_dir  = current_app.config.get("UPLOAD_OUTPUT_DIR", current_app.config.get("upload_folder"))
    minimal_cfg = {
        "unleashed_group_to_inventory_groups": current_app.config.get("unleashed_group_to_inventory_groups", {})
    }

    def unleashed_sync_runner():
        db = create_db_manager(db_path)
        try:
            update_job(job_id, 1, "Job started…", db=db)
            progress = make_progress(job_id, db=db)

            code_provider = build_sequential_code_provider(db, minimal_cfg, default_start=10000)
            result = sync_unleashed_fabrics(
                db=db,
                config_path=cfg_path,
                output_dir=out_dir,
                code_provider=code_provider,
                price_provider=None,
                pricing_tolerance=0.005,
                progress=progress,
            )

            # Store result in job record
            update_job(job_id, pct=100, message="Completed", done=True, result=result, db=db)

        except Exception as e:
            logging.exception("Unleashed sync failed")
            update_job(job_id, pct=0, message=f"Error: {str(e)}", error=str(e), db=db)

        finally:
            try:
                db.close()
            except Exception:
                pass

    threading.Thread(
        name=f"unleashed-sync-{job_id[:8]}",
        target=unleashed_sync_runner,
        daemon=True
    ).start()
    return redirect(url_for("main_routes.unleashed_sync_progress", job_id=job_id))


@main_routes_bp.route("/sync-unleashed/progress/<job_id>", methods=["GET"])
@auth.login_required
def unleashed_sync_progress(job_id):
    job = get_job(job_id)
    if not job:
        flash("Unknown job id.", "warning")
        return redirect(url_for("main_routes.unleashed_sync_landing"))
    return render_template("unleashed_sync_progress.html", job_id=job_id)


@main_routes_bp.route("/sync-unleashed/status/<job_id>", methods=["GET"])
@auth.login_required
def unleashed_sync_status(job_id):
    job = get_job(job_id)
    if not job:
        data = {"pct": 0, "log": [], "done": False, "error": None, "result": None}
    else:
        data = {
            "pct": job.get("pct", 0),
            "log": job.get("log", []),
            "done": job.get("done", False),
            "error": job.get("error"),
            "result": job.get("result")
        }

    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@main_routes_bp.post("/admin/run-curtain-fabric-sync")
@auth.login_required
def run_curtain_fabric_sync_endpoint():
    # db manager lives at current_app.extensions['db_manager'] in your app
    db = current_app.extensions["db_manager"]
    res = run_curtain_fabric_sync_db(current_app, db, use_google_sheet=True)  # or False to use unleashed_products
    return jsonify(res), 200


STALE_THRESHOLD = timedelta(hours=6)  # tweak to taste


@main_routes_bp.route("/update_combo_bo_fabrics_group_options", methods=["GET", "POST"])
@auth.login_required
def update_combo_bo_fabrics_group_options():
    from services.combo_bo_fabrics_group_options_updater import ComboBOFabricsGroupOptionsUpdater
    import os, tempfile, uuid  # ensure uuid is in scope

    # Always fetch last upload time to show in the UI
    last_inventory_upload = get_last_upload_time(g.db, "inventory_items")

    def _prepare_view_summary(summary: dict[str, dict]) -> tuple[dict[str, dict], bool]:
        """
        Convert the raw updater summary into a template-friendly dict
        with explicit counts and booleans, and report if any sheet changed.
        """
        view = {}
        any_changed = False
        for sheet, data in summary.items():
            status = data.get("status", "unchanged")
            changed = (status == "changed")
            any_changed = any_changed or changed

            fabrics_added = data.get("fabrics_added", []) or []
            fabrics_removed = data.get("fabrics_removed", []) or []
            triples_added = data.get("triples_added", []) or []
            triples_removed = data.get("triples_removed", []) or []

            view[sheet] = {
                "status": status,
                "changed": changed,
                # Totals if present (fall back to lengths)
                "fabrics_total": data.get("fabrics_total", data.get("fabrics", len(fabrics_added) + len(fabrics_removed))),
                "triples_total": data.get("triples_total", data.get("triples", len(triples_added) + len(triples_removed))),
                # Deltas with counts
                "fabrics_added_count": len(fabrics_added),
                "fabrics_removed_count": len(fabrics_removed),
                "triples_added_count": len(triples_added),
                "triples_removed_count": len(triples_removed),
                # Optional details for “expand” section
                "fabrics_added": fabrics_added,
                "fabrics_removed": fabrics_removed,
                "triples_added": triples_added,
                "triples_removed": triples_removed,
            }
        return view, any_changed

    if request.method == "POST":
        # Optional: warn or block if stale
        if last_inventory_upload:
            # last_inventory_upload is a naive/aware dt depending on SQLite; normalize a bit
            if isinstance(last_inventory_upload, str):
                # SQLite may return ISO string; best effort parse
                try:
                    last_inventory_upload = datetime.fromisoformat(last_inventory_upload.replace("Z", "+00:00"))
                except Exception:
                    last_inventory_upload = None
            now = datetime.now(timezone.utc)
            try:
                # make both aware
                if last_inventory_upload.tzinfo is None:
                    last_inventory_upload = last_inventory_upload.replace(tzinfo=timezone.utc)
                is_stale = (now - last_inventory_upload) > STALE_THRESHOLD
            except Exception:
                is_stale = True
        else:
            is_stale = True

        if is_stale:
            flash("Heads up: your inventory items look stale. Upload fresh items before running this for accurate results.", "warning")

        uploaded_file = request.files.get("group_options_file")
        if not uploaded_file or uploaded_file.filename == "":
            flash("No file uploaded", "danger")
            return redirect(request.url)

        # Preserve original extension (helps with xlsm)
        _, ext = os.path.splitext(uploaded_file.filename)
        ext = ext.lower() if ext else ".xlsx"

        # Save uploaded file temporarily
        temp_in = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
        uploaded_file.save(temp_in.name)
        temp_in.close()

        # Prepare output path in uploads folder
        out_dir = current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config["upload_folder"]
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"corrected_group_options_{uuid.uuid4().hex}.xlsx")

        updater = ComboBOFabricsGroupOptionsUpdater(g.db)
        try:
            summary = updater.update_options_file(temp_in.name, out_path)
            view_summary, any_changed = _prepare_view_summary(summary)
        except Exception as e:
            current_app.logger.exception("Failed to update group options")
            flash(f"Error while processing file: {e}", "danger")
            return redirect(request.url)
        finally:
            try:
                os.unlink(temp_in.name)
            except Exception:
                pass

        # Use the prepared view model in both branches
        if not any_changed:
            flash("✅ No changes required", "success")
            return render_template("combo_bo_fabrics_update.html", summary=view_summary, output_file=None)

        filename = os.path.basename(out_path)
        return render_template(
            "combo_bo_fabrics_update.html",
            summary=view_summary,
            output_file=filename,
        )

    # GET
    return render_template("combo_bo_fabrics_update.html",last_inventory_upload=last_inventory_upload)


try:
    from zoneinfo import ZoneInfo  # py3.9+
    LOCAL_TZ = ZoneInfo("Australia/Sydney")
except Exception:
    LOCAL_TZ = pytz.timezone("Australia/Sydney")


@main_routes_bp.app_template_filter('datetimeformat')
def datetimeformat(value):
    if not value:
        return "N/A"

    # Accept ISO strings too
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return value  # fallback: show the raw string

    # Make timezone-aware if naive (assume UTC if no tz)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    # Convert to local tz
    local_dt = value.astimezone(LOCAL_TZ)

    # Cross-platform hour without leading zero
    fmt = "%a %d %b %Y, %-I:%M %p" if sys.platform != "win32" else "%a %d %b %Y, %#I:%M %p"
    try:
        return local_dt.strftime(fmt)
    except ValueError:
        # Last-resort: use %I (may include leading zero) and strip it
        s = local_dt.strftime("%a %d %b %Y, %I:%M %p")
        return re.sub(r"\b0(\d:)", r"\1", s)


# --- Curtain Sync (async with progress) ---
@main_routes_bp.route("/curtain-sync", methods=["GET"])
@auth.login_required
def curtain_sync_landing():
    last_inventory_upload = get_last_upload_time(g.db, "inventory_items")
    return render_template(
        "curtain_sync.html",
        ran=False,
        last_inventory_upload=last_inventory_upload,
        stale_threshold_hours=6,
        job_id=None,     # NEW: allow template to show a "Run" button or progress
    )


@main_routes_bp.route("/curtain-sync/start", methods=["POST"])
@auth.login_required
def curtain_sync_start():
    job_id = uuid.uuid4().hex
    create_job(job_id)

    out_dir = current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config["upload_folder"]
    headers_cfg = current_app.config["headers"]
    curtain_fabric_groups = current_app.config.get("curtain_fabric_groups")
    db_path = current_app.config["database"]
    spreadsheet_id = current_app.config["spreadsheets"]["master_curtain_fabric_list"]["id"]
    worksheet_tab = current_app.config["spreadsheets"]["master_curtain_fabric_list"]["tab"]

    os.makedirs(out_dir, exist_ok=True)

    def curtain_sync_runner(job_id, db_path, out_dir, headers_cfg, curtain_fabric_groups, spreadsheet_id, worksheet_tab):
        # No Flask globals in here.
        import logging
        logger = logging.getLogger("curtain_sync")
        db = create_db_manager(db_path)  # don't use g.db in threads

        # Ensure Sentry captures this background job's context
        try:
            import sentry_sdk
            with sentry_sdk.start_transaction(op="task", name="curtain_sync_job"):
                _run_curtain_sync(job_id, db, out_dir, headers_cfg, curtain_fabric_groups, spreadsheet_id, worksheet_tab, logger)
        except ImportError:
            # Sentry not installed, run without transaction
            _run_curtain_sync(job_id, db, out_dir, headers_cfg, curtain_fabric_groups, spreadsheet_id, worksheet_tab, logger)
        finally:
            try:
                db.close()
            except Exception:
                pass

    def _run_curtain_sync(job_id, db, out_dir, headers_cfg, curtain_fabric_groups, spreadsheet_id, worksheet_tab, logger):
        try:
            progress = make_progress(job_id, db)
            progress("Starting…", 1)

            gs_service = GoogleSheetsService()  # uses your default credentials path
            gs_source = {
                "svc": gs_service,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": worksheet_tab,
            }

            result = generate_uploads_from_db(
                gs_source,
                db,
                output_dir=out_dir,
                headers_cfg=headers_cfg,
                curtain_fabric_groups=curtain_fabric_groups,
                progress=progress,
            )

            if not result.get("change_log"):
                update_job(job_id, 100, "No changes found, skipping file generation",
                           result={
                                "elapsed_sec": result.get("elapsed_sec", 0),
                                "summary": result.get("summary", {}),
                                "change_log": [],
                                "files": [],
                            },
                           done=True,
                           db=db)
                return

            # friendly filenames
            items_name = f"items_upload_{uuid.uuid4().hex}.xlsx"
            pricing_name = f"pricing_upload_{uuid.uuid4().hex}.xlsx"
            os.replace(result["items_path"],   os.path.join(out_dir, items_name))
            os.replace(result["pricing_path"], os.path.join(out_dir, pricing_name))
            result["files"] = [
                {"label": "Items upload", "filename": items_name},
                {"label": "Pricing upload", "filename": pricing_name},
            ]

            update_job(job_id, 100, "Completed successfully", result=result, db=db, done=True)

        except Exception as e:
            logger.exception("Curtain sync failed")
            update_job(job_id, pct=0, message=f"Error: {str(e)}", error=str(e), db=db, done=True)

    threading.Thread(
        target=curtain_sync_runner,
        args=(job_id, db_path, out_dir, headers_cfg, curtain_fabric_groups, spreadsheet_id, worksheet_tab),
        daemon=True,
    ).start()

    return redirect(url_for("main_routes.curtain_sync_progress", job_id=job_id))


@main_routes_bp.route("/curtain-sync/progress/<job_id>", methods=["GET"])
@auth.login_required
def curtain_sync_progress(job_id):
    job = get_job(job_id)
    if not job:
        flash("Unknown job id.", "warning")
        return redirect(url_for("main_routes.curtain_sync_landing"))

    last_inventory_upload = get_last_upload_time(g.db, "inventory_items")

    # If finished successfully, render results
    if job.get("done") and job.get("result"):
        res = job["result"]
        return render_template(
            "curtain_sync.html",
            ran=True,
            elapsed_sec=res.get("elapsed_sec", 0.0),
            summary=res.get("summary", {}),
            per_tab=res.get("per_tab", {}),
            change_log=res.get("change_log", []),
            files=res.get("files", []),
            last_inventory_upload=last_inventory_upload,
            stale_threshold_hours=6,
        )

    # Otherwise keep showing progress mode
    return render_template(
        "curtain_sync.html",
        ran=False,
        job_id=job_id,
        last_inventory_upload=last_inventory_upload,
        stale_threshold_hours=6,
    )


@main_routes_bp.route("/curtain-sync/status/<job_id>", methods=["GET"])
@auth.login_required
def curtain_sync_status(job_id):
    job = get_job(job_id)
    if not job:
        data = {"pct": 0, "log": [], "done": False, "error": None, "result": None}
    else:
        data = {
            "pct": job.get("pct", 0),
            "log": job.get("log", []),
            "done": job.get("done", False),
            "error": job.get("error"),
            "result": job.get("result")
        }

    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store"
    return resp


# ========== Blinds & Awnings Sync Routes ==========

@main_routes_bp.route("/blinds-awnings-sync", methods=["GET"])
@auth.login_required
def blinds_awnings_sync_landing():
    """Landing page for blinds/awnings sync."""
    last_inventory_upload = get_last_upload_time(g.db, "inventory_items")
    return render_template(
        "blinds_awnings_sync.html",
        ran=False,
        last_inventory_upload=last_inventory_upload,
        stale_threshold_hours=6,
        job_id=None,
    )


@main_routes_bp.route("/blinds-awnings-sync/start", methods=["POST"])
@auth.login_required
def blinds_awnings_sync_start():
    """Start blinds/awnings sync job."""
    job_id = uuid.uuid4().hex
    create_job(job_id)

    out_dir = current_app.config.get("UPLOAD_OUTPUT_DIR") or current_app.config["upload_folder"]
    db_path = current_app.config["database"]
    config_dict = dict(current_app.config)  # Copy config for thread

    os.makedirs(out_dir, exist_ok=True)

    def blinds_awnings_sync_runner(job_id, db_path, out_dir, config_dict):
        """Background thread runner."""
        import logging
        logger = logging.getLogger("blinds_awnings_sync")
        db = create_db_manager(db_path)

        try:
            import sentry_sdk
            with sentry_sdk.start_transaction(op="task", name="blinds_awnings_sync_job"):
                _run_blinds_awnings_sync(job_id, db, out_dir, config_dict, logger)
        except ImportError:
            _run_blinds_awnings_sync(job_id, db, out_dir, config_dict, logger)
        finally:
            try:
                db.close()
            except Exception:
                pass

    def _run_blinds_awnings_sync(job_id, db, out_dir, config_dict, logger):
        """Actual sync logic."""
        from services.blinds_awnings_sync import sync_blinds_awnings_fabrics

        try:
            progress = make_progress(job_id, db)
            progress("Starting...", 1)

            gs_service = GoogleSheetsService()

            result = sync_blinds_awnings_fabrics(
                db=db,
                config=config_dict,
                sheets_service=gs_service,
                output_dir=out_dir,
                progress=progress
            )

            # Store result
            update_job(
                job_id,
                pct=100,
                message="Sync complete!",
                done=True,
                result=result,
                db=db
            )

        except Exception as e:
            logger.exception("Blinds/awnings sync failed")
            update_job(
                job_id,
                pct=0,
                message=f"Error: {str(e)}",
                error=str(e),
                db=db
            )

    threading.Thread(
        name=f"blinds-awnings-sync-{job_id[:8]}",
        target=blinds_awnings_sync_runner,
        args=(job_id, db_path, out_dir, config_dict),
        daemon=True
    ).start()

    return redirect(url_for("main_routes.blinds_awnings_sync_progress", job_id=job_id))


@main_routes_bp.route("/blinds-awnings-sync/progress/<job_id>", methods=["GET"])
@auth.login_required
def blinds_awnings_sync_progress(job_id):
    """Show progress page for running job."""
    job = get_job(job_id)
    if not job:
        flash("Unknown job ID.", "warning")
        return redirect(url_for("main_routes.blinds_awnings_sync_landing"))

    # If job is done, show results
    if job.get("done"):
        result = job.get("result", {})
        summary = result.get("summary", {})
        change_log = result.get("change_log", [])
        error = job.get("error")

        # Check if there are changes to download
        items_changes = result.get("items_changes", {})
        pricing_changes = result.get("pricing_changes", {})
        has_items = any(len(rows) > 0 for rows in items_changes.values())
        has_pricing = any(len(rows) > 0 for rows in pricing_changes.values())

        files = []
        if has_items:
            files.append({
                "label": "Items Upload",
                "filename": "blinds_awnings_items_upload.xlsx",
                "url": url_for("main_routes.blinds_awnings_download_items", job_id=job_id)
            })
        if has_pricing:
            files.append({
                "label": "Pricing Upload",
                "filename": "blinds_awnings_pricing_upload.xlsx",
                "url": url_for("main_routes.blinds_awnings_download_pricing", job_id=job_id)
            })

        last_inventory_upload = get_last_upload_time(g.db, "inventory_items")

        return render_template(
            "blinds_awnings_sync.html",
            ran=True,
            error=error,
            summary=summary,
            change_log=change_log,
            files=files,
            last_inventory_upload=last_inventory_upload,
            stale_threshold_hours=6,
            job_id=job_id,
        )

    # Job still running, show progress
    return render_template("blinds_awnings_sync.html", job_id=job_id)


@main_routes_bp.route("/blinds-awnings-sync/status/<job_id>", methods=["GET"])
@auth.login_required
def blinds_awnings_sync_status(job_id):
    """AJAX endpoint for polling job status."""
    job = get_job(job_id)
    if not job:
        data = {"pct": 0, "log": [], "done": False, "error": None, "result": None}
    else:
        data = {
            "pct": job.get("pct", 0),
            "log": job.get("log", []),
            "done": job.get("done", False),
            "error": job.get("error"),
            "result": job.get("result")
        }

    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@main_routes_bp.route("/blinds-awnings-sync/apply-to-db/<job_id>", methods=["POST"])
@auth.login_required
def blinds_awnings_sync_apply_to_db(job_id):
    """Apply changes from sync job to database."""
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Unknown job ID"}), 404

    if not job.get("done"):
        return jsonify({"success": False, "error": "Job not complete"}), 400

    result = job.get("result", {})
    items_changes = result.get("items_changes")
    pricing_changes = result.get("pricing_changes")

    if not items_changes or not pricing_changes:
        return jsonify({"success": False, "error": "No changes data found in job result"}), 400

    # Apply changes
    try:
        from services.blinds_awnings_sync import apply_changes_to_database

        stats = apply_changes_to_database(
            items_changes=items_changes,
            pricing_changes=pricing_changes,
            db=g.db
        )

        return jsonify({
            "success": True,
            "stats": stats
        })

    except Exception as e:
        logger.exception("Failed to apply changes to database")
        return jsonify({"success": False, "error": str(e)}), 500


@main_routes_bp.route("/blinds-awnings-sync/download/items/<job_id>", methods=["GET"])
@auth.login_required
def blinds_awnings_download_items(job_id):
    """Generate and download items workbook on-demand."""
    from services.blinds_awnings_sync import generate_workbooks_in_memory
    from io import BytesIO

    job = get_job(job_id)
    if not job or not job.get("done"):
        return "Job not found or not complete", 404

    result = job.get("result", {})
    items_changes = result.get("items_changes", {})
    headers_cfg = result.get("headers_cfg", {})

    # Check if there are actually any items to download
    has_items = any(len(rows) > 0 for rows in items_changes.values())
    if not has_items or not headers_cfg:
        return "No items to download", 404

    # Generate workbook in memory
    try:
        items_stream, _ = generate_workbooks_in_memory(
            items_changes,
            {},  # Don't need pricing for items download
            headers_cfg
        )

        return send_file(
            items_stream,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="blinds_awnings_items_upload.xlsx"
        )
    except Exception as e:
        logger.exception("Failed to generate items workbook")
        return f"Error generating file: {str(e)}", 500


@main_routes_bp.route("/blinds-awnings-sync/download/pricing/<job_id>", methods=["GET"])
@auth.login_required
def blinds_awnings_download_pricing(job_id):
    """Generate and download pricing workbook on-demand."""
    from services.blinds_awnings_sync import generate_workbooks_in_memory
    from io import BytesIO

    job = get_job(job_id)
    if not job or not job.get("done"):
        return "Job not found or not complete", 404

    result = job.get("result", {})
    pricing_changes = result.get("pricing_changes", {})
    headers_cfg = result.get("headers_cfg", {})

    # Check if there are actually any pricing changes to download
    has_pricing = any(len(rows) > 0 for rows in pricing_changes.values())
    if not has_pricing or not headers_cfg:
        return "No pricing changes to download", 404

    # Generate workbook in memory
    try:
        _, pricing_stream = generate_workbooks_in_memory(
            {},  # Don't need items for pricing download
            pricing_changes,
            headers_cfg
        )

        return send_file(
            pricing_stream,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="blinds_awnings_pricing_upload.xlsx"
        )
    except Exception as e:
        logger.exception("Failed to generate pricing workbook")
        return f"Error generating file: {str(e)}", 500


@main_routes_bp.route("/lead-times", methods=["GET"])
@auth.login_required
def lead_times_start():
    return render_template('lead_times.html')


