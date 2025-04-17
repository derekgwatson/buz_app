import tempfile
from flask import Blueprint, current_app
import os
from flask import render_template, request, url_for, flash, redirect, send_file, g, send_from_directory
from datetime import timezone
from services.group_options_check import extract_codes_from_excel_flat_dedup
from services.excel import OpenPyXLFileHandler
from services.config_service import ConfigManager
import logging
from services.auth import auth


# Create Blueprint
main_routes = Blueprint('main_routes', __name__)


@auth.verify_password
def verify_password(username, password):
    users = current_app.config['USERS']
    if username in users and users[username] == password:
        return username


@main_routes.route('/debug')
def debug():
    """Debug route to check g variables."""
    return f"g.start_time: {getattr(g, 'start_time', 'None')}, g.request_duration: {getattr(g, 'request_duration', 'None')}"


@main_routes.route('/')
@auth.login_required
def homepage():
    return render_template('home.html')


@main_routes.route('/upload', methods=['GET', 'POST'])
@auth.login_required
def upload_route():
    from services.upload import upload, init_last_upload_times
    from services.helper import parse_headers

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
        unleashed_file_expected_headers, _ = parse_headers(
            current_app.config["headers"], "unleashed_fields"
        )

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
            unleashed_file_expected_headers=unleashed_file_expected_headers,
            upload_folder=current_app.config['upload_folder'],
            invalid_pkid=current_app.config['invalid_pkid'],
            override_friendly_descriptions_id=current_app.config["spreadsheets"]["friendly_descriptions"]["id"],
            override_friendly_descriptions_range=current_app.config["spreadsheets"]["friendly_descriptions"]["range"],
            ignored_groups=current_app.config["ignored_inventory_groups"]
        )

        if uploaded_files:
            last_upload_times.update(uploaded_files)
            flash('Files successfully uploaded and data stored in the database')
        else:
            flash('No files to upload')

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


@main_routes.app_template_filter('datetimeformat')
def datetimeformat(value):
    if value:
        # Convert to UTC and format as ISO 8601
        return value.astimezone(timezone.utc).isoformat() + "Z"
    return "N/A"


@main_routes.route('/upload_inventory_groups', methods=['POST'])
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
@main_routes.route('/search', methods=['GET', 'POST'])
@auth.login_required
def search():
    from services.data_processing import search_items_by_supplier_product_code

    results = []
    if request.method == 'POST':
        code = request.form['code']
        results = search_items_by_supplier_product_code(db_manager=g.db, code=code)
    return render_template('search.html', results=results)


@main_routes.route('/manage_inventory_groups', methods=['GET', 'POST'])
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


@main_routes.route('/delete_inventory_group/<string:inventory_group_code>', methods=['POST'])
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


@main_routes.route('/download/<filename>')
@auth.login_required
def download_file(filename):
    file_path = os.path.join(current_app.config['upload_folder'], filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        flash('File not found.', 'warning')


@main_routes.route('/get_items_not_in_unleashed', methods=['GET', 'POST'])
@auth.login_required
def get_items_not_in_unleashed():
    from services.remove_old_items import remove_old_items

    if request.method == 'POST':
        output_file = 'output_file.xlsx'
        if remove_old_items(db_manager=g.db, app_config=current_app.config, output_file=output_file):
            return render_template('delete_items_not_in_unleashed.html', output_file=output_file)
        flash("No items found to delete!")

    return render_template('delete_items_not_in_unleashed.html')


@main_routes.route('/get_group_option_codes', methods=['GET', 'POST'])
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


@main_routes.route('/get_duplicate_codes', methods=["GET", "POST"])
@auth.login_required
def get_duplicate_codes():
    if request.method == 'POST':
        from services.group_options_check import extract_duplicate_codes_with_locations

        # Initialize the file handler for the input workbook
        g_file_handler = OpenPyXLFileHandler.from_file_like(file=request.files.get('group_options_file'))
        items = extract_codes_from_excel_flat_dedup(g_file_handler)
        items = extract_duplicate_codes_with_locations(items)

        return render_template('get_duplicate_codes.html', codes=items)

    else:
        return render_template('get_duplicate_codes.html')


@main_routes.route('/generate_codes', methods=["GET", "POST"])
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
            flash('Error {e}')
            return

        return render_template('show_generated_ids.html', ids=ids)

    else:
        return render_template('show_generated_ids.html')


@main_routes.route('/generate_backorder_file', methods=["GET", "POST"])
@auth.login_required
def generate_backorder_file():
    from services.google_sheets_service import GoogleSheetsService
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
            '..', 'credentials', 'buz-app-439103-b6ae046c4723.json'
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

        upload_wb.save(current_app.config["upload_folder"] + '/' + upload_filename)
        original_wb.save(current_app.config["upload_folder"] + '/' + original_filename)

        return render_template(
            'generate_backorder_file.html',
            original_filename=original_filename,
            upload_filename=upload_filename,
        )

    return render_template(
        'generate_backorder_file.html',
        spreadsheet_id=current_app.config["spreadsheets"]["backorders"]["id"],
        spreadsheet_range=current_app.config["spreadsheets"]["backorders"]["range"]
    )


@main_routes.route('/robots.txt')
def robots_txt():
    return send_from_directory(current_app.static_folder, 'robots.txt')


@main_routes.route('/get_buz_items_by_supplier_product_codes', methods=['GET', 'POST'])
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


@main_routes.route("/get_matching_buz_items", methods=["GET", "POST"])
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
        output_path = os.path.join("static", "filtered_output.xlsx")
        matches_found = process_matching_buz_items(first_path, second_path, output_path)

        if not matches_found:
            flash("No matches found in any sheets.")
            return render_template("get_matching_buz_items.html")

        # Provide the output file for download
        return send_file(output_path, as_attachment=True)

    return render_template("get_matching_buz_items.html")


@main_routes.route("/sync_pricing", methods=["GET", "POST"])
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


@main_routes.route('/fabrics/create', methods=['GET'])
@auth.login_required
def create_fabric():
    """
    Render the form to create a new fabric.
    """
    return render_template('fabric_create.html')


@main_routes.route('/generate-deactivation-file', methods=['GET', 'POST'])
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
                upload_filename=upload_filename
            )
        else:
            flash('Failed to generate deactivation file. Check logs for details.', 'danger')

    return render_template('generate_deactivation_file.html', upload_filename=None)


@main_routes.route('/fabric-duplicates-report', methods=['GET', 'POST'])
@auth.login_required
def generate_duplicates_report():
    from services.fabrics import get_duplicate_fabric_details
    from services.inventory_items import create_inventory_workbook_creator

    if request.method == 'POST':

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

        # Step 3: Create the workbook creator instance
        inventory_creator = create_inventory_workbook_creator(current_app)

        # Step 4: Populate the workbook
        inventory_creator.populate_workbook(grouped_data)
        inventory_creator.auto_fit_columns()

        # Step 5: Save the workbook
        output_path = os.path.join(current_app.config["upload_folder"], "Duplicates_Report.xlsx")
        inventory_creator.save_workbook(output_path)

        return render_template('fabric_duplicates.html', output_path=output_path)

    return render_template('fabric_duplicates.html')


@main_routes.route('/buz', methods=['GET', 'POST'])
@auth.login_required
def get_buz_data():
    from services.buz_data import get_buz_data

    return render_template('show_buz_data.html', buzdata=get_buz_data("CBR"))


@main_routes.route('/get_combo_list/empire')
@auth.login_required
def get_combo_list_empire():
    from services.combo_roller_blockout_fabrics import get_inventory_items

    """Route to display inventory items."""
    items, unique_desc_part_1 = get_inventory_items(g.db, "ROLLEMPIRE")  # Fetch data
    return render_template('blockout_fabric_combo_options_list.html', title='Empire', items=items, fabrics=unique_desc_part_1)  # Pass data to HTML template


@main_routes.route('/get_combo_list/acmeda')
@auth.login_required
def get_combo_list_acmeda():
    from services.combo_roller_blockout_fabrics import get_inventory_items

    """Route to display inventory items."""
    items, unique_desc_part_1 = get_inventory_items(g.db, "ROLL")  # Fetch data
    return render_template('blockout_fabric_combo_options_list.html', title='Acmeda', items=items, fabrics=unique_desc_part_1)  # Pass data to HTML template


@main_routes.route('/check_inventory_groups', methods=['GET'])
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


@main_routes.route('/pricing_update', methods=['GET'])
@auth.login_required
def pricing_update():
    from services.update_pricing import generate_pricing_upload_from_unleashed
    from services.google_sheets_service import GoogleSheetsService

    credentials_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'credentials', 'buz-app-439103-b6ae046c4723.json'
    )

    result = generate_pricing_upload_from_unleashed(
        g.db,
        GoogleSheetsService(json_file=credentials_path),
        current_app.config["headers"]["buz_pricing_file"]
    )

    log = result.get("log", [])
    if result.get("file"):
        filename = "buz_pricing_upload.xlsx"
        upload_dir = os.path.join(os.path.dirname(__file__), "..", "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        output_path = os.path.join(upload_dir, filename)

        try:
            result["file"].save_workbook(output_path)
            return render_template(
                "pricing_result.html",
                updated=True,
                file_path=f"/uploads/{filename}",
                log=log
            )
        except PermissionError:
            log.insert(0, "❌ Failed to save Excel file — is it open in another program?")
            return render_template("pricing_result.html", updated=False, spreadsheet_failed=True, log=log)
    else:
        spreadsheet_failed = any("Failed to load" in msg for msg in log)
        return render_template("pricing_result.html", updated=False, spreadsheet_failed=spreadsheet_failed, log=log)


@main_routes.route('/unleashed', methods=['GET'])
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


@main_routes.route("/allowed_codes", methods=["GET", "POST"])
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


@main_routes.route("/clean_excel_upload", methods=["GET", "POST"])
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


@main_routes.route("/motorisation-data", methods=["GET", "POST"])
def motorisation_data():
    data = []
    pricing_fields = []

    if request.method == "POST":
        file = request.files.get("file")
        if file:
            handler = OpenPyXLFileHandler.from_file_like(file)
            data, pricing_fields = handler.extract_motorisation_data(g.db)

    return render_template("motorisation_data.html", data=data, pricing_fields=pricing_fields)
